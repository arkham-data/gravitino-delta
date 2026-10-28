#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Closed native-evidence contract for the local governed CDF-to-MERGE matrix."""
from collections import Counter
import csv
import hashlib
import json
import zipfile
from pathlib import Path


def bag(rows):
    """Compare full rows with multiplicity; execution order is not a contract."""
    return Counter(tuple(row) for row in rows)


def digest(value):
    return isinstance(value,str) and len(value)==64 and set(value)<=set('0123456789abcdef')


def verify_dataframe_frontend(frontend, request, manifest):
    """Require a matching packaged import and actual public-API request/identity evidence."""
    from cdf_read_live_package import verify_python_package
    verify_python_package(manifest)
    assert request['frontend']=='dataframe'
    assert frontend['frontend']=='classic-pyspark-dataframe'
    assert frontend['apiVersion']==manifest['pythonApiVersion']==1
    assert frontend['pysparkVersion']==manifest['spark'] and frontend['python']
    assert frontend['operations']==[step['operation'] for step in request['steps']]
    expected={'gravitino_delta':'gravitino_delta/__init__.py','gravitino_delta.tables':'gravitino_delta/tables.py'}
    assert set(frontend['modules'])==set(expected)
    for module,member in expected.items():
        assert frontend['modules'][module]=={'file':manifest['pythonZip']+'/'+member,'archive':manifest['pythonZip'],'sha256':manifest['pythonZipMembers'][member]}
    prefix='org.apache.gravitino.spark.connector.delta.ranger.'
    assert {prefix+name for name in ['GravitinoDeltaTable','GravitinoDeltaTable$','GravitinoDeltaMergeBuilder']}<=set(manifest['providers'])
    assert set(frontend['queryIdentities'])=={s['name'] for s in request['steps']}
    for value in frontend['queryIdentities'].values():
        assert value['subject']==value['ugi']==request['subject'] and value['thread']


def verify_semantics(result: dict) -> None:
    """Raise on incomplete pins, process provenance, policy evidence or native outcomes."""
    assert result.get('frontend','sql') in ['sql','dataframe']
    assert result.get('frontend','sql')!='dataframe' or result.get('profile')=='partitioned'
    assert result['spark'] in ['3.5.4','3.5.6'] and result['delta'] in ['3.3.0','3.3.2']
    assert (result['java'],result['scala'],result['ranger'])==('17','2.12.21','2.8.0')
    assert set(result['exits'])==({'outer','native','python'} if result.get('profile')=='partitioned' else {'outer','child','python'})
    assert all(type(v) is int for v in result['exits'].values())
    ids=result['identities'];assert set(ids)=={'setup','metadata','application'} and len(set(ids.values()))==3 and all(ids.values())
    for name,jar in [('org.apache.spark.sql.SparkSession','spark-sql_2.12-'+result['spark']+'.jar'),
                     ('org.apache.spark.sql.delta.DeltaLog','delta-spark_2.12-'+result['delta']+'.jar'),('scala.Predef$','scala-library-2.12.21.jar')]:
        origin=result['origins'][name];assert Path(origin['jar']).name==jar and digest(origin['sha256'])
    assert {'source','manifest','companion','app','python'}<=set(result['binding'])
    assert all(digest(v) for v in result['binding'].values())
    if result.get('profile')=='partitioned':
        verify_partition_semantics(result)
        return
    transitions=result['transitions']
    for name,allowed in [('grant',True),('revoke',False),('regrant',True)]:
        state=transitions[name]
        assert state['privileges']==['SELECT_TABLE','MODIFY_TABLE'] and state['select'] is allowed and state['modify'] is allowed
        assert type(state['policy']) is int and digest(state['downloadSha256'])
    assert transitions['grant']['policy']<transitions['revoke']['policy']<transitions['regrant']['policy']
    native=result['sourceNative']
    assert bag(native)==bag([[1,'changed','allowed','update_preimage',1],[1,'new','allowed','update_postimage',1],[2,'gone','allowed','delete',1],[4,'four','allowed','insert',1],[5,'five','blocked','insert',1]])
    assert len(native)==5 and {r[3] for r in native}=={'insert','delete','update_preimage','update_postimage'}
    assert all(len(r)==5 and r[4]==1 for r in native)
    assert bag(result['sourceGoverned'])==bag([[r[0]+100,'MASKED',r[2],r[3],0] for r in native if r[2]=='allowed'])
    expected=[[1,'MASKED|TARGET_MASK','allowed'],[4,'hidden','blocked'],[9,'stable','allowed'],[104,'MASKED','allowed']]
    expected_cdc=[[1,'old','allowed','update_preimage'],[1,'MASKED|TARGET_MASK','allowed','update_postimage'],[2,'old2','allowed','delete'],[104,'MASKED','allowed','insert']]
    names={'distinct','duplicate','empty','ambiguous','missing','columnDenied','postHelperRevoked','selectDenied','modifyDenied','regrant','same','python','pythonDenied'}
    assert names<=set(result['stages'])
    failures={'ambiguous','missing','columnDenied','postHelperRevoked','selectDenied','modifyDenied','pythonDenied'}
    changed={'distinct','regrant','same','python'}
    for name in names:
        stage=result['stages'][name];before=stage['before'];after=stage['after']
        assert 'MERGE INTO ' in stage['sql'] and 'table_changes(' in stage['sql']
        assert type(before['version']) is int and after['version']==before['version']+(1 if name in changed else 0), 'Unexpected native commit: '+name
        assert stage['outcome']==('failed' if name in failures else 'success')
        authority=stage['authorization'];assert type(authority['policy']) is int and digest(authority['downloadSha256'])
        if name=='selectDenied':assert authority['sourceAccess']==[] and authority['targetAccess']==['alter','select','update']
        if name=='modifyDenied':assert authority['sourceAccess']==['select'] and authority['targetAccess']==['select']
        if name in failures:
            assert stage['errors'] and stage['checkpointAfter']==stage['checkpointBefore']
            expected_error='DeltaUnsupportedOperationException' if name=='ambiguous' else ('FileNotFoundException' if name=='missing' else 'SecurityException')
            assert any(expected_error in error for error in stage['errors'])
        else:
            assert not stage['errors'] and stage['checkpointAfter']==(2 if name=='empty' else 1)
        if name not in changed:
            assert bag(before['rows'])==bag(after['rows']) and not stage['cdc'] and not after['actions']
        else:
            actions=after['actions'];types=Counter(key for action in actions for key in action)
            assert types['commitInfo']==1 and types['add']>0 and types['remove']>0 and types['cdc']>0
            assert next(a['commitInfo']['operation'] for a in actions if 'commitInfo' in a)=='MERGE'
            if name!='same':
                assert bag(after['rows'])==bag(expected) and bag(stage['cdc'])==bag(expected_cdc)
        assert digest(stage['sourceBefore']) and digest(stage['sourceAfter'])
        if name!='same':assert stage['sourceBefore']==stage['sourceAfter']
    distinct=result['stages']['distinct'];duplicate=result['stages']['duplicate']
    assert distinct['txnAppId']==duplicate['txnAppId'] and distinct['sql']==duplicate['sql']
    assert [a['txn'] for a in distinct['after']['actions'] if 'txn' in a] and all(a['txn']['appId']==distinct['txnAppId'] and a['txn']['version']==1 for a in distinct['after']['actions'] if 'txn' in a)
    same=result['stages']['same']
    assert same['sourceBefore']!=same['sourceAfter'] and same['before']['version']==2
    assert bag(same['after']['rows'])==bag([[1,'MASKED|MASKED','allowed'],[3,'hidden','blocked'],[4,'MASKED|MASKED','allowed'],[5,'five','blocked'],[6,'dup','allowed'],[6,'dup','allowed']])
    assert bag(same['cdc'])==bag([[1,'new','allowed','update_preimage'],[1,'MASKED|MASKED','allowed','update_postimage'],[4,'four','allowed','update_preimage'],[4,'MASKED|MASKED','allowed','update_postimage']])
    initial=result['nativeInitial']
    targets={'python':'python_target','pythonDenied':'python_target','ambiguous':'ambiguous_target','postHelperRevoked':'post_target','regrant':'regrant_target','same':'same'}
    order=['python','pythonDenied','distinct','duplicate','empty','ambiguous','missing','columnDenied','postHelperRevoked','selectDenied','modifyDenied','regrant','same']
    assert set(initial)=={'source','same','incomplete','target','regrant_target','python_target','post_target','ambiguous_target'}
    fresh=[[1,'old','allowed'],[2,'old2','allowed'],[4,'hidden','blocked'],[9,'stable','allowed']]
    source=[[1,'new','allowed'],[3,'hidden','blocked'],[4,'four','allowed'],[5,'five','blocked'],[6,'dup','allowed'],[6,'dup','allowed']]
    for name,value in initial.items():
        assert isinstance(value['id'],str) and value['id']
        assert value['version']==(2 if name in ['source','same','incomplete'] else 0)
        expected_initial=source if name in ['source','same','incomplete'] else ([[6,'old','allowed'],[9,'stable','allowed']] if name=='ambiguous_target' else fresh)
        assert bag(value['rows'])==bag(expected_initial),'Initial native fixture differs: '+name
    history=dict(initial)
    for name in order:
        stage=result['stages'][name];target=targets.get(name,'target');before=stage['before'];after=stage['after'];previous=history[target]
        assert before['id']==after['id']==previous['id'] and before['version']==previous['version'],'Native identity/version chain differs: '+name
        assert bag(before['rows'])==bag(previous['rows']),'Native row chain differs: '+name
        history[target]=after
    registration=result['registration'];assert registration['beforeRestart'] and registration['beforeRestart']==registration['afterRestart']==registration['persisted']
    assert set(registration['persisted'])==set(history)
    for name,value in history.items():
        receipt=registration['persisted'][name]
        assert type(receipt['id']) is int and receipt['id']>0 and receipt['uuid']==value['id'] and receipt['nativeVersion']==value['version'],'Persisted registration differs from native target: '+name
    helper=result['postHelper'];assert helper['held'] is True and helper['publishedBeforeRelease'] is False and any('DeltaRangerMergeCommit' in frame for frame in helper['stack'])


def file_ref(path):
    path=Path(path).resolve()
    return {'path':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}


def canonical(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def read_ref(ref):
    assert file_ref(ref['path'])==ref,'Original evidence bytes changed'
    return json.loads(Path(ref['path']).read_text())


def runtime_source_paths(manifest):
    stems={Path(name).name.split('$')[0].removesuffix('.class') for name in manifest['thinClasses']}
    required={'delta/acceptance/'+name for name in ['cdf_merge_live.py','cdf_merge_live_checks.py','cdf_merge_native.py','cdf_merge_pyspark.py']}
    assert required<=set(manifest['sourceInputs'])
    if manifest.get('pythonZip'):
        required.update({'delta/acceptance/cdf_merge_dataframe_pyspark.py','delta/acceptance/cdf_read_live_package.py','delta/acceptance/cdf_live_package.py'})
        required.update(manifest['pythonSourceInputs'])
        assert required<=set(manifest['sourceInputs'])
    return {p for p in manifest['sourceInputs'] if '/src/main/' in p or p in required or ('/src/test/' in p and Path(p).stem in stems) or p.endswith(('.gradle','.gradle.kts'))}


def bind_evidence(result,cell,runtime_path,source_overrides=None):
    """Link original files without changing their bytes or manufacturing process outcomes."""
    cell=Path(cell).resolve();runtime_path=Path(runtime_path).resolve();manifest=json.loads(runtime_path.read_text())
    root=Path(__file__).resolve().parents[2];overrides=source_overrides or {}
    references={'cell':str(cell),'runtime':file_ref(runtime_path),'sources':{p:file_ref(overrides.get(p,root/p)) for p in runtime_source_paths(manifest)},
        'outerCommand':file_ref(cell.parent/(cell.name+'-outer')/'command.json'),'outerExit':file_ref(cell.parent/(cell.name+'-outer')/'exit.json'),
        'nativeInitial':file_ref(cell/'fixture/native/ready.json'),'nativeCommand':file_ref(cell/'fixture/native/process/command.json'),'nativeExit':file_ref(cell/'fixture/native/process/exit.json'),
        'plan':file_ref(cell/'fixture/state.private.json'),'sourceGoverned':file_ref(cell/('python' if result.get('profile')=='partitioned' else 'scala')/'app-evidence/source-governed.json'),
        'registrationBefore':file_ref(cell/'registration-before-restart/tables.private.json'),'registrationAfter':file_ref(cell/'registration-after-restart/tables.private.json'),
        'persisted':file_ref(cell/'persistence/tables.csv'),'ranger':file_ref(cell/'deployed-ranger/association.json'),'apps':{},'stages':{}}
    for kind in (['python'] if result.get('profile')=='partitioned' else ['python','scala']):
        directory=cell/kind;app=directory/'app-evidence';request=json.loads((directory/'request/request.private.json').read_text())
        references['apps'][kind]={name:file_ref(directory/path) for name,path in [('command','process/command.json'),('exit','process/exit.json'),('request','request/request.private.json'),('origins','app-evidence/origins.json'),('observed','app-evidence/observed.json'),('metadata','app-evidence/metadata-identity-final.json')]}
        references['apps'][kind]['identities']={step['name']:file_ref(app/(step['name']+'-identity.json')) for step in request['steps']}
        for step in request['steps']:references['stages'][step['name']]=file_ref(directory/(step['name']+'-result.json'))
    outer_argv=read_ref(references['outerCommand'])['argv'];references['inputs']=file_ref(outer_argv[outer_argv.index('--inputs')+1])
    references['frontend']=file_ref(cell/'python/app-evidence/python-observed.json')
    if result.get('executionProfile')=='adaptive-stock':
        primary='python' if result.get('profile')=='partitioned' else 'scala'
        references['adaptiveEvidence']=file_ref(cell/primary/'app-evidence/adaptive-evidence.json')
        references['readRows']=file_ref(cell/primary/'app-evidence/ordinaryRead-rows.json')
        references['downloads']=file_ref(cell/primary/'app-evidence/downloads.jsonl')
        references['predicatePolicy']=file_ref(cell/primary/'predicate-fixture-policy.json')
        references['grantTransitions']={p.parent.name:file_ref(p) for p in (cell/primary).glob('*/completed.json')}
        references['serverRootProof']=file_ref(read_ref(references['ranger'])['rootProof'])
    result=dict(result,provenance=references)
    result['exits']=dict(result['exits'],outer=read_ref(references['outerExit'])['exitCode'])
    return result


def row_document(value):
    """Original row order may differ; every other evidence field remains exact."""
    if isinstance(value,dict):
        return {k:(sorted(v,key=lambda r:json.dumps(r,sort_keys=True)) if k in ['rows','cdc','files'] else row_document(v)) for k,v in value.items()}
    if isinstance(value,list):return [row_document(v) for v in value]
    return value


def verify_app_archive(manifest, receipt):
    """Preserve both container identities when packaging reuses equal member bytes."""
    from cdf_read_live_package import archive_entries
    built=receipt['appBuildArtifact']
    assert file_ref(built)['sha256']==receipt['artifacts'][built]
    assert file_ref(manifest['app'])['sha256']==manifest['jarHashes'][manifest['app']]
    assert archive_entries(manifest['app'])==archive_entries(built)


def verify_evidence(result):
    """Re-read original processes, authenticated frontends, source files and immutable artifacts."""
    proof=result['provenance'];manifest=read_ref(proof['runtime']);binding=result['binding']
    from cdf_read_live_package import verify_supplemental_artifacts
    verify_supplemental_artifacts(manifest)
    assert canonical(manifest['sourceInputs'])==manifest['sourceInputDigest']==binding['source']
    assert binding['manifest']==proof['runtime']['sha256']
    assert binding['companion']==file_ref(manifest['changedRuntime'])['sha256']
    assert binding['app']==file_ref(manifest['app'])['sha256']
    assert set(proof['sources'])==runtime_source_paths(manifest)
    for path,ref in proof['sources'].items():
        assert file_ref(ref['path'])==ref and ref['sha256']==manifest['sourceInputs'][path],'Executed source differs: '+path
    dataframe=result.get('frontend','sql')=='dataframe'
    python_entry='cdf_merge_dataframe_pyspark.py' if dataframe else 'cdf_merge_pyspark.py'
    assert binding['python']==proof['sources']['delta/acceptance/'+python_entry]['sha256']
    if dataframe:
        assert binding['pythonZip']==manifest['pythonZipSha256']
        receipt=read_ref({'path':manifest['freshBuildReceipt'],'sha256':manifest['freshBuildReceiptSha256']})
        assert json.loads(Path(receipt['exit']).read_text())['exitCode']==0
        assert json.loads(Path(receipt['before']).read_text())['files']==json.loads(Path(receipt['after']).read_text())['files']==manifest['sourceInputs']
        for path in [manifest['pythonZip'],manifest['changedRuntime']]:
            assert file_ref(path)['sha256']==receipt['artifacts'][path]
        verify_app_archive(manifest, receipt)
    assert (manifest['spark'],manifest['delta'])==(result['spark'],result['delta'])
    for path,digest_value in manifest['jarHashes'].items():assert file_ref(path)['sha256']==digest_value,'Runtime artifact changed'
    for path,target in manifest['runtimeJarLinks'].items():assert str(Path(path).resolve())==target and target in manifest['jarHashes']
    expected_entries={name.replace('.','/')+'.class' for name in manifest['providers']};counts=Counter()
    for path in manifest['runtimeJarLinks'].values():
        with zipfile.ZipFile(path) as archive:counts.update(expected_entries.intersection(archive.namelist()))
    assert set(counts)==expected_entries and all(count==1 for count in counts.values()),'Provider is absent or duplicated'
    archives={path:zipfile.ZipFile(path) for path in set(manifest['providers'].values())}
    try:
        for name,path in manifest['providers'].items():
            assert path in manifest['jarHashes']
            assert hashlib.sha256(archives[path].read(name.replace('.','/')+'.class')).hexdigest()==manifest['providerClasses'][name]
        for kind,refs in proof['apps'].items():
            assert kind in ['scala','python']
            origins=read_ref(refs['origins']);assert set(origins)==set(manifest['providers'])
            for name,entry in origins.items():
                assert entry['jar']==manifest['providers'][name] and entry['jarSha256']==manifest['jarHashes'][entry['jar']] and entry['classSha256']==manifest['providerClasses'][name]
            request=read_ref(refs['request']);observed=read_ref(refs['observed']);metadata=read_ref(refs['metadata']);command=read_ref(refs['command']);exit_value=read_ref(refs['exit'])
            assert request['packageManifest']==proof['runtime']['path'] and (request['expectedSparkVersion'],request['expectedDeltaVersion'])==(result['spark'],result['delta'])
            assert observed['spark']==result['spark'] and observed['java']==result['java'] and observed['scala']==result['scala']
            assert request['subject']==observed['subject']==observed['ugi']==result['identities']['application']==command['identity']
            assert request['metadataPrincipal']==metadata['principal']==result['identities']['metadata'] and metadata['successfulTargetGet'] is True
            assert set(refs['identities'])=={s['name'] for s in request['steps']}
            for step in request['steps']:
                identity=read_ref(refs['identities'][step['name']]);assert identity['subject']==identity['ugi']==result['identities']['application'] and identity['thread']
                if dataframe:
                    assert step['operation']==result['stages'][step['name']]['operation']
                    frontend_identity=read_ref(proof['frontend'])['queryIdentities'][step['name']]
                    assert frontend_identity==identity
                else:assert step['sql']==result['stages'][step['name']]['sql']
            argv=command['argv'];assert argv[0]==manifest['sparkHome']+'/bin/spark-submit' and argv[-1]==refs['request']['path']
            assert argv[argv.index('--properties-file')+1]==request['sparkConfigurationFile']
            assert argv[-2]==(str(Path(__file__).resolve().parent/python_entry) if kind=='python' else manifest['app'])
            if dataframe:
                assert kind=='python' and argv[argv.index('--py-files')+1]==manifest['pythonZip']
                assert request['frontend']=='dataframe'
            else:assert request.get('frontend','sql')=='sql'
            if kind=='scala':assert argv[argv.index('--class')+1]=='org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerLiveCdfMerge'
            assert exit_value['exitCode']==result['exits']['python' if kind=='python' else 'child']==0 and exit_value['terminated'] is False
    finally:
        for archive in archives.values():archive.close()
    assert set(proof['apps'])==({'python'} if result.get('profile')=='partitioned' else {'python','scala'})
    frontend=read_ref(proof['frontend']);python_request=read_ref(proof['apps']['python']['request'])
    if dataframe:verify_dataframe_frontend(frontend,python_request,manifest)
    else:
        assert frontend['frontend']=='classic-pyspark' and frontend['pysparkVersion']==result['spark'] and frontend['python']
        assert frontend['queries']==[step['sql'] for step in python_request['steps']]
    outer=read_ref(proof['outerCommand'])['argv'];outer_exit=read_ref(proof['outerExit'])
    assert len(outer)>1
    assert outer[1]==str(Path(__file__).resolve().parent/'cdf_merge_live.py')
    assert outer[outer.index('--inputs')+1]==proof['inputs']['path'] and file_ref(proof['inputs']['path'])==proof['inputs']
    assert outer[outer.index('--runtime')+1]==proof['runtime']['path'] and outer[outer.index('--out')+1]==proof['cell'] and '--child' in outer
    assert outer_exit['exitCode']==result['exits']['outer'] and outer_exit['terminated'] is False
    native=read_ref(proof['nativeInitial']);native_command=read_ref(proof['nativeCommand']);native_exit=read_ref(proof['nativeExit']);assert native_exit['exitCode']==0
    if result.get('profile')=='partitioned':assert native_exit['exitCode']==result['exits']['native'] and native_exit.get('terminated') is False
    assert native_command['argv'][0]==manifest['sparkHome']+'/bin/spark-submit' and native_command['argv'][-2]==str(Path(__file__).resolve().parent/'cdf_merge_native.py')
    assert set(native['identity'].values())=={result['identities']['setup']} and native_command['identity']==result['identities']['setup']
    assert (native['spark'],native['delta'],native['java'],native['scala'])==(result['spark'],result['delta'],result['java'],result['scala'])
    assert row_document(native['tables'])==row_document(result['nativeInitial']) and bag(native['sourceNative'])==bag(result['sourceNative'])
    assert result['origins']=={k:{'jar':v['origin'],'sha256':v['sha256']} for k,v in native['origins'].items()}
    assert bag(read_ref(proof['sourceGoverned']))==bag(result['sourceGoverned'])
    assert set(proof['stages'])==set(result['stages'])
    for name,ref in proof['stages'].items():assert row_document(read_ref(ref))==row_document(result['stages'][name]),'Original stage differs: '+name
    if dataframe:assert outer[outer.index('--frontend')+1]=='dataframe'
    elif '--frontend' in outer:assert outer[outer.index('--frontend')+1]=='sql'
    if result.get('profile')=='partitioned':
        assert '--partitioned' in outer
        for name in result['stages']:
            app=Path(proof['apps']['python']['observed']['path']).parent
            stage=result['stages'][name]
            assert row_document(read_ref(stage['nativeBefore']))==row_document(stage['before'])
            assert row_document(read_ref(stage['nativeAfter']))==row_document(stage['after'])
            assert stage['consumerBefore']['path']==str(app/(name+'-checkpoint-before.json'))
            assert stage['consumerAfter']['path']==str(app/(name+'-checkpoint-after.json'))
            before=read_ref(stage['consumerBefore']);after=read_ref(stage['consumerAfter'])
            source=next(step['source'] for step in python_request['steps'] if step['name']==name)
            assert before['sourceId']==after['sourceId']==result['nativeInitial'][source]['id']
            assert before['end']==result['stages'][name]['checkpointBefore'] and after['end']==result['stages'][name]['checkpointAfter']
    plan=read_ref(proof['plan']);assert {plan['subject'],plan['metadataPrincipal'],plan['serviceAdmin']}==set(result['identities'].values())
    assert result['identities']=={'application':plan['subject'],'metadata':plan['metadataPrincipal'],'setup':plan['serviceAdmin']}
    if dataframe:
        for step in python_request['steps']:
            prefix=plan['catalog']+'.'+plan['database']+'.'
            assert step['operation']['source']==prefix+step['source']
            assert step['operation']['target']==prefix+step['target']
            if step['operation'].get('kind')!='ordinary-read':assert step['operation']['end']==step['end']==1
    ranger=read_ref(proof['ranger']);assert ranger['deployedRangerVersion']==result['ranger']=='2.8.0'
    assert plan['deployedRangerEvidenceSha256']==proof['ranger']['sha256']
    from cdf_merge_live import receipt_identity
    assert receipt_identity(read_ref(proof['registrationBefore']))==result['registration']['beforeRestart']
    assert receipt_identity(read_ref(proof['registrationAfter']))==result['registration']['afterRestart']
    assert file_ref(proof['persisted']['path'])==proof['persisted']
    with Path(proof['persisted']['path']).open() as stream:rows=list(csv.DictReader(stream))
    persisted={}
    for row in rows:
        state=json.loads(json.loads(row['PROPERTIES'])['delta.registration.internal.state']);assert state['state']=='PUBLISHED'
        persisted[row['TABLE_NAME']]={'id':state['id'],'generation':state['generation'],'token':state['token'],'nativeVersion':state['observation']['version'],'uuid':state['observation']['uuid']}
    assert len(rows)==len(persisted) and persisted==result['registration']['persisted']


def verify_adaptive_observation(observation):
    """Validate original payload diagnostics without treating them as authorization tokens."""
    assert observation['isPayload'] is True, 'adaptive metadata is not payload'
    assert observation['phase'] in ['ordinary','touched','write','cdf'], 'adaptive phase differs'
    assert file_ref(observation['plan']['path'])==observation['plan'], 'adaptive original plan changed'
    plan=Path(observation['plan']['path']).read_text()
    operators=observation['operators']
    assert observation['rootClass'].split('.')[-1] in operators
    for name,operator in [('coalesced','AQEShuffleReadExec'),('local','AQEShuffleReadExec'),
                          ('skew','SortMergeJoinExec'),('reuse','ReusedExchangeExec')]:
        assert type(observation[name]) is bool
        if observation[name]:assert operator in operators, 'adaptive operator claim differs'
    for name,operator,text in [('materializedShuffle','ShuffleQueryStageExec','ShuffleQueryStage'),
                               ('materializedBroadcast','BroadcastQueryStageExec','BroadcastQueryStage')]:
        assert type(observation[name]) is int and observation[name]>=0
        assert observation[name]<=operators.count(operator), 'adaptive stage count differs'
        if observation[name]:assert text in plan, 'adaptive original operator absent'
    if observation['phase']!='cdf':
        assert observation['isFinalPlan'] is True and 'isFinalPlan=true' in plan, 'adaptive final plan absent'
        assert 'AdaptiveSparkPlanExec' in operators
        assert observation['materializedShuffle']+observation['materializedBroadcast']>0, 'adaptive forceApply-only witness'
    else:
        version=observation.get('diagnosticVersion',1)
        assert version in [1,2], 'Unknown diagnostic version'
        outcomes=['exchange-free','adaptive-payload'] + (['nonadaptive-exchange'] if version==2 else [])
        assert observation['outcome'] in outcomes
        if observation['outcome'] in ['exchange-free','nonadaptive-exchange']:
            assert observation['materializedShuffle']==observation['materializedBroadcast']==0
            if version==2:
                assert 'AdaptiveSparkPlanExec' not in operators and observation['isFinalPlan'] is False
                has_exchange=bool(set(operators)&{'ShuffleExchangeExec','BroadcastExchangeExec','ReusedExchangeExec'})
                assert has_exchange==(observation['outcome']=='nonadaptive-exchange'), 'Nonadaptive exchange label differs'
                if has_exchange:assert 'Exchange' in plan
        elif version==2:
            assert observation['isFinalPlan'] is True and 'isFinalPlan=true' in plan
            assert 'AdaptiveSparkPlanExec' in operators
            assert observation['materializedShuffle']+observation['materializedBroadcast']>0
    if observation['coalesced']:assert 'AQEShuffleRead coalesced' in plan, 'adaptive coalescing claim differs'
    if observation['local']:assert 'AQEShuffleRead local' in plan, 'adaptive local claim differs'
    if observation['reuse']:assert 'ReusedExchange' in plan, 'adaptive reuse claim differs'


def verify_adaptive_evidence(result):
    """Bind final payloads, seven fresh calls, downloaded policy transitions and startup options."""
    evidence=result.get('adaptiveEvidence');assert evidence, 'Missing adaptive payload evidence'
    proof=result['provenance']
    manifest=read_ref(proof['runtime'])
    assert manifest.get('supplementalArtifacts'), 'Missing supplemental runtime artifacts'
    with zipfile.ZipFile(manifest['providers']['org.apache.ranger.plugin.service.RangerBasePlugin']) as archive:
        properties=archive.read('META-INF/maven/org.apache.ranger/ranger-plugins-common/pom.properties').decode()
        assert 'version=2.8.0' in properties.splitlines(), 'Loaded Ranger client version differs'
    assert read_ref(proof['adaptiveEvidence'])==evidence
    primary='python' if result.get('profile')=='partitioned' else 'scala'
    request=read_ref(proof['apps'][primary]['request'])
    assert evidence['executionProfile']==request['executionProfile']=='adaptive-stock'
    assert evidence['requestedSettings']==request['requestedSettings']==evidence['effectiveSettings']
    assert evidence['effectiveSettings']['spark.sql.adaptive.enabled']=='true'
    assert evidence['stockSettingsBefore']==evidence['stockSettingsAfter']
    for key in ['adaptive.enabled','adaptive.coalescePartitions.enabled','adaptive.localShuffleReader.enabled',
                'adaptive.skewJoin.enabled','optimizer.dynamicPartitionPruning.enabled',
                'optimizer.runtime.bloomFilter.enabled','exchange.reuse','execution.reuseSubquery']:
        assert evidence['stockSettingsBefore']['spark.sql.'+key]=='true', 'Stock optimization disabled'
    assert evidence['stockSettingsBefore']['spark.sql.adaptive.forceApply']=='false'
    assert evidence['stockSettingsBefore']['spark.sql.adaptive.customCostEvaluatorClass']=='<unset>'
    outer=read_ref(proof['outerCommand'])['argv']
    assert outer[outer.index('--execution-profile')+1]=='adaptive-stock'
    observations=evidence['observations'];assert observations, 'Missing adaptive observations'
    for observation in observations:verify_adaptive_observation(observation)
    for name,phases in [('ordinaryRead',{'ordinary'}),('distinct',{'touched','write','cdf'}),('same',{'touched','write'})]:
        selected=[o for o in observations if o['step']==name]
        assert phases<={o['phase'] for o in selected}, 'Missing adaptive payload phase: '+name
    assert any(o['coalesced'] for o in observations), 'Missing adaptive coalescing'
    assert not any(o['step'] in ['predicateDenied','columnDenied','selectDenied','modifyDenied'] for o in observations)
    expected=[[101,'MASKED'],[104,'MASKED']]+[[106,'MASKED']]*4
    assert bag(read_ref(proof['readRows']))==bag(result['stages']['ordinaryRead']['rows'])==bag(expected)
    for name in ['ordinaryRead','predicateDenied']:
        stage=result['stages'][name]
        if result.get('frontend','sql')=='dataframe':assert stage['operation']['kind']=='ordinary-read'
        else:assert stage['sql'].startswith('SELECT ')
        keys=['id','version','rows']+(['files','fileObjects','partitionColumns','schema'] if result.get('profile')=='partitioned' else [])
        for key in keys:
            assert row_document({key:stage['before'][key]})==row_document({key:stage['after'][key]})
        assert not stage['after']['actions'] and not stage['cdc']
        assert stage['checkpointBefore']==stage['checkpointAfter']==0
        assert stage['outcome']==('success' if name=='ordinaryRead' else 'failed')
        if name=='predicateDenied':assert any('SecurityException' in e for e in stage['errors'])
    assert file_ref(proof['downloads']['path'])==proof['downloads']
    downloads=[json.loads(line) for line in Path(proof['downloads']['path']).read_text().splitlines()]
    from cdf_live_checks import exact_access, canonical_privileges
    plan=read_ref(proof['plan'])
    association=read_ref(proof['ranger']);root=read_ref(proof['serverRootProof'])
    assert association['rootProofSha256']==proof['serverRootProof']['sha256']
    assert root['webappMaven']['version']==association['deployedRangerVersion']=='2.8.0'
    assert root['installedJarSha256']==association['installedJarHashesRechecked']
    assert root['containerId']==association['containerId'] and root['imageId']==association['imageId']
    policies=[]
    for name in ['distinct','ordinaryRead','predicateDenied','columnDenied','selectDenied','modifyDenied','same']:
        stage=result['stages'][name];policy=stage['policyEvidence']
        state=read_ref(policy['state']);applied=read_ref(policy['applied']);ranger=read_ref(policy['rangerState'])
        public=read_ref(policy['publicState'])
        assert public['appUser']['roles']==[plan['appRole']]
        assert public['metadataUser']['roles']==[plan['metaRole']]
        grants={o['fullName']:canonical_privileges(o['privileges']) for o in public['appRole']['securableObjects']}
        for table in result['nativeInitial']:
            allowed=grants.get(plan['catalog']+'.'+plan['database']+'.'+table,set())
            accesses=({'select'} if ('SELECT_TABLE','ALLOW') in allowed else set())|({'update','alter'} if ('MODIFY_TABLE','ALLOW') in allowed else set())
            assert exact_access(ranger['policies']['policies'],plan['catalog'],plan['database'],table,plan['rangerAppRole'])==accesses
        assert state['publicStateFile']==policy['publicState']['path'] and state['rangerStateFile']==policy['rangerState']['path']
        assert applied['sameJvm'] is True and applied['stateSha256']==policy['state']['sha256']
        assert state['policy']==applied['policy']==ranger['policies']['policyVersion']==stage['authorization']['policy']
        assert state['role']==applied['role']==ranger['roles']['roleVersion']
        assert applied['downloadSha256']==stage['authorization']['downloadSha256']
        completed=[e for e in downloads if e.get('downstreamResponseCompleted') is True and e.get('responseSha256')==applied['downloadSha256']]
        assert completed, 'Original policy download absent'
        for event in completed:
            downloaded=read_ref({'path':event['responseBodyFile'],'sha256':applied['downloadSha256']})
            assert {k:v for k,v in downloaded.items() if k!='policyUpdateTime'}=={k:v for k,v in ranger['policies'].items() if k!='policyUpdateTime'}
        policies.append(state['policy'])
    assert policies[0]==policies[1]<policies[2]<policies[3]<policies[4]<policies[5]<policies[6]
    required={'grant','select-revoke','select-regrant-modify-revoke','same-regrant'} if result.get('profile')=='partitioned' else {'grant','revoke','modify-regrant','select-regrant-modify-revoke','regrant'}
    assert required<=set(proof['grantTransitions'])
    for name in required:
        transition=read_ref(proof['grantTransitions'][name])
        assert transition['after']['policy']>transition['before']['policy']
        assert transition['mutations'] and all(m['code']==0 for m in transition['mutations'])
        assert any(t['method']=='PUT' and '/service/public/v2/api/policy/' in t['path'] for t in transition['callbacks'])
    fixture=read_ref(proof['predicatePolicy'])
    assert fixture['provenance']=='test-authored unique denial; not propagated grant'
    assert fixture['policy']['resources']['column']['values']==['secret']
    assert fixture['policy']['resources']['table']['values']==['source']
    assert fixture['policy']['denyPolicyItems'][0]['accesses']==[{'type':'select','isAllowed':True}]


def verify_result(result: dict) -> None:
    """Require consistent native semantics and original successful processes with bound provenance."""
    if result.get("executionProfile") == "adaptive-stock":
        verify_adaptive_evidence(result)
    verify_semantics(result)
    verify_evidence(result)
    assert all(value==0 for value in result['exits'].values()),'Original outer or child process failed'


def verify_partition_files(before, after, adds, removes, cdc):
    """Apply original Delta actions and require raw maps, paths and retained object bytes."""
    from urllib.parse import unquote
    assert before['partitionColumns']==after['partitionColumns']==['part']
    assert before['schema']==after['schema']
    initial={f['path']:f for f in before['files']};final={f['path']:f for f in after['files']}
    assert len(initial)==len(before['files']) and len(final)==len(after['files'])
    assert all(f['dataChange'] is False for f in list(initial.values())+list(final.values()))
    assert set(before['fileObjects'])==set(initial) and set(after['fileObjects'])==set(final)
    expected=dict(initial);seen={key:[] for key in ['add','remove','cdc']}
    for action in after['actions']:
        for key in seen:
            if key not in action:continue
            value=action[key];part=value['partitionValues'];path=value['path']
            assert set(part)=={'part'} and (part['part'] is None or isinstance(part['part'],str))
            assert type(value['size']) is int and value['size']>0
            # Delta action paths escape the partition name as well as unsafe value bytes.
            partitions=[unquote(component[5:]) for component in unquote(path).split('/') if component.startswith('part=')]
            assert partitions==['__HIVE_DEFAULT_PARTITION__' if part['part'] is None else part['part']]
            assert value['dataChange'] is (False if key=='cdc' else True)
            seen[key].append(part['part'])
            if key=='remove':
                assert path in expected and part==expected[path]['partitionValues']
                expected.pop(path)
            if key=='add':
                assert path not in expected and value['dataChange'] is True
                expected[path]=value
    assert set(seen['add'])==adds and set(seen['remove'])==removes and set(seen['cdc'])==cdc
    # Snapshot replay clears dataChange; commit flags were checked independently above.
    # Other default snapshot fields may be absent in JSON actions; compare the owned payload.
    assert set(expected)==set(final)
    for path,value in expected.items():
        for key in ['path','partitionValues','size','stats']:
            assert value.get(key)==final[path].get(key)
        assert digest(after['fileObjects'][path]['sha256'])
        assert after['fileObjects'][path]['size']==final[path]['size']
    untouched=set(initial)-{a['remove']['path'] for a in after['actions'] if 'remove' in a}
    assert untouched,'Fixture must retain at least one original active file'
    for path in untouched:
        assert initial[path]==final[path] and before['fileObjects'][path]==after['fileObjects'][path]


def verify_partition_semantics(result):
    """Exact bounded STRING-partition live profile; no general scalar/settings claim."""
    source=[[1,'changed','allowed','new','update_preimage',1],[1,'new','allowed','new','update_postimage',1],
            [2,'gone','allowed','old2','delete',1],[4,'four','allowed','four','insert',1],[5,'five','blocked','blocked','insert',1]]
    assert bag(result['sourceNative'])==bag(source)
    assert bag(result['sourceGoverned'])==bag([[r[0]+100,'MASKED',r[2],'moved/= %',r[4],0] for r in source if r[2]=='allowed'])
    fresh=[[1,'old','allowed','old'],[2,'old2','allowed','old2'],[4,'hidden','blocked','old'],[9,'stable','allowed',None]]
    native_source=[[1,'new','allowed','new'],[3,'hidden','blocked','new'],[4,'four','allowed','four'],[5,'five','blocked','blocked'],[6,'dup','allowed','dup'],[6,'dup','allowed','dup']]
    target=[[1,'MASKED|TARGET_MASK','allowed','moved/= %'],[4,'hidden','blocked','old'],[9,'stable','allowed',None],[104,'MASKED','allowed','moved/= %']]
    same=[[1,'MASKED|MASKED','allowed','moved/= %'],[3,'hidden','blocked','new'],[4,'MASKED|MASKED','allowed','moved/= %'],[5,'five','blocked','blocked'],[6,'dup','allowed','dup'],[6,'dup','allowed','dup']]
    cdf_target=[[1,'old','allowed','old','update_preimage'],[1,'MASKED|TARGET_MASK','allowed','moved/= %','update_postimage'],[2,'old2','allowed','old2','delete'],[104,'MASKED','allowed','moved/= %','insert']]
    cdf_same=[[1,'new','allowed','new','update_preimage'],[1,'MASKED|MASKED','allowed','moved/= %','update_postimage'],[4,'four','allowed','four','update_preimage'],[4,'MASKED|MASKED','allowed','moved/= %','update_postimage']]
    initial=result['nativeInitial'];assert set(initial)=={'source','same','target'}
    for name,value in initial.items():
        assert value['id'] and value['version']==(0 if name=='target' else 2)
        assert value['partitionColumns']==['part']
        assert bag(value['rows'])==bag(fresh if name=='target' else native_source)
    order=['distinct','columnDenied','selectDenied','modifyDenied','same']
    extra={'ordinaryRead','predicateDenied'} if result.get('executionProfile')=='adaptive-stock' else set()
    assert set(result['stages'])==set(order)|extra
    history=dict(initial)
    for name in order:
        stage=result['stages'][name];before=stage['before'];after=stage['after'];table='same' if name=='same' else 'target'
        for key in ['id','version','rows','files','fileObjects','partitionColumns','schema']:
            assert row_document({key:before[key]})==row_document({key:history[table][key]})
        assert before['id']==after['id'] and before['schema']==after['schema']
        assert stage['checkpointBefore']==0 and stage['checkpointAfter']==(1 if name in ['distinct','same'] else 0)
        if result.get('frontend','sql')=='dataframe':
            operation=stage['operation']
            assert set(operation)=={'source','target','start','end','clauseProfile','expressionMode'}
            assert operation['start']==operation['end']==1 and operation['clauseProfile']=='partitioned-cdf'
            assert operation['expressionMode']==('columns-mixed' if name=='same' else 'strings')
            assert operation['source'].split('.')[-1]==('same' if name=='same' else 'source')
            assert operation['target'].split('.')[-1]==table
            assert 'sql' not in stage
        else:assert 'MERGE INTO ' in stage['sql'] and 'table_changes(' in stage['sql'] and 'part=s.part' in stage['sql']
        assert digest(stage['sourceBefore']) and digest(stage['sourceAfter'])
        assert (stage['sourceBefore']!=stage['sourceAfter'])==(name=='same')
        authority=stage['authorization'];assert type(authority['policy']) is int and digest(authority['downloadSha256'])
        assert authority['sourceAccess']==([] if name=='selectDenied' else (['alter','select','update'] if name=='same' else ['select']))
        assert authority['targetAccess']==(['select'] if name=='modifyDenied' else ['alter','select','update'])
        if name in ['distinct','same']:
            assert stage['outcome']=='success' and not stage['errors'] and after['version']==before['version']+1
            assert bag(after['rows'])==bag(same if name=='same' else target)
            assert bag(stage['cdc'])==bag(cdf_same if name=='same' else cdf_target) and bag(after['cdc'])==bag(stage['cdc'])
            assert [a['commitInfo']['operation'] for a in after['actions'] if 'commitInfo' in a]==['MERGE']
            verify_partition_files(before,after,{'moved/= %','new' if name=='same' else 'old'}, {'new','four'} if name=='same' else {'old','old2'}, {'moved/= %','new','four'} if name=='same' else {'moved/= %','old','old2'})
            # Visible and filtered rows share the physical file that gets rewritten.
            shared='new' if name=='same' else 'old'
            assert sum(f['partitionValues']=={'part':shared} for f in before['files'])==1
            hidden=[3,'hidden','blocked','new'] if name=='same' else [4,'hidden','blocked','old']
            assert hidden in before['rows'] and hidden in after['rows']
        else:
            assert stage['outcome']=='failed' and any('SecurityException' in e for e in stage['errors'])
            assert after['version']==before['version'] and not after['actions'] and not stage['cdc']
            for key in ['rows','files','fileObjects','partitionColumns','schema']:assert row_document({key:before[key]})==row_document({key:after[key]})
        history[table]=after
    registration=result['registration'];assert registration['beforeRestart']==registration['afterRestart']==registration['persisted']
    assert set(registration['persisted'])==set(initial)
    for name,value in history.items():
        receipt=registration['persisted'][name]
        assert type(receipt['id']) is int and receipt['id']>0 and receipt['uuid']==value['id'] and receipt['nativeVersion']==value['version']

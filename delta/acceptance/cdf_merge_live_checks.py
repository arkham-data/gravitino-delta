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


def verify_semantics(result: dict) -> None:
    """Raise on incomplete pins, process provenance, policy evidence or native outcomes."""
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
    result=dict(result,provenance=references)
    result['exits']=dict(result['exits'],outer=read_ref(references['outerExit'])['exitCode'])
    return result


def row_document(value):
    """Original row order may differ; every other evidence field remains exact."""
    if isinstance(value,dict):
        return {k:(sorted(v,key=lambda r:json.dumps(r,sort_keys=True)) if k in ['rows','cdc','files'] else row_document(v)) for k,v in value.items()}
    if isinstance(value,list):return [row_document(v) for v in value]
    return value


def verify_evidence(result):
    """Re-read original processes, authenticated frontends, source files and immutable artifacts."""
    proof=result['provenance'];manifest=read_ref(proof['runtime']);binding=result['binding']
    assert canonical(manifest['sourceInputs'])==manifest['sourceInputDigest']==binding['source']
    assert binding['manifest']==proof['runtime']['sha256']
    assert binding['companion']==file_ref(manifest['changedRuntime'])['sha256']
    assert binding['app']==file_ref(manifest['app'])['sha256']
    assert set(proof['sources'])==runtime_source_paths(manifest)
    for path,ref in proof['sources'].items():
        assert file_ref(ref['path'])==ref and ref['sha256']==manifest['sourceInputs'][path],'Executed source differs: '+path
    assert binding['python']==proof['sources']['delta/acceptance/cdf_merge_pyspark.py']['sha256']
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
                assert step['sql']==result['stages'][step['name']]['sql']
            argv=command['argv'];assert argv[0]==manifest['sparkHome']+'/bin/spark-submit' and argv[-1]==refs['request']['path']
            assert argv[argv.index('--properties-file')+1]==request['sparkConfigurationFile']
            assert argv[-2]==(str(Path(__file__).resolve().parent/'cdf_merge_pyspark.py') if kind=='python' else manifest['app'])
            if kind=='scala':assert argv[argv.index('--class')+1]=='org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerLiveCdfMerge'
            assert exit_value['exitCode']==result['exits']['python' if kind=='python' else 'child']==0 and exit_value['terminated'] is False
    finally:
        for archive in archives.values():archive.close()
    assert set(proof['apps'])==({'python'} if result.get('profile')=='partitioned' else {'python','scala'})
    frontend=read_ref(proof['frontend']);python_request=read_ref(proof['apps']['python']['request'])
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


def verify_result(result: dict) -> None:
    """Require consistent native semantics and original successful processes with bound provenance."""
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
    order=['distinct','columnDenied','selectDenied','modifyDenied','same'];assert set(result['stages'])==set(order)
    history=dict(initial)
    for name in order:
        stage=result['stages'][name];before=stage['before'];after=stage['after'];table='same' if name=='same' else 'target'
        for key in ['id','version','rows','files','fileObjects','partitionColumns','schema']:
            assert row_document({key:before[key]})==row_document({key:history[table][key]})
        assert before['id']==after['id'] and before['schema']==after['schema']
        assert stage['checkpointBefore']==0 and stage['checkpointAfter']==(1 if name in ['distinct','same'] else 0)
        assert 'MERGE INTO ' in stage['sql'] and 'table_changes(' in stage['sql'] and 'part=s.part' in stage['sql']
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

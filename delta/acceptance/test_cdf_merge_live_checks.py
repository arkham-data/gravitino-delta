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

"""Corrupt independent evidence, never accept boolean-only MERGE attestations."""
import copy
import unittest
from cdf_merge_live_checks import verify_result


def fixture():
    before=[[1,'old','allowed'],[2,'old2','allowed'],[4,'hidden','blocked'],[9,'stable','allowed']]
    after=[[1,'MASKED|TARGET_MASK','allowed'],[4,'hidden','blocked'],[9,'stable','allowed'],[104,'MASKED','allowed']]
    cdc=[[1,'old','allowed','update_preimage'],[1,'MASKED|TARGET_MASK','allowed','update_postimage'],[2,'old2','allowed','delete'],[104,'MASKED','allowed','insert']]
    source=[[1,'changed','allowed','update_preimage',1],[1,'new','allowed','update_postimage',1],[2,'gone','allowed','delete',1],[4,'four','allowed','insert',1],[5,'five','blocked','insert',1]]
    stages={}
    for name in ['distinct','duplicate','empty','ambiguous','missing','columnDenied','postHelperRevoked','selectDenied','modifyDenied','regrant','same','python','pythonDenied']:
        changed=name in ['distinct','regrant','same','python']
        denied=name in ['ambiguous','missing','columnDenied','postHelperRevoked','selectDenied','modifyDenied','pythonDenied']
        stages[name]={'outcome':'failed' if denied else 'success','errors':['java.lang.SecurityException'] if denied else [],
            'before':{'version':0,'rows':before,'actions':[]},
            'after':{'version':1 if changed else 0,'rows':after if changed else before,'actions':[{'commitInfo':{'operation':'MERGE'}},{'add':{}},{'remove':{}},{'cdc':{}}] if changed else []},
            'cdc':cdc if changed else [],'checkpointBefore':0,'checkpointAfter':0 if denied else (2 if name=='empty' else 1),
            'sql':"MERGE INTO c.d.target USING (SELECT * FROM table_changes('c.d.source', 1, 1)) s ON t.id=s.id",
            'sourceBefore':'a'*64,'sourceAfter':'a'*64}
    for stage in stages.values():stage['authorization']={'sourceAccess':['select'],'targetAccess':['alter','select','update'],'policy':1,'downloadSha256':'a'*64}
    stages['selectDenied']['authorization']['sourceAccess']=[]
    stages['modifyDenied']['authorization']['targetAccess']=['select']
    for name in ['distinct','duplicate']:stages[name]['txnAppId']='merge-fixture'
    stages['distinct']['after']['actions'].append({'txn':{'appId':'merge-fixture','version':1}})
    stages['ambiguous']['errors']=['org.apache.spark.sql.delta.DeltaUnsupportedOperationException']
    stages['missing']['errors']=['java.io.FileNotFoundException']
    stages['same']['before']['version']=2;stages['same']['after']['version']=3
    stages['same']['before']['rows']=[[1,'new','allowed'],[3,'hidden','blocked'],[4,'four','allowed'],[5,'five','blocked'],[6,'dup','allowed'],[6,'dup','allowed']]
    stages['same']['after']['rows']=[[1,'MASKED|MASKED','allowed'],[3,'hidden','blocked'],[4,'MASKED|MASKED','allowed'],[5,'five','blocked'],[6,'dup','allowed'],[6,'dup','allowed']]
    stages['same']['cdc']=[[1,'new','allowed','update_preimage'],[1,'MASKED|MASKED','allowed','update_postimage'],[4,'four','allowed','update_preimage'],[4,'MASKED|MASKED','allowed','update_postimage']]
    stages['same']['sourceAfter']='b'*64
    result={'spark':'3.5.4','delta':'3.3.0','java':'17','scala':'2.12.21','ranger':'2.8.0',
        'exits':{'outer':0,'child':0,'python':0},'identities':{'setup':'s','metadata':'m','application':'a'},
        'origins':{'org.apache.spark.sql.SparkSession':{'jar':'spark-sql_2.12-3.5.4.jar','sha256':'a'*64},'org.apache.spark.sql.delta.DeltaLog':{'jar':'delta-spark_2.12-3.3.0.jar','sha256':'b'*64},'scala.Predef$':{'jar':'scala-library-2.12.21.jar','sha256':'c'*64}},
        'binding':{'source':'a'*64,'manifest':'b'*64,'companion':'c'*64,'app':'d'*64,'python':'e'*64},
        'sourceNative':source,'sourceGoverned':[[r[0]+100,'MASKED',r[2],r[3],0] for r in source if r[2]=='allowed'],
        'stages':stages,'transitions':{name:{'policy':i,'downloadSha256':str(i)*64,'privileges':['SELECT_TABLE','MODIFY_TABLE'],'select':name!='revoke','modify':name!='revoke'} for i,name in enumerate(['grant','revoke','regrant'],1)},
        'registration':{'beforeRestart':{'id':1,'version':1},'afterRestart':{'id':1,'version':1},'persisted':{'id':1,'version':1}},
        'postHelper':{'stack':['DeltaRangerMergeCommit.<init>'],'held':True,'publishedBeforeRelease':False}}

    return artifact_fixture(consistent_fixture(result))


def consistent_fixture(result):
    initial_rows=[[1,'new','allowed'],[3,'hidden','blocked'],[4,'four','allowed'],[5,'five','blocked'],[6,'dup','allowed'],[6,'dup','allowed']]
    fresh=[[1,'old','allowed'],[2,'old2','allowed'],[4,'hidden','blocked'],[9,'stable','allowed']]
    names=['source','same','incomplete','target','regrant_target','python_target','post_target','ambiguous_target']
    initial={n:{'id':'uuid-'+n,'version':2 if n in names[:3] else 0,'rows':copy.deepcopy(initial_rows if n in names[:3] else ([[6,'old','allowed'],[9,'stable','allowed']] if n=='ambiguous_target' else fresh)),'actions':[]} for n in names}
    order=['python','pythonDenied','distinct','duplicate','empty','ambiguous','missing','columnDenied','postHelperRevoked','selectDenied','modifyDenied','regrant','same']
    targets={'python':'python_target','pythonDenied':'python_target','ambiguous':'ambiguous_target','postHelperRevoked':'post_target','regrant':'regrant_target','same':'same'}
    history=copy.deepcopy(initial)
    for name in order:
        stage=result['stages'][name];target=targets.get(name,'target');before=copy.deepcopy(history[target]);stage['before']=before
        if name in ['python','distinct','regrant','same']:
            stage['after'].update(id=before['id'],version=before['version']+1)
        else:stage['after']=dict(copy.deepcopy(before),actions=[])
        history[target]=copy.deepcopy(stage['after'])
    registration={name:{'id':i+1,'generation':1,'token':'token-'+name,'nativeVersion':value['version'],'uuid':value['id']} for i,(name,value) in enumerate(history.items())}
    result['registration']={name:copy.deepcopy(registration) for name in ['beforeRestart','afterRestart','persisted']}
    result['nativeInitial']=initial
    return result


class TestCdfMergeLiveChecks(unittest.TestCase):
    def test_accepts_original_and_permutations(self):
        value=fixture();verify_result(value)
        value['sourceNative'].reverse();value['sourceGoverned'].reverse()
        for stage in value['stages'].values():
            stage['before']['rows'].reverse();stage['after']['rows'].reverse();stage['cdc'].reverse()
        verify_result(value)

    def test_rejects_incomplete_or_corrupted_evidence(self):
        mutations=[
            lambda r:r.pop('spark'),lambda r:r['origins'].pop('org.apache.spark.sql.delta.DeltaLog'),
            lambda r:r['exits'].update(outer=1),lambda r:r['exits'].update(python=1),
            lambda r:r['transitions']['regrant'].pop('downloadSha256'),
            lambda r:r['stages']['distinct']['after']['rows'].append([99,'extra','allowed']),
            lambda r:r['sourceGoverned'].pop(),lambda r:r['stages']['distinct']['after']['rows'].pop(1),
            lambda r:r['stages']['duplicate']['after'].update(version=2),
            lambda r:r['stages']['missing'].update(checkpointAfter=1),lambda r:r['stages'].pop('same'),
            lambda r:r['stages']['same']['after']['rows'].pop(),
            lambda r:r['identities'].update(application='s'),lambda r:r['registration']['afterRestart'].update(id=2),
            lambda r:r['stages']['distinct'].update(sourceAfter='b'*64),
            lambda r:r['stages']['distinct']['cdc'].pop(),lambda r:r['stages']['distinct']['after'].update(actions=[]),
            lambda r:r['postHelper'].update(stack=[]),lambda r:r['transitions']['regrant'].update(policy=1),
            lambda r:r['binding'].pop('python'),lambda r:r['sourceNative'][0].__setitem__(1,'altered'),
            lambda r:r['stages']['selectDenied']['authorization'].update(targetAccess=['select']),
            lambda r:r['stages']['modifyDenied']['authorization'].update(sourceAccess=[]),
            lambda r:r['stages']['distinct']['after']['actions'].pop(),
            lambda r:r['stages']['duplicate'].update(txnAppId='foreign'),
            lambda r:[r['stages']['duplicate'][k].update(version=0) for k in ['before','after']],
            lambda r:[r['stages']['empty'][k].update(rows=[]) for k in ['before','after']],
            lambda r:r['stages']['distinct']['after'].update(id='foreign'),
            lambda r:[v['target'].update(nativeVersion=0) for v in r['registration'].values()],
            lambda r:[r['stages']['pythonDenied'][k].update(rows=[]) for k in ['before','after']]]
        for mutate in mutations:
            value=fixture();mutate(value)
            with self.subTest(mutation=mutations.index(mutate)),self.assertRaises((AssertionError,KeyError,ValueError)):
                verify_result(value)




class TestMergeAppSelection(unittest.TestCase):
    def test_profiles_name_real_init_scripts_and_isolate_apps(self):
        from pathlib import Path
        from cdf_read_live_package import app_profile, ROOT
        for name in ['read','merge']:
            profile=app_profile(name)
            self.assertTrue((ROOT/profile['init']).is_file())
            self.assertEqual('cdf-'+name+'-live.jar',profile['jar'])
        self.assertNotEqual(app_profile('read')['class'],app_profile('merge')['class'])
        with self.assertRaises(AssertionError):app_profile('foreign')


class TestMergeHarnessPreflight(unittest.TestCase):
    def plan(self):
        return {'catalog':'catalog','database':'db','resourceService':'service','rangerAppRole':'role','nonce':'fixture',
            'nativeInitial':{'tables':{n:{'id':'uuid-'+n} for n in ['source','same','incomplete']}}}

    def test_application_role_lookup_reads_all_pages(self):
        from cdf_merge_live import application_role
        class RangerPages:
            def __init__(self,pages):self.pages=iter(pages);self.calls=[]
            def call(self,method,path):self.calls.append((method,path));return next(self.pages)
        pages=[{'startIndex':0,'pageSize':2,'totalCount':3,'resultSize':2,'roles':[{'id':1,'name':'older'},{'id':2,'name':'another'}]},
               {'startIndex':2,'pageSize':2,'totalCount':3,'resultSize':1,'roles':[{'id':3,'name':'GRAVITINO_METALAKE_1_ROLE_query'}]}]
        ranger=RangerPages(pages)
        self.assertEqual('GRAVITINO_METALAKE_1_ROLE_query',application_role(ranger,{'resourceService':'service','appRole':'query'}))
        self.assertEqual([('GET','/service/roles/roles?serviceName=service&startIndex=0&pageSize=200'),
                          ('GET','/service/roles/roles?serviceName=service&startIndex=2&pageSize=200')],ranger.calls)

    def test_application_role_lookup_rejects_duplicate_stalled_and_malformed_pages(self):
        from cdf_merge_live import application_role
        def page(start,roles,total=3):return {'startIndex':start,'pageSize':2,'totalCount':total,'resultSize':len(roles),'roles':roles}
        first=page(0,[{'id':1,'name':'older'},{'id':2,'name':'GRAVITINO_METALAKE_1_ROLE_query'}])
        last=page(2,[{'id':3,'name':'last'}])
        cases=[ [first,page(2,[{'id':3,'name':'GRAVITINO_METALAKE_2_ROLE_query'}])],
                [first,page(2,[])], [dict(first,resultSize=1),last],
                [first,dict(last,startIndex=0)], [first,dict(last,totalCount=4)],
                [first,page(2,[{'id':2,'name':'last'}])], [dict(first,pageSize=0),last],
                [dict(first,totalCount=True),last], [page(0,[],total=0)],
                [page(0,[{'id':1,'name':'older'}]),last] ]
        for index,pages in enumerate(cases):
            class RangerPages:
                def __init__(self):self.pages=iter(copy.deepcopy(pages))
                def call(self,method,path):return next(self.pages)
            with self.subTest(index=index),self.assertRaises((AssertionError,KeyError)):
                application_role(RangerPages(),{'resourceService':'service','appRole':'query'})

    def test_column_deny_is_exact_and_does_not_grant_any_access(self):
        from cdf_merge_live import deny_policy
        policy=deny_policy(self.plan())
        self.assertEqual(0,policy['policyType'])
        self.assertNotIn('policyItems',policy)
        self.assertEqual({'catalog':['catalog'],'database':['db'],'table':['target'],'column':['secret']},
            {key:value['values'] for key,value in policy['resources'].items()})
        self.assertTrue(all(not v['isExcludes'] and not v['isRecursive'] for v in policy['resources'].values()))
        self.assertEqual([{'roles':['role'],'users':[],'groups':[],'accesses':[{'type':'update','isAllowed':True}]}],policy['denyPolicyItems'])

    def test_steps_keep_source_identity_same_table_and_duplicate_intent(self):
        from cdf_merge_live import steps, witness_label
        plan=self.plan();scala={s['name']:s for s in steps(plan)};python=steps(plan,True)
        self.assertEqual(11,len(scala));self.assertEqual(['python','pythonDenied'],[s['name'] for s in python])
        self.assertEqual(scala['distinct']['sql'],scala['duplicate']['sql'])
        self.assertEqual(scala['distinct']['txnAppId'],scala['duplicate']['txnAppId'])
        self.assertIn('MERGE INTO catalog.db.same',scala['same']['sql'])
        self.assertIn("table_changes('catalog.db.same', 1, 1)",scala['same']['sql'])
        self.assertEqual('uuid-same',scala['same']['sourceId'])
        self.assertEqual(2,scala['empty']['end'])
        self.assertTrue(all('delta.`' not in step['sql'] for step in list(scala.values())+python))
        self.assertEqual(['distinct-before','post-helper','regrant-before'],[witness_label(n) for n in ['grant','revoke','regrant']])

    def test_registration_identity_matches_actual_receipt_types(self):
        from cdf_merge_live import receipt_identity, RECEIPT
        fields={'id':'3646841081197916551','generation':'2','token':'token','nativeVersion':'3','nativeId':'native-uuid'}
        tables={'same':{'properties':{RECEIPT+k:v for k,v in fields.items()}}}
        self.assertEqual({'same':{'id':3646841081197916551,'generation':2,'token':'token','nativeVersion':3,'uuid':'native-uuid'}},receipt_identity(tables))
        tables['same']['properties'].pop(RECEIPT+'nativeId')
        with self.assertRaises(KeyError):receipt_identity(tables)


def artifact_fixture(result):
    """Real temporary receipt/artifact files exercise the standalone validator without services."""
    import atexit,csv,hashlib,json,shutil,tempfile,zipfile
    from pathlib import Path
    from cdf_merge_live import steps,RECEIPT
    from cdf_merge_live_checks import bind_evidence,canonical,file_ref
    root=Path(__file__).resolve().parents[2];temporary=Path(tempfile.mkdtemp(prefix='cdf-merge-checker-')).resolve();atexit.register(shutil.rmtree,temporary,ignore_errors=True)
    cell=temporary/'cell';home=temporary/'spark-home';(home/'jars').mkdir(parents=True)
    def save(path,value):
        path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value));return path
    jars={};providers={};classes={}
    names=[('spark-sql_2.12-3.5.4.jar','org.apache.spark.sql.SparkSession'),('delta-spark_2.12-3.3.0.jar','org.apache.spark.sql.delta.DeltaLog'),('scala-library-2.12.21.jar','scala.Predef$'),('companion.jar','org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerMergeCommand'),('cdf-merge-live.jar','org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerLiveCdfMerge$')]
    for filename,name in names:
        path=temporary/filename;body=('class '+name).encode()
        with zipfile.ZipFile(path,'w') as jar:jar.writestr(name.replace('.','/')+'.class',body)
        jars[str(path)]=file_ref(path)['sha256'];providers[name]=str(path);classes[name]=hashlib.sha256(body).hexdigest();(home/'jars'/filename).symlink_to(path)
    dataframe=result.get('frontend','sql')=='dataframe'
    if dataframe:
        for name in ['GravitinoDeltaTable','GravitinoDeltaTable$','GravitinoDeltaMergeBuilder']:
            full='org.apache.gravitino.spark.connector.delta.ranger.'+name;body=('class '+full).encode();path=temporary/'companion.jar'
            with zipfile.ZipFile(path,'a') as jar:jar.writestr(full.replace('.','/')+'.class',body)
            providers[full]=str(path);classes[full]=hashlib.sha256(body).hexdigest()
        jars[str(path)]=file_ref(path)['sha256']
    sources=['delta/acceptance/'+n for n in ['cdf_merge_live.py','cdf_merge_live_checks.py','cdf_merge_native.py','cdf_merge_pyspark.py']]+['spark-connector/v3.5/spark-delta-ranger/src/'+path for path in ['test/scala/org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerLiveCdfMerge.scala','main/scala/org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerMergeCommand.scala']]
    if dataframe:sources+=['delta/acceptance/cdf_merge_dataframe_pyspark.py','delta/acceptance/cdf_read_live_package.py','delta/acceptance/cdf_live_package.py']+[str(p.relative_to(root)) for p in (root/'spark-connector/v3.5/spark-delta-ranger/python/gravitino_delta').glob('*.py')]
    inputs={p:file_ref(root/p)['sha256'] for p in sources}
    manifest={'spark':'3.5.4','delta':'3.3.0','sparkHome':str(home),'app':str(temporary/'cdf-merge-live.jar'),'changedRuntime':str(temporary/'companion.jar'),'sourceInputs':inputs,'sourceInputDigest':canonical(inputs),'providers':providers,'providerClasses':classes,'jarHashes':jars,'runtimeJarLinks':{str(home/'jars'/Path(p).name):p for p in jars},'thinClasses':{names[-1][1].replace('.','/')+'.class':classes[names[-1][1]]}}
    if dataframe:
        from cdf_read_live_package import python_sources,archive_entries
        python_zip=temporary/'gravitino-delta-python-fixture.zip'
        with zipfile.ZipFile(python_zip,'w') as archive:
            for path in python_sources(inputs):archive.write(root/path,path.split('/python/')[1])
        manifest.update(pythonApiVersion=1,pythonZip=str(python_zip),pythonZipSha256=file_ref(python_zip)['sha256'],pythonSourceInputs=python_sources(inputs),pythonZipMembers=archive_entries(python_zip))
    if dataframe:
        before=save(temporary/'source-before.json',{'files':inputs});after=save(temporary/'source-after.json',{'files':inputs});exit_path=save(temporary/'build-exit.json',{'exitCode':0})
        receipt=save(temporary/'build-receipt.json',{'before':str(before),'after':str(after),'exit':str(exit_path),'appBuildArtifact':manifest['app'],'artifacts':{p:file_ref(p)['sha256'] for p in [manifest['app'],manifest['changedRuntime'],manifest['pythonZip']]}})
        manifest.update(freshBuildReceipt=str(receipt),freshBuildReceiptSha256=file_ref(receipt)['sha256'])
    manifest_path=save(temporary/'manifest.json',manifest)
    result['origins']={name:{'jar':providers[name],'sha256':jars[providers[name]]} for _,name in names[:3]}
    result['binding']={'source':canonical(inputs),'manifest':file_ref(manifest_path)['sha256'],'app':jars[manifest['app']],'companion':jars[manifest['changedRuntime']],'python':inputs['delta/acceptance/'+('cdf_merge_dataframe_pyspark.py' if dataframe else 'cdf_merge_pyspark.py')],**({'pythonZip':manifest['pythonZipSha256']} if dataframe else {})}
    partitioned=result.get('profile')=='partitioned'
    plan={'partitioned':partitioned,'catalog':'c','database':'d','nonce':'fixture','nativeInitial':{'tables':result['nativeInitial']},'subject':'a','metadataPrincipal':'m','serviceAdmin':'s'}
    ranger=save(cell/'deployed-ranger/association.json',{'deployedRangerVersion':'2.8.0'});plan['deployedRangerEvidenceSha256']=file_ref(ranger)['sha256'];save(cell/'fixture/state.private.json',plan)
    native={'tables':result['nativeInitial'],'sourceNative':result['sourceNative'],'spark':'3.5.4','delta':'3.3.0','java':'17','scala':'2.12.21','identity':{'ugi':'s','sparkUser':'s'},'origins':{k:{'origin':v['jar'],'sha256':v['sha256']} for k,v in result['origins'].items()}}
    save(cell/'fixture/native/ready.json',native);save(cell/'fixture/native/process/command.json',{'identity':'s','argv':[str(home/'bin/spark-submit'),str(root/'delta/acceptance/cdf_merge_native.py'),str(cell/'fixture/native/inputs.private.json')]});save(cell/'fixture/native/process/exit.json',{'exitCode':0,'terminated':False})
    save(cell/('python' if partitioned else 'scala')/'app-evidence/source-governed.json',result['sourceGoverned'])
    for kind in (['python'] if partitioned else ['python','scala']):
        directory=cell/kind;app=directory/'app-evidence';selected=steps(plan,kind=='python','dataframe' if dataframe else 'sql')
        request={'frontend':'dataframe' if dataframe else 'sql','packageManifest':str(manifest_path),'expectedSparkVersion':'3.5.4','expectedDeltaVersion':'3.3.0','subject':'a','metadataPrincipal':'m','steps':selected,'sparkConfigurationFile':str(directory/'request/spark.properties')}
        request_path=save(directory/'request/request.private.json',request)
        argv=[str(home/'bin/spark-submit'),'--properties-file',request['sparkConfigurationFile']]
        if kind=='scala':argv+=['--class','org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerLiveCdfMerge']
        if dataframe:argv+=['--py-files',manifest['pythonZip']]
        argv+=[str(root/('delta/acceptance/cdf_merge_dataframe_pyspark.py' if dataframe else 'delta/acceptance/cdf_merge_pyspark.py')) if kind=='python' else manifest['app'],str(request_path)]
        save(directory/'process/command.json',{'identity':'a','argv':argv});save(directory/'process/exit.json',{'exitCode':0,'terminated':False})
        save(app/'origins.json',{n:{'jar':p,'jarSha256':jars[p],'classSha256':classes[n]} for n,p in providers.items()})
        save(app/'observed.json',{'subject':'a','ugi':'a','spark':'3.5.4','java':'17','scala':'2.12.21'});save(app/'metadata-identity-final.json',{'principal':'m','successfulTargetGet':True})
        for step in selected:
            if dataframe:
                result['stages'][step['name']].pop('sql')
                result['stages'][step['name']]['operation']=step['operation']
            else:result['stages'][step['name']]['sql']=step['sql']
            if partitioned:
                stage=result['stages'][step['name']]
                for field,path,data in [('nativeBefore',cell/'fixture/native'/(step['name']+'-before.json'),stage['before']),('nativeAfter',cell/'fixture/native'/(step['name']+'-after.json'),stage['after']),('consumerBefore',app/(step['name']+'-checkpoint-before.json'),{'sourceId':step['sourceId'],'end':stage['checkpointBefore']}),('consumerAfter',app/(step['name']+'-checkpoint-after.json'),{'sourceId':step['sourceId'],'end':stage['checkpointAfter']})]:
                    stage[field]=file_ref(save(path,data))
            save(app/(step['name']+'-identity.json'),{'subject':'a','ugi':'a','thread':'query-thread'});save(directory/(step['name']+'-result.json'),result['stages'][step['name']])
        if dataframe:
            frontend={'frontend':'classic-pyspark-dataframe','pysparkVersion':'3.5.4','python':'3.11','apiVersion':1,'operations':[s['operation'] for s in selected],
                'modules':{name:{'file':manifest['pythonZip']+'/'+member,'archive':manifest['pythonZip'],'sha256':manifest['pythonZipMembers'][member]} for name,member in [('gravitino_delta','gravitino_delta/__init__.py'),('gravitino_delta.tables','gravitino_delta/tables.py')]},
                'queryIdentities':{s['name']:{'subject':'a','ugi':'a','thread':'query-thread'} for s in selected}}
            save(app/'python-observed.json',frontend)
        elif kind=='python':save(app/'python-observed.json',{'frontend':'classic-pyspark','pysparkVersion':'3.5.4','python':'3.11','queries':[s['sql'] for s in selected]})
    save(temporary/'inputs.private.json',{'testFixture':True})
    outer=cell.parent/(cell.name+'-outer');save(outer/'command.json',{'argv':['python',str(root/'delta/acceptance/cdf_merge_live.py'),'--child','--runtime',str(manifest_path),'--inputs',str(temporary/'inputs.private.json'),'--out',str(cell)]+(['--partitioned'] if partitioned else [])+(['--frontend','dataframe'] if dataframe else [])});save(outer/'exit.json',{'exitCode':0,'terminated':False})
    registration={}
    for name,value in result['registration']['persisted'].items():
        registration[name]={'properties':{RECEIPT+key:str(value[field]) for key,field in [('id','id'),('generation','generation'),('token','token'),('nativeId','uuid'),('nativeVersion','nativeVersion')]}}
    save(cell/'registration-before-restart/tables.private.json',registration);save(cell/'registration-after-restart/tables.private.json',registration)
    path=cell/'persistence/tables.csv';path.parent.mkdir()
    with path.open('w') as stream:
        writer=csv.DictWriter(stream,fieldnames=['TABLE_NAME','PROPERTIES']);writer.writeheader()
        for name,value in result['registration']['persisted'].items():
            state={'state':'PUBLISHED','id':value['id'],'generation':value['generation'],'token':value['token'],'observation':{'uuid':value['uuid'],'version':value['nativeVersion']}}
            writer.writerow({'TABLE_NAME':name,'PROPERTIES':json.dumps({'delta.registration.internal.state':json.dumps(state)})})
    return bind_evidence(result,cell,manifest_path)


class TestOriginalMergeProvenance(unittest.TestCase):
    def test_rejects_missing_or_changed_original_frontend_identity_and_artifacts(self):
        import json
        from pathlib import Path
        from cdf_merge_live_checks import file_ref
        def change(ref,mutation):
            path=Path(ref['path']);value=json.loads(path.read_text());mutation(value);path.write_text(json.dumps(value));ref.update(file_ref(path))
        mutations=[lambda r:r['provenance'].pop('frontend'),
            lambda r:change(r['provenance']['frontend'],lambda v:v.update(pysparkVersion='3.5.6')),
            lambda r:change(r['provenance']['frontend'],lambda v:v.update(frontend='scala-launcher')),
            lambda r:change(r['provenance']['frontend'],lambda v:v.update(queries=[])),
            lambda r:change(r['provenance']['apps']['python']['origins'],lambda v:v.pop('org.apache.spark.sql.delta.DeltaLog')),
            lambda r:change(r['provenance']['apps']['python']['observed'],lambda v:v.update(ugi='s')),
            lambda r:change(r['provenance']['apps']['python']['identities']['python'],lambda v:v.update(subject='m')),
            lambda r:change(r['provenance']['apps']['python']['exit'],lambda v:v.update(exitCode=1)),
            lambda r:r['binding'].update(app='0'*64),lambda r:r['binding'].update(source='0'*64),
            lambda r:change(r['provenance']['outerCommand'],lambda v:v.update(argv=['foreign'])),
            lambda r:r['provenance']['sources']['delta/acceptance/cdf_merge_pyspark.py'].update(sha256='0'*64),
            lambda r:change(r['provenance']['outerCommand'],lambda v:v['argv'].__setitem__(1,'foreign.py')),
            lambda r:change(r['provenance']['nativeCommand'],lambda v:v['argv'].__setitem__(-2,'foreign.py'))]
        for index,mutation in enumerate(mutations):
            result=fixture();mutation(result)
            with self.subTest(mutation=index),self.assertRaises((AssertionError,KeyError,ValueError)):
                verify_result(result)


    def test_rejects_duplicate_provider_even_with_updated_manifest_hashes(self):
        import json,shutil
        from pathlib import Path
        from cdf_merge_live_checks import file_ref
        result=fixture();ref=result['provenance']['runtime'];path=Path(ref['path']);manifest=json.loads(path.read_text())
        original=Path(manifest['providers']['org.apache.spark.sql.SparkSession']);duplicate=original.parent/'foreign-provider.jar';shutil.copyfile(original,duplicate)
        link=Path(manifest['sparkHome'])/'jars'/duplicate.name;link.symlink_to(duplicate)
        manifest['jarHashes'][str(duplicate)]=file_ref(duplicate)['sha256'];manifest['runtimeJarLinks'][str(link)]=str(duplicate)
        path.write_text(json.dumps(manifest));ref.update(file_ref(path));result['binding']['manifest']=ref['sha256']
        with self.assertRaises(AssertionError):verify_result(result)


if __name__=='__main__':unittest.main()

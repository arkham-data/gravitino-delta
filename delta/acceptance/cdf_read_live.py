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

"""One fresh deployed Ranger/MinIO cell for installed named CDF consumption."""
import argparse
import copy
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import time
import uuid
import xml.etree.ElementTree as ET
from urllib.parse import quote

from cdf_live import (ROOT, JAVA, Public, Ranger, Server, RecordingProxy, OwnedObjects, envfile,
    save, sha, read, launch, clean_environment, catalog_properties, submit, command, identity,
    RECEIPT, source_resources, verify_deployed, prepare_request)
from cdf_live_checks import exact_access, canonical_privileges, admin_role_state
from cdf_read_live_checks import verify_result
from package_binding import capture_sources, canonical_digest


def native(plan,runtime,mode,output,table=None):
    output.mkdir(mode=0o700)
    data={'primary':plan['primary'],'spark':runtime['spark'],'delta':runtime['delta'],
          'table':table or plan['tables'][0],'mode':mode,'output':str(output),'nativeIdentity':plan['serviceAdmin']}
    save(output/'inputs.private.json',data)
    environment=clean_environment(plan)
    environment.update(SPARK_HOME=runtime['sparkHome'],PYSPARK_PYTHON=plan['python'],SPARK_USER=plan['serviceAdmin'],HADOOP_USER_NAME=plan['serviceAdmin'])
    launch([runtime['sparkHome']+'/bin/spark-submit','--driver-memory','2g','--master','local[2]',ROOT/'delta/acceptance/cdf_read_native.py',output/'inputs.private.json'],environment,output/'process')
    result=read(output/'result.json')
    assert all(runtime['jarHashes'].get(v['origin'])==v['sha256'] for v in result['origins'].values())
    return result


def provision(runtime,inputs,output,association,handles):
    output.mkdir(mode=0o700)
    for child in ['private','public-trace','ranger-trace']: (output/child).mkdir()
    endpoints=read(inputs['endpoints'])
    admin=envfile(inputs['rangerAdminFile'])
    nonce=uuid.uuid4().hex[:12]
    plan={'nonce':nonce,'subject':'cdf_app_'+nonce,'metadataPrincipal':'cdf_meta_'+nonce,'serviceAdmin':'cdf_setup_'+nonce,
        'metalake':'cdf_lake_'+nonce,'catalog':'cdf_catalog','database':'shared','resourceService':'cdf_service_'+nonce,'resourceDefinition':'cdf_def_'+nonce,
        'appRole':'cdf_query_'+nonce,'metaRole':'cdf_metadata_'+nonce,'prefix':'cdf-live-'+nonce,'seedAppId':'cdf-seed-'+nonce,
        'javaHome':JAVA,'python':inputs['python'],'rangerUrl':endpoints['ranger-admin'],
        'primary':{'endpoint':endpoints['minio'],'region':'us-east-1','bucket':'delta-fixtures','credentialsFile':inputs['minioCredentialsFile']},
        'rangerAdmin':{'username':admin['RANGER_ADMIN_USER'],'password':admin['RANGER_ADMIN_PASSWORD']},
        'plugin':{'username':'cdf_download_'+nonce,'password':'Aa1!'+secrets.token_urlsafe(24)},'fixtureDirectory':str(output),
        'deployedRangerEvidence':str(association),'deployedRangerEvidenceSha256':sha(association)}
    names=['source','incomplete']
    plan['tables']=[{'name':name,'prefix':plan['prefix']+'/adoption/'+name+'/',
        'location':'s3a://delta-fixtures/'+plan['prefix']+'/adoption/'+name} for name in names]
    save(output/'state.private.json',plan)
    ranger=Ranger(plan,output/'ranger-trace');owned=[]
    def create(kind,path,body):
        result=ranger.call('POST',path,body);owned.append({'kind':kind,'id':result['id'],'name':body['name']});save(output/'ranger-owned.json',owned);return result
    create('downloader','/service/xusers/secure/users',{'name':plan['plugin']['username'],'firstName':'CDF','lastName':'Fixture','password':plan['plugin']['password'],
        'emailAddress':plan['plugin']['username']+'@fixture.invalid','userRoleList':['ROLE_USER'],'status':1,'userSource':0,'isVisible':1})
    definition=read(ROOT/'authorizations/authorization-ranger-delta/src/main/resources/ranger-delta-servicedef.json');definition['name']=plan['resourceDefinition']
    create('definition','/service/public/v2/api/servicedef',definition)
    create('service','/service/public/v2/api/service',{'name':plan['resourceService'],'type':plan['resourceDefinition'],'isEnabled':True,
        'configs':{'create.default.policy.per.hierarchy':'false','policy.download.auth.users':plan['plugin']['username'],'ranger.plugin.spark.gravitino.metalake':plan['metalake']}})
    callbacks=RecordingProxy(plan['rangerUrl'],output/'callbacks.private.jsonl');callbacks.__enter__();handles['callbacks']=callbacks
    plan['callbackUri']='http://127.0.0.1:'+str(callbacks.port)
    server=Server(Path(runtime['assemblyManifest']).parent/'server',output,plan);handles['server']=server
    # Expose partial ownership before startup; caller always stops this exact fixture.
    server.start();plan['gravitinoUrl']=server.uri;save(output/'state.private.json',plan)
    rest=Public(server.uri,plan['serviceAdmin'],output/'public-trace');base='/api/metalakes/'+plan['metalake']
    rest.call('POST','/api/metalakes',{'name':plan['metalake'],'comment':'Owned CDF qualification','properties':{}})
    props=catalog_properties(plan,'primary',plan['prefix'])
    props.update({'authorization-provider':'ranger-delta','authorization.ranger.admin.url':plan['callbackUri'],
        'authorization.ranger.auth.type':'simple','authorization.ranger.username':plan['rangerAdmin']['username'],
        'authorization.ranger.password':plan['rangerAdmin']['password'],'authorization.ranger.service.name':plan['resourceService'],
        'authorization.ranger.service.type':plan['resourceDefinition'],'authorization.ranger.service.create-if-absent':'false',
        'authorization.ranger.delta.metalake':plan['metalake'],'authorization.ranger.delta.catalog':plan['catalog']})
    rest.call('POST',base+'/catalogs',{'name':plan['catalog'],'type':'RELATIONAL','provider':'lakehouse-delta','properties':props})
    rest.call('POST',base+'/catalogs/'+plan['catalog']+'/schemas',{'name':plan['database'],'properties':{}})
    native(plan,runtime,'seed',output/'native-seed')
    native(plan,runtime,'seed',output/'native-fault-seed',plan['tables'][1])
    receipts={}
    for table in plan['tables']:
        receipt=submit(rest,base,plan['catalog'],table['name'],command('ADOPT',token=str(uuid.uuid4()),location=table['location'],runId=str(uuid.uuid4()),reference='owned-native-seed-exit-zero'))
        receipts[table['name']]=receipt;save(output/'receipts.private.json',receipts)
        receipts[table['name']]=submit(rest,base,plan['catalog'],table['name'],command('PUBLISH',expected=identity(receipt)))
        assert receipts[table['name']][RECEIPT+'state']=='PUBLISHED';save(output/'receipts.private.json',receipts)
    def obj(kind,name,priv):return {'type':kind,'fullName':name,'privileges':[{'name':priv,'condition':'ALLOW'}]}
    use=[obj('CATALOG',plan['catalog'],'USE_CATALOG'),obj('SCHEMA',plan['catalog']+'.'+plan['database'],'USE_SCHEMA')]
    for role,user,selected in [(plan['appRole'],plan['subject'],['incomplete']),(plan['metaRole'],plan['metadataPrincipal'],names)]:
        rest.call('POST',base+'/users',{'name':user})
        rest.call('POST',base+'/roles',{'name':role,'properties':{},'securableObjects':use+[obj('TABLE',plan['catalog']+'.'+plan['database']+'.'+name,'SELECT_TABLE') for name in selected]})
        rest.call('PUT',base+'/permissions/users/'+quote(user,safe='')+'/grant/',{'roleNames':[role]})
    roles=ranger.call('GET','/service/roles/roles?serviceName='+plan['resourceService'])['roles']
    matches=[r for r in roles if r['name'].endswith('_ROLE_'+plan['appRole'])];assert len(matches)==1
    app=matches[0]['name'];plan['rangerAppRole']=app
    for column,expression in [('id','CAST(0 AS BIGINT)'),('secret',"'MASKED'"),('_commit_version','CAST(0 AS BIGINT)'),('_commit_timestamp',"TIMESTAMP '2000-01-01 00:00:00'"),(None,"region = 'allowed'")]:
        policy_type=1 if column else 2
        resources=source_resources(policy_type,plan)
        if column:resources['column']['values']=[column]
        item={'roles':[app],'users':[],'groups':[],'accesses':[{'type':'select','isAllowed':True}]}
        item['dataMaskInfo' if column else 'rowFilterInfo']=({'dataMaskType':'CUSTOM','valueExpr':expression} if column else {'filterExpr':expression})
        created=create('mask' if column else 'filter','/service/public/v2/api/policy',{'service':plan['resourceService'],
            'name':'cdf_read_'+(column or 'filter')+'_'+nonce,'policyType':policy_type,'isEnabled':True,'isAuditEnabled':True,
            'resources':resources,('dataMaskPolicyItems' if column else 'rowFilterPolicyItems'):[item]})
        plan.setdefault('sourcePolicyIds',[]).append(created['id'])
    security=output/'private/source-security';security.mkdir();(output/'private/source-cache').mkdir()
    values={'ranger.plugin.spark.service.name':plan['resourceService'],'ranger.plugin.spark.policy.rest.url':plan['rangerUrl'],
        'ranger.plugin.spark.policy.rest.client.username':plan['plugin']['username'],'ranger.plugin.spark.policy.rest.client.password':plan['plugin']['password'],
        'ranger.plugin.spark.policy.cache.dir':str(output/'private/source-cache'),'ranger.plugin.spark.policy.pollIntervalMs':'500',
        'ranger.plugin.spark.policy.rest.client.connection.timeoutMs':'3000','ranger.plugin.spark.policy.rest.client.read.timeoutMs':'3000',
        'ranger.plugin.spark.policy.rest.client.max.retry.attempts':'0','ranger.plugin.spark.policy.rest.client.cookie.enabled':'false','ranger.plugin.spark.supports.policy.deltas':'false'}
    xml=ET.Element('configuration')
    for key,value in values.items():node=ET.SubElement(xml,'property');ET.SubElement(node,'name').text=key;ET.SubElement(node,'value').text=value
    ET.indent(xml);ET.ElementTree(xml).write(security/'ranger-spark-security.xml',encoding='utf-8',xml_declaration=True)
    shutil.copyfile(Path(inputs['sourceSecurityDirectory'])/'log4j2.xml',security/'log4j2.xml')
    (output/'private/metadata.properties').write_text('auth-type=simple\nprincipal='+plan['metadataPrincipal']+'\nlifetime-seconds=3600\n')
    plan['sourceSecurityDirectory']=str(security);save(output/'state.private.json',plan)
    return plan,server,callbacks


def snapshot(plan, selected, output):
    output.mkdir(mode=0o700)
    rest=Public(plan['gravitinoUrl'],plan['serviceAdmin'],output/'public-trace');ranger=Ranger(plan,output/'ranger-trace')
    base='/api/metalakes/'+plan['metalake'];full=plan['catalog']+'.'+plan['database']+'.source'
    public={key:rest.call('GET',base+path)['role' if 'Role' in key else 'user'] for key,path in [
        ('appRole','/roles/'+plan['appRole']),('metadataRole','/roles/'+plan['metaRole']),('appUser','/users/'+plan['subject']),('metadataUser','/users/'+plan['metadataPrincipal'])]}
    assert len({plan[k] for k in ['subject','metadataPrincipal','serviceAdmin']})==3
    for label,user,role,allow in [('app',plan['subject'],plan['appRole'],selected),('metadata',plan['metadataPrincipal'],plan['metaRole'],True)]:
        assert public[label+'User']['roles']==[role]
        objects=public[label+'Role']['securableObjects'];assert len({o['fullName'] for o in objects})==len(objects)
        expected={plan['catalog']:{('USE_CATALOG','ALLOW')},plan['catalog']+'.'+plan['database']:{('USE_SCHEMA','ALLOW')}}
        expected[plan['catalog']+'.'+plan['database']+'.incomplete']={('SELECT_TABLE','ALLOW')}
        if allow:expected[full]={('SELECT_TABLE','ALLOW')}
        actual={o['fullName']:canonical_privileges(o['privileges']) for o in objects if o['privileges']}
        assert actual==expected
    policies=ranger.call('GET','/service/plugins/secure/policies/download/'+plan['resourceService']+'?lastKnownVersion=-1&pluginId=cdf-read-supervisor',download=True)
    roles=ranger.call('GET','/service/roles/secure/download/'+plan['resourceService']+'?lastKnownRoleVersion=-1&pluginId=cdf-read-supervisor',download=True)
    admin=ranger.call('GET','/service/roles/roles?serviceName='+plan['resourceService'])
    assert policies['serviceName']==roles['serviceName']==plan['resourceService']
    for subject,role in [(plan['subject'],plan['rangerAppRole']),(plan['metadataPrincipal'],None)]:
        matches=[r for r in admin['roles'] if any(u['name']==subject for u in r.get('users',[]))]
        assert len(matches)==1 and matches[0]['users']==[{'name':subject,'isAdmin':False}] and not matches[0].get('groups') and not matches[0].get('roles')
        if role:assert matches[0]['name']==role
        else:metadata=matches[0]['name']
    assert exact_access(policies['policies'],plan['catalog'],plan['database'],'source',plan['rangerAppRole'])==({'select'} if selected else set())
    assert exact_access(policies['policies'],plan['catalog'],plan['database'],'source',metadata)=={'select'}
    assert exact_access(policies['policies'],plan['catalog'],plan['database'],'incomplete',plan['rangerAppRole'])=={'select'}
    for policy in policies['policies']:
        if policy.get('policyType',0)!=0:continue
        for item in policy.get('policyItems',[]):
            assert not {plan['subject'],plan['metadataPrincipal']}.intersection(item.get('users',[])) and not item.get('groups')
            if plan['rangerAppRole'] in item.get('roles',[]) and any(a['type'] in ['select','all','update','alter'] for a in item['accesses']):
                resources=policy['resources'];table=resources['table']['values'];assert table in [['source'],['incomplete']]
                assert resources=={k:{'values':[v],'isExcludes':False,'isRecursive':False} for k,v in [('catalog',plan['catalog']),('database',plan['database']),('table',table[0]),('column','*')]}
                assert all(a['type']=='select' and a['isAllowed'] is True for a in item['accesses'])
                assert 'gravitino-managed' in policy['policyLabels']
    state={'policies':policies,'roles':roles,'adminRoles':admin}
    save(output/'public-state.private.json',public);save(output/'ranger-state.private.json',state)
    descriptor={'publicStateFile':str(output/'public-state.private.json'),'rangerStateFile':str(output/'ranger-state.private.json'),
        'policy':policies['policyVersion'],'role':roles['roleVersion'],'selected':selected}
    save(output/'state-evidence.private.json',descriptor)
    return descriptor


def transition(plan, grant, output):
    output.mkdir(mode=0o700)
    before=snapshot(plan,not grant,output/'before')
    path=Path(plan['fixtureDirectory'])/'callbacks.private.jsonl';cursor=len(path.read_text().splitlines())
    save(output/'intent.json',{'grant':grant,'privilege':'SELECT_TABLE','callbackCursor':cursor,'replayAllowed':False})
    rest=Public(plan['gravitinoUrl'],plan['serviceAdmin'],output/'public-trace')
    full=plan['catalog']+'.'+plan['database']+'.source'
    response=rest.call('PUT','/api/metalakes/'+plan['metalake']+'/permissions/roles/'+plan['appRole']+'/table/'+full+('/grant/' if grant else '/revoke/'),{'privileges':[{'name':'SELECT_TABLE','condition':'ALLOW'}]})
    after=snapshot(plan,grant,output/'after')
    callbacks=[json.loads(line) for line in path.read_text().splitlines()[cursor:]];save(output/'callbacks.private.json',callbacks)
    old=read(before['rangerStateFile']);new=read(after['rangerStateFile'])
    assert old['roles']==new['roles'] and admin_role_state(old['adminRoles'])==admin_role_state(new['adminRoles'])
    def without_select(state):
        policies=copy.deepcopy(state['policies']['policies'])
        for policy in policies:
            for key in ['version','updateTime','updatedBy']:policy.pop(key,None)
            if policy.get('policyType',0)==0:
                items=[]
                for item in policy.get('policyItems',[]):
                    if policy['resources']['table']['values']==['source'] and plan['rangerAppRole'] in item.get('roles',[]) and any(a['type']=='select' for a in item['accesses']):
                        assert item['accesses']==[{'type':'select','isAllowed':True}]
                        item['roles'].remove(plan['rangerAppRole'])
                        if not item['roles']:continue
                    if 'roles' in item:item['roles'].sort()
                    items.append(item)
                policy['policyItems']=items
        return {p['id']:p for p in policies}
    assert without_select(old)==without_select(new),'Unrelated policy drift'
    mutations=[v for v in callbacks if v['method']!='GET']
    assert mutations and all(v['method']=='PUT' and '/service/public/v2/api/policy/' in v['path'] for v in mutations)
    assert after['policy']>before['policy']
    save(output/'completed.json',{'before':before,'after':after,'code':response['code'],'callbacksSha256':sha(output/'callbacks.private.json')})
    return after


def read_snapshot(plan, output):
    output.mkdir(mode=0o700)
    objects=OwnedObjects(plan['primary'],plan['tables'][0]['prefix']).manifest(output/'objects')
    rest=Public(plan['gravitinoUrl'],plan['serviceAdmin'],output/'public-trace')
    base='/api/metalakes/'+plan['metalake']
    table=rest.call('GET',base+'/catalogs/'+plan['catalog']+'/schemas/'+plan['database']+'/tables/source')
    owner=rest.call('GET',base+'/owners/table/'+plan['catalog']+'.'+plan['database']+'.source')
    fault=rest.call('GET',base+'/catalogs/'+plan['catalog']+'/schemas/'+plan['database']+'/tables/incomplete')
    registration={'table':table,'owner':owner,'incomplete':fault}
    save(output/'objects.json',objects);save(output/'registration.private.json',registration)
    return objects,registration


def run_cell(runtime_path: Path, inputs_path: Path, output: Path) -> None:
    output.mkdir(mode=0o700)
    runtime=read(runtime_path);runtime['manifestPath']=str(runtime_path);inputs=read(inputs_path)
    from cdf_read_live_package import verify_runtime
    verify_runtime(runtime)
    handles={};process=None;log=None
    try:
        association=verify_deployed(inputs,output/'deployed-ranger')
        plan,server,callbacks=provision(runtime,inputs,output/'fixture',association,handles)
        oracle=read(output/'fixture/native-seed/result.json')
        initial=snapshot(plan,False,output/'initial-state')
        request=prepare_request(plan,runtime,initial,output/'request')
        request.update(output=str(output/'app-evidence'))
        save(output/'request/request.private.json',request)
        before,registration=read_snapshot(plan,output/'before')
        environment=clean_environment(plan);environment.update(SPARK_HOME=runtime['sparkHome'],SPARK_USER=plan['subject'],HADOOP_USER_NAME=plan['subject'])
        selected=[p for p in runtime['jarHashes'] if Path(p).name in runtime['addedJarNames'] and Path(p).name!='cdf-read-live.jar']
        argv=[runtime['sparkHome']+'/bin/spark-submit','--driver-memory','4g','--class','org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerLiveCdfRead','--master','local[2]',
            '--properties-file',request['sparkConfigurationFile'],'--driver-class-path',request['driverSecurityDirectory'],'--jars',','.join(selected),runtime['app'],str(output/'request/request.private.json')]
        save(output/'command.json',{'argv':argv,'started':time.time()})
        log=(output/'process.private.log').open('wb');process=subprocess.Popen(argv,env=environment,cwd=ROOT,stdout=log,stderr=subprocess.STDOUT)
        save(output/'process.json',{'pid':process.pid,'started':time.time()})
        transitions={}
        for stage,grant in [('grant',True),('fault',None),('revoke',False),('regrant',True)]:
            ready=Path(request['controlDirectory'])/(stage+'-ready.json');deadline=time.monotonic()+300
            while not ready.exists():
                assert process.poll() is None and time.monotonic()<deadline,'Original app failed before '+stage+'; no replay'
                time.sleep(.05)
            if stage=='fault':
                observed=read(output/'app-evidence/fault-before.json')
                oracle_fault=read(output/'fixture/native-fault-seed/result.json')
                from collections import Counter
                from datetime import datetime
                def row_key(row):return tuple(row[:5])+(datetime.fromisoformat(row[5]),)
                assert observed['outcome']=='success' and Counter(map(row_key,observed['rows']))==Counter(map(row_key,oracle_fault['nativeRows']))
                assert observed['rows'], 'Fault fixture must succeed with nonempty authorized rows before removal'
                before_fault,_=read_snapshot(plan,output/'pre-fault-primary');assert before_fault==before
                damage=native(plan,runtime,'damage',output/'native-fault-damage',plan['tables'][1])
                after_fault=OwnedObjects(plan['primary'],plan['tables'][1]['prefix']).manifest(output/'fault-after-objects')
                assert after_fault==[v for v in damage['objects'] if v['key']!=damage['fault']['removed']]
                save(Path(request['controlDirectory'])/(stage+'-verified.json'),{'verified':True})
                continue
            state=transition(plan,grant,output/('public-'+stage));transitions[stage]=state
            save(Path(request['controlDirectory'])/(stage+'-verified.json'),{'verified':True,'stateEvidenceFile':str(Path(state['publicStateFile']).parent/'state-evidence.private.json')})
        code=process.wait(timeout=120);log.close();save(output/'exit.json',{'pid':process.pid,'exitCode':code,'finished':time.time()});assert code==0
        after,final_registration=read_snapshot(plan,output/'after')
        assert before==after and registration==final_registration,'Reader changed source or registration'
        app=read(output/'app-evidence/observed.json')
        metadata=read(output/'app-evidence/metadata-identity-final.json')
        assert metadata['principal']==plan['metadataPrincipal'] and metadata['successfulTargetGet'] is True
        origins=read(output/'app-evidence/origins.json')
        assert set(origins)==set(runtime['providers'])
        for name,entry in origins.items():
            assert entry['jar']==runtime['providers'][name] and entry['jarSha256']==runtime['jarHashes'][entry['jar']]
            assert entry['classSha256']==runtime['providerClasses'][name]
        server.stop()
        from cdf_provider import store_rows
        persistence=output/'persistence';persistence.mkdir()
        persisted=store_rows(server,persistence,'table',"SELECT t.table_id,t.current_version,v.properties FROM table_meta t JOIN table_version_info v ON t.table_id=v.table_id AND t.current_version=v.version WHERE t.table_name='source' AND t.deleted_at=0")
        assert len(persisted)==1
        state=json.loads(json.loads(persisted[0]['PROPERTIES'])['delta.registration.internal.state'])
        assert state['state']=='PUBLISHED' and state['observation']['uuid']==oracle['nativeId'] and state['observation']['version']==4
        receipt=registration['table']['table']['properties']
        for key in ['id','generation','token']:assert str(state[key])==receipt[RECEIPT+key]
        columns=server.column_snapshot('cdf-read');assert len(columns)==6 and len({v['id'] for v in columns.values()})==6
        server.start();plan['gravitinoUrl']=server.uri
        restarted,loaded=read_snapshot(plan,output/'restarted')
        assert restarted==before and loaded==registration
        server.stop();assert columns==server.column_snapshot('cdf-read-restart')
        result={key:oracle[key] for key in ['nativeRows','nativeHead','coveredVersions','requested']}
        result.update({key:app[key] for key in ['sql','dataframe','empty','incomplete','retained','freshDenied','freshGranted','productionDiscovery','spark','java','scala']})
        result.update(faultBefore=read(output/'app-evidence/fault-before.json'),faultNativeRows=read(output/'fixture/native-fault-seed/result.json')['nativeRows'])
        result.update(delta=runtime['delta'],ranger=read(association)['deployedRangerVersion'],identities={'setup':plan['serviceAdmin'],'metadata':plan['metadataPrincipal'],'application':app['subject']},
            sourceBefore=canonical_digest(before),sourceAfter=canonical_digest(after),registrationBefore=canonical_digest(registration),registrationAfter=canonical_digest(final_registration),
            persistedRegistration=True,runtimeBound=True,exitCode=code,manifestSha256=sha(runtime_path))
        for stage in ['grant','revoke','regrant']:
            witness=read(output/'app-evidence'/(stage+'-applied.json'));state=transitions[stage]
            assert witness['sameJvm'] is True and witness['policy']==state['policy'] and witness['role']==state['role']
            assert witness['stateSha256']==sha(Path(state['publicStateFile']).parent/'state-evidence.private.json')
            result[stage]={'publicPrivilege':'SELECT_TABLE','downloadedSelect':state['selected'],'policy':witness['policy'],'downloadSha256':witness['downloadSha256']}
        verify_runtime(runtime);verify_result(result);save(output/'result.json',result)
    except BaseException as error:
        save(output/'failure.json',{'class':type(error).__name__,'noReplay':True,'noCleanup':True});raise
    finally:
        if process is not None:
            terminated=process.poll() is None
            if terminated:
                process.terminate()
                try:process.wait(timeout=30)
                except subprocess.TimeoutExpired:process.kill();process.wait(timeout=10)
            if not (output/'exit.json').exists():save(output/'exit.json',{'pid':process.pid,'exitCode':process.returncode,'terminatedBySupervisor':terminated,'finished':time.time()})
        if log is not None:log.close()
        if handles.get('server'):handles['server'].stop()
        if handles.get('callbacks'):handles['callbacks'].wait_drained(20);handles['callbacks'].__exit__()
        save(output/'lane-release.json',{'released':True,'failed':(output/'failure.json').exists()})


def main():
    parser=argparse.ArgumentParser()
    for key in ['runtime','inputs','out']:parser.add_argument('--'+key,required=True)
    args=parser.parse_args();os.umask(0o077)
    run_cell(Path(args.runtime).resolve(),Path(args.inputs).resolve(),Path(args.out).resolve())


if __name__=='__main__':main()

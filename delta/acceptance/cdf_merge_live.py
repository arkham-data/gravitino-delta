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
import sys
from urllib.parse import quote

from cdf_live import (ROOT, JAVA, Public, Ranger, Server, RecordingProxy, OwnedObjects, envfile,
    save, sha, read, launch, clean_environment, catalog_properties, submit, command, identity,
    RECEIPT, source_resources, verify_deployed, prepare_request)
from cdf_live_checks import exact_access, canonical_privileges, admin_role_state
from cdf_merge_live_checks import verify_result, verify_semantics, bind_evidence, file_ref
from package_binding import capture_sources, canonical_digest


def start_process(argv, environment, output):
    output.mkdir(mode=0o700)
    save(output/'command.json',{'argv':list(map(str,argv)),'started':time.time(),'identity':environment.get('SPARK_USER')})
    log=(output/'process.private.log').open('wb')
    process=subprocess.Popen(list(map(str,argv)),cwd=ROOT,env=environment,stdout=log,stderr=subprocess.STDOUT)
    save(output/'process.json',{'pid':process.pid,'started':time.time()})
    return process,log


def finish_process(process,log,output,terminate=False):
    if terminate and process.poll() is None:process.terminate()
    try:code=process.wait(timeout=45)
    except subprocess.TimeoutExpired:process.kill();code=process.wait(timeout=10)
    log.close()
    if not (output/'exit.json').exists():save(output/'exit.json',{'pid':process.pid,'exitCode':code,'terminated':terminate,'finished':time.time()})
    return code


def await_file(path,process,seconds=300):
    deadline=time.monotonic()+seconds
    while not path.exists():
        assert process.poll() is None and time.monotonic()<deadline,'Original child failed/timed out before '+path.name+'; no replay'
        time.sleep(.05)
    return read(path)


class NativeObserver:
    """A separate setup-identity Spark process; its credentials never enter application inputs."""
    def __init__(self,plan,runtime,output):
        output.mkdir(mode=0o700);self.output=output;self.cursor=0
        data={k:plan[k] for k in ['primary','tables']};data.update(partitioned=plan.get('partitioned',False),spark=runtime['spark'],delta=runtime['delta'],output=str(output),nativeIdentity=plan['serviceAdmin'])
        save(output/'inputs.private.json',data)
        env=clean_environment(plan);env.update(SPARK_HOME=runtime['sparkHome'],PYSPARK_PYTHON=plan['python'],SPARK_USER=plan['serviceAdmin'],HADOOP_USER_NAME=plan['serviceAdmin'])
        self.process,self.log=start_process([runtime['sparkHome']+'/bin/spark-submit','--driver-memory','3g','--master','local[2]',ROOT/'delta/acceptance/cdf_merge_native.py',output/'inputs.private.json'],env,output/'process')
        try:self.initial=await_file(output/'ready.json',self.process,500)
        except BaseException:
            finish_process(self.process,self.log,output/'process',self.process.poll() is None);raise
        assert all(runtime['jarHashes'].get(v['origin'])==v['sha256'] for v in self.initial['origins'].values())
    def call(self,mode='inspect',**args):
        number=self.cursor;self.cursor+=1;save(self.output/(str(number)+'-request.json'),dict(mode=mode,**args))
        return await_file(self.output/(str(number)+'-result.json'),self.process)
    def close(self):
        if self.process.poll() is None:save(self.output/(str(self.cursor)+'-request.json'),{'mode':'stop'})
        return finish_process(self.process,self.log,self.output/'process')


def application_role(ranger,plan):
    """Find the unique owned application role across complete Ranger 2.8 result pages."""
    offset=0;total=None;roles=[];ids=set()
    while total is None or offset<total:
        page=ranger.call('GET','/service/roles/roles?serviceName='+plan['resourceService']+'&startIndex='+str(offset)+'&pageSize=200')
        assert all(type(page[key]) is int for key in ['startIndex','pageSize','totalCount','resultSize'])
        assert page['startIndex']==offset and 0<page['pageSize']<=200 and page['totalCount']>=0
        if total is None:total=page['totalCount']
        else:assert page['totalCount']==total,'Role pagination changed while inspecting fixture'
        current=page['roles'];assert isinstance(current,list)
        assert page['resultSize']==len(current)==min(page['pageSize'],total-offset),'Incomplete or stalled role page'
        for role in current:
            assert type(role['id']) is int and role['id'] not in ids and isinstance(role['name'],str)
            ids.add(role['id']);roles.append(role)
        offset+=len(current)
        if offset==total:break
    assert offset==total
    matches=[role['name'] for role in roles if role['name'].endswith('_ROLE_'+plan['appRole'])]
    assert len(matches)==1,'Expected one owned application role across all pages'
    return matches[0]


def provision(runtime,inputs,output,association,handles,partitioned=False):
    output.mkdir(mode=0o700)
    for child in ['private','public-trace','ranger-trace']: (output/child).mkdir()
    endpoints=read(inputs['endpoints'])
    admin=envfile(inputs['rangerAdminFile'])
    nonce=uuid.uuid4().hex[:12]
    plan={'partitioned':partitioned,'nonce':nonce,'subject':'cdf_app_'+nonce,'metadataPrincipal':'cdf_meta_'+nonce,'serviceAdmin':'cdf_setup_'+nonce,
        'metalake':'cdf_lake_'+nonce,'catalog':'cdf_catalog','database':'shared','resourceService':'cdf_service_'+nonce,'resourceDefinition':'cdf_def_'+nonce,
        'appRole':'cdf_query_'+nonce,'metaRole':'cdf_metadata_'+nonce,'prefix':'cdf-live-'+nonce,'seedAppId':'cdf-seed-'+nonce,
        'javaHome':JAVA,'python':inputs['python'],'rangerUrl':endpoints['ranger-admin'],
        'primary':{'endpoint':endpoints['minio'],'region':'us-east-1','bucket':'delta-fixtures','credentialsFile':inputs['minioCredentialsFile']},
        'rangerAdmin':{'username':admin['RANGER_ADMIN_USER'],'password':admin['RANGER_ADMIN_PASSWORD']},
        'plugin':{'username':'cdf_download_'+nonce,'password':'Aa1!'+secrets.token_urlsafe(24)},'fixtureDirectory':str(output),
        'deployedRangerEvidence':str(association),'deployedRangerEvidenceSha256':sha(association)}
    names=['source','same','incomplete','target','regrant_target','python_target','post_target','ambiguous_target']
    if partitioned:names=['source','same','target']
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
    observer=NativeObserver(plan,runtime,output/'native');handles['observer']=observer
    plan['nativeInitial']=observer.initial
    receipts={}
    for table in plan['tables']:
        receipt=submit(rest,base,plan['catalog'],table['name'],command('ADOPT',token=str(uuid.uuid4()),location=table['location'],runId=str(uuid.uuid4()),reference='owned-native-seed-exit-zero'))
        receipts[table['name']]=receipt;save(output/'receipts.private.json',receipts)
        receipts[table['name']]=submit(rest,base,plan['catalog'],table['name'],command('PUBLISH',expected=identity(receipt)))
        assert receipts[table['name']][RECEIPT+'state']=='PUBLISHED';save(output/'receipts.private.json',receipts)
    def obj(kind,name,priv):return {'type':kind,'fullName':name,'privileges':[{'name':priv,'condition':'ALLOW'}]}
    use=[obj('CATALOG',plan['catalog'],'USE_CATALOG'),obj('SCHEMA',plan['catalog']+'.'+plan['database'],'USE_SCHEMA')]
    for role,user,selected in [(plan['appRole'],plan['subject'],[n for n in names if n not in ['source','same','incomplete']]),(plan['metaRole'],plan['metadataPrincipal'],names)]:
        rest.call('POST',base+'/users',{'name':user})
        rest.call('POST',base+'/roles',{'name':role,'properties':{},'securableObjects':use+[obj('TABLE',plan['catalog']+'.'+plan['database']+'.'+name,'SELECT_TABLE') for name in selected]})
        rest.call('PUT',base+'/permissions/users/'+quote(user,safe='')+'/grant/',{'roleNames':[role]})
    app=application_role(ranger,plan);plan['rangerAppRole']=app
    for table in names:
        source=table in ['source','same','incomplete']
        masks=[('id','{col} + CAST(100 AS BIGINT)'),('secret',"'MASKED'" if source else "'TARGET_MASK'"),('_commit_version','CAST(0 AS BIGINT)'),('_commit_timestamp',"TIMESTAMP '2000-01-01 00:00:00'"),(None,"region = 'allowed'")]
        if partitioned and source:masks.append(('part',"'moved/= %'"))
        for column,expression in masks:
            if table not in ['source','same','incomplete'] and column in ['_commit_version','_commit_timestamp']:continue
            resources=source_resources(1 if column else 2,plan);resources['table']['values']=[table]
            if column:resources['column']['values']=[column]
            item={'roles':[app],'users':[],'groups':[],'accesses':[{'type':'select','isAllowed':True}]}
            item['dataMaskInfo' if column else 'rowFilterInfo']=({'dataMaskType':'CUSTOM','valueExpr':expression} if column else {'filterExpr':expression})
            created=create('mask' if column else 'filter','/service/public/v2/api/policy',{'service':plan['resourceService'],
                'name':'merge_'+table+'_'+(column or 'filter')+'_'+nonce,'policyType':1 if column else 2,'isEnabled':True,'isAuditEnabled':True,
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



def state_snapshot(plan,output):
    output.mkdir(mode=0o700)
    rest=Public(plan['gravitinoUrl'],plan['serviceAdmin'],output/'public-trace');ranger=Ranger(plan,output/'ranger-trace')
    base='/api/metalakes/'+plan['metalake']
    public={key:rest.call('GET',base+path)['role' if 'Role' in key else 'user'] for key,path in [('appRole','/roles/'+plan['appRole']),('metadataRole','/roles/'+plan['metaRole']),('appUser','/users/'+plan['subject']),('metadataUser','/users/'+plan['metadataPrincipal'])]}
    assert public['appUser']['roles']==[plan['appRole']] and public['metadataUser']['roles']==[plan['metaRole']]
    expected={plan['catalog']:{('USE_CATALOG','ALLOW')},plan['catalog']+'.'+plan['database']:{('USE_SCHEMA','ALLOW')}}
    expected.update({plan['catalog']+'.'+plan['database']+'.'+name:{(v,'ALLOW') for v in privileges} for name,privileges in plan['grants'].items() if privileges})
    actual={o['fullName']:canonical_privileges(o['privileges']) for o in public['appRole']['securableObjects'] if o['privileges']}
    assert actual==expected,'Public application privileges differ'
    policies=ranger.call('GET','/service/plugins/secure/policies/download/'+plan['resourceService']+'?lastKnownVersion=-1&pluginId=merge-supervisor',download=True)
    roles=ranger.call('GET','/service/roles/secure/download/'+plan['resourceService']+'?lastKnownRoleVersion=-1&pluginId=merge-supervisor',download=True)
    assert policies['serviceName']==roles['serviceName']==plan['resourceService']
    for table,privileges in plan['grants'].items():
        expected_access=({'select'} if 'SELECT_TABLE' in privileges else set())|({'update','alter'} if 'MODIFY_TABLE' in privileges else set())
        assert exact_access(policies['policies'],plan['catalog'],plan['database'],table,plan['rangerAppRole'])==expected_access,'Downloaded table accesses differ'
    save(output/'public-state.private.json',public);save(output/'ranger-state.private.json',{'policies':policies,'roles':roles})
    descriptor={'publicStateFile':str(output/'public-state.private.json'),'rangerStateFile':str(output/'ranger-state.private.json'),'policy':policies['policyVersion'],'role':roles['roleVersion']}
    save(output/'state-evidence.private.json',descriptor);return descriptor


def privileges(plan,output,select=None,modify=None,only=None):
    output.mkdir(mode=0o700)
    before=state_snapshot(plan,output/'before')
    rest=Public(plan['gravitinoUrl'],plan['serviceAdmin'],output/'public-trace')
    callbacks=Path(plan['fixtureDirectory'])/'callbacks.private.jsonl';cursor=len(callbacks.read_text().splitlines())
    mutations=[]
    for table in plan['grants']:
        if only and table!=only:continue
        for privilege,allowed,relevant in [('SELECT_TABLE',select,table in ['source','same','incomplete']),('MODIFY_TABLE',modify,table not in ['source','incomplete'])]:
            if allowed is None or not relevant or ((privilege in plan['grants'][table])==allowed):continue
            full=plan['catalog']+'.'+plan['database']+'.'+table
            response=rest.call('PUT','/api/metalakes/'+plan['metalake']+'/permissions/roles/'+plan['appRole']+'/table/'+full+('/grant/' if allowed else '/revoke/'),{'privileges':[{'name':privilege,'condition':'ALLOW'}]})
            if allowed:plan['grants'][table].append(privilege)
            else:plan['grants'][table].remove(privilege)
            mutations.append({'table':table,'privilege':privilege,'grant':allowed,'code':response['code']})
    after=state_snapshot(plan,output/'after')
    traces=[json.loads(line) for line in callbacks.read_text().splitlines()[cursor:]]
    assert mutations and after['policy']>before['policy'] and after['role']==before['role']
    assert any(t['method']=='PUT' and '/service/public/v2/api/policy/' in t['path'] for t in traces)
    save(output/'completed.json',{'before':before,'after':after,'mutations':mutations,'callbacks':traces})
    return after


def deny_policy(plan):
    """Exact fixture-only column UPDATE denial; this never creates a positive access grant."""
    resources={key:{'values':[value],'isExcludes':False,'isRecursive':False} for key,value in [('catalog',plan['catalog']),('database',plan['database']),('table','target'),('column','secret')]}
    return {'service':plan['resourceService'],'name':'merge-column-deny-'+plan['nonce'],'policyType':0,'isEnabled':True,'isAuditEnabled':True,'resources':resources,'denyPolicyItems':[{'roles':[plan['rangerAppRole']],'users':[],'groups':[],'accesses':[{'type':'update','isAllowed':True}]}]}


def receipt_identity(tables):
    """Extract independently observed public receipt identities for comparison with H2 state."""
    return {name:{'id':int(table['properties'][RECEIPT+'id']),'generation':int(table['properties'][RECEIPT+'generation']),'token':table['properties'][RECEIPT+'token'],'nativeVersion':int(table['properties'][RECEIPT+'nativeVersion']),'uuid':table['properties'][RECEIPT+'nativeId']} for name,table in tables.items()}


def witness_label(label):
    return {'grant':'distinct-before','revoke':'post-helper','regrant':'regrant-before'}[label]


def column_deny(plan,output,enabled):
    output.mkdir(mode=0o700);ranger=Ranger(plan,output/'ranger-trace')
    if enabled:
        policy=ranger.call('POST','/service/public/v2/api/policy',deny_policy(plan))
        plan['denyPolicy']=policy['id']
    else:
        path='/service/public/v2/api/policy/'+str(plan['denyPolicy'])
        policy=ranger.call('GET',path);assert policy['name']=='merge-column-deny-'+plan['nonce'] and policy['isEnabled'] is True
        policy['isEnabled']=False
        disabled=ranger.call('PUT',path,policy);assert disabled['id']==policy['id'] and disabled['isEnabled'] is False
    return state_snapshot(plan,output/'state')


def merge_sql(plan,target,source='source',start=1,end=1,ambiguous=False):
    prefix=plan['catalog']+'.'+plan['database']+'.'
    predicate="_change_type <> 'update_preimage' AND _commit_version = 0"
    if ambiguous:predicate="id = 106 AND _change_type = 'insert'"
    sql=("MERGE INTO "+prefix+target+" t USING (SELECT * FROM table_changes('"+prefix+source+"', "+str(start)+", "+str(end)+") WHERE "+predicate+") s ON t.id=s.id "
        "WHEN MATCHED AND s._change_type='delete' THEN DELETE "
        "WHEN MATCHED THEN UPDATE SET secret=concat(s.secret, '|', t.secret), region=s.region "
        "WHEN NOT MATCHED AND s._change_type<>'delete' THEN INSERT (id,secret,region) VALUES(s.id,s.secret,s.region)")
    if plan.get('partitioned'):
        sql=sql.replace('region=s.region','region=s.region, part=s.part').replace('INSERT (id,secret,region) VALUES(s.id,s.secret,s.region)','INSERT (id,secret,region,part) VALUES(s.id,s.secret,s.region,s.part)')
    return sql


def steps(plan,python=False):
    specs=[('python','python_target','source',1,1),('pythonDenied','python_target','source',1,1)] if python else [
        ('distinct','target','source',1,1),('duplicate','target','source',1,1),('empty','target','source',2,2),
        ('ambiguous','ambiguous_target','source',0,0),('missing','target','incomplete',1,1),('columnDenied','target','source',1,1),
        ('postHelperRevoked','post_target','source',1,1),('selectDenied','target','source',1,1),('modifyDenied','target','source',1,1),
        ('regrant','regrant_target','source',1,1),('same','same','same',1,1)]
    if plan.get('partitioned'):
        assert python
        specs=[('distinct','target','source',1,1),('columnDenied','target','source',1,1),('selectDenied','target','source',1,1),('modifyDenied','target','source',1,1),('same','same','same',1,1)]
    result=[]
    for name,target,source,start,end in specs:
        step={'name':name,'target':target,'source':source,'end':end,'sourceId':plan['nativeInitial']['tables'][source]['id'],'sql':merge_sql(plan,target,source,start,end,name=='ambiguous')}
        if name in ['distinct','duplicate'] and not plan.get('partitioned'):step['txnAppId']='merge-'+plan['nonce']
        result.append(step)
    return result


def source_digest(plan,name):
    table=next(t for t in plan['tables'] if t['name']==name)
    return canonical_digest(OwnedObjects(plan['primary'],table['prefix']).manifest())


def registration(plan,output):
    output.mkdir(mode=0o700);rest=Public(plan['gravitinoUrl'],plan['serviceAdmin'],output/'public-trace')
    prefix='/api/metalakes/'+plan['metalake']+'/catalogs/'+plan['catalog']+'/schemas/'+plan['database']+'/tables/'
    rows={t['name']:rest.call('GET',prefix+t['name'])['table'] for t in plan['tables']}
    save(output/'tables.private.json',rows);return rows


def application(plan,runtime,output,observer,python=False):
    output.mkdir(mode=0o700);initial=state_snapshot(plan,output/'initial')
    request=prepare_request(plan,runtime,initial,output/'request');request.update(output=str(output/'app-evidence'),steps=steps(plan,python),partitioned=plan.get('partitioned',False))
    save(output/'request/request.private.json',request)
    env=clean_environment(plan);env.update(SPARK_HOME=runtime['sparkHome'],PYSPARK_PYTHON=plan['python'],SPARK_USER=plan['subject'],HADOOP_USER_NAME=plan['subject'])
    selected=[p for p in runtime['jarHashes'] if Path(p).name in runtime['addedJarNames']]
    argv=[runtime['sparkHome']+'/bin/spark-submit','--driver-memory','4g','--master','local[2]','--properties-file',request['sparkConfigurationFile'],'--driver-class-path',request['driverSecurityDirectory'],'--jars',','.join(selected)]
    argv+=([str(ROOT/'delta/acceptance/cdf_merge_pyspark.py')] if python else ['--class','org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerLiveCdfMerge',runtime['app']])
    argv+=[str(output/'request/request.private.json')]
    process,log=start_process(argv,env,output/'process');observations={};transition_evidence={}
    control=Path(request['controlDirectory']);app=output/'app-evidence';current_state=initial;current_witness='initial'
    def release(label,state=None):
        value={'verified':True}
        if state:value['stateEvidenceFile']=str(Path(state['publicStateFile']).parent/'state-evidence.private.json')
        save(control/(label+'-verified.json'),value)
    try:
        for step in request['steps']:
            name=step['name'];await_file(control/(name+'-before-ready.json'),process)
            state=None
            if name=='python':state=privileges(plan,output/'grant',True,True)
            if name=='pythonDenied':state=privileges(plan,output/'revoke',modify=False,only='python_target')
            if name=='distinct' and plan.get('partitioned'):state=privileges(plan,output/'grant',select=True,modify=True)
            if name=='distinct' and not plan.get('partitioned'):state=privileges(plan,output/'grant',modify=True,only='python_target');transition_evidence['grant']=state
            if name=='missing':observer.call('damage')
            if name=='columnDenied':state=column_deny(plan,output/'column-deny',True)
            if name=='postHelperRevoked':state=column_deny(plan,output/'column-restore',False)
            if name=='selectDenied' and plan.get('partitioned'):
                column_deny(plan,output/'column-restore',False)
                state=privileges(plan,output/'select-revoke',select=False)
            if name=='same' and plan.get('partitioned'):state=privileges(plan,output/'same-regrant',modify=True)
            if name=='selectDenied' and not plan.get('partitioned'):state=privileges(plan,output/'modify-regrant',modify=True)
            if name=='modifyDenied':state=privileges(plan,output/'select-regrant-modify-revoke',select=True,modify=False)
            if name=='regrant':state=privileges(plan,output/'regrant',modify=True);transition_evidence['regrant']=state
            before=observer.call(table=step['target']);source_before=source_digest(plan,step['source'])
            native_before=observer.output/(str(observer.cursor-1)+'-result.json')
            release(name+'-before',state)
            if state:current_state=state;current_witness=name+'-before'
            if name=='postHelperRevoked':
                await_file(control/'post-helper-ready.json',process)
                held=read(app/'post-helper-held.json');during=observer.call(table=step['target'],after=before['version'])
                assert during['version']==before['version'] and not during['actions']
                state=privileges(plan,output/'revoke',False,False);transition_evidence['revoke']=state
                save(output/'post-helper.json',dict(held,publishedBeforeRelease=False,nativeDuring=during))
                release('post-helper',state);current_state=state;current_witness='post-helper'
            await_file(control/(name+'-after-ready.json'),process)
            after=observer.call(table=step['target'],after=before['version'])
            outcome=read(app/(name+'-outcome.json'))
            cp_before=read(app/(name+'-checkpoint-before.json'));cp_after=read(app/(name+'-checkpoint-after.json'))
            assert cp_before['sourceId']==cp_after['sourceId']==step['sourceId']
            value={'before':before,'after':after,'cdc':after['cdc'],'checkpointBefore':cp_before['end'],'checkpointAfter':cp_after['end'],'sql':step['sql'],'sourceBefore':source_before,'sourceAfter':source_digest(plan,step['source']),**outcome}
            if plan.get('partitioned'):
                value['nativeBefore']=file_ref(native_before);value['nativeAfter']=file_ref(observer.output/(str(observer.cursor-1)+'-result.json'))
                value['consumerBefore']=file_ref(app/(name+'-checkpoint-before.json'));value['consumerAfter']=file_ref(app/(name+'-checkpoint-after.json'))
            downloaded=read(current_state['rangerStateFile'])['policies']['policies'];witness=read(app/(current_witness+'-applied.json'))
            value['authorization']={'sourceAccess':sorted(exact_access(downloaded,plan['catalog'],plan['database'],step['source'],plan['rangerAppRole'])),'targetAccess':sorted(exact_access(downloaded,plan['catalog'],plan['database'],step['target'],plan['rangerAppRole'])),'policy':witness['policy'],'downloadSha256':witness['downloadSha256']}
            if step.get('txnAppId'):value['txnAppId']=step['txnAppId']
            observations[name]=value;save(output/(name+'-result.json'),value)
            expected_success=name in ['python','distinct','duplicate','empty','regrant','same']
            assert (outcome['outcome']=='success')==expected_success,'Unexpected original '+name+' outcome; no replay'
            release(name+'-after')
        code=finish_process(process,log,output/'process');assert code==0
        origins=read(app/'origins.json');assert set(origins)==set(runtime['providers'])
        for name,entry in origins.items():
            assert entry['jar']==runtime['providers'][name] and entry['jarSha256']==runtime['jarHashes'][entry['jar']] and entry['classSha256']==runtime['providerClasses'][name]
        for step in request['steps']:
            identity=read(app/(step['name']+'-identity.json'));assert identity['subject']==identity['ugi']==plan['subject']
        metadata=read(app/'metadata-identity-final.json');assert metadata['principal']==plan['metadataPrincipal']
        observed=read(app/'observed.json');assert observed['subject']==observed['ugi']==plan['subject']
        for label,state in transition_evidence.items():
            witness=read(app/(witness_label(label)+'-applied.json'))
            assert witness['policy']==state['policy'] and witness['role']==state['role'] and witness['stateSha256']==sha(Path(state['publicStateFile']).parent/'state-evidence.private.json')
            transition_evidence[label]={'policy':state['policy'],'downloadSha256':witness['downloadSha256'],'privileges':['SELECT_TABLE','MODIFY_TABLE'],'select':label!='revoke','modify':label!='revoke'}
        return observations,transition_evidence,observed,code
    finally:
        if not (output/'process/exit.json').exists():finish_process(process,log,output/'process',process.poll() is None)


def run_cell(runtime_path,inputs_path,output,partitioned=False):
    output.mkdir(mode=0o700);runtime=read(runtime_path);runtime['manifestPath']=str(runtime_path);inputs=read(inputs_path)
    from cdf_read_live_package import verify_runtime
    verify_runtime(runtime);handles={}
    try:
        association=verify_deployed(inputs,output/'deployed-ranger')
        plan,server,callbacks=provision(runtime,inputs,output/'fixture',association,handles,partitioned)
        plan['grants']={t['name']:([] if t['name'] in ['source','same','incomplete'] else ['SELECT_TABLE']) for t in plan['tables']}
        observer=handles['observer']
        py,_,py_observed,py_exit=application(plan,runtime,output/'python',observer,True)
        if partitioned:scala={};transitions={};observed=py_observed;child_exit=py_exit
        else:scala,transitions,observed,child_exit=application(plan,runtime,output/'scala',observer)
        before=registration(plan,output/'registration-before-restart');server.stop()
        from cdf_provider import store_rows
        persistence=output/'persistence';persistence.mkdir()
        persisted=store_rows(server,persistence,'tables',"SELECT t.table_name,t.table_id,t.current_version,v.properties FROM table_meta t JOIN table_version_info v ON t.table_id=v.table_id AND t.current_version=v.version WHERE t.deleted_at=0")
        stored={}
        for row in persisted:
            state=json.loads(json.loads(row['PROPERTIES'])['delta.registration.internal.state']);name=row['TABLE_NAME'];receipt=before[name]['properties']
            assert state['state']=='PUBLISHED'
            for key in ['id','generation','token']:assert str(state[key])==receipt[RECEIPT+key]
            assert str(state['observation']['version'])==receipt[RECEIPT+'nativeVersion']
            stored[name]={'id':state['id'],'generation':state['generation'],'token':state['token'],'nativeVersion':state['observation']['version'],'uuid':state['observation']['uuid']}
        columns=server.column_snapshot('merge');assert len(columns)==(4 if partitioned else 3)*len(plan['tables'])
        server.start();plan['gravitinoUrl']=server.uri;after=registration(plan,output/'registration-after-restart');assert before==after
        server.stop();assert columns==server.column_snapshot('merge-restart')
        native_exit=observer.close();assert native_exit==0;handles.pop('observer')
        result={'profile':'partitioned' if partitioned else 'legacy',**{k:observed[k] for k in ['spark','java','scala']},'delta':runtime['delta'],'ranger':read(association)['deployedRangerVersion'],
            'exits':({'outer':0,'native':native_exit,'python':py_exit} if partitioned else {'outer':0,'child':child_exit,'python':py_exit}),'identities':{'setup':plan['serviceAdmin'],'metadata':plan['metadataPrincipal'],'application':plan['subject']},
            'origins':{k:{'jar':v['origin'],'sha256':v['sha256']} for k,v in plan['nativeInitial']['origins'].items()},
            'binding':{'source':runtime['sourceInputDigest'],'manifest':sha(runtime_path),'companion':sha(runtime['changedRuntime']),'app':sha(runtime['app']),'python':sha(ROOT/'delta/acceptance/cdf_merge_pyspark.py')},
            'nativeInitial':plan['nativeInitial']['tables'],'sourceNative':plan['nativeInitial']['sourceNative'],'sourceGoverned':read(output/('python' if partitioned else 'scala')/'app-evidence/source-governed.json'),
            'stages':dict(scala,**py),'transitions':transitions,'registration':{'beforeRestart':receipt_identity(before),'afterRestart':receipt_identity(after),'persisted':stored},'postHelper':None if partitioned else read(output/'scala/post-helper.json')}
        verify_runtime(runtime)
        save(output/'candidate-result.json',result)
        # Original outer exit is supplied by the parent process after this child returns.
        verify_semantics(result)
        return result
    except BaseException as error:
        save(output/'failure.json',{'class':type(error).__name__,'message':str(error),'noReplay':True});raise
    finally:
        if handles.get('observer'):handles['observer'].close()
        if handles.get('server'):handles['server'].stop()
        if handles.get('callbacks'):handles['callbacks'].wait_drained(20);handles['callbacks'].__exit__()
        save(output/'lane-release.json',{'released':True,'fixturesRetained':True,'cleanup':'Owned processes stopped; isolated policies/objects retained for evidence'})


def main():
    parser=argparse.ArgumentParser()
    for key in ['runtime','inputs','out']:parser.add_argument('--'+key,required=True)
    parser.add_argument('--partitioned',action='store_true');parser.add_argument('--child',action='store_true');args=parser.parse_args();os.umask(0o077)
    runtime=Path(args.runtime).resolve();inputs=Path(args.inputs).resolve();output=Path(args.out).resolve()
    if args.child:run_cell(runtime,inputs,output,args.partitioned);return
    process_dir=output.parent/(output.name+'-outer')
    argv=[sys.executable,__file__,'--child','--runtime',str(runtime),'--inputs',str(inputs),'--out',str(output)]
    if args.partitioned:argv.append('--partitioned')
    process,log=start_process(argv,dict(os.environ),process_dir)
    try:code=process.wait(timeout=2400)
    except subprocess.TimeoutExpired:code=finish_process(process,log,process_dir,True);raise
    finish_process(process,log,process_dir);assert code==0,'Original pipeline process failed; no replay'
    result=bind_evidence(read(output/'candidate-result.json'),output,runtime);verify_result(result);save(output/'result.json',result)


if __name__=='__main__':main()

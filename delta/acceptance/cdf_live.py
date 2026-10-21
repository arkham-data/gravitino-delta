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

"""Fresh owned discovered CDF lane using historical fixture transport primitives.

Every mutation has a single original invocation. Failed/uncertain states are retained.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import sys
import time
import uuid
import xml.etree.ElementTree as ET
from urllib.parse import quote

ROOT=Path(__file__).resolve().parents[2]
H=ROOT/'.superpowers/sdd/2026-10-14-delta-native-governed-append/public-grants-live-harness'
sys.path.insert(0,str(H))
from live_common import (Public as TransportPublic,Ranger as TransportRanger,Server,RecordingProxy,OwnedObjects,envfile,save,sha,clean_environment,
                         catalog_properties,submit,command,identity,RECEIPT,source_resources,credentials,write_logging_configuration)
from cdf_live_checks import exact_access,project_modify,cdf_rows,diagnostic_update,canonical_privileges,admin_role_state,canonical_owner_type,canonical_protocol,cdf_configuration
from evidence_checks import verify_transition_documents,source_policy_digest
JAVA='/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home'


class Public(TransportPublic):
    """Reuse exact public transport with a new private trace directory."""
    def __init__(self, uri, actor, evidence):
        Path(evidence).mkdir(mode=0o700,parents=True,exist_ok=True)
        super().__init__(uri,actor,evidence)


class Ranger(TransportRanger):
    """Reuse exact Ranger transport with a new private trace directory."""
    def __init__(self, plan, output):
        Path(output).mkdir(mode=0o700,parents=True,exist_ok=True)
        super().__init__(plan,output)


def read(path): return json.loads(Path(path).read_text())


def launch(argv,environment,output,timeout=600):
    output=Path(output);output.mkdir(mode=0o700)
    save(output/'command.json',{'argv':list(map(str,argv)),'started':time.time(),
        'environmentAllowlist':{k:environment[k] for k in ['JAVA_HOME','SPARK_HOME','SPARK_USER','HADOOP_USER_NAME'] if k in environment}})
    with (output/'process.private.log').open('wb') as log:
        process=subprocess.Popen(list(map(str,argv)),env=environment,cwd=ROOT,stdout=log,stderr=subprocess.STDOUT)
        save(output/'process.json',{'pid':process.pid,'started':time.time()})
        timed=False
        try: code=process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed=True;process.terminate()
            try:code=process.wait(timeout=20)
            except subprocess.TimeoutExpired:process.kill();code=process.wait(timeout=10)
    save(output/'exit.json',{'pid':process.pid,'exitCode':code,'timedOut':timed,'finished':time.time()})
    assert code==0 and not timed, 'Original process failed; retained; no replay'


def native(plan,runtime,mode,output,intervals=None):
    output=Path(output);output.mkdir(mode=0o700)
    data={'primary':plan['primary'],'spark':runtime['spark'],'delta':runtime['delta'],'tables':plan['tables'],
          'mode':mode,'output':str(output),'seedAppId':plan['seedAppId'],'nativeIdentity':plan['serviceAdmin'],'intervals':intervals or []}
    save(output/'inputs.private.json',data)
    environment=clean_environment(plan)
    environment.update(SPARK_HOME=runtime['sparkHome'],PYSPARK_PYTHON=plan['python'],SPARK_USER=plan['serviceAdmin'],HADOOP_USER_NAME=plan['serviceAdmin'])
    launch([runtime['sparkHome']+'/bin/spark-submit','--driver-memory','2g','--master','local[2]',ROOT/'delta/acceptance/cdf_live_native.py',output/'inputs.private.json'],environment,output/'process')
    return read(output/'result.json')


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
    names=['source','cdf_target','enabled_diag','update_diag','denied_diag']
    plan['tables']=[{'name':name,'prefix':plan['prefix']+'/adoption/'+name+'/',
        'location':'s3a://delta-fixtures/'+plan['prefix']+'/adoption/'+name,
        'rows':[[1,'alpha','allowed'],[1,'alpha','allowed'],[2,'beta','blocked'],[3,'gamma','allowed']] if name=='source' else [[0,'sentinel','seed']],
        'cdf':name in ['enabled_diag','update_diag']} for name in names]
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
    receipts={}
    for table in plan['tables']:
        receipt=submit(rest,base,plan['catalog'],table['name'],command('ADOPT',token=str(uuid.uuid4()),location=table['location'],runId=str(uuid.uuid4()),reference='owned-native-seed-exit-zero'))
        receipts[table['name']]=receipt;save(output/'receipts.private.json',receipts)
        receipts[table['name']]=submit(rest,base,plan['catalog'],table['name'],command('PUBLISH',expected=identity(receipt)))
        assert receipts[table['name']][RECEIPT+'state']=='PUBLISHED';save(output/'receipts.private.json',receipts)
    def obj(kind,name,priv):return {'type':kind,'fullName':name,'privileges':[{'name':priv,'condition':'ALLOW'}]}
    use=[obj('CATALOG',plan['catalog'],'USE_CATALOG'),obj('SCHEMA',plan['catalog']+'.'+plan['database'],'USE_SCHEMA')]
    for role,user,selected in [(plan['appRole'],plan['subject'],names[:3]),(plan['metaRole'],plan['metadataPrincipal'],names)]:
        rest.call('POST',base+'/users',{'name':user})
        rest.call('POST',base+'/roles',{'name':role,'properties':{},'securableObjects':use+[obj('TABLE',plan['catalog']+'.'+plan['database']+'.'+name,'SELECT_TABLE') for name in selected]})
        rest.call('PUT',base+'/permissions/users/'+quote(user,safe='')+'/grant/',{'roleNames':[role]})
    roles=ranger.call('GET','/service/roles/roles?serviceName='+plan['resourceService'])['roles']
    matches=[r for r in roles if r['name'].endswith('_ROLE_'+plan['appRole'])];assert len(matches)==1
    app=matches[0]['name'];plan['rangerAppRole']=app
    for kind,policy_type in [('mask',1),('filter',2)]:
        item={'roles':[app],'users':[],'groups':[],'accesses':[{'type':'select','isAllowed':True}]}
        if kind=='mask':item['dataMaskInfo']={'dataMaskType':'CUSTOM','valueExpr':"'MASKED'"}
        else:item['rowFilterInfo']={'filterExpr':"region = 'allowed'"}
        created=create(kind,'/service/public/v2/api/policy',{'service':plan['resourceService'],'name':'cdf_source_'+kind+'_'+nonce,'policyType':policy_type,'isEnabled':True,'isAuditEnabled':True,
            'resources':source_resources(policy_type,plan),('dataMaskPolicyItems' if kind=='mask' else 'rowFilterPolicyItems'):[item]})
        plan.setdefault('sourcePolicyIds',[]).append(created['id'])
    resources=source_resources(2,plan);resources['table']['values']=['update_diag'];resources['column']={'values':['*'],'isExcludes':False,'isRecursive':False}
    downloaded=ranger.call('GET','/service/plugins/secure/policies/download/'+plan['resourceService']+'?lastKnownVersion=-1&pluginId=cdf-diagnostic-setup',download=True)
    matches=[p for p in downloaded['policies'] if p.get('policyType',0)==0 and p['resources']==resources]
    assert len(matches)==1
    diagnostic=ranger.call('GET','/service/public/v2/api/policy/'+str(matches[0]['id']))
    save(output/'diagnostic-before.private.json',diagnostic)
    changed=diagnostic_update(diagnostic,app)
    result=ranger.call('PUT','/service/public/v2/api/policy/'+str(diagnostic['id']),changed)
    save(output/'diagnostic-after.private.json',result)
    assert result['id']==diagnostic['id'] and result['resources']==diagnostic['resources'] and result['policyLabels']==diagnostic['policyLabels']
    assert result['policyItems'][:-1]==diagnostic['policyItems']
    owned.append({'kind':'owned-legacy-update-diagnostic','id':result['id'],'name':result['name'],'explicitFixtureRoleTransition':True})
    save(output/'ranger-owned.json',owned)
    plan['diagnosticPolicyId']=diagnostic['id']
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


def snapshot_state(plan,granted,output):
    output.mkdir(mode=0o700)
    rest=Public(plan['gravitinoUrl'],plan['serviceAdmin'],output/'public-trace');ranger=Ranger(plan,output/'ranger-trace')
    base='/api/metalakes/'+plan['metalake']
    public={key:rest.call('GET',base+path)['role' if 'Role' in key else 'user'] for key,path in [
        ('appRole','/roles/'+plan['appRole']),('metadataRole','/roles/'+plan['metaRole']),('appUser','/users/'+plan['subject']),('metadataUser','/users/'+plan['metadataPrincipal'])]}
    assert len({plan[k] for k in ['subject','metadataPrincipal','serviceAdmin']})==3
    assert public['appUser']['roles']==[plan['appRole']] and public['metadataUser']['roles']==[plan['metaRole']]
    for role in [public['appRole'],public['metadataRole']]:
        objects=role['securableObjects'];assert len({o['fullName'] for o in objects})==len(objects)
        for obj in objects:
            canonical_privileges(obj['privileges'])
    full=plan['catalog']+'.'+plan['database']+'.cdf_target'
    selected=[o for o in public['appRole']['securableObjects'] if o['fullName']==full];assert len(selected)==1
    expected={'SELECT_TABLE'}|({'MODIFY_TABLE'} if granted else set())
    assert canonical_privileges(selected[0]['privileges'])=={(x,'ALLOW') for x in expected}
    roles=ranger.call('GET','/service/roles/roles?serviceName='+plan['resourceService'])
    assert all(not role.get('roles') and not role.get('groups') for role in roles['roles']), 'Nested/group role bypass'
    actual=[r for r in roles['roles'] if any(u['name']==plan['subject'] for u in r.get('users',[]))]
    assert len(actual)==1 and actual[0]['name']==plan['rangerAppRole']
    assert actual[0].get('users')==[{'name':plan['subject'],'isAdmin':False}] and not actual[0].get('groups') and not actual[0].get('roles')
    metadata=[r for r in roles['roles'] if any(u['name']==plan['metadataPrincipal'] for u in r.get('users',[]))]
    assert len(metadata)==1 and metadata[0]['name'].endswith('_ROLE_'+plan['metaRole'])
    assert metadata[0]['users']==[{'name':plan['metadataPrincipal'],'isAdmin':False}] and not metadata[0].get('groups') and not metadata[0].get('roles')
    policies=ranger.call('GET','/service/plugins/secure/policies/download/'+plan['resourceService']+'?lastKnownVersion=-1&pluginId=cdf-supervisor',download=True)
    downloaded=ranger.call('GET','/service/roles/secure/download/'+plan['resourceService']+'?lastKnownRoleVersion=-1&pluginId=cdf-supervisor',download=True)
    assert policies['serviceName']==downloaded['serviceName']==plan['resourceService']
    for policy in policies['policies']:
        if policy.get('policyType',0)!=0:continue
        for item in policy.get('policyItems',[]):
            assert not {plan['subject'],plan['metadataPrincipal']}.intersection(item.get('users',[])), 'Direct subject grant bypass'
            assert not item.get('groups'), 'Unqualified group grant bypass'
            accesses={x['type'] for x in item.get('accesses',[]) if x.get('isAllowed')}
            if not accesses.intersection({'select','update','alter','all'}) or not {plan['rangerAppRole'],metadata[0]['name']}.intersection(item.get('roles',[])): continue
            resources=policy['resources'];assert set(resources)=={'catalog','database','table','column'}
            assert all(not r.get('isExcludes',False) and not r.get('isRecursive',False) for r in resources.values())
            assert resources['catalog']['values']==[plan['catalog']] and resources['database']['values']==[plan['database']] and resources['column']['values']==['*']
            assert resources['table']['values'] in [[t['name']] for t in plan['tables']]
            if policy['id']==plan['diagnosticPolicyId'] and plan['rangerAppRole'] in item.get('roles',[]):
                assert resources['table']['values']==['update_diag'] and accesses=={'update'} and item['roles']==[plan['rangerAppRole']]
            else:
                labels=policy.get('policyLabels',[])
                assert len(labels)==3 and 'gravitino-managed' in labels
                assert sum(x.startswith('gravitino-metalake-id:') for x in labels)==1 and sum(x.startswith('gravitino-catalog-id:') for x in labels)==1
    app=plan['rangerAppRole']
    for table,expected in [('source',{'select'}),('cdf_target',{'select','update','alter'} if granted else {'select'}),('enabled_diag',{'select'}),('update_diag',{'update'}),('denied_diag',set())]:
        assert exact_access(policies['policies'],plan['catalog'],plan['database'],table,app)==expected, 'App exact privilege mismatch: '+table
    for table in plan['tables']:
        assert exact_access(policies['policies'],plan['catalog'],plan['database'],table['name'],metadata[0]['name'])=={'select'}
    source=[p for p in policies['policies'] if p['id'] in plan['sourcePolicyIds']];assert {p['policyType'] for p in source}=={1,2}
    for p in source:
        assert p['resources']==source_resources(p['policyType'],plan)
        items=p.get('dataMaskPolicyItems',p.get('rowFilterPolicyItems',[]));assert len(items)==1 and items[0]['roles']==[app]
        if p['policyType']==1:assert items[0]['dataMaskInfo']['valueExpr']=="'MASKED'"
        else:assert items[0]['rowFilterInfo']['filterExpr']=="region = 'allowed'"
    state={'policies':policies,'roles':downloaded,'adminRoles':roles,'sourcePolicies':source}
    save(output/'public-state.private.json',public);save(output/'ranger-state.private.json',state)
    descriptor={'publicStateFile':str(output/'public-state.private.json'),'rangerStateFile':str(output/'ranger-state.private.json'),
        'policy':policies['policyVersion'],'role':downloaded['roleVersion'],'updateAbsent':not granted,'alterAbsent':not granted,'sourcePolicySha256':source_policy_digest(source)}
    save(output/'state-evidence.private.json',descriptor)
    return descriptor


def modify(plan,grant,output):
    output.mkdir(mode=0o700)
    before=snapshot_state(plan,not grant,output/'before')
    callback_path=Path(plan['fixtureDirectory'])/'callbacks.private.jsonl'
    existing=callback_path.read_text().splitlines()
    save(output/'intent.json',{'grant':grant,'target':'cdf_target','callbackCursor':len(existing),'startedNs':time.time_ns(),'replayAllowed':False})
    rest=Public(plan['gravitinoUrl'],plan['serviceAdmin'],output/'public-trace')
    full=plan['catalog']+'.'+plan['database']+'.cdf_target'
    path='/api/metalakes/'+plan['metalake']+'/permissions/roles/'+plan['appRole']+'/table/'+full+('/grant/' if grant else '/revoke/')
    response=rest.call('PUT',path,{'privileges':[{'name':'MODIFY_TABLE','condition':'ALLOW'}]})
    callbacks=[json.loads(line) for line in callback_path.read_text().splitlines()[len(existing):]]
    save(output/'callbacks.private.json',callbacks)
    after=snapshot_state(plan,grant,output/'after')
    old_public=read(before['publicStateFile']);new_public=read(after['publicStateFile'])
    verify_transition_documents(plan,'cdf_target',grant,before,after,old_public,new_public,callbacks)
    old=read(before['rangerStateFile']);new=read(after['rangerStateFile'])
    left,lid,lp=project_modify(old['policies']['policies'],plan['rangerAppRole'],'cdf_target')
    right,rid,rp=project_modify(new['policies']['policies'],plan['rangerAppRole'],'cdf_target')
    assert lid==rid and left==right and (lp,rp)==((False,True) if grant else (True,False)), 'Unrelated policy drift'
    assert old['roles'].get('rangerRoles')==new['roles'].get('rangerRoles') and admin_role_state(old['adminRoles'])==admin_role_state(new['adminRoles'])
    mutations=[c for c in callbacks if c['method']!='GET']
    assert mutations and all(c['method']=='PUT' and c['path'].split('?')[0].rstrip('/')=='/service/public/v2/api/policy/'+str(lid) for c in mutations)
    save(output/'completed.json',{'publicCode':response['code'],'grant':grant,'before':before,'after':after,'callbacksSha256':sha(output/'callbacks.private.json'),'finishedNs':time.time_ns()})
    return after


def prepare_request(plan,runtime,state,output):
    output.mkdir(mode=0o700)
    access,secret=credentials(plan['primary']['credentialsFile'])
    props={'spark.master':'local[2]','spark.ui.enabled':'false','spark.sql.shuffle.partitions':'2','spark.default.parallelism':'2','spark.sql.adaptive.enabled':'false',
        'spark.databricks.delta.snapshotPartitions':'2','spark.driver.host':'127.0.0.1','spark.driver.bindAddress':'127.0.0.1',
        'spark.driver.userClassPathFirst':'false','spark.executor.userClassPathFirst':'false','spark.databricks.delta.catalog.update.enabled':'false',
        'spark.plugins':'org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin','spark.gravitino.enableDeltaSupport':'true','spark.gravitino.enableDeltaRangerSupport':'true',
        'spark.gravitino.deltaRanger.service':plan['resourceService'],'spark.gravitino.deltaRanger.definition':plan['resourceDefinition'],
        'spark.gravitino.deltaRanger.metadata.config':str(Path(plan['fixtureDirectory'])/'private/metadata.properties'),
        'spark.sql.gravitino.authType':'simple','spark.sql.gravitino.metalake':plan['metalake'],'spark.sql.gravitino.uri':plan['gravitinoUrl'],
        'spark.sql.catalog.spark_catalog':'org.apache.spark.sql.delta.catalog.DeltaCatalog',
        'spark.sql.extensions':'org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerSparkExtension,io.delta.sql.DeltaSparkSessionExtension',
        'spark.sql.catalog.'+plan['catalog']+'.journal-directory':str(output/'journal'),
        'spark.hadoop.fs.s3a.impl':'org.apache.hadoop.fs.s3a.S3AFileSystem','spark.hadoop.fs.s3a.endpoint':plan['primary']['endpoint'],
        'spark.hadoop.fs.s3a.endpoint.region':plan['primary']['region'],'spark.hadoop.fs.s3a.path.style.access':'true','spark.hadoop.fs.s3a.connection.ssl.enabled':'false',
        'spark.hadoop.fs.s3a.aws.credentials.provider':'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider','spark.hadoop.fs.s3a.access.key':access,'spark.hadoop.fs.s3a.secret.key':secret,
        'spark.redaction.regex':'(?i)secret|password|token|access[.]?key|s3-access-key-id'}
    logging=write_logging_configuration(output)
    props['spark.driver.extraJavaOptions']='-Dlog4j.configurationFile='+logging;props['spark.executor.extraJavaOptions']='-Dlog4j.configurationFile='+logging
    (output/'spark.properties').write_text(''.join(k+'='+v+'\n' for k,v in sorted(props.items())))
    (output/'driver-security').mkdir();(output/'control').mkdir()
    request={k:plan[k] for k in ['subject','metadataPrincipal','metalake','catalog','database','resourceService','resourceDefinition']}
    request.update(schemaVersion=1,source='source',target='cdf_target',enabledDiagnostic='enabled_diag',updateDiagnostic='update_diag',deniedDiagnostic='denied_diag',
        sourceSecurityDirectory=plan['sourceSecurityDirectory'],driverSecurityDirectory=str(output/'driver-security'),sparkConfigurationFile=str(output/'spark.properties'),
        expectedSparkVersion=runtime['spark'],expectedDeltaVersion=runtime['delta'],minPolicy=state['policy'],minRole=state['role'],
        publicStateSha256=sha(state['publicStateFile']),rangerStateSha256=sha(state['rangerStateFile']),stateEvidenceFile=str(Path(state['publicStateFile']).parent/'state-evidence.private.json'),
        packageManifest=runtime['manifestPath'],controlDirectory=str(output/'control'),deadlineSeconds=600,beforeVersion=0)
    save(output/'request.private.json',request)
    return request


def observe(plan,runtime,output,stage,intervals=None):
    output.mkdir(mode=0o700,exist_ok=True)
    directory=output/stage;directory.mkdir(mode=0o700)
    objects=OwnedObjects(plan['primary'],plan['prefix']+'/')
    before=objects.manifest(directory/'raw-before');save(directory/'raw-before.json',before)
    observed=native(plan,runtime,'verify',directory/'native',intervals)
    assert all(runtime['jarHashes'].get(v['origin'])==v['sha256'] for v in observed['origins'].values()), 'Native actual origin differs from runtime manifest'
    after=objects.manifest(directory/'raw-after');save(directory/'raw-after.json',after)
    assert before==after, 'Native readback changed bytes'
    rest=Public(plan['gravitinoUrl'],plan['serviceAdmin'],directory/'public-trace')
    base='/api/metalakes/'+plan['metalake']
    registrations={table['name']:rest.call('GET',base+'/catalogs/'+plan['catalog']+'/schemas/'+plan['database']+'/tables/'+table['name']) for table in plan['tables']}
    owners={table['name']:rest.call('GET',base+'/owners/table/'+plan['catalog']+'.'+plan['database']+'.'+table['name'])['owner'] for table in plan['tables']}
    save(directory/'registrations.private.json',registrations);save(directory/'owners.json',owners)
    from verify import verify_registration_native
    native_by_name={x['name']:x for x in observed['tables']}
    verify_registration_native(registrations,native_by_name)
    target=native_by_name['cdf_target']
    receipt={k:v for k,v in registrations['cdf_target']['table']['properties'].items() if k.startswith(RECEIPT)}
    metadata=target['metadata'].get('metaData',target['metadata'])
    record={'nativeVersion':target['version'],'nativeId':target['native']['id'],'nativeSchema':metadata['schemaString'],
        'nativeProtocol':target['protocol'],'nativeConfiguration':metadata['configuration'],'receipt':receipt,
        'owner':owners['cdf_target']['name'],'ownerType':canonical_owner_type(owners['cdf_target']['type'])}
    save(directory/'record.json',record)
    return {'raw':before,'native':native_by_name,'registrations':registrations,'owners':owners,'record':record,'directory':str(directory)}


def verify_observation(plan,before,current,version,cdf,appends):
    from collections import Counter
    from verify import strict_json
    import hashlib
    target=current['native']['cdf_target'];original=before['native']['cdf_target']
    assert target['version']==version
    assert target['native']['id']==original['native']['id'] and target['schemaJson']==original['schemaJson']
    configuration=cdf_configuration(before['record']['nativeConfiguration'],cdf)
    assert target['native']['properties']==configuration and current['record']['nativeConfiguration']==configuration
    expected_protocol=(1,2,(),()) if version==0 else (1,7,(),('appendOnly','changeDataFeed','invariants'))
    assert canonical_protocol(current['record']['nativeProtocol'])==expected_protocol
    assert current['owners']==before['owners']
    sanitized=[[1,'MASKED','allowed'],[1,'MASKED','allowed'],[3,'MASKED','allowed']]
    expected=[[0,'sentinel','seed']]+sanitized*appends
    assert Counter(tuple(row) for row in target['rows'])==Counter(tuple(row) for row in expected)
    for table in ['source','enabled_diag','update_diag','denied_diag']:
        assert current['native'][table]==before['native'][table]
        assert current['registrations'][table]==before['registrations'][table]
    old=before['record']['receipt'];new=current['record']['receipt']
    assert {k:v for k,v in old.items() if not k.endswith('nativeVersion')}=={k:v for k,v in new.items() if not k.endswith('nativeVersion')}
    old_table=before['registrations']['cdf_target']['table'];new_table=current['registrations']['cdf_target']['table']
    assert {k:v for k,v in old_table.items() if k!='properties'}=={k:v for k,v in new_table.items() if k!='properties'}
    expected_properties=dict(old_table['properties'])
    if cdf is not None:expected_properties['delta.enableChangeDataFeed']=cdf
    expected_properties[RECEIPT+'nativeVersion']=str(version)
    assert new_table['properties']==expected_properties
    raw={r['key']:r for r in current['raw']};prefix=plan['prefix']+'/adoption/cdf_target/'
    old_raw={r['key']:r for r in before['raw']}
    assert all(raw.get(k)==v for k,v in old_raw.items()), 'Existing native object changed'
    assert all(k.startswith(prefix) or old_raw.get(k)==v for k,v in raw.items()), 'Unrelated table changed'
    def archived(key):
        data=(Path(current['directory'])/'raw-before'/hashlib.sha256(key.encode()).hexdigest()).read_bytes()
        assert hashlib.sha256(data).hexdigest()==raw[key]['sha256'] and len(data)==raw[key]['size']
        return data
    commits={int(Path(k).stem):k for k in raw if k.startswith(prefix+'_delta_log/') and Path(k).name.endswith('.json') and Path(k).stem.isdecimal()}
    assert set(commits)==set(range(version+1))
    for v,key in commits.items():
        actions=[strict_json(line) for line in archived(key).splitlines() if line]
        assert sum('commitInfo' in x for x in actions)==1
        if v in [1,3]:
            assert sum('metaData' in x for x in actions)==1 and not any('add' in x or 'remove' in x or 'cdc' in x for x in actions)
            metadata=next(x['metaData'] for x in actions if 'metaData' in x)
            original_metadata=original['metadata'].get('metaData',original['metadata'])
            expected_metadata=dict(original_metadata,configuration=cdf_configuration(original_metadata['configuration'],'true' if v==1 else 'false'))
            assert metadata==expected_metadata
            info=next(x['commitInfo'] for x in actions if 'commitInfo' in x)
            assert info['operation']=='SET TBLPROPERTIES' and info['readVersion']==v-1
        if v in [2,4]:
            assert any('add' in x for x in actions) and not any('metaData' in x or 'protocol' in x or 'remove' in x for x in actions)
    return True


def run_cell(runtime_path,inputs_path,output):
    output.mkdir(mode=0o700)
    runtime=read(runtime_path);runtime['manifestPath']=str(runtime_path)
    inputs=read(inputs_path)
    from package_binding import capture_sources
    assert capture_sources(ROOT)==runtime['sourceInputs'], 'Source moved after runtime capture'
    assert all(sha(path)==digest for path,digest in runtime['jarHashes'].items())
    handles={};process=None;plan=None;log=None
    try:
        association=verify_deployed(inputs,output/'deployed-ranger')
        plan,server,callbacks=provision(runtime,inputs,output/'fixture',association,handles)
        state=snapshot_state(plan,False,output/'initial-state')
        request=prepare_request(plan,runtime,state,output/'request')
        baseline=observe(plan,runtime,output/'observations','initial')
        verify_observation(plan,baseline,baseline,0,None,0)
        environment=clean_environment(plan);environment.update(SPARK_HOME=runtime['sparkHome'],SPARK_USER=plan['subject'],HADOOP_USER_NAME=plan['subject'])
        selected=[p for p in runtime['jarHashes'] if Path(p).name in runtime['addedJarNames'] and Path(p).name!='public-grants-live-append.jar']
        argv=[runtime['sparkHome']+'/bin/spark-submit','--driver-memory','4g','--class','org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerLiveAppend','--master','local[2]',
            '--properties-file',request['sparkConfigurationFile'],'--driver-class-path',request['driverSecurityDirectory'],'--jars',','.join(selected),runtime['app'],'--cdf',str(output/'request/request.private.json'),str(output/'app-evidence')]
        save(output/'command.json',{'argv':argv,'started':time.time(),'environmentAllowlist':{k:environment[k] for k in ['JAVA_HOME','SPARK_HOME','SPARK_USER','HADOOP_USER_NAME']}})
        log=(output/'process.private.log').open('wb');process=subprocess.Popen(argv,env=environment,cwd=ROOT,stdout=log,stderr=subprocess.STDOUT)
        save(output/'process.json',{'pid':process.pid,'started':time.time()})
        control=Path(request['controlDirectory'])
        observations={'initial':baseline};previous=baseline
        sequence=[('baseline',0,None,0),('grant',None,None,None),('enabled',1,'true',0),('enabled-append',2,'true',1),('revoke',None,None,None),('revoked',2,'true',1),('regrant',None,None,None),('disabled',3,'false',1),('disabled-append',4,'false',2)]
        for stage,version,cdf,appends in sequence:
            deadline=time.monotonic()+480
            ready=control/(stage+'-ready.json')
            while not ready.exists():
                assert process.poll() is None and time.monotonic()<deadline, 'Original app failed before checkpoint; no replay'
                time.sleep(.05)
            release={'checkpoint':stage,'verified':True}
            if stage in ['grant','revoke','regrant']:
                state=modify(plan,stage!='revoke',output/('public-'+stage))
                release['stateEvidenceFile']=str(Path(state['publicStateFile']).parent/'state-evidence.private.json')
            else:
                intervals=[[1,2]] if stage=='enabled-append' else ([[1,2],[3,4]] if stage=='disabled-append' else [])
                current=observe(plan,runtime,output/'observations',stage,intervals)
                verify_observation(plan,baseline,current,version,cdf,appends)
                if stage in ['baseline','revoked']:assert current['raw']==previous['raw'] and current['registrations']==previous['registrations']
                if stage in ['disabled','disabled-append']:assert canonical_protocol(current['native']['cdf_target']['protocol'])==canonical_protocol(observations['enabled']['native']['cdf_target']['protocol'])
                if stage in ['enabled','disabled']:
                    old_objects={r['key']:r for r in previous['raw']};new_objects={r['key']:r for r in current['raw']}
                    assert all(new_objects.get(k)==v for k,v in old_objects.items())
                    assert all(k.startswith(plan['prefix']+'/adoption/cdf_target/_delta_log/') for k in set(new_objects)-set(old_objects)), 'Metadata mutation added a data object'
                if intervals:
                    actual=current['native']['cdf_target']['intervals'];expected=[[1,'MASKED','allowed','insert',2],[1,'MASKED','allowed','insert',2],[3,'MASKED','allowed','insert',2]]
                    assert actual[0]['error'] is None;cdf_rows(actual[0]['rows'],expected,2)
                    if stage=='disabled-append':
                        assert actual[0]==observations['enabled-append']['native']['cdf_target']['intervals'][0]
                        assert actual[1]['error'] and actual[1]['error']['errorClass']=='DELTA_MISSING_CHANGE_DATA'
                observations[stage]=current;previous=current
            save(control/(stage+'-verified.json'),release)
        code=process.wait(timeout=90);log.close()
        save(output/'exit.json',{'pid':process.pid,'exitCode':code,'timedOut':False,'finished':time.time()});assert code==0
        assert read(output/'app-evidence/app-completed.json')['completed'] and read(output/'app-evidence/cdf-result.json')['completed']
        verify_app(output,observations)
        server.stop()
        verify_persistence(server,plan,observations,output/'persistence')
        callbacks.wait_drained(20);callbacks.__exit__();handles['callbacks']=None
        assert capture_sources(ROOT)==runtime['sourceInputs'], 'Source moved during original live cell'
        save(output/'result.json',{'passed':True,'spark':runtime['spark'],'delta':runtime['delta'],'manifestSha256':sha(runtime_path),'sameJvm':True,
            'nativeVersions':{k:v['record']['nativeVersion'] for k,v in observations.items()},'productionSource':'Verified shared Ranger artifact, unchanged server/base components and separately built CDF thin app','laneScope':'catalog CDF enablement; native CDF oracle'})
    except BaseException as error:
        save(output/'failure.json',{'class':type(error).__name__,'noReplay':True,'noCleanup':True})
        raise
    finally:
        if process is not None:
            terminated=process.poll() is None
            if terminated:
                process.terminate()
                try:process.wait(timeout=30)
                except subprocess.TimeoutExpired:process.kill();process.wait(timeout=10)
            if not (output/'exit.json').exists():
                save(output/'exit.json',{'pid':process.pid,'exitCode':process.returncode,'finished':time.time(),'terminatedBySupervisor':terminated,'supervisorFailed':(output/'failure.json').exists()})
        if log is not None:log.close()
        if handles.get('server'):handles['server'].stop()
        if handles.get('callbacks'):
            handles['callbacks'].wait_drained(20);handles['callbacks'].__exit__()
        save(output/'lane-release.json',{'released':True,'finished':time.time(),'failed':(output/'failure.json').exists()})


def verify_deployed(inputs,output):
    import hashlib
    from urllib.parse import urlsplit
    output.mkdir(mode=0o700)
    proof_path=Path(inputs['rangerRootProof']);proof=read(proof_path)
    raw=subprocess.check_output(['docker','inspect',proof['container']]);(output/'container-inspect.private.json').write_bytes(raw)
    container=json.loads(raw)[0]
    assert container['Id']==proof['containerId'] and container['Image']==proof['imageId'] and container['State']['Running'] is True
    endpoint=read(inputs['endpoints'])['ranger-admin'];route=urlsplit(endpoint)
    assert route.scheme=='http' and route.hostname in ['127.0.0.1','localhost'] and not route.username and route.path in ['', '/']
    bindings=container['NetworkSettings']['Ports']['6080/tcp']
    assert any(int(b['HostPort'])==route.port and b['HostIp'] in ['0.0.0.0','127.0.0.1','::'] for b in bindings)
    home=subprocess.check_output(['docker','exec',proof['container'],'readlink','-f','/opt/ranger/admin'],text=True).strip()
    assert home==proof['installedAdminHome']
    for path,digest in proof['installedJarSha256'].items():
        assert subprocess.check_output(['docker','exec',proof['container'],'sha256sum',path],text=True).split()[0]==digest
    assert proof['webappMaven']['version']=='2.8.0'
    # The original installed Maven-version observation is joined to actual rechecked jar bytes.
    association=output/'association.json'
    save(association,{'associated':True,'containerId':container['Id'],'imageId':container['Image'],'installedAdminHome':home,
        'rootProof':str(proof_path),'rootProofSha256':sha(proof_path),'endpointSha256':hashlib.sha256(endpoint.encode()).hexdigest(),
        'installedJarHashesRechecked':proof['installedJarSha256'],'deployedRangerVersion':'2.8.0'})
    return association


def verify_app(output,observations):
    from collections import Counter
    from verify import verify_downloads
    plan=read(output/'fixture/state.private.json');request=read(output/'request/request.private.json');app=output/'app-evidence'
    applied=read(app/'applied-downloads.json');state=read(output/'initial-state/ranger-state.private.json')
    verify_downloads(app,state,applied)
    assert read(app/'initial-downloads.json')['freshEmptyCacheAtPreparation'] is True
    metadata=read(app/'metadata-identity-final.json');assert metadata['principal']==plan['metadataPrincipal'] and metadata['successfulTargetGet'] is True
    assert set(metadata['successfulNamedGets'])=={'/api/metalakes/'+plan['metalake']+'/catalogs/'+plan['catalog']+'/schemas/'+plan['database']+'/tables/'+t['name'] for t in plan['tables']}
    profile=read(app/'runtime-profile.json');assert profile['wrapperClass']=='org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerCatalogSpark35'
    runtime=read(request['packageManifest'])
    for entry in profile['classOrigins'].values():assert entry['sameLoader'] is True and runtime['jarHashes'][entry['jar']]==entry['jarSha256']
    for stage in ['grant','revoke','regrant']:
        witness=read(app/(stage+'-applied.json'));transition=read(output/('public-'+stage)/'completed.json')
        state=transition['after'];downloaded=read(state['rangerStateFile'])
        assert witness['sameJvm'] is True and witness['policy']==state['policy'] and witness['role']==state['role']
        assert witness['publicStateSha256']==sha(state['publicStateFile']) and witness['rangerStateSha256']==sha(state['rangerStateFile'])
        verify_downloads(app,downloaded,{'policyVersion':witness['policy'],'policyResponseSha256':witness['policyResponseSha256'],
            'roleVersion':applied['roleVersion'],'roleResponseSha256':applied['roleResponseSha256']})
    sanitized=[[1,'MASKED','allowed'],[1,'MASKED','allowed'],[3,'MASKED','allowed']]
    for stage,key in [('baseline','initial'),('enabled','enabled'),('enabled-append','enabled-append'),('revoked','revoked'),('disabled','disabled'),('disabled-append','disabled-append')]:
        readback=read(app/(stage+'-read.json'))
        assert Counter(map(tuple,readback['targetRows']))==Counter(map(tuple,observations[key]['native']['cdf_target']['rows']))
        assert Counter(map(tuple,readback['sourceRows']))==Counter(map(tuple,sanitized))
    denied=['select-enable-denied','select-disable-denied','update-enable-denied','update-disable-denied','denied-enable-denied','denied-disable-denied','revoked-disable-denied']
    for label in denied:
        result=read(app/(label+'.json'));assert result['allowed'] is False and result['beforeVersion']==result['afterVersion'] and result['failure']
        assert any(f['class']=='java.lang.SecurityException' and f['stack'][0].startswith('org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerPolicyEngine.authorize(') for f in result['failure'])
    for label in ['enabled','disabled']:
        result=read(app/(label+'.json'));assert result['allowed'] is True and result['afterVersion']==result['beforeVersion']+1 and result['failure']==[]
    save(output/'app-verified.json',{'productionDiscovery':True,'realDownloads':True,'sameJvmGrantRevokeRegrant':True,'distinctIdentities':True,'exactMultisetGovernedReadback':True,'namedAuthorizationDenials':denied})


def verify_persistence(server,plan,observations,output):
    from cdf_provider import verify_store
    output.mkdir(mode=0o700)
    names=['initial','enabled','enabled-append','revoked','disabled','disabled-append']
    for name in names:save(output/(name+'.json'),observations[name]['record'])
    verify_store(server,output/'store',output,table_name='cdf_target',column_label='cdf-live',record_names=names)
    columns=server.column_snapshot('cdf-live-all')
    assert len(columns)==15 and len({value['id'] for value in columns.values()})==15
    save(output/'all-columns.json',columns)
    server.start()
    rest=Public(server.uri,plan['serviceAdmin'],output/'reload-trace')
    final=rest.call('GET','/api/metalakes/'+plan['metalake']+'/catalogs/'+plan['catalog']+'/schemas/'+plan['database']+'/tables/cdf_target')['table']
    assert final==observations['disabled-append']['registrations']['cdf_target']['table']
    server.stop()
    verify_store(server,output/'restarted-store',output,table_name='cdf_target',column_label='cdf-live-restart',record_names=names)
    assert columns==server.column_snapshot('cdf-live-restarted-all')
    save(output/'restarted-readonly.json',{'sameReceipt':True,'sameColumns':True,'nativeVersion':4,'sameServerRestart':True})


def main():
    parser=argparse.ArgumentParser()
    for key in ['runtime','inputs','out']:parser.add_argument('--'+key,required=True)
    args=parser.parse_args();os.umask(0o077)
    run_cell(Path(args.runtime).resolve(),Path(args.inputs).resolve(),Path(args.out).resolve())


if __name__=='__main__':main()

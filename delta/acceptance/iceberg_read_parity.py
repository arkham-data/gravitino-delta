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

"""Pinned native/wrapped Iceberg comparator with isolated Hive-service resources."""
import argparse
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

from cdf_live import ROOT, JAVA, Ranger, Public, Server, RecordingProxy, envfile, read, save, sha, verify_deployed
from cdf_merge_live import await_file
from read_parity_live import start, finish
from cdf_merge_live_checks import file_ref
from read_parity_checks import CASE_SPECS, verify_audit_slice
from package_binding import capture_sources, canonical_digest

CACHE=Path.home()/'.gradle/caches/modules-2/files-2.1'

def artifact(group, name, version):
    matches = list((CACHE/group/name/version).glob('*/*.jar'))
    assert len(matches) == 1, (group,name,version)
    return matches[0]

def runtime(spark, out):
    stock = ROOT/f'.superpowers/runtime-20260914/spark-{spark}/lib/python3.11/site-packages/pyspark'
    home = out/'spark-home';home.mkdir()
    for p in stock.iterdir():
        if p.name != 'jars': (home/p.name).symlink_to(p)
    jars = home/'jars';jars.mkdir()
    replacements = [('org.scala-lang','scala-library','2.12.21'),('org.scala-lang','scala-compiler','2.12.21'),('org.scala-lang','scala-reflect','2.12.21'),('org.apache.hadoop','hadoop-client-api','3.3.6'),('org.apache.hadoop','hadoop-client-runtime','3.3.6'),('org.apache.hadoop','hadoop-aws','3.3.6'),('com.amazonaws','aws-java-sdk-bundle','1.12.367'),('org.apache.iceberg','iceberg-spark-runtime-3.5_2.12','1.11.0'),('org.apache.kyuubi','kyuubi-spark-authz_2.12','1.10.2'),('org.apache.kyuubi','kyuubi-util-scala_2.12','1.10.2'),('org.apache.kyuubi','kyuubi-util','1.10.2'),('org.apache.ranger','ranger-plugins-common','2.8.0'),('org.apache.ranger','ranger-audit-core','2.8.0'),('org.apache.ranger','ranger-plugins-cred','2.8.0'),('org.apache.ranger','ranger-intg','2.8.0'),('org.apache.ranger','ranger-plugin-classloader','2.8.0'),('com.sun.jersey','jersey-client','1.19.4'),('com.sun.jersey','jersey-core','1.19.4'),('org.apache.commons','commons-configuration2','2.10.1'),('commons-beanutils','commons-beanutils','1.11.0'),('org.apache.commons','commons-collections4','4.4'),('com.google.guava','guava','32.1.3-jre'),('com.google.guava','failureaccess','1.0.1')]
    replacements += [('org.apache.ranger','ranger-authz-api','2.8.0'),('org.apache.ranger','ugsync-util','2.8.0')]
    replacements += [('com.fasterxml.jackson.jaxrs','jackson-jaxrs-json-provider','2.15.2'),('com.fasterxml.jackson.jaxrs','jackson-jaxrs-base','2.15.2'),('com.fasterxml.jackson.module','jackson-module-jaxb-annotations','2.15.2')]
    names = [name+'-' for _,name,_ in replacements]
    for p in (stock/'jars').glob('*.jar'):
        if not any(p.name.startswith(name) for name in names): (jars/p.name).symlink_to(p)
    for args in replacements:
        p = artifact(*args);(jars/p.name).symlink_to(p)
    save(out/'artifacts.json', {p.name:{'path':str(p.resolve()),'sha256':sha(p)} for p in jars.iterdir()})
    return home


def run(spark,route,inputs_path,out):
    os.umask(0o077);out.mkdir(mode=0o700)
    inputs=read(inputs_path);endpoints=read(inputs['endpoints']);admin=envfile(inputs['rangerAdminFile'])
    association=verify_deployed(inputs,out/'deployed-ranger')
    sources=capture_sources(ROOT);save(out/'source-inputs.json',{'files':sources,'digest':canonical_digest(sources)})
    nonce=uuid.uuid4().hex[:12]
    subjects={name:'ice_read_'+name.replace('-','_')+'_'+nonce for name in ['id-only','table-wide','no-grant','denied-email']}
    setup='ice_setup_'+nonce;service='ice_read_'+nonce;database='ice_read_db_'+nonce
    plan={'rangerUrl':endpoints['ranger-admin'],'rangerAdmin':{'username':admin['RANGER_ADMIN_USER'],'password':admin['RANGER_ADMIN_PASSWORD']},
          'plugin':{'username':'ice_read_download_'+nonce,'password':'Aa1!'+secrets.token_urlsafe(20)}}
    save(out/'state.private.json',dict(plan,subjects=subjects,setup=setup,service=service,database=database))
    ranger=Ranger(plan,out/'ranger-trace')
    for user,password in [(plan['plugin']['username'],plan['plugin']['password'])]+[(user,'Aa1!'+secrets.token_urlsafe(20)) for user in subjects.values()]:
        ranger.call('POST','/service/xusers/secure/users',{'name':user,'firstName':'Iceberg','lastName':'ReadFixture','password':password,
            'emailAddress':user+'@fixture.invalid','userRoleList':['ROLE_USER'],'status':1,'userSource':0,'isVisible':1})
    definition=ranger.call('GET','/service/public/v2/api/servicedef/name/hive');save(out/'service-definition.json',definition)
    created=ranger.call('POST','/service/public/v2/api/service',{'name':service,'type':'hive','isEnabled':True,
        'configs':{'username':plan['plugin']['username'],'password':plan['plugin']['password'],'jdbc.driverClassName':'org.apache.hive.jdbc.HiveDriver',
                   'jdbc.url':'jdbc:hive2://localhost:10000','create.default.policy.per.hierarchy':'false','policy.download.auth.users':plan['plugin']['username']}})
    save(out/'service.json',{'id':created['id'],'name':service,'database':database,'subjects':subjects,'setup':setup})
    def policy(label,kind,columns,users):
        resources={key:{'values':values,'isExcludes':False,'isRecursive':False} for key,values in [('database',[database]),('table',['source','primitive']),('column',columns)]}
        if kind==2:resources.pop('column')
        body={'service':service,'name':label+'_'+nonce,'policyType':kind,'isEnabled':True,'isAuditEnabled':True,
              'resources':resources,'policyLabels':['read-parity-fixture-authored']}
        item={'users':users,'groups':[],'roles':[],'accesses':[{'type':'select','isAllowed':True}]}
        return body,item
    allows={}
    for profile,columns in [('id-only',['id']),('table-wide',['*']),('denied-email',['*'])]:
        body,item=policy('allow_'+profile,0,columns,[subjects[profile]]);body['policyItems']=[item]
        allows[profile]=ranger.call('POST','/service/public/v2/api/policy',body)
    body,item=policy('deny_email',0,['email'],[subjects['denied-email']]);body['denyPolicyItems']=[item]
    ranger.call('POST','/service/public/v2/api/policy',body)
    body,item=policy('east_only',2,[],list(subjects.values()));item['rowFilterInfo']={'filterExpr':"region = 'east'"};body['rowFilterPolicyItems']=[item]
    ranger.call('POST','/service/public/v2/api/policy',body)
    security=out/'security';security.mkdir(mode=0o700);cache=out/'policy-cache';cache.mkdir()
    proxy=RecordingProxy(plan['rangerUrl'],out/'plugin-http.jsonl');proxy.__enter__()
    properties={'ranger.plugin.spark.service.name':service,'ranger.plugin.spark.policy.rest.url':'http://127.0.0.1:'+str(proxy.port),
        'ranger.plugin.spark.policy.rest.client.username':plan['plugin']['username'],'ranger.plugin.spark.policy.rest.client.password':plan['plugin']['password'],
        'ranger.plugin.spark.policy.cache.dir':str(cache),'ranger.plugin.spark.policy.pollIntervalMs':'500',
        'ranger.plugin.spark.policy.source.impl':'org.apache.ranger.admin.client.RangerAdminRESTClient',
        'ranger.plugin.spark.policy.rest.client.connection.timeoutMs':'3000','ranger.plugin.spark.policy.rest.client.read.timeoutMs':'3000',
        'ranger.plugin.spark.policy.rest.client.cookie.enabled':'false','ranger.plugin.spark.supports.policy.deltas':'false',
        'ranger.plugin.spark.enable.implicit.userstore.enricher':'false'}
    xml=ET.Element('configuration')
    for key,value in properties.items():
        prop=ET.SubElement(xml,'property');ET.SubElement(prop,'name').text=key;ET.SubElement(prop,'value').text=value
    ET.indent(xml);ET.ElementTree(xml).write(security/'ranger-spark-security.xml',encoding='utf-8',xml_declaration=True)
    (security/'ranger-spark-audit.xml').write_text('<configuration><property><name>xasecure.audit.is.enabled</name><value>false</value></property></configuration>')
    home=runtime(spark,out);server=None;server_proxy=None;wrapper_config={}
    warehouse='s3a://delta-fixtures/iceberg-read-parity-'+nonce
    mask=None
    def snapshot(name):
        result=ranger.call('GET','/service/plugins/secure/policies/download/'+service+'?lastKnownVersion=-1&pluginId=iceberg-read-supervisor',download=True)
        path=out/(name+'-policies.json');save(path,result)
        return result,file_ref(path)
    try:
        if route=='gravitino':
            assembly=Path(inputs['sharedAssemblyManifest'])
            assert assembly.is_file(),'Fresh shared assembly manifest required'
            server=Server(assembly.parent/'server',out,{'serviceAdmin':setup,'javaHome':JAVA})
            conf=server.home/'conf/gravitino.conf'
            conf.write_text(conf.read_text().replace('gravitino.authorization.enable = true','gravitino.authorization.enable = false'))
            catalog_home=server.home/'catalogs/lakehouse-iceberg'
            shutil.copytree(ROOT/'catalogs/catalog-lakehouse-iceberg/build/libs',catalog_home/'libs')
            for args in [('org.apache.hadoop','hadoop-aws','3.3.6'),('com.amazonaws','aws-java-sdk-bundle','1.12.367')]:
                jar=artifact(*args);shutil.copyfile(jar,catalog_home/'libs'/jar.name)
            (catalog_home/'conf').mkdir()
            (catalog_home/'conf/lakehouse-iceberg.conf').write_text('# Isolated stock HadoopCatalog comparator backend.\n')
            credentials=envfile(inputs['minioCredentialsFile'])
            hadoop={'fs.s3a.endpoint':endpoints['minio'],'fs.s3a.path.style.access':'true','fs.s3a.connection.ssl.enabled':'false',
                'fs.s3a.aws.credentials.provider':'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
                'fs.s3a.access.key':credentials['MINIO_ROOT_USER'],'fs.s3a.secret.key':credentials['MINIO_ROOT_PASSWORD']}
            core=ET.Element('configuration')
            for key,value in hadoop.items():
                prop=ET.SubElement(core,'property');ET.SubElement(prop,'name').text=key;ET.SubElement(prop,'value').text=value
            ET.ElementTree(core).write(server.home/'conf/core-site.xml',encoding='utf-8',xml_declaration=True)
            extras=[ROOT/'spark-connector/v3.5/spark/build/libs/gravitino-spark-3.5_2.12-2.0.0-SNAPSHOT.jar',
                    ROOT/'spark-connector/v3.4/spark/build/libs/gravitino-spark-3.4_2.12-2.0.0-SNAPSHOT.jar',
                    ROOT/'spark-connector/spark-common/build/libs/gravitino-spark-common-2.0.0-SNAPSHOT.jar',
                    ROOT/'clients/client-java-runtime/build/libs/gravitino-client-java-runtime-2.0.0-SNAPSHOT.jar']
            supplemental=out/'supplemental';supplemental.mkdir()
            for jar in extras:
                retained=supplemental/jar.name;shutil.copyfile(jar,retained);(home/'jars'/jar.name).symlink_to(retained)
            save(out/'wrapper-artifacts.json',{str(p):sha(p) for p in supplemental.iterdir()})
            server.start();rest=Public(server.uri,setup,out/'public-trace');metalake='ice_read_lake_'+nonce
            rest.call('POST','/api/metalakes',{'name':metalake,'comment':'Ordinary Iceberg read comparator','properties':{}})
            props={'catalog-backend':'custom','catalog-backend-impl':'org.apache.iceberg.hadoop.HadoopCatalog','catalog-backend-name':'iceberg',
                   'uri':'file:///unused-hadoop-catalog-uri','warehouse':warehouse,'table-metadata-cache-impl':'','io-impl':'org.apache.iceberg.hadoop.HadoopFileIO'}
            rest.call('POST','/api/metalakes/'+metalake+'/catalogs',{'name':'iceberg','type':'RELATIONAL','provider':'lakehouse-iceberg','properties':props})
            server_proxy=RecordingProxy(server.uri,out/'gravitino-http.jsonl');server_proxy.__enter__()
            wrapper_config={'wrapper':True,'gravitinoUrl':'http://127.0.0.1:'+str(server_proxy.port),'metalake':metalake}
            save(out/'wrapper-registration.json',dict(wrapper_config,provider='lakehouse-iceberg',properties=props,metadataAuthorizationEnabled=False,
                                                    sharedAssembly=file_ref(assembly)))
        python=str(ROOT/('.superpowers/runtime-20260914/spark-'+spark+'/bin/python'))
        environment=dict(os.environ,JAVA_HOME=JAVA,TZ='UTC',SPARK_HOME=str(home),SPARK_LOCAL_IP='127.0.0.1',PYSPARK_PYTHON=python)
        def launch(mode,subject,profile=None):
            target=out/mode;target.mkdir()
            initial,reference=snapshot(mode+'-initial')
            request={'mode':'seed' if mode=='seed' else 'application','spark':spark,'output':str(target),
                'pythonOutput':str(target/'python-evidence'),'credentials':inputs['minioCredentialsFile'],
                'warehouse':warehouse,'endpoint':endpoints['minio'],'database':database,'identityProfile':profile,
                'minPolicy':initial['policyVersion'],'initialPolicySnapshot':reference}
            if mode!='seed':request.update(wrapper_config)
            audit=target/'audit';audit.mkdir()
            request['auditDirectory']=str(audit)
            local_security=target/'security';shutil.copytree(security,local_security)
            audit_values={'xasecure.audit.is.enabled':'true','xasecure.audit.destination.file':'true',
                          'xasecure.audit.destination.file.dir':str(audit),
                          'xasecure.audit.destination.file.filename.format':'permission.log',
                          'xasecure.audit.destination.file.file.rollover.sec':'86400',
                          'xasecure.audit.destination.file.queue':'none',
                          'xasecure.audit.provider.summary.enabled':'false'}
            audit_xml=ET.Element('configuration')
            for key,value in audit_values.items():
                prop=ET.SubElement(audit_xml,'property');ET.SubElement(prop,'name').text=key;ET.SubElement(prop,'value').text=value
            ET.indent(audit_xml);ET.ElementTree(audit_xml).write(local_security/'ranger-spark-audit.xml',encoding='utf-8',xml_declaration=True)
            save(target/'request.private.json',request)
            argv=[str(home/'bin/spark-submit'),'--master','local[2]','--driver-memory','2g','--driver-class-path',str(local_security),
                  ROOT/'delta/acceptance/iceberg_read_parity_app.py',target/'request.private.json']
            return start(argv,dict(environment,SPARK_USER=subject,HADOOP_USER_NAME=subject),target/'process'),reference
        (process,stdout,stderr),_=launch('seed',setup)
        try:process.wait(timeout=300)
        finally:code=finish(process,stdout,stderr,out/'seed/process',process.poll() is None)
        assert code==0,'Original Iceberg seed failed'
        if server is not None:
            for table in ['source','primitive']:
                rest.call('GET','/api/metalakes/'+metalake+'/catalogs/iceberg/schemas/'+database+'/tables/'+table)
        cases=[];exits={'native':file_ref(out/'seed/process/exit.json')}
        for profile in ['id-only','table-wide','no-grant','denied-email']:
            (process,stdout,stderr),initial_ref=launch(profile,subjects[profile],profile)
            states={'initial':initial_ref}
            try:
                for phase in {'id-only':['revoke-id'],'table-wide':['constant-mask','null-mask']}.get(profile,[]):
                    await_file(out/profile/(phase+'-ready.json'),process,300)
                    if phase=='revoke-id':
                        allow=allows['id-only'];allow['isEnabled']=False
                        ranger.call('PUT','/service/public/v2/api/policy/'+str(allow['id']),allow)
                    else:
                        if mask is None:mask,item=policy('mask_email',1,['email'],list(subjects.values()))
                        else:item={'users':list(subjects.values()),'groups':[],'roles':[],'accesses':[{'type':'select','isAllowed':True}]}
                        item['dataMaskInfo']={'dataMaskType':'CUSTOM','valueExpr':"'M'"} if phase=='constant-mask' else {'dataMaskType':'MASK_NULL'}
                        mask['dataMaskPolicyItems']=[item]
                        if 'id' in mask:mask=ranger.call('PUT','/service/public/v2/api/policy/'+str(mask['id']),mask)
                        else:mask=ranger.call('POST','/service/public/v2/api/policy',mask)
                    state,reference=snapshot(profile+'-'+phase);states[phase]=reference
                    save(out/profile/(phase+'-verified.json'),{'policyVersion':state['policyVersion'],'policySnapshot':reference})
                process.wait(timeout=300)
            finally:code=finish(process,stdout,stderr,out/profile/'process',process.poll() is None)
            assert code==0,'Original Iceberg application failed: '+profile
            exits[profile]=file_ref(out/profile/'process/exit.json')
            for case in read(out/profile/'python-evidence/cases.json'):
                case['policySnapshot']=states[case['policyPhase']];cases.append(case)
        assert len(cases)==len(CASE_SPECS) and {case['name'] for case in cases}==set(CASE_SPECS),'Comparator case inventory differs'
        aqe=next(case for case in cases if case['name']=='aqe-join')
        assert aqe['outcome']=='allowed' and aqe['observedRows']==aqe['expectedRows'],'Comparator AQE payload failed'
        assert any(node.get('isFinalPlan') is True for node in aqe['planNodes']),'Comparator final AQE absent'
        assert any(node['class'].endswith('ShuffleQueryStageExec') and node.get('materialized') is True for node in aqe['planNodes']),'Comparator materialized payload stage absent'
        assert aqe['rangerRequests'],'Positive-control actual SELECT audits absent'
        for case in cases:
            sinks=[file_ref(path) for path in (out/case['identityProfile']/'audit').glob('*.log')]
            verify_audit_slice(case,sinks)
            assert case['pluginPolicyVersion']==read(case['policySnapshot'])['policyVersion'],'Comparator policy application differs'
        artifacts={str(path.resolve()):sha(path) for path in (home/'jars').iterdir()}
        for profile in subjects:
            observed=read(out/profile/'runtime.json')
            assert observed['spark']==spark and observed['scala']=='2.12.21'
            assert observed['sparkUser']==subjects[profile]
            for origin in observed['origins'].values():
                assert artifacts[origin['artifact']['path']]==origin['artifact']['sha256'],'Actual comparator provider differs'
            actual=read(out/profile/'catalog.json')['actualClass']
            assert actual==('org.apache.iceberg.spark.SparkCatalog' if route=='native' else 'org.apache.gravitino.spark.connector.iceberg.GravitinoIcebergCatalogSpark35'),'Actual comparator catalog route differs'
        
        save(out/'runtime-manifest.json',{'spark':spark,'iceberg':'1.11.0','kyuubi':'1.10.2','ranger':'2.8.0','jars':artifacts})
        assert capture_sources(ROOT)==sources,'Source changed during comparator'
        save(out/'result.json',{'schemaVersion':1,'spark':spark,'engine':'iceberg','iceberg':'1.11.0','kyuubi':'1.10.2',
            'catalogRoute':route,'runtimeManifest':file_ref(out/'runtime-manifest.json'),'rangerServerVersion':'2.8.0','rangerClientVersion':'2.8.0',
            'deployedRanger':file_ref(association),'auditSinks':{profile:[file_ref(path) for path in (out/profile/'audit').glob('*.log')] for profile in subjects},'cases':cases,'processExits':exits,'database':database,
            'differences':[case['name'] for case in cases if case['comparison']=='measured-difference'],
            'deploymentExclusions':['native seeding','metadata authorization disabled for wrapper','fixture-authored policies; no grant propagation claim']})
    finally:
        proxy.__exit__(None,None,None)
        if server_proxy is not None:server_proxy.__exit__(None,None,None)
        if server is not None:server.stop()


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--spark',required=True,choices=['3.5.4','3.5.6'])
    parser.add_argument('--catalog',required=True,choices=['native','gravitino']);parser.add_argument('--inputs',required=True)
    parser.add_argument('--out',required=True);parser.add_argument('--worker',action='store_true')
    args=parser.parse_args();output=Path(args.out).resolve()
    if args.worker:run(args.spark,args.catalog,Path(args.inputs).resolve(),output);return
    assert not output.exists(),'Fresh comparator directory required'
    directory=output.parent/(output.name+'-supervisor')
    argv=[sys.executable,Path(__file__).resolve(),'--worker','--spark',args.spark,'--catalog',args.catalog,'--inputs',Path(args.inputs).resolve(),'--out',output]
    process,stdout,stderr=start(argv,dict(os.environ),directory)
    try:process.wait(timeout=2400)
    finally:code=finish(process,stdout,stderr,directory,process.poll() is None)
    assert code==0,'Original comparator supervisor failed'
    result=read(output/'result.json');result['processExits']['supervisor']=file_ref(directory/'exit.json')
    save(output/'final-result.json',result)
    print('Comparator measured',args.spark,args.catalog,len(result['differences']),'differences')


if __name__=='__main__':main()

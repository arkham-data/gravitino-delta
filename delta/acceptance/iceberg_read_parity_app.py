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

"""Evidence-only stock Iceberg / Kyuubi Spark runtime probe."""
import json
import os
from pathlib import Path
import shlex
import sys
import time
import hashlib
import threading
from read_parity_pyspark import execute_cases, normalize, ref, clear_job_group
from pyspark.sql import SparkSession

request = json.loads(Path(sys.argv[1]).read_text())
out = Path(request['output'])

def save(name, value):
    (out / name).write_text(json.dumps(value, indent=2, sort_keys=True))

def envfile(path):
    return {k: shlex.split(v)[0] for k, v in (line.split('=', 1) for line in Path(path).read_text().splitlines() if line and not line.startswith('#'))}

creds = envfile(request['credentials'])
builder = (SparkSession.builder.appName('Iceberg ordinary read comparator')
    .config('spark.sql.adaptive.enabled', 'true')
    .config('spark.sql.adaptive.coalescePartitions.enabled', 'true')
    .config('spark.sql.adaptive.coalescePartitions.parallelismFirst', 'false')
    .config('spark.sql.adaptive.advisoryPartitionSizeInBytes', '67108864')
    .config('spark.sql.shuffle.partitions', '8')
    .config('spark.sql.autoBroadcastJoinThreshold', '-1')
    .config('spark.sql.adaptive.autoBroadcastJoinThreshold', '-1')
    .config('spark.hadoop.fs.s3a.endpoint', request['endpoint'])
    .config('spark.hadoop.fs.s3a.path.style.access', 'true')
    .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
    .config('spark.hadoop.fs.s3a.access.key', creds['MINIO_ROOT_USER'])
    .config('spark.hadoop.fs.s3a.secret.key', creds['MINIO_ROOT_PASSWORD'])
    .config('spark.ui.enabled', 'false')
    .config('spark.sql.session.timeZone', 'UTC')
    .config('spark.sql.execution.arrow.pyspark.fallback.enabled', 'false'))
if request.get('wrapper'):
    builder = (builder.config('spark.plugins', 'org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin')
        .config('spark.sql.gravitino.uri', request['gravitinoUrl'])
        .config('spark.sql.gravitino.metalake', request['metalake'])
        .config('spark.sql.gravitino.enableIcebergSupport', 'true')
        .config('spark.sql.gravitino.iceberg.rest-routing-enabled', 'false'))
else:
    builder = (builder.config('spark.sql.catalog.iceberg', 'org.apache.iceberg.spark.SparkCatalog')
        .config('spark.sql.catalog.iceberg.type', 'hadoop')
        .config('spark.sql.catalog.iceberg.warehouse', request['warehouse'])
        .config('spark.sql.catalog.iceberg.cache-enabled', 'false'))
if request['mode'] != 'seed':
    builder = builder.config('spark.sql.extensions', 'org.apache.kyuubi.plugin.spark.authz.ranger.RangerSparkExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
else:
    builder = builder.config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
spark = builder.getOrCreate()
assert spark.version == request['spark']
assert os.environ.get('TZ') == 'UTC'
jvm = spark.sparkContext._jvm
loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()
classes = ['org.apache.iceberg.spark.SparkCatalog', 'org.apache.kyuubi.plugin.spark.authz.ranger.RangerSparkExtension', 'org.apache.ranger.plugin.service.RangerBasePlugin', 'org.apache.ranger.audit.provider.AuditProviderFactory', 'org.apache.spark.sql.SparkSession', 'scala.Option']
if request.get('wrapper'):
    classes += ['org.apache.gravitino.spark.connector.iceberg.GravitinoIcebergCatalogSpark35', 'org.apache.gravitino.spark.connector.iceberg.GravitinoIcebergCatalog', 'org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin']
origins = {}
for name in classes:
    cls = loader.loadClass(name)
    origins[name] = {'source': str(cls.getProtectionDomain().getCodeSource().getLocation()), 'implementationVersion': cls.getPackage().getImplementationVersion(), 'artifact': ref(Path(str(cls.getProtectionDomain().getCodeSource().getLocation().getPath())).resolve())}
save('runtime.json', {'spark': spark.version, 'scala': jvm.scala.util.Properties.versionNumberString(), 'java': jvm.java.lang.System.getProperty('java.version'), 'sparkUser': spark.sparkContext.sparkUser(), 'ugi': str(jvm.org.apache.hadoop.security.UserGroupInformation.getCurrentUser()), 'origins': origins, 'mode': request['mode'], 'warehouse': request['warehouse']})

catalog=spark._jsparkSession.sessionState().catalogManager().catalog('iceberg')
save('catalog.json',{'actualClass':str(catalog.getClass().getName()),'extensions':spark.conf.get('spark.sql.extensions')})
ns = 'iceberg.' + request['database']
if request['mode'] == 'seed':
    spark.sql('CREATE NAMESPACE ' + ns)
    spark.sql('CREATE TABLE ' + ns + '.source (id BIGINT, region STRING, email STRING, secret STRING) USING iceberg')
    spark.sql("INSERT INTO " + ns + ".source VALUES (1,'east','z','s1'),(2,'east','a','s2'),(3,'east',NULL,'s3'),(4,'west','hidden','s4'),(5,'east','b','s5')")
    spark.sql('CREATE TABLE '+ns+'.primitive (id BIGINT, region STRING, email STRING, nlong BIGINT, text STRING, flag BOOLEAN, number DOUBLE, money DECIMAL(10,2), day DATE, stamp TIMESTAMP) USING iceberg')
    spark.sql("INSERT INTO "+ns+".primitive VALUES (1,'east','z',7,'visible',true,1.5,12.30,DATE '2026-10-26',TIMESTAMP '2026-10-26 12:34:56.123456'),(2,'east',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),(4,'west','hidden',9,'hidden',false,2.0,99.99,DATE '2026-10-27',TIMESTAMP '2026-10-27 00:00:00')")
    save('seed.json', {name:[[normalize(v) for v in row] for row in spark.table(ns+'.'+name).orderBy('id').collect()] for name in ['source','primitive']})
    spark.stop()
    sys.exit(0)

from pyspark.java_gateway import ensure_callback_server_started
ensure_callback_server_started(spark.sparkContext._gateway)
plugin = getattr(getattr(jvm.org.apache.kyuubi.plugin.spark.authz.ranger, 'SparkRangerAdminPlugin$'), 'MODULE$')
deadline=time.monotonic()+90
while int(plugin.getPoliciesVersion()) < request['minPolicy']:
    assert time.monotonic()<deadline,'Initial policy download did not converge'
    time.sleep(.1)
audit_provider=plugin.getAuditProviderFactory().getAuditProvider()


class Listener:
    class Java:
        implements=['org.apache.spark.sql.util.QueryExecutionListener']

    def record(self,function,query,failed):
        if bridge.active:
            with bridge.lock:
                bridge.queries.append(query)
                bridge.callbacks.append({'case':bridge.active,'caseSequence':bridge.sequence,'function':function,'queryId':int(query.id()),'failed':failed})

    def onSuccess(self,function,query,nanos):self.record(function,query,False)
    def onFailure(self,function,query,error):self.record(function,query,True)


class Bridge:
    def __init__(self):
        self.active='';self.sequence=0;self.lock=threading.Lock()
        self.requests=[];self.queries=[];self.callbacks=[];self.audit_start=0

    def drain(self):
        spark.sparkContext._jsc.sc().listenerBus().waitUntilEmpty(30000)
        total=int(audit_provider.getTotalCount())
        deadline=time.monotonic()+30
        while True:
            events=[]
            for path in sorted(Path(request['auditDirectory']).glob('*.log')):
                raw=path.read_bytes()
                if raw and not raw.endswith(b'\n'):break
                events.extend(json.loads(line) for line in raw.decode().splitlines() if line)
            else:
                if len(events)==total:
                    assert int(audit_provider.getTotalFailedCount())==int(audit_provider.getTotalDeferredCount())==0,'Ranger audit queue lost/deferred events'
                    self.audit_bytes=b''.join(path.read_bytes() for path in sorted(Path(request['auditDirectory']).glob('*.log')))
                    assert len(list(Path(request['auditDirectory']).iterdir()))<=1,'Unexpected audit rotation'
                    return events
                assert len(events)<=total,'Unexpected audit events outside this isolated process'
            assert time.monotonic()<deadline,'Ranger audit sink did not flush submitted event count'
            time.sleep(.01)

    def begin(self,name):
        events=self.drain()
        with self.lock:
            self.audit_start=len(events);self.audit_start_bytes=len(self.audit_bytes)
            self.requests=[];self.queries=[];self.callbacks=[]
            self.sequence+=1;self.active=name
        spark.sparkContext.setJobGroup(name,name)

    def phase(self,name):
        self.drain();self.active=''
        save(name+'-ready.json',{'ready':True})
        gate=out/(name+'-verified.json');deadline=time.monotonic()+180
        while not gate.exists():
            assert time.monotonic()<deadline,'Policy phase gate timed out'
            time.sleep(.05)
        state=json.loads(gate.read_text())
        while int(plugin.getPoliciesVersion()) < state['policyVersion']:
            assert time.monotonic()<deadline,'Policy application did not converge'
            time.sleep(.1)
        save(name+'-applied.json',{'policy':int(plugin.getPoliciesVersion()),'policySnapshot':state['policySnapshot']})

    def finish(self,name,frame,allowed):
        events=self.drain();assert self.active==name;self.active=''
        selected_events=events[self.audit_start:]
        audit_path=out/(name+'-audit.raw.jsonl');audit_path.write_bytes(self.audit_bytes[self.audit_start_bytes:])
        clear_job_group(spark)
        query=self.queries[-1] if self.queries else (frame.queryExecution() if frame is not None else None)
        analyzed=out/(name+'-analyzed.txt');final=out/(name+'-final.txt')
        analyzed.write_text(str(query.analyzed()) if query is not None else 'Lookup rejected before analyzed plan')
        final.write_text(str(query.executedPlan()) if allowed and self.queries and query is not None else 'Rejected before final payload plan')
        value={'originalDemands':[], 'demandBasis':'actual rewritten-plan Ranger requests; no Delta original-occurrence claim',
               'rangerRequests':[dict(event,observationKind='audited-permission-event') for event in selected_events],
               'auditEvidence':ref(audit_path),'auditRange':{'firstEvent':self.audit_start,'endEvent':len(events),'firstByte':self.audit_start_bytes,'endByte':len(self.audit_bytes),'submittedTotal':int(audit_provider.getTotalCount()),'failedTotal':int(audit_provider.getTotalFailedCount()),'deferredTotal':int(audit_provider.getTotalDeferredCount())},
               'permissionObservation':'audited SELECT events' if selected_events else 'unqualified: no SELECT audit event observed for this action',
               'unobservedEvaluations':['mask evaluation requests','row-filter evaluation requests'],
               'callbacks':self.callbacks,'caseSequence':self.sequence,
               'listenerQueuesDrained':True,'actualActionQueries':len(self.queries),
               'analyzedPlan':ref(analyzed),'finalPlan':ref(final),'pluginPolicyVersion':int(plugin.getPoliciesVersion())}
        if allowed and self.queries and query is not None:value['planNodes']=plan_nodes(query.executedPlan())
        save(name+'-observations.json',value)
        return str(out/(name+'-observations.json'))

    def close(self):
        try:
            self.drain();self.active=''
            spark._jsparkSession.listenerManager().unregister(listener)
        finally:
            spark.stop()

def plan_nodes(plan):
    result = []
    seen = set()
    def visit(node):
        ident = int(jvm.java.lang.System.identityHashCode(node))
        if ident in seen:
            return
        seen.add(ident)
        cls = str(node.getClass().getName())
        rec = {'class': cls, 'nodeName': str(node.nodeName()), 'id': ident}
        if cls.endswith('AdaptiveSparkPlanExec'):
            rec['isFinalPlan'] = bool(node.isFinalPlan())
            visit(node.executedPlan())
        if cls.endswith('ShuffleQueryStageExec') or cls.endswith('BroadcastQueryStageExec'):
            rec['stageId'] = int(node.id())
            rec['materialized'] = bool(node.isMaterialized())
            visit(node.plan())
        if cls.endswith('AQEShuffleReadExec'):
            rec['partitionSpecs'] = str(node.partitionSpecs())
            rec['partitions'] = int(node.partitionSpecs().size())
        result.append(rec)
        iterator = node.children().iterator()
        while iterator.hasNext():
            visit(iterator.next())
    visit(plan)
    return result

bridge=Bridge();listener=Listener()
spark._jsparkSession.listenerManager().register(listener)
request.update(catalog='iceberg',comparator=True)
execute_cases(spark,bridge,request)
save('success.json',{'completed':True,'policyObservations':'audited SELECT permission events; mask/filter evaluation requests unobserved','qualification':'measured comparator differences are not Delta assertions'})

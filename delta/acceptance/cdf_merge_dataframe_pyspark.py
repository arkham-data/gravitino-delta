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

"""Actual packaged classic PySpark DataFrames and public builder under application identity."""
import hashlib
import json
from pathlib import Path
import sys

import gravitino_delta
from gravitino_delta import GravitinoDeltaTable
import gravitino_delta.tables
from py4j.protocol import Py4JJavaError
from pyspark import SparkContext
from pyspark.sql import SparkSession, functions as F


def module_origins():
    """Capture loader-owned bytes and archive paths, rejecting checkout imports at preflight."""
    result = {}
    for module in [gravitino_delta, gravitino_delta.tables]:
        loader = module.__loader__
        result[module.__name__] = {'file':module.__file__, 'archive':getattr(loader, 'archive', None),
            'sha256':hashlib.sha256(loader.get_data(module.__file__)).hexdigest()}
    return result


def execute_operation(spark, operation):
    """Build one fresh bounded source and invoke the public API without a SQL payload bridge."""
    assert operation['clauseProfile'] == 'partitioned-cdf'
    assert operation['expressionMode'] in ['strings', 'columns-mixed']
    target = GravitinoDeltaTable.forName(spark, operation['target']).alias('t')
    source = (spark.read.format('delta').option('readChangeFeed', 'true')
        .option('startingVersion', operation['start']).option('endingVersion', operation['end'])
        .table(operation['source'])
        .filter("_change_type <> 'update_preimage' AND _commit_version = 0").alias('s'))
    if operation['expressionMode'] == 'strings':
        (target.merge(source, 't.id=s.id')
            .whenMatchedDelete("s._change_type='delete'")
            .whenMatchedUpdate(set={'secret':"concat(s.secret, '|', t.secret)", 'region':'s.region', 'part':'s.part'})
            .whenNotMatchedInsert("s._change_type<>'delete'", values={'id':'s.id','secret':'s.secret','region':'s.region','part':'s.part'})
            .execute())
    else:
        (target.merge(source, F.col('t.id') == source['id'])
            .whenMatchedDelete(source['_change_type'] == F.lit('delete'))
            .whenMatchedUpdate(set={'secret':F.concat(source['secret'],F.lit('|'),F.col('t.secret')), 'region':'s.region', 'part':source['part']})
            .whenNotMatchedInsert(source['_change_type'] != F.lit('delete'), values={'id':source['id'],'secret':'s.secret','region':source['region'],'part':'s.part'})
            .execute())


def main():
    request=json.loads(Path(sys.argv[1]).read_text())
    runtime=json.loads(Path(request['packageManifest']).read_text())
    modules=module_origins()
    assert request['frontend']=='dataframe' and request['partitioned']
    for name,member in [('gravitino_delta','gravitino_delta/__init__.py'),('gravitino_delta.tables','gravitino_delta/tables.py')]:
        assert modules[name]=={'file':runtime['pythonZip']+'/'+member,'archive':runtime['pythonZip'],'sha256':runtime['pythonZipMembers'][member]}, 'Packaged import origin differs'
    assert hashlib.sha256(Path(runtime['pythonZip']).read_bytes()).hexdigest()==runtime['pythonZipSha256']
    SparkContext._ensure_initialized()
    bridge=SparkContext._jvm.org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerLiveCdfMerge(sys.argv[1])
    jsc=SparkContext._jvm.org.apache.spark.api.java.JavaSparkContext.fromSparkContext(bridge.session().sparkContext())
    context=SparkContext(gateway=SparkContext._gateway,jsc=jsc)
    spark=SparkSession(context,bridge.session())
    identities={}
    try:
        # Complete the public API/version handshake before any target mutation.
        GravitinoDeltaTable.forName(spark, request['steps'][0]['operation']['target'])
        for step in request['steps']:
            bridge.before(step['name'])
            if step['name']=='distinct':bridge.observeSource()
            identities[step['name']]=dict(bridge.queryIdentity())
            failure=None
            try:
                execute_operation(spark, step['operation'])
            except Py4JJavaError as error:
                failure=error.java_exception
            except Exception as error:
                failure=getattr(error,'_origin',None)
                if failure is None:raise
            bridge.after(step['name'],failure)
        observed={'frontend':'classic-pyspark-dataframe','pysparkVersion':__import__('pyspark').__version__,
            'python':sys.version,'apiVersion':1,'modules':modules,'operations':[s['operation'] for s in request['steps']],
            'queryIdentities':identities}
        Path(request['output'],'python-observed.json').write_text(json.dumps(observed)+'\n')
    finally:bridge.close()


if __name__=='__main__':main()

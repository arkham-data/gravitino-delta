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

"""Real classic PySpark frontend: Python submits every named MERGE via spark.sql."""
import json
from pathlib import Path
import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from py4j.protocol import Py4JJavaError


def main():
    request=json.loads(Path(sys.argv[1]).read_text())
    SparkContext._ensure_initialized()
    bridge=SparkContext._jvm.org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerLiveCdfMerge(sys.argv[1])
    jsc=SparkContext._jvm.org.apache.spark.api.java.JavaSparkContext.fromSparkContext(bridge.session().sparkContext())
    context=SparkContext(gateway=SparkContext._gateway,jsc=jsc)
    spark=SparkSession(context,bridge.session())
    try:
        for step in request['steps']:
            sql=bridge.before(step['name'])
            if request.get('partitioned') and step['name']=='distinct':bridge.observeSource()
            failure=None
            try:
                # The actual Python frontend call is the proof: no Scala SQL forwarding helper.
                spark.sql(sql).collect()
            except Py4JJavaError as error:
                failure=error.java_exception
            except Exception as error:
                # PySpark's captured Java exceptions retain their original JVM throwable.
                failure=getattr(error,'_origin',None)
                if failure is None:raise
            bridge.after(step['name'],failure)
        Path(request['output'],'python-observed.json').write_text(json.dumps({'frontend':'classic-pyspark','pysparkVersion':__import__('pyspark').__version__,'queries':[s['sql'] for s in request['steps']],'python':sys.version})+'\n')
    finally:bridge.close()


if __name__=='__main__':main()

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

"""Independent setup/native CDF interval oracle; no governed authorization claims."""
import json
import hashlib
import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession
from harness import credentials


def main():
    config = json.loads(Path(sys.argv[1]).read_text())
    output = Path(config['output'])
    access, secret = credentials(config['primary']['credentialsFile'])
    builder = SparkSession.builder.master('local[2]').appName('cdf-live-native-oracle')
    options = {'spark.ui.enabled':'false','spark.driver.host':'127.0.0.1','spark.driver.bindAddress':'127.0.0.1',
        'spark.sql.shuffle.partitions':'2','spark.databricks.delta.snapshotPartitions':'2',
        'spark.sql.extensions':'io.delta.sql.DeltaSparkSessionExtension',
        'spark.sql.catalog.spark_catalog':'org.apache.spark.sql.delta.catalog.DeltaCatalog',
        'spark.hadoop.fs.s3a.endpoint':config['primary']['endpoint'],'spark.hadoop.fs.s3a.endpoint.region':config['primary']['region'],
        'spark.hadoop.fs.s3a.path.style.access':'true','spark.hadoop.fs.s3a.connection.ssl.enabled':'false',
        'spark.hadoop.fs.s3a.aws.credentials.provider':'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
        'spark.hadoop.fs.s3a.access.key':access,'spark.hadoop.fs.s3a.secret.key':secret,
        'spark.redaction.regex':'(?i)secret|password|token|access[.]?key|s3-access-key-id'}
    for key,value in options.items(): builder = builder.config(key,value)
    spark = builder.getOrCreate()
    try:
        jvm = spark._jvm
        identity = {'sparkUser':spark.sparkContext._jsc.sc().sparkUser(), 'ugi':jvm.org.apache.hadoop.security.UserGroupInformation.getCurrentUser().getShortUserName(),
                    'SPARK_USER':os.environ.get('SPARK_USER'), 'HADOOP_USER_NAME':os.environ.get('HADOOP_USER_NAME')}
        assert set(identity.values()) == {config['nativeIdentity']}
        assert spark.version == config['spark']
        assert jvm.java.lang.System.getProperty('java.specification.version')=='17'
        origins={}
        loader=jvm.java.lang.Thread.currentThread().getContextClassLoader()
        for name in ['org.apache.spark.sql.SparkSession','org.apache.spark.sql.delta.DeltaLog','org.apache.hadoop.fs.FileSystem','org.apache.hadoop.fs.s3a.S3AFileSystem','com.amazonaws.services.s3.AmazonS3Client','scala.Predef$']:
            cls=loader.loadClass(name)
            path=Path(str(cls.getProtectionDomain().getCodeSource().getLocation().getPath())).resolve()
            origins[name]={'origin':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
        assert Path(origins['org.apache.spark.sql.delta.DeltaLog']['origin']).name=='delta-spark_2.12-'+config['delta']+'.jar'
        assert Path(origins['org.apache.spark.sql.SparkSession']['origin']).name=='spark-sql_2.12-'+config['spark']+'.jar'
        assert Path(origins['scala.Predef$']['origin']).name=='scala-library-2.12.21.jar'
        results=[]
        for table in config['tables']:
            if config['mode'] == 'seed':
                from s3_objects import OwnedObjects
                assert not OwnedObjects(config['primary'],table['prefix']).manifest(), 'Fresh prefix required; no replay'
                writer = spark.createDataFrame(table['rows'], 'id LONG, secret STRING, region STRING').coalesce(1).write.format('delta').mode('errorifexists')
                if table.get('cdf'): writer=writer.option('delta.enableChangeDataFeed','true')
                writer.option('txnAppId',config['seedAppId']).option('txnVersion','0').save(table['location'])
            frame=spark.read.format('delta').load(table['location'])
            rows=[list(r) for r in frame.orderBy('id','secret','region').collect()]
            detail=spark.sql('DESCRIBE DETAIL delta.`'+table['location']+'`').select('id','minReaderVersion','minWriterVersion','properties').first().asDict()
            history=[r.asDict() for r in spark.sql('DESCRIBE HISTORY delta.`'+table['location']+'`').collect()]
            log=jvm.org.apache.spark.sql.delta.DeltaLog.forTable(spark._jsparkSession,table['location'])
            snapshot=log.update(False,jvm.scala.Option.empty(),jvm.scala.Option.empty())
            record={'name':table['name'],'location':table['location'],'rows':rows,'version':snapshot.version(),'schemaJson':frame.schema.json(),
                    'native':detail,'protocol':json.loads(snapshot.protocol().json()),'metadata':json.loads(snapshot.metadata().json()),'history':history}
            intervals=[]
            for interval in config.get('intervals',[]) if table['name']=='cdf_target' else []:
                attempt={'startingVersion':interval[0],'endingVersion':interval[1]}
                try:
                    changes=(spark.read.format('delta').option('readChangeFeed','true').option('startingVersion',str(interval[0])).option('endingVersion',str(interval[1])).load(table['location'])
                        .select('id','secret','region','_change_type','_commit_version'))
                    attempt['rows']=[list(r) for r in changes.orderBy('id','secret','region','_change_type','_commit_version').collect()]
                    attempt['error']=None
                except Exception as error:
                    attempt['error']={'class':type(error).__name__,'errorClass':error.getErrorClass() if hasattr(error,'getErrorClass') else None}
                intervals.append(attempt)
            record['intervals']=intervals
            results.append(record)
            (output/'progress.json').write_text(json.dumps({'completed':results},default=str,indent=2))
        (output/'result.json').write_text(json.dumps({'mode':config['mode'],'identity':identity,'spark':spark.version,'delta':config['delta'],'java':'17','origins':origins,'tables':results},default=str,indent=2)+'\n')
    finally: spark.stop()


if __name__ == '__main__': main()

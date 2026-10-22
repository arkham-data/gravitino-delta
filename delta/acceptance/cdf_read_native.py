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
    builder = SparkSession.builder.master('local[2]').appName('cdf-read-native-oracle')
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
        table=config['table']; location=table['location']; target='delta.`'+location+'`'
        if config['mode']=='seed':
            from s3_objects import OwnedObjects
            assert not OwnedObjects(config['primary'],table['prefix']).manifest(), 'Fresh prefix required; no replay'
            spark.createDataFrame([[1,'alpha','allowed'],[1,'alpha','allowed'],[2,'beta','blocked'],[3,'gamma','allowed']],
                'id LONG, secret STRING, region STRING').coalesce(1).write.format('delta').mode('errorifexists').option('delta.enableChangeDataFeed','true').save(location)
            spark.sql("UPDATE "+target+" SET region = 'blocked' WHERE id = 3").collect()
            spark.sql('DELETE FROM '+target+' WHERE id = 1 OR id = 2').collect()
            spark.sql("MERGE INTO "+target+" t USING (SELECT 3L id, 'new' secret, 'allowed' region UNION ALL SELECT 4L, 'four', 'allowed' UNION ALL SELECT 5L, 'five', 'blocked') s ON t.id=s.id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *").collect()
            spark.sql("ALTER TABLE "+target+" SET TBLPROPERTIES ('fixture.empty'='true')").collect()
        log=jvm.org.apache.spark.sql.delta.DeltaLog.forTable(spark._jsparkSession,location)
        snapshot=log.update(False,jvm.scala.Option.empty(),jvm.scala.Option.empty())
        assert snapshot.version()==4
        def changes(start,end):
            frame=spark.read.format('delta').option('readChangeFeed','true').option('startingVersion',start).option('endingVersion',end).load(location)
            return [[r.id,r.secret,r.region,r._change_type,r._commit_version,str(r._commit_timestamp)] for r in frame.collect()]
        rows=changes(0,4); assert changes(4,4)==[]
        from s3_objects import OwnedObjects
        objects=OwnedObjects(config['primary'],table['prefix']).manifest()
        covered=sorted(int(Path(v['key']).stem) for v in objects if '/_delta_log/' in v['key'] and v['key'].endswith('.json'))
        result={'identity':identity,'spark':spark.version,'delta':config['delta'],'java':'17','scala':'2.12.21','origins':origins,
          'nativeRows':rows,'nativeHead':snapshot.version(),'coveredVersions':covered,'requested':[0,4],'emptyRows':[],
          'nativeId':snapshot.metadata().id(),'schemaJson':snapshot.metadata().schemaString(),'objects':objects}
        if config['mode']=='damage':
            candidates=[v['key'] for v in objects if '/_change_data/' in v['key'] and v['key'].endswith('.parquet')]
            assert candidates, 'Missing dedicated CDF file for owned fault injection'
            key=sorted(candidates)[0]
            path=jvm.org.apache.hadoop.fs.Path('s3a://'+config['primary']['bucket']+'/'+key)
            fs=path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
            assert fs.delete(path,False), 'Single owned CDF file removal failed; no replay'
            assert not fs.exists(path)
            result['fault']={'removed':key,'originalObjects':objects,'controlledSetup':True}
        (output/'result.json').write_text(json.dumps(result,default=str,indent=2)+'\n')
    finally: spark.stop()


if __name__ == '__main__': main()

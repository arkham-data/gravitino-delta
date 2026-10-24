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
import time

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
        from s3_objects import OwnedObjects
        tables={table['name']:table for table in config['tables']}
        for name,table in tables.items():
            assert not OwnedObjects(config['primary'],table['prefix']).manifest(), 'Fresh prefix required'
            location=table['location'];target='delta.`'+location+'`'
            if name in ['source','same','incomplete']:
                rows=[[1,'changed','allowed'],[2,'gone','allowed'],[3,'hidden','blocked'],[6,'dup','allowed'],[6,'dup','allowed']]
            elif name=='ambiguous_target':rows=[[6,'old','allowed'],[9,'stable','allowed']]
            else:rows=[[1,'old','allowed'],[2,'old2','allowed'],[4,'hidden','blocked'],[9,'stable','allowed']]
            spark.createDataFrame(rows,'id LONG, secret STRING, region STRING').coalesce(1).write.format('delta').mode('errorifexists').option('delta.enableChangeDataFeed','true').save(location)
            if name in ['source','same','incomplete']:
                spark.sql("MERGE INTO "+target+" t USING (SELECT 1L id, 'new' secret, 'allowed' region, 'update' op UNION ALL SELECT 2L, 'gone', 'allowed', 'delete' UNION ALL SELECT 4L, 'four', 'allowed', 'insert' UNION ALL SELECT 5L, 'five', 'blocked', 'insert') s ON t.id=s.id WHEN MATCHED AND s.op='delete' THEN DELETE WHEN MATCHED THEN UPDATE SET secret=s.secret, region=s.region WHEN NOT MATCHED THEN INSERT (id,secret,region) VALUES(s.id,s.secret,s.region)").collect()
                spark.sql("ALTER TABLE "+target+" SET TBLPROPERTIES ('fixture.empty'='true')").collect()
        def snapshot(name,after=-1):
            table=tables[name];location=table['location']
            log=jvm.org.apache.spark.sql.delta.DeltaLog.forTable(spark._jsparkSession,location)
            snap=log.update(False,jvm.scala.Option.empty(),jvm.scala.Option.empty());version=snap.version()
            objects=OwnedObjects(config['primary'],table['prefix'])
            logs=[]
            for number in range(after+1,version+1):
                key=table['prefix']+'_delta_log/'+str(number).zfill(20)+'.json'
                logs.extend(json.loads(line) for line in objects.request('GET',key).decode().splitlines())
            rows=[list(row) for row in spark.read.format('delta').load(location).collect()]
            changes=[]
            if after>=0 and version>after:
                changes=[[row.id,row.secret,row.region,row._change_type] for row in spark.read.format('delta').option('readChangeFeed','true').option('startingVersion',after+1).option('endingVersion',version).load(location).collect()]
            return {'version':version,'id':snap.metadata().id(),'rows':rows,'actions':logs,'cdc':changes}
        initial={name:snapshot(name) for name in tables}
        source=[[row.id,row.secret,row.region,row._change_type,row._commit_version] for row in spark.read.format('delta').option('readChangeFeed','true').option('startingVersion',1).option('endingVersion',1).load(tables['source']['location']).collect()]
        (output/'ready.json').write_text(json.dumps({'identity':identity,'spark':spark.version,'delta':config['delta'],'java':'17','scala':'2.12.21','origins':origins,'tables':initial,'sourceNative':source},indent=2)+'\n')
        cursor=0
        while True:
            path=output/(str(cursor)+'-request.json');deadline=time.monotonic()+1800
            while not path.exists():
                assert time.monotonic()<deadline,'Observer control timeout'
                time.sleep(.05)
            request=json.loads(path.read_text());mode=request['mode']
            if mode=='stop':break
            if mode=='damage':
                table=tables['incomplete'];objects=OwnedObjects(config['primary'],table['prefix']).manifest()
                key=next(v['key'] for v in objects if '/_change_data/' in v['key'] and v['key'].endswith('.parquet'))
                path=jvm.org.apache.hadoop.fs.Path('s3a://'+config['primary']['bucket']+'/'+key)
                fs=path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration());assert fs.delete(path,False) and not fs.exists(path)
                result={'removed':key,'objectsBefore':objects}
            else:result=snapshot(request['table'],request.get('after',-1))
            (output/(str(cursor)+'-result.json')).write_text(json.dumps(result,indent=2)+'\n');cursor+=1
    finally:spark.stop()


if __name__=='__main__':main()

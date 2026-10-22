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

"""Reject incomplete live consumption evidence while comparing exact multisets."""
import copy
import unittest
import tempfile
import zipfile
import warnings
from pathlib import Path
from cdf_read_live_checks import verify_result


def valid():
    native=[[1,'raw','allowed','insert',0,'2001-01-01 00:00:00.0'],[1,'raw','allowed','insert',0,'2001-01-01 00:00:00.0'],[2,'b','blocked','delete',1,'2001-01-01 00:00:00.0'],[3,'x','allowed','update_preimage',2,'2001-01-01 00:00:00.0'],[3,'y','allowed','update_postimage',2,'2001-01-01 00:00:00.0'],[4,'z','allowed','delete',3,'2001-01-01 00:00:00.0']]
    rows=[[0,'MASKED',r[2],r[3],0,'2000-01-01 00:00:00.0'] for r in native if r[2]=='allowed']
    return {'spark':'3.5.4','delta':'3.3.0','java':'17','scala':'2.12.21','ranger':'2.8.0',
      'requested':[0,4],'nativeHead':4,'coveredVersions':[0,1,2,3,4],'nativeRows':native,
      'sql':{'outcome':'success','rows':rows},'dataframe':{'outcome':'success','rows':copy.deepcopy(rows)},
      'empty':{'outcome':'success','bounds':[4,4],'rows':[]},
      'incomplete':{'outcome':'failed','bounds':[0,4],'table':'incomplete','authorized':True,'rows':None,'failure':['java.io.FileNotFoundException']},
      'retained':{'outcome':'denied','rows':None,'failure':['java.lang.SecurityException']},
      'freshDenied':{'outcome':'denied','rows':None,'failure':['java.lang.SecurityException']},
      'freshGranted':{'outcome':'success','rows':copy.deepcopy(rows)},
      'grant':{'publicPrivilege':'SELECT_TABLE','downloadedSelect':True,'policy':10,'downloadSha256':'a'*64},
      'revoke':{'publicPrivilege':'SELECT_TABLE','downloadedSelect':False,'policy':11,'downloadSha256':'b'*64},
      'identities':{'setup':'setup','metadata':'meta','application':'app'},
      'sourceBefore':'c'*64,'sourceAfter':'c'*64,'registrationBefore':'d'*64,'registrationAfter':'d'*64,
      'faultBefore':{'outcome':'success','rows':copy.deepcopy(native)},'faultNativeRows':native,
      'persistedRegistration':True,'productionDiscovery':True,'runtimeBound':True,'exitCode':0}


class TestCdfReadLiveChecks(unittest.TestCase):
    def test_valid_and_order_independent(self):
        result=valid(); verify_result(result)
        result['sql']['rows'].reverse(); verify_result(result)

    def test_missing_terminal_coverage(self):
        result=valid(); result['coveredVersions'].pop()
        with self.assertRaises(AssertionError): verify_result(result)

    def test_rows_and_multiplicity(self):
        for mode in ['sql','dataframe','freshGranted']:
            for mutation in ['missing','changed','duplicate_loss','raw_metadata','raw_version','boolean_key','raw_key']:
                result=valid(); rows=result[mode]['rows']
                if mutation in ['missing','duplicate_loss']: rows.pop(0)
                elif mutation=='changed': rows[0][3]='delete'
                elif mutation=='raw_metadata': rows[0][5]='2001-01-01 00:00:00.0'
                elif mutation=='raw_version': rows[0][4]=42
                elif mutation=='boolean_key': rows[0][0]=False
                else: rows[0][0]=1
                with self.assertRaises(AssertionError): verify_result(result)

    def test_runtime_pins(self):
        for key,value in [('spark','3.4.0'),('delta','3.2.0'),('ranger','2.7.0'),('scala','2.12.18'),('java','21')]:
            result=valid(); result[key]=value
            with self.assertRaises(AssertionError): verify_result(result)

    def test_public_download_and_denial_required(self):
        for key in ['grant','revoke','retained','freshDenied','faultBefore','faultNativeRows']:
            result=valid(); del result[key]
            with self.assertRaises(AssertionError): verify_result(result)
        for key in ['retained','freshDenied','incomplete']:
            result=valid(); result[key]={'outcome':'success','rows':[]}
            with self.assertRaises(AssertionError): verify_result(result)

    def test_changed_download_or_missing_file_proof(self):
        for key,value in [('grant',False),('revoke',True)]:
            result=valid(); result[key]['downloadedSelect']=value
            with self.assertRaises(AssertionError): verify_result(result)
        result=valid();result['revoke']['policy']=result['grant']['policy']
        with self.assertRaises(AssertionError):verify_result(result)
        for field,value in [('authorized',False),('failure',['java.lang.SecurityException'])]:
            result=valid();result['incomplete'][field]=value
            with self.assertRaises(AssertionError):verify_result(result)

    def test_shared_source_inventory_includes_additions_and_deletions(self):
        from cdf_read_live_package import shared_sources, companion_sources
        original={'server/src/main/java/S.java':'a','spark-connector/v3.5/spark-delta-ranger/src/main/scala/C.scala':'b'}
        self.assertEqual({'server/src/main/java/S.java':'a'},shared_sources(original))
        added=dict(original,**{'server/src/main/java/New.java':'c'})
        self.assertNotEqual(shared_sources(original),shared_sources(added))
        self.assertNotEqual(shared_sources(original),shared_sources({}))
        tests=dict(original,**{'spark-connector/v3.5/spark-delta-ranger/src/test/scala/App.scala':'d'})
        self.assertEqual(companion_sources(original),companion_sources(tests))
        changed=dict(original);changed['spark-connector/v3.5/spark-delta-ranger/src/main/scala/C.scala']='changed'
        self.assertNotEqual(companion_sources(original),companion_sources(changed))

    def test_application_reuse_requires_all_zip_entry_bytes(self):
        from cdf_read_live_package import archive_entries
        with tempfile.TemporaryDirectory() as directory:
            first=Path(directory)/'one.jar';second=Path(directory)/'two.jar'
            for path,year in [(first,2020),(second,2021)]:
                with zipfile.ZipFile(path,'w') as jar:
                    jar.writestr(zipfile.ZipInfo('A.class',(year,1,1,0,0,0)),b'class-body')
                    jar.writestr('META-INF/MANIFEST.MF',b'manifest')
            self.assertNotEqual(first.read_bytes(),second.read_bytes())
            self.assertEqual(archive_entries(first),archive_entries(second))
            with zipfile.ZipFile(second,'a') as jar:jar.writestr('extra-resource',b'changed')
            self.assertNotEqual(archive_entries(first),archive_entries(second))
            with warnings.catch_warnings():
                warnings.simplefilter('ignore',UserWarning)
                with zipfile.ZipFile(second,'a') as jar:jar.writestr('A.class',b'other')
            with self.assertRaises(AssertionError):archive_entries(second)

    def test_readonly_identity_and_discovery(self):
        for key in ['sourceAfter','registrationAfter','persistedRegistration','productionDiscovery','runtimeBound','exitCode']:
            result=valid(); result[key]=False
            with self.assertRaises(AssertionError): verify_result(result)
        result=valid(); result['identities']['application']='meta'
        with self.assertRaises(AssertionError): verify_result(result)


if __name__=='__main__': unittest.main()

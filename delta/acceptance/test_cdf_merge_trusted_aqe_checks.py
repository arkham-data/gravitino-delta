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

"""Reject AQE labels without original final payload and immutable runtime evidence."""
import copy
import json
import zipfile
import unittest
import tempfile
from pathlib import Path

from cdf_merge_live_checks import verify_result
from test_cdf_merge_partitioned_checks import partition_fixture


class TestTrustedAqeChecks(unittest.TestCase):
    def test_flag_without_payload_is_not_qualification(self):
        value = partition_fixture(frontend="dataframe")
        value["executionProfile"] = "adaptive-stock"
        value["requestedSettings"] = {"spark.sql.adaptive.enabled": "true"}
        value["effectiveSettings"] = dict(value["requestedSettings"])
        with self.assertRaisesRegex(AssertionError, "adaptive"):
            verify_result(value)

    def test_rejects_metadata_force_apply_missing_final_and_wrong_operator(self):
        from cdf_merge_live_checks import verify_adaptive_observation, file_ref
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'plan.txt'
            path.write_text('AdaptiveSparkPlan isFinalPlan=true\nShuffleQueryStage 0\nAQEShuffleRead coalesced')
            record={'step':'ordinaryRead','phase':'ordinary','isPayload':True,
                'rootClass':'org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec','isFinalPlan':True,
                'materializedShuffle':1,'materializedBroadcast':0,'coalesced':True,'local':False,'skew':False,'reuse':False,
                'operators':['AdaptiveSparkPlanExec','ShuffleQueryStageExec','AQEShuffleReadExec'], 'plan':file_ref(path)}
            verify_adaptive_observation(record)
            for mutate in [lambda r:r.update(isPayload=False),lambda r:r.update(materializedShuffle=0),
                           lambda r:r.update(isFinalPlan=False),lambda r:r.update(reuse=True),
                           lambda r:r.update(materializedBroadcast=1),lambda r:r.update(phase='metadata')]:
                changed=copy.deepcopy(record);mutate(changed)
                with self.assertRaises(AssertionError):verify_adaptive_observation(changed)
            path.write_text('AdaptiveSparkPlan isFinalPlan=false')
            with self.assertRaises(AssertionError):verify_adaptive_observation(record)

    def test_profile_is_written_into_startup_request(self):
        import cdf_merge_live
        with tempfile.TemporaryDirectory() as directory:
            properties = Path(directory) / "spark.properties"
            properties.write_text("spark.sql.adaptive.enabled=false\nspark.sql.shuffle.partitions=2\n")
            request = {"sparkConfigurationFile": str(properties)}
            self.assertTrue(hasattr(cdf_merge_live, "configure_execution_profile"))
            cdf_merge_live.configure_execution_profile(request, "adaptive-stock")
            self.assertEqual("adaptive-stock", request["executionProfile"])
            self.assertIn("spark.sql.adaptive.enabled=true", properties.read_text())
            self.assertEqual("true", request["requestedSettings"]["spark.sql.adaptive.enabled"])

    def test_supplemental_archive_original_exits_pair_import_and_rows(self):
        from cdf_read_live_package import verify_supplemental_artifacts, archive_entries, sha
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'supplemental.jar'
            with zipfile.ZipFile(path,'w') as archive:archive.writestr('Provider.class',b'original')
            manifest={'supplementalArtifacts':{'original.jar':{'path':str(path),'sha256':sha(path),'members':archive_entries(path)}}}
            verify_supplemental_artifacts(manifest)
            path.write_bytes(b'modified')
            with self.assertRaises(AssertionError):verify_supplemental_artifacts(manifest)
        for mutate in [lambda r:r['provenance'].pop('nativeExit'),
                       lambda r:r['provenance']['apps']['python'].pop('exit'),
                       lambda r:r.update(spark='3.5.6'),
                       lambda r:r['stages']['same']['cdc'].append(r['stages']['same']['cdc'][0])]:
            value=partition_fixture(frontend='dataframe');mutate(value)
            with self.assertRaises((AssertionError,KeyError)):verify_result(value)

    def test_reused_app_binds_both_archive_hashes_and_equal_members(self):
        from cdf_merge_live_checks import verify_app_archive, file_ref
        with tempfile.TemporaryDirectory() as directory:
            original=Path(directory)/'original.jar';built=Path(directory)/'built.jar'
            for path,year in [(original,2025),(built,2026)]:
                with zipfile.ZipFile(path,'w') as archive:
                    info=zipfile.ZipInfo('App.class',(year,1,1,0,0,0));archive.writestr(info,b'compiled')
            self.assertNotEqual(file_ref(original)['sha256'],file_ref(built)['sha256'])
            manifest={'app':str(original),'jarHashes':{str(original):file_ref(original)['sha256']}}
            receipt={'appBuildArtifact':str(built),'artifacts':{str(built):file_ref(built)['sha256']}}
            verify_app_archive(manifest,receipt)
            with zipfile.ZipFile(built,'w') as archive:archive.writestr('App.class',b'changed')
            receipt['artifacts'][str(built)]=file_ref(built)['sha256']
            with self.assertRaises(AssertionError):verify_app_archive(manifest,receipt)

    def test_adaptive_sql_request_preserves_sql_frontend(self):
        from cdf_merge_live import steps
        plan={'executionProfile':'adaptive-stock','partitioned':True,'catalog':'c','database':'d',
              'nativeInitial':{'tables':{n:{'id':n} for n in ['source','same']}}}
        actual=steps(plan,True,'sql')
        self.assertEqual(7,len(actual))
        self.assertTrue(all('sql' in s and 'operation' not in s for s in actual))
        self.assertNotIn('/*+',actual[1]['sql'])
        plan['partitioned']=False;plan['nonce']='fixture';plan['nativeInitial']['tables']['incomplete']={'id':'incomplete'}
        self.assertTrue(all('sql' in s for s in steps(plan,False,'sql')))

    def test_nonadaptive_exchange_has_truthful_label(self):
        from cdf_merge_live_checks import verify_adaptive_observation, file_ref
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "plan.txt"
            path.write_text("Exchange hashpartitioning(id, 2)")
            value = {"step": "cdf", "phase": "cdf", "isPayload": True,
                     "diagnosticVersion": 2, "outcome": "nonadaptive-exchange",
                     "rootClass": "ShuffleExchangeExec", "operators": ["ShuffleExchangeExec"],
                     "isFinalPlan": False, "materializedShuffle": 0, "materializedBroadcast": 0,
                     "coalesced": False, "local": False, "skew": False, "reuse": False,
                     "plan": file_ref(path)}
            verify_adaptive_observation(value)
            value["outcome"] = "exchange-free"
            with self.assertRaises(AssertionError):
                verify_adaptive_observation(value)
            # Historical v1 used this label for either kind of nonadaptive plan.
            value.pop("diagnosticVersion")
            verify_adaptive_observation(value)
            value["diagnosticVersion"] = 2
            value["outcome"] = "nonadaptive-exchange"
            value["operators"] = ["LocalTableScanExec"]
            value["rootClass"] = "LocalTableScanExec"
            with self.assertRaises(AssertionError):
                verify_adaptive_observation(value)
            value["outcome"] = "exchange-free"
            path.write_text("LocalTableScan")
            value["plan"] = file_ref(path)
            verify_adaptive_observation(value)

    def test_read_parity_package_profile(self):
        from cdf_read_live_package import app_profile
        profile = app_profile("read-parity")
        self.assertEqual("readParityLiveThinJar", profile["task"])
        self.assertEqual("readParityLiveAppOut", profile["property"])
        self.assertEqual("delta/acceptance/read_parity_live.gradle", profile["init"])
        self.assertTrue(profile["class"].endswith("/DeltaRangerLiveReadParity"))
        self.assertEqual("cdfReadLiveThinJar", app_profile("read")["task"])
        self.assertEqual("cdfMergeLiveThinJar", app_profile("merge")["task"])

    def test_legacy_nonadaptive_evidence_stays_valid(self):
        verify_result(partition_fixture(frontend="dataframe"))


if __name__ == "__main__":
    unittest.main()

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

import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from read_parity_checks import verify_result


class TestReadParityChecks(unittest.TestCase):
    def setUp(self):
        from read_parity_checks import CASE_SPECS
        from cdf_merge_live_checks import file_ref
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        root = Path(self.directory.name)
        def artifact(name, value):
            path = root / name
            path.write_text(json.dumps(value))
            return file_ref(path)
        policy = artifact("policy.json", {"policyVersion": 10})
        applied = artifact("applied.json", {"policy": 10, "downloadSha256": "downloaded"})
        plan = root / "plan.txt"
        plan.write_text("AdaptiveSparkPlan isFinalPlan=true\nShuffleQueryStage 0")
        ref = file_ref(plan)
        runtime = {"spark":"3.5.4", "delta":"3.3.0", "providers":{"Provider":"provider.jar"},
                   "jarHashes":{"provider.jar":"jarhash"}, "providerClasses":{"Provider":"classhash"}}
        self.value = {"schemaVersion":1,"spark":"3.5.4","delta":"3.3.0","engine":"delta",
                      "catalogRoute":"gravitino","catalog":"c","database":"db",
                      "rangerServerVersion":"2.8.0","rangerClientVersion":"2.8.0",
                      "runtimeManifest":artifact("runtime.json",runtime),
                      "deployedRanger":artifact("ranger.json",{"deployedRangerVersion":"2.8.0"}),
                      "processExits":{name:artifact(name+".json",{"exitCode":0}) for name in ["python","native","supervisor"]},
                      "origins":artifact("origins.json",{"Provider":{"jar":"provider.jar","jarSha256":"jarhash","classSha256":"classhash"}}),
                      "registration":artifact("registration.json",{"beforeRestart":{"source":"id"},"afterRestart":{"source":"id"},"persisted":{"source":"id"}}),
                      "cases":[]}
        for name,(api,rows,columns) in CASE_SPECS.items():
            case = {"name":name,"api":api,"actionRoute":"native-pyspark",
                    "outcome":"denied" if rows is None else "allowed","expectedRows":rows,"observedRows":rows,
                    "expectedColumns":sorted(columns),"originalDemands":[{"columns":columns}],
                    "rangerRequests":[{"resource":{"catalog":"c","database":"db","table":"source"}}],
                    "policySnapshot":policy,"appliedPolicy":applied,"analyzedPlan":ref,"finalPlan":ref}
            if api == "toPandas":
                case["arrow"] = {"enabled":name=="pandas-arrow-on","fallback":False,"observed":name=="pandas-arrow-on",
                                 "pandas":"2.2.3","pyarrow":"17.0.0","dtypes":{"id":"int64","email":"object"},
                                 "pythonTZ":"UTC","sessionTZ":"UTC"}
            if name == "aqe-join":
                case["execution"] = {"isPayload":True,"phase":"ordinary","plan":ref,
                                     "operators":["AdaptiveSparkPlanExec","ShuffleQueryStageExec"],
                                     "rootClass":"AdaptiveSparkPlanExec","isFinalPlan":True,
                                     "materializedShuffle":1,"materializedBroadcast":0,
                                     "coalesced":False,"local":False,"skew":False,"reuse":False}
            self.value["cases"].append(case)
        # Real immutable package verification has its own artifact tests. Isolate only that
        # external prerequisite here so each mutation exercises this checker, not a setup failure.
        self.package = patch("read_parity_checks.verify_runtime")
        self.package.start()
        self.addCleanup(self.package.stop)

    def case(self, name):
        return next(case for case in self.value["cases"] if case["name"] == name)

    def test_complete_semantics_pass(self):
        verify_result(self.value)

    def test_wrong_mask_count_distinct_and_null_order(self):
        for name, observed in [("constant-email-count",[[3]]),("null-email-count",[[4]]),
                               ("constant-distinct",[["z"]]),("null-distinct",[]),
                               ("raw-null-order",[[1,"z"],[2,"a"],[3,None],[5,"b"]])]:
            with self.subTest(name=name):
                value=copy.deepcopy(self.value)
                next(case for case in value["cases"] if case["name"]==name)["observedRows"]=observed
                with self.assertRaisesRegex(AssertionError,"exact rows/order"):
                    verify_result(value)

    def test_arrow_flag_without_transport(self):
        self.case("pandas-arrow-on")["arrow"]["observed"] = False
        with self.assertRaisesRegex(AssertionError,"actual Arrow transport"):
            verify_result(self.value)

    def test_arrow_fallback_enabled(self):
        self.case("pandas-arrow-on")["arrow"]["fallback"] = True
        with self.assertRaisesRegex(AssertionError,"Arrow flags"):
            verify_result(self.value)

    def test_missing_original_demand(self):
        self.case("id-count")["originalDemands"] = []
        with self.assertRaisesRegex(AssertionError,"missing original demand"):
            verify_result(self.value)

    def test_view_alias_is_not_a_resource(self):
        self.case("view-count")["rangerRequests"][0]["resource"]["table"] = "local_view"
        with self.assertRaisesRegex(AssertionError,"view alias"):
            verify_result(self.value)

    def test_count_cannot_claim_adaptive_payload_route(self):
        self.case("table-count")["actionRoute"] = "adaptive-payload"
        with self.assertRaisesRegex(AssertionError,"native action route"):
            verify_result(self.value)

    def test_missing_child_exit(self):
        del self.value["processExits"]["native"]
        with self.assertRaisesRegex(AssertionError,"child exits missing"):
            verify_result(self.value)

    def test_failed_child_exit(self):
        from cdf_merge_live_checks import file_ref
        path=Path(self.value["processExits"]["python"]["path"])
        path.write_text(json.dumps({"exitCode":1}))
        self.value["processExits"]["python"] = file_ref(path)
        with self.assertRaisesRegex(AssertionError,"child exit failed"):
            verify_result(self.value)

    def test_wrong_pins(self):
        for key,value in [("spark","3.5.5"),("delta","3.3.1"),
                          ("rangerServerVersion","2.7.0"),("rangerClientVersion","2.7.0")]:
            with self.subTest(key=key):
                changed=copy.deepcopy(self.value);changed[key]=value
                with self.assertRaisesRegex(AssertionError,"pin"):
                    verify_result(changed)

    def test_wrong_loaded_provider(self):
        from cdf_merge_live_checks import file_ref
        path=Path(self.value["origins"]["path"])
        origins=json.loads(path.read_text());origins["Provider"]["jar"]="wrong.jar"
        path.write_text(json.dumps(origins));self.value["origins"]=file_ref(path)
        with self.assertRaisesRegex(AssertionError,"loaded provider differs"):
            verify_result(self.value)

    def test_altered_supplemental_jar(self):
        from cdf_merge_live_checks import file_ref
        from cdf_read_live_package import sha, archive_entries
        import zipfile
        root=Path(self.directory.name);jar=root/"extra.jar"
        with zipfile.ZipFile(jar,"w") as output:output.writestr("Original.class",b"original")
        path=Path(self.value["runtimeManifest"]["path"]);runtime=json.loads(path.read_text())
        runtime["supplementalArtifacts"]={"original.jar":{"path":str(jar),"sha256":sha(jar),"members":archive_entries(jar)}}
        path.write_text(json.dumps(runtime));self.value["runtimeManifest"]=file_ref(path)
        jar.write_bytes(b"changed")
        with self.assertRaisesRegex(AssertionError,"Supplemental artifact changed"):
            verify_result(self.value)


if __name__ == "__main__":
    unittest.main()

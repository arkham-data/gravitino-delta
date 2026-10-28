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
        applied = artifact("applied.json", {"policy": 10, "downloadSha256":policy["sha256"],"download":policy,
                           "downloadEvent":{"downstreamResponseCompleted":True,"downloadKind":"policy",
                                            "responseBodyFile":policy["path"],"responseSha256":policy["sha256"]}})
        plan = root / "plan.txt"
        plan.write_text("AdaptiveSparkPlan isFinalPlan=true\nShuffleQueryStage 0")
        ref = file_ref(plan)
        import zipfile
        self.provider="org.apache.ranger.plugin.service.RangerBasePlugin"
        jar=root/"provider.jar"
        with zipfile.ZipFile(jar,"w") as archive:
            archive.writestr("META-INF/maven/org.apache.ranger/ranger-plugins-common/pom.properties","version=2.8.0\n")
            archive.writestr(self.provider.replace(".","/")+".class",b"bytecode")
        jarhash=hashlib.sha256(jar.read_bytes()).hexdigest();classhash=hashlib.sha256(b"bytecode").hexdigest()
        runtime = {"spark":"3.5.4", "delta":"3.3.0", "providers":{self.provider:str(jar)},
                   "jarHashes":{str(jar):jarhash}, "providerClasses":{self.provider:classhash}}
        self.value = {"schemaVersion":1,"spark":"3.5.4","delta":"3.3.0","engine":"delta",
                      "catalogRoute":"gravitino","catalog":"c","database":"db",
                      "rangerServerVersion":"2.8.0","rangerClientVersion":"2.8.0",
                      "runtimeManifest":artifact("runtime.json",runtime),
                      "deployedRanger":artifact("ranger.json",{"deployedRangerVersion":"2.8.0"}),
                      "processExits":{name:artifact(name+".json",{"exitCode":0}) for name in ["python","native","supervisor"]},
                      "origins":artifact("origins.json",{self.provider:{"jar":str(jar),"jarSha256":jarhash,"classSha256":classhash}}),
                      "registration":artifact("registration.json",{"beforeRestart":{"source":"id"},"afterRestart":{"source":"id"},"persisted":{"source":"id"}}),
                      "cases":[]}
        properties={"delta.registration.receipt."+key:value for key,value in {"state":"PUBLISHED","id":"1","generation":"2","token":"token","nativeId":"uuid","nativeVersion":"0"}.items()}
        tables={name:{"properties":properties} for name in ["source","primitive"]}
        state={"state":"PUBLISHED","id":1,"generation":2,"token":"token","observation":{"uuid":"uuid","version":0}}
        self.value["registration"]=artifact("registration.json",{"beforeRestart":tables,"afterRestart":tables,"persisted":{name:state for name in tables}})
        self.value["nativeSeed"]=artifact("native-seed.json",{"tables":{name:{"id":"uuid"} for name in tables}})
        python_exit=Path(self.value["processExits"]["python"]["path"])
        children={name:artifact(name+"-exit.json",{"exitCode":0}) for name in ["id-only","table-wide","no-grant","denied-email"]}
        python_exit.write_text(json.dumps({"exitCode":0,"children":children}))
        self.value["processExits"]["python"]=file_ref(python_exit)
        for name,(api,rows,columns) in CASE_SPECS.items():
            if rows=="ranger-outcome":rows=[[4]]
            case = {"name":name,"api":api,"actionRoute":"native-pyspark",
                    "listenerQueuesDrained":True,"actualActionQueries":1,"caseSequence":1,
                    "callbacks":[{"case":name,"caseSequence":1,"queryId":7,"function":api}],
                    "error":None if rows is not None else {"class":"Py4JJavaError","message":"TABLE_OR_VIEW_NOT_FOUND" if name=="view-dropped" else "java.lang.SecurityException: Delta Ranger access denied"},
                    "outcome":"denied" if rows is None else "allowed","expectedRows":rows,"observedRows":rows,
                    "expectedColumns":sorted(columns),"originalDemands":[{"columns":columns}],
                    "rangerRequests":[{"resource":{"catalog":"c","database":"db","table":"source","column":"*"},"allowed":True,"policyType":0}],
                    "policySnapshot":policy,"appliedPolicy":applied,"analyzedPlan":ref,"finalPlan":ref}
            if api == "toPandas":
                case["arrow"] = {"enabled":name.endswith("-on"),"fallback":False,"observed":name.endswith("-on"),
                                 "pandas":"2.2.3","pyarrow":"17.0.0","dtypes":{"id":"int64","email":"object"},
                                 "pythonTZ":"UTC","sessionTZ":"UTC"}
                if name.startswith("primitive-"):
                    case["arrow"]["dtypes"].update(nlong="float64",text="object",flag="object",number="float64",money="object",day="object",stamp="datetime64[us]" if name.endswith("-on") else "datetime64[ns]")
            if name == "aqe-join":
                case["execution"] = {"isPayload":True,"phase":"ordinary","plan":ref,
                                     "operators":["AdaptiveSparkPlanExec","ShuffleQueryStageExec"],
                                     "rootClass":"AdaptiveSparkPlanExec","isFinalPlan":True,
                                     "materializedShuffle":1,"materializedBroadcast":0,
                                     "coalesced":False,"local":False,"skew":False,"reuse":False}
            self.value["cases"].append(case)
        subjects={profile:"subject-"+profile for profile in children}
        self.value.update(subjects=subjects,metadataPrincipal="metadata",setupIdentity="setup")
        self.value["profiles"]={profile:{"origins":self.value["origins"],
            "observed":artifact(profile+"-observed.json",{"spark":"3.5.4","scala":"2.12.21","java":"17","subject":subject}),
            "metadata":artifact(profile+"-metadata.json",{"principal":"metadata","successfulTargetGet":True}),
            "security":artifact(profile+"-security.json",{})} for profile,subject in subjects.items()}
        for case in self.value["cases"]:
            case["identityProfile"]="table-wide"
            for request in case["rangerRequests"]:request["user"]=subjects["table-wide"]
        # Real immutable package verification has its own artifact tests. Isolate only that
        # external prerequisite here so each mutation exercises this checker, not a setup failure.
        self.package = patch("read_parity_checks.verify_runtime")
        self.package.start()
        self.addCleanup(self.package.stop)

    def test_stale_publication_has_no_fabricated_request(self):
        case=self.case("retained-revoked")
        case["rangerRequests"]=[]
        case["error"]["message"]="java.lang.SecurityException: Delta Ranger publication changed; reanalyze the query"
        case["requestObservation"]="no evaluation observed"
        verify_result(self.value)
        case["error"]["message"]="java.lang.SecurityException: unrelated denial"
        with self.assertRaises(AssertionError):verify_result(self.value)

    def test_audit_slice_must_match_original_sink(self):
        from read_parity_checks import verify_audit_slice
        from cdf_merge_live_checks import file_ref
        root=Path(self.directory.name)
        raw=b'{"access":"select","resource":"db/source/id"}\n'
        original=root/"audit.log";original.write_bytes(raw)
        selected=root/"slice.jsonl";selected.write_bytes(raw)
        case={"auditEvidence":file_ref(selected),"auditRange":{"firstEvent":0,"endEvent":1,"firstByte":0,"endByte":len(raw),"submittedTotal":1,"failedTotal":0,"deferredTotal":0},
              "rangerRequests":[dict(json.loads(raw),observationKind="audited-permission-event")]}
        verify_audit_slice(case,[file_ref(original)])
        selected.write_bytes(raw.replace(b"id",b"xx"));case["auditEvidence"]=file_ref(selected)
        with self.assertRaises(AssertionError):verify_audit_slice(case,[file_ref(original)])

    def test_wrong_profile_subject_is_rejected(self):
        self.case("id-count")["rangerRequests"][0]["user"]="metadata"
        with self.assertRaisesRegex(AssertionError,"actual request subject"):verify_result(self.value)

    def test_native_launcher_exposes_generated_security_to_ranger(self):
        from read_parity_live import application_command
        request={"sparkConfigurationFile":"/fixture/spark.properties","driverSecurityDirectory":"/fixture/driver-security"}
        argv=list(map(str,application_command({"sparkHome":"/runtime"},request,Path("/fixture/request.json"))))
        self.assertEqual("/fixture/driver-security",argv[argv.index("--driver-class-path")+1])
        self.assertEqual("/fixture/spark.properties",argv[argv.index("--properties-file")+1])
        self.assertTrue(argv[-2].endswith("/read_parity_pyspark.py"))
        self.assertEqual("/fixture/request.json",argv[-1])

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
        origins=json.loads(path.read_text());origins[self.provider]["jar"]="wrong.jar"
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

    def test_wrong_actual_ranger_maven_version(self):
        from cdf_merge_live_checks import file_ref
        import zipfile
        runtime_path=Path(self.value["runtimeManifest"]["path"]);runtime=json.loads(runtime_path.read_text())
        jar=Path(runtime["providers"][self.provider])
        with zipfile.ZipFile(jar,"w") as archive:
            archive.writestr("META-INF/maven/org.apache.ranger/ranger-plugins-common/pom.properties","version=2.7.0\n")
            archive.writestr(self.provider.replace(".","/")+".class",b"bytecode")
        digest=hashlib.sha256(jar.read_bytes()).hexdigest();runtime["jarHashes"][str(jar)]=digest
        runtime_path.write_text(json.dumps(runtime));self.value["runtimeManifest"]=file_ref(runtime_path)
        origin_path=Path(self.value["origins"]["path"]);origins=json.loads(origin_path.read_text())
        origins[self.provider]["jarSha256"]=digest;origin_path.write_text(json.dumps(origins))
        self.value["origins"]=file_ref(origin_path)
        with self.assertRaisesRegex(AssertionError,"actual Ranger client version"):
            verify_result(self.value)

    def test_setup_failure_cannot_pass_as_denial(self):
        self.case("email-denied")["error"]={"class":"OSError","message":"network setup failed"}
        with self.assertRaisesRegex(AssertionError,"actual permission denial"):
            verify_result(self.value)

    def test_allowed_case_requires_actual_callback(self):
        self.case("table-count")["actualActionQueries"]=0
        with self.assertRaisesRegex(AssertionError,"actual action query"):
            verify_result(self.value)

    def test_missing_download_body_is_rejected(self):
        path=Path(self.case("id-count")["appliedPolicy"]["path"])
        applied=json.loads(path.read_text());applied.pop("download",None)
        path.write_text(json.dumps(applied))
        from cdf_merge_live_checks import file_ref
        for case in self.value["cases"]:case["appliedPolicy"]=file_ref(path)
        with self.assertRaisesRegex(AssertionError,"actual policy download"):
            verify_result(self.value)

    def test_native_normalization_retains_precision_and_nulls(self):
        import datetime
        import decimal
        from read_parity_pyspark import normalize
        self.assertIsNone(normalize(None))
        self.assertIsNone(normalize(float("nan")))
        self.assertEqual({"decimal":"12.30"},normalize(decimal.Decimal("12.30")))
        self.assertEqual({"date":"2026-10-26"},normalize(datetime.date(2026,10,26)))
        self.assertEqual({"timestamp":"2026-10-26T12:34:56.123456"},
                         normalize(datetime.datetime(2026,10,26,12,34,56,123456)))

    def test_original_process_exit_and_streams_are_retained(self):
        import sys
        from read_parity_live import start,finish
        for code in [0,7]:
            directory=Path(self.directory.name)/("process-"+str(code))
            process,out,err=start([sys.executable,"-c","import sys; print('stdout'); print('stderr',file=sys.stderr); sys.exit("+str(code)+")"],dict(__import__("os").environ),directory)
            process.wait(timeout=10)
            self.assertEqual(code,finish(process,out,err,directory))
            self.assertEqual(code,json.loads((directory/"exit.json").read_text())["exitCode"])
            self.assertEqual("stdout\n",(directory/"stdout").read_text())
            self.assertEqual("stderr\n",(directory/"stderr").read_text())


if __name__ == "__main__":
    unittest.main()

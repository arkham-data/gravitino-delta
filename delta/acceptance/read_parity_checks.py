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

"""Independent checks for finite native ordinary-read qualification evidence."""
import json
from pathlib import Path
import zipfile

from cdf_merge_live_checks import file_ref, verify_adaptive_observation
from cdf_read_live_package import verify_runtime, verify_supplemental_artifacts

# Fixed relational oracle for the approved five-row fixture, independent of query builders.
CASE_SPECS = {
    "id-count": ("count", [[4]], ["id"]),
    "id-composition": ("collect", [[1],[2],[3],[5]], ["id"]),
    "explicit-star-denied": ("count", None, ["id","region","email","secret"]),
    "authored-region-denied": ("count", None, ["id","region"]),
    "email-denied": ("collect", None, ["email"]),
    "sort-email-denied": ("collect", None, ["id","email"]),
    "table-count": ("count", [[4]], ["*"]),
    "count-one": ("sql", [[4]], ["*"]),
    "raw-email-count": ("sql", [[3]], ["email"]),
    "constant-email-count": ("sql", [[4]], ["email"]),
    "null-email-count": ("sql", [[0]], ["email"]),
    "constant-distinct": ("distinct", [["M"]], ["email"]),
    "null-distinct": ("distinct", [[None]], ["email"]),
    "masked-order": ("orderBy", [[1,"M"],[2,"M"],[3,"M"],[5,"M"]], ["id","email"]),
    "raw-null-order": ("orderBy", [[3,None],[2,"a"],[5,"b"],[1,"z"]], ["id","email"]),
    "sorted-limit": ("limit", [[1],[2]], ["id"]),
    "zero-limit": ("limit", [], ["id"]),
    "false-count": ("count", [[0]], ["id"]),
    "show": ("show", [[1,"M"],[2,"M"]], ["id","email"]),
    "pandas-arrow-off": ("toPandas", [[1,"M"],[2,"M"],[3,"M"],[5,"M"]], ["id","email"]),
    "pandas-arrow-on": ("toPandas", [[1,"M"],[2,"M"],[3,"M"],[5,"M"]], ["id","email"]),
    "view-count": ("count", [[4]], ["id"]),
    "view-explicit-denied": ("count", None, ["id","email"]),
    "view-replaced": ("collect", [[2]], ["id"]),
    "view-retained": ("collect", [[1]], ["id"]),
    "view-dropped": ("table", None, []),
    "retained-mask-denied": ("collect", None, ["id","email"]),
    "fresh-null-mask": ("collect", [[1,None],[2,None],[3,None],[5,None]], ["id","email"]),
    "retained-revoked": ("collect", None, ["id"]),
    "no-grant-denied": ("count", None, ["id"]),
    "aqe-join": ("collect", [[1,"M"],[2,"M"],[3,"M"],[5,"M"]], ["id","email"]),
}


def read_ref(reference):
    assert file_ref(reference["path"]) == reference, "artifact changed"
    return json.loads(Path(reference["path"]).read_text())


def verify_result(result: dict) -> None:
    """Reject absent observations, incorrect semantics and mismatched immutable provenance."""
    assert result.get("schemaVersion") == 1, "schemaVersion"
    assert result["spark"] in ["3.5.4", "3.5.6"], "Spark pin"
    assert result["delta"] in ["3.3.0", "3.3.2"], "Delta pin"
    assert result["engine"] == "delta" and result["catalogRoute"] == "gravitino", "engine route"
    assert result["rangerServerVersion"] == result["rangerClientVersion"] == "2.8.0", "Ranger pin"
    runtime = read_ref(result["runtimeManifest"])
    assert (runtime["spark"], runtime["delta"]) == (result["spark"], result["delta"]), "runtime pins"
    assert read_ref(result["deployedRanger"])["deployedRangerVersion"] == "2.8.0", "deployed Ranger"
    assert set(result["processExits"]) == {"python", "native", "supervisor"}, "child exits missing"
    for reference in result["processExits"].values():
        assert read_ref(reference)["exitCode"] == 0, "child exit failed"
    origins = read_ref(result["origins"])
    for name, path in runtime["providers"].items():
        observed = origins[name]
        assert observed["jar"] == path, "loaded provider differs"
        assert observed["jarSha256"] == runtime["jarHashes"][path], "provider hash differs"
        assert observed["classSha256"] == runtime["providerClasses"][name], "provider class differs"
    registration = read_ref(result["registration"])
    assert registration["beforeRestart"] == registration["afterRestart"], "registration restart differs"
    assert registration["persisted"], "missing persistent registration"
    cases = result["cases"]
    assert len(cases) == len(CASE_SPECS) and {c["name"] for c in cases} == set(CASE_SPECS), "case inventory"
    for case in cases:
        api, rows, columns = CASE_SPECS[case["name"]]
        assert case["api"] == api and case["actionRoute"] == "native-pyspark", "native action route"
        assert case["outcome"] == ("denied" if rows is None else "allowed"), "outcome"
        assert case["expectedRows"] == rows and case["observedRows"] == rows, "exact rows/order"
        assert case["expectedColumns"] == sorted(columns), "original expected demand"
        if columns:
            assert case["originalDemands"], "missing original demand"
            demanded = set().union(*(set(d["columns"]) for d in case["originalDemands"]))
            assert demanded == set(columns), "original demand differs"
            assert case["rangerRequests"], "missing Ranger requests"
            for request in case["rangerRequests"]:
                assert request["resource"]["catalog"] == result["catalog"], "canonical catalog"
                assert request["resource"]["database"] == result["database"], "canonical database"
                assert request["resource"]["table"] in ["source", "primitive"], "view alias Ranger resource"
        assert read_ref(case["policySnapshot"])["policyVersion"] >= 0, "policy snapshot"
        applied = read_ref(case["appliedPolicy"])
        assert applied["policy"] == read_ref(case["policySnapshot"])["policyVersion"], "applied policy differs"
        assert applied["downloadSha256"], "missing actual policy download"
        for key in ["analyzedPlan", "finalPlan"]:
            assert file_ref(case[key]["path"]) == case[key], "original plan artifact"
        if api == "toPandas":
            arrow = case["arrow"]
            enabled = case["name"] == "pandas-arrow-on"
            assert arrow["enabled"] is enabled and arrow["fallback"] is False, "Arrow flags"
            assert arrow["observed"] is enabled, "actual Arrow transport"
            assert arrow["pandas"] == "2.2.3" and arrow["pyarrow"] == "17.0.0", "Python versions"
            assert arrow["dtypes"] == {"id":"int64", "email":"object"}, "Pandas dtypes"
            assert arrow["pythonTZ"] == arrow["sessionTZ"] == "UTC", "qualified UTC environment"
        if case["name"] == "aqe-join":
            verify_adaptive_observation(case["execution"])
    verify_supplemental_artifacts(runtime)
    verify_runtime(runtime)

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
from collections import Counter
from pathlib import Path
import zipfile

from cdf_merge_live_checks import file_ref, verify_adaptive_observation
from cdf_read_live_package import verify_runtime, verify_supplemental_artifacts

# Fixed relational oracle for the approved five-row fixture, independent of query builders.
CASE_SPECS = {
    "id-only-bare-count": ("count", None, ["*"]),
    "wildcard-email-denied": ("collect", None, ["email"]),
    "wildcard-deny-count": ("count", "ranger-outcome", ["*"]),
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
    "id-retained-warmup": ("collect", [[1],[2],[3],[5]], ["id"]),
    "masked-retained-warmup": ("collect", [[1,"M"],[2,"M"],[3,"M"],[5,"M"]], ["id","email"]),
    "masked-view": ("collect", [[1,"M"],[2,"M"],[3,"M"],[5,"M"]], ["id","email"]),
    "view-after-drop": ("collect", [[1]], ["id"]),
    "view-retained-mask-denied": ("collect", None, ["id","email"]),
    "view-fresh-null-mask": ("collect", [[1,None],[2,None],[3,None],[5,None]], ["id","email"]),
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
PRIMITIVE_COLUMNS = ["id","email","nlong","text","flag","number","money","day","stamp"]
PRIMITIVE_ROWS = [[1,"M",7.0,"visible",True,1.5,{"decimal":"12.30"},{"date":"2026-10-26"},
                   {"timestamp":"2026-10-26T12:34:56.123456"}],
                  [2,"M",None,None,None,None,None,None,None]]
for mode in ["off","on"]:
    CASE_SPECS["primitive-pandas-"+mode] = ("toPandas", PRIMITIVE_ROWS, PRIMITIVE_COLUMNS)


def read_ref(reference):
    assert file_ref(reference["path"]) == reference, "artifact changed"
    return json.loads(Path(reference["path"]).read_text())


def verify_pandas_transport(case):
    """Require measured native Python transport and the finite UTC dtype profile."""
    arrow = case["arrow"]
    enabled = case["name"].endswith("-on")
    assert arrow["enabled"] is enabled and arrow["fallback"] is False, "Arrow flags"
    assert arrow["observed"] is enabled, "actual Arrow transport"
    assert arrow["pandas"] == "2.2.3" and arrow["pyarrow"] == "17.0.0", "Python versions"
    dtypes={"id":"int64", "email":"object"}
    if case["name"].startswith("primitive-"):
        dtypes.update(nlong="float64",text="object",flag="object",number="float64",money="object",day="object",
                      stamp="datetime64[us]" if enabled else "datetime64[ns]")
    assert arrow["dtypes"] == dtypes, "Pandas dtypes"
    assert arrow["pythonTZ"] == arrow["sessionTZ"] == "UTC", "qualified UTC environment"


def verify_comparator_policy(case):
    """Bind the applied client version to the immutable policy snapshot."""
    assert case["pluginPolicyVersion"] == read_ref(case["policySnapshot"])["policyVersion"], "Comparator policy application differs"


def verify_jar_property(path,member,key,expected):
    """Check embedded version metadata in the actual class-provider JAR."""
    with zipfile.ZipFile(path) as archive:
        lines=archive.read(member).decode().splitlines()
    assert [line for line in lines if line.startswith(key+"=")]==[key+"="+expected], "actual provider version differs"


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
    child_exits=read_ref(result["processExits"]["python"]).get("children", {})
    assert set(child_exits)=={"id-only","table-wide","no-grant","denied-email"}, "original Python children missing"
    for reference in child_exits.values():
        assert read_ref(reference)["exitCode"]==0, "original Python child failed"
    origins = read_ref(result["origins"])
    for name, path in runtime["providers"].items():
        observed = origins[name]
        assert observed["jar"] == path, "loaded provider differs"
        assert observed["jarSha256"] == runtime["jarHashes"][path], "provider hash differs"
        assert observed["classSha256"] == runtime["providerClasses"][name], "provider class differs"
    ranger_key="org.apache.ranger.plugin.service.RangerBasePlugin"
    assert ranger_key in runtime["providers"], "actual Ranger client provider missing"
    verify_jar_property(runtime["providers"][ranger_key],"META-INF/maven/org.apache.ranger/ranger-plugins-common/pom.properties","version","2.8.0")
    assert set(result["profiles"])==set(result["subjects"])==set(child_exits), "identity profile inventory"
    assert len(set(result["subjects"].values()) | {result["metadataPrincipal"],result["setupIdentity"]})==6, "separate identities"
    for profile, references in result["profiles"].items():
        observed=read_ref(references["observed"])
        assert (observed["spark"],observed["scala"],observed["java"],observed["subject"])==(result["spark"],"2.12.21","17",result["subjects"][profile]), "actual profile identity/runtime"
        metadata=read_ref(references["metadata"])
        assert metadata["principal"]==result["metadataPrincipal"] and metadata["successfulTargetGet"] is True, "metadata identity"
        profile_origins=read_ref(references["origins"])
        assert profile_origins==origins, "profile origins differ"
        read_ref(references["security"])
    registration = read_ref(result["registration"])
    assert registration["beforeRestart"] == registration["afterRestart"], "registration restart differs"
    assert set(registration["persisted"])==set(registration["beforeRestart"])=={"source","primitive"}, "missing persistent registration"
    native=read_ref(result["nativeSeed"])
    prefix="delta.registration.receipt."
    for table,state in registration["persisted"].items():
        receipt=registration["beforeRestart"][table]["properties"]
        assert state["state"]==receipt[prefix+"state"]=="PUBLISHED", "published registration"
        for key in ["id","generation","token"]:
            assert str(state[key])==receipt[prefix+key], "persistent receipt differs"
        assert state["observation"]["uuid"]==receipt[prefix+"nativeId"]==native["tables"][table]["id"], "native UUID binding differs"
        assert str(state["observation"]["version"])==receipt[prefix+"nativeVersion"], "native version differs"

    cases = result["cases"]
    assert len(cases) == len(CASE_SPECS) and {c["name"] for c in cases} == set(CASE_SPECS), "case inventory"
    for case in cases:
        api, rows, columns = CASE_SPECS[case["name"]]
        assert case["api"] == api and case["actionRoute"] == "native-pyspark", "native action route"
        if rows == "ranger-outcome":
            access=[r["allowed"] for r in case["rangerRequests"] if r["policyType"]==0 and r["resource"].get("column")=="*"]
            assert access and len(set(access))==1, "missing consistent wildcard outcome"
            rows=[[4]] if access[0] else None
        assert case["outcome"] == ("denied" if rows is None else "allowed"), "outcome"
        assert case["expectedRows"] == rows and case["observedRows"] == rows, "exact rows/order"
        assert case.get("listenerQueuesDrained") is True, "actual action query drain absent"
        if rows is not None:
            assert case.get("actualActionQueries",0)>0 and case.get("callbacks"), "actual action query absent"
            assert all(callback["case"]==case["name"] and callback["caseSequence"]==case["caseSequence"] for callback in case["callbacks"]), "actual action query correlation"
            assert case.get("error") is None, "allowed action error"
        elif case["name"]=="view-dropped":
            assert "TABLE_OR_VIEW_NOT_FOUND" in case.get("error",{}).get("message",""), "actual dropped view lookup"
        else:
            error=case.get("error") or {}
            assert any(kind in error.get("message","") for kind in ["SecurityException","AccessControlException","Permission denied"]), "actual permission denial absent"
        assert case["expectedColumns"] == sorted(columns), "original expected demand"
        if columns:
            assert case["originalDemands"], "missing original demand"
            demanded = set().union(*(set(d["columns"]) for d in case["originalDemands"]))
            assert demanded == set(columns), "original demand differs"
            table="primitive" if case["name"].startswith("primitive-") else "source"
            expected_occurrences=[set(columns)] if case["name"]!="aqe-join" else [{"id","email"},{"id"}]
            expected_keys=Counter((result["catalog"],result["database"],table,tuple(sorted(value))) for value in expected_occurrences)
            actual_keys=Counter((value["catalog"],value["database"],value["table"],tuple(sorted(value["columns"]))) for value in case["originalDemands"])
            assert actual_keys==expected_keys, "original occurrence binding differs"
            permissions=[record for record in case["rangerRequests"] if record["policyType"]==0]
            actual_columns={record["resource"].get("column") for record in permissions}
            assert actual_columns<=set(columns), "unexpected permission column"
            if rows is not None:
                assert actual_columns==set(columns), "missing permission column"

            if not case["rangerRequests"]:
                assert case["name"] in {"retained-revoked","retained-mask-denied","view-retained-mask-denied"}, "missing Ranger requests"
                assert "Delta Ranger publication changed; reanalyze the query" in case["error"]["message"], "missing Ranger requests without early publication denial"
                assert case.get("requestObservation")=="no evaluation observed", "missing request observation"

            for request in case["rangerRequests"]:
                assert request["user"]==result["subjects"][case["identityProfile"]], "actual request subject"
                assert request["resource"]["catalog"] == result["catalog"], "canonical catalog"
                assert request["resource"]["database"] == result["database"], "canonical database"
                assert request["resource"]["table"] == table, "view alias Ranger resource"
        assert read_ref(case["policySnapshot"])["policyVersion"] >= 0, "policy snapshot"
        applied = read_ref(case["appliedPolicy"])
        assert applied["policy"] == read_ref(case["policySnapshot"])["policyVersion"], "applied policy differs"
        assert applied.get("download") and applied.get("downloadEvent"), "missing actual policy download"
        downloaded=read_ref(applied["download"])
        assert applied["download"]["sha256"]==applied["downloadSha256"], "actual policy download hash"
        event=applied["downloadEvent"]
        assert event["downstreamResponseCompleted"] is True and event["downloadKind"]=="policy", "actual policy download completion"
        assert event["responseBodyFile"]==applied["download"]["path"] and event["responseSha256"]==applied["downloadSha256"], "actual policy download event"
        policy=read_ref(case["policySnapshot"])
        assert {k:v for k,v in downloaded.items() if k!="policyUpdateTime"}=={k:v for k,v in policy.items() if k!="policyUpdateTime"}, "actual policy download content"
        for key in ["analyzedPlan", "finalPlan"]:
            assert file_ref(case[key]["path"]) == case[key], "original plan artifact"
        if api == "toPandas":
            verify_pandas_transport(case)
        if case["name"] == "aqe-join":
            verify_adaptive_observation(case["execution"])
    verify_supplemental_artifacts(runtime)
    verify_runtime(runtime)


def verify_audit_slice(case, sinks):
    """Verify raw comparator offsets against the original unique process audit sink."""
    assert len(sinks)<=1, "audit rotation is not qualified"
    raw=b""
    for reference in sinks:
        assert file_ref(reference["path"])==reference, "original audit sink changed"
        raw+=Path(reference["path"]).read_bytes()
    position=case["auditRange"]
    assert position["failedTotal"]==position["deferredTotal"]==0, "audit delivery failed"
    start,end=position["firstByte"],position["endByte"]
    first,last=position["firstEvent"],position["endEvent"]
    assert 0<=start<=end<=len(raw), "audit byte bounds"
    assert 0<=first<=last==position["submittedTotal"], "audit event bounds"
    assert len(raw[:start].splitlines())==first and len(raw[:end].splitlines())==last, "audit event offsets"
    reference=case["auditEvidence"]
    assert file_ref(reference["path"])==reference, "audit slice changed"
    selected=Path(reference["path"]).read_bytes()
    assert selected==raw[start:end], "audit slice differs from original"
    events=[dict(json.loads(line),observationKind="audited-permission-event") for line in selected.splitlines()]
    assert events==case["rangerRequests"], "actual audit event fields differ"


def verify_policy_http(events, raw_policy, service, version):
    """Join a stock client's delivered response hash to retained actual policy bytes."""
    assert read_ref(raw_policy)["policyVersion"]==version, "policy wire version differs"
    matching=[event for event in events if event.get("method")=="GET"
              and event.get("path","").startswith("/service/plugins/secure/policies/download/"+service+"?")
              and event.get("completion")=="confirmed" and event.get("downstream")=="sent"
              and event.get("upstream_status")==200 and event.get("response_sha256")==raw_policy["sha256"]]
    assert matching, "actual completed client policy download absent"
    return matching[0]


VIEW_PREREQUISITES={
    "id-view-created":{"view-count","view-replaced","view-retained","view-dropped","view-after-drop"},
    "explicit-view-created":{"view-explicit-denied"},
}


def verify_comparator_failure(name,error):
    """Never relabel transport/setup failures as measured authorization differences."""
    message=(error or {}).get("message","")
    if name=="view-dropped":
        assert "TABLE_OR_VIEW_NOT_FOUND" in message, "unexpected comparator lookup failure"
    else:
        assert "org.apache.kyuubi.plugin.spark.authz.AccessControlException" in message and "Permission denied:" in message, "unexpected comparator action failure"


def verify_comparator_cases(cases,prerequisites):
    """Keep observed comparator denials distinct from causally unexecuted terminal actions."""
    assert len(cases)==len(CASE_SPECS) and {case["name"] for case in cases}==set(CASE_SPECS), "comparator inventory"
    blocked={}
    for reference in prerequisites:
        value=read_ref(reference)
        assert value["name"] in VIEW_PREREQUISITES, "unsupported prerequisite"
        assert value["api"]==value["actualApi"]=="createTempView" and value["operationInvoked"] is True, "actual prerequisite operation absent"
        assert value["actionRoute"]=="native-pyspark", "prerequisite route"
        assert value["outcome"] in ["allowed","denied"], "prerequisite outcome"
        if value["outcome"]=="denied":
            message=value.get("error",{}).get("message","")
            assert "org.apache.kyuubi.plugin.spark.authz.AccessControlException" in message and "Permission denied:" in message, "unsupported prerequisite failure"
            assert value["rangerRequests"], "prerequisite actual audit absent"
            assert file_ref(value["analyzedPlan"]["path"])==value["analyzedPlan"], "prerequisite plan absent"
            for name in VIEW_PREREQUISITES[value["name"]]:
                assert name not in blocked, "duplicate prerequisite"
                blocked[name]=reference
    for case in cases:
        assert case["outcome"] in ["allowed","denied","not-executed"], "comparator outcome"
        api,expected,columns=CASE_SPECS[case["name"]]
        wildcard=expected=="ranger-outcome"
        if wildcard:
            assert case["expectedRows"] in [None,[[4]]], "wildcard observed outcome"
        else:
            assert case["expectedRows"]==expected, "independent comparator oracle"
        assert case["api"]==api and case["expectedColumns"]==sorted(columns), "comparator API/demand declaration"
        if case["outcome"]!="not-executed":
            assert case["actionRoute"]=="native-pyspark", "native comparator action route"
            assert case["listenerQueuesDrained"] is True, "comparator listener drain"
            assert case["actualActionQueries"]==len(case["callbacks"]), "comparator callback count"
            if case["outcome"]=="allowed":
                assert case["callbacks"], "successful action callback absent"
            for callback in case["callbacks"]:
                assert callback["case"]==case["name"] and callback["caseSequence"]==case["caseSequence"], "comparator callback attribution"
            for key in ["analyzedPlan","finalPlan"]:
                assert file_ref(case[key]["path"])==case[key], "comparator plan reference"
            if api=="toPandas" and case["outcome"]=="allowed":verify_pandas_transport(case)
            if wildcard:
                assert case["observedRows"]==([[4]] if case["outcome"]=="allowed" else None), "wildcard actual payload"
                comparison="measured-policy-outcome"
            else:
                comparison="matches-delta-contract" if case["observedRows"]==expected and (case["outcome"]=="denied")== (expected is None) else "measured-difference"
            baseline_denied=case["name"]=="retained-revoked" and next(value for value in cases if value["name"]=="id-retained-warmup")["outcome"]!="allowed"
            if baseline_denied:
                assert case.get("qualification")=="unqualified", "denied baseline cannot prove revoke-on-reuse"
                comparison="measured-difference"
            assert case["comparison"]==comparison, "independent comparator classification"
        if case["outcome"]=="allowed":
            assert case.get("error") is None and case["observedRows"] is not None, "allowed comparator action failure"
        elif case["outcome"]=="denied":
            verify_comparator_failure(case["name"],case.get("error"))
            assert case["observedRows"] is None, "denied comparator payload"
        if case["name"] in blocked:
            assert case["outcome"]=="not-executed" and case.get("blockedBy")==blocked[case["name"]], "dependent case falsely executed"
        if case["outcome"]=="not-executed":
            assert case["name"] in blocked, "missing prerequisite"
            assert case["qualification"]=="unqualified" and case["comparison"]=="measured-difference", "unexecuted qualification"
            assert case["actionRoute"]=="not-executed" and case["actualApi"] is None, "fabricated action route"
            assert case["observedRows"] is None and case["originalDemands"]==case["rangerRequests"]==case["callbacks"]==[], "fabricated action observation"
            assert case["actualActionQueries"]==0 and not any(case.get(key) for key in ["analyzedPlan","finalPlan","execution","arrow"]), "fabricated action plan/transport"
        if case["name"]=="retained-revoked":
            baseline=next(value for value in cases if value["name"]=="id-retained-warmup")
            if baseline["outcome"]!="allowed":
                assert case.get("qualification")=="unqualified" and case["comparison"]=="measured-difference", "denied baseline cannot prove revoke-on-reuse"
                observed=read_ref(case["baseline"])
                assert observed["name"]==baseline["name"] and observed["outcome"]==baseline["outcome"], "retained baseline reference differs"
    not_executed=sum(case["outcome"]=="not-executed" for case in cases)
    return {"total":len(cases),"executed":len(cases)-not_executed,"notExecuted":not_executed}

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

"""Independent fixed oracle for stock Iceberg governed append observations."""

import json
from pathlib import Path
from read_parity_checks import (
    verify_audit_slice,
    verify_policy_http,
    verify_jar_property,
)
from collections import Counter
from cdf_merge_live_checks import file_ref, read_ref

APIS = ("insertInto", "saveAsTable", "writeTo", "sql")
FORMS = ("plain", "partitioned")
IDENTITIES = ("allowed", "source-denied", "target-denied")
ORIGINAL = [[0, "seed", "original", True]]
APPENDED = [[1, "east", "M", True], [1, "east", "M", True], [2, "west", "M", True]]
SCHEMA = "id BIGINT, region STRING, email STRING, visible BOOLEAN"


def case_specs():
    return [
        dict(
            name=api + "_" + form + "_" + identity.replace("-", "_"),
            api=api,
            form=form,
            identity=identity,
        )
        for api in APIS
        for form in FORMS
        for identity in IDENTITIES
    ]


def typed_rows(rows):
    for row in rows:
        assert len(row) == 4, "row width"
        assert all(
            type(value) is expected
            for value, expected in zip(row, (int, str, str, bool))
        ), "typed row differs"
    return Counter(json.dumps(row, separators=(",", ":")) for row in rows)


def adaptive_payload(case):
    for callback in case["callbacks"]:
        if callback["kind"] != "write-payload" or callback["failed"]:
            continue
        nodes = callback["planNodes"]
        if any(
            n["className"].endswith("AdaptiveSparkPlanExec")
            and n.get("isFinalPlan") is True
            for n in nodes
        ) and any(
            n["className"].endswith("ShuffleQueryStageExec")
            and n.get("materialized") is True
            for n in nodes
        ):
            return True
    return False


def verify_iceberg_append(result: dict) -> None:
    """Fail closed on unsupported write, authority, runtime, exit or row evidence."""
    assert result["spark"] in ("3.5.4", "3.5.6")
    assert (
        result["catalogClass"]
        == {
            "native": "org.apache.iceberg.spark.SparkCatalog",
            "gravitino": "org.apache.gravitino.spark.connector.iceberg.GravitinoIcebergCatalogSpark35",
        }[result["catalogRoute"]]
    ), "actual catalog identity"
    assert result["verificationSubject"] not in result["subjects"].values(), (
        "independent readback identity"
    )
    executed_profiles = {
        case["identity"]
        for case in result["cases"] + result.get("supplementalCases", [])
    }
    required = {
        "seed",
        "verify",
        "supervisor",
        *IDENTITIES,
        *result["subjects"],
        *executed_profiles,
    }
    assert required <= set(result["processExits"]), "missing original exit"
    for ref in result["processExits"].values():
        value = read_ref(ref)
        assert value["exitCode"] == 0 and value.get("terminated") is False, (
            "original process failed"
        )

    def refs(value):
        if isinstance(value, dict):
            if "path" in value and "sha256" in value:
                assert file_ref(value["path"]) == value, "referenced bytes changed"
            else:
                for child in value.values():
                    refs(child)
        elif isinstance(value, list):
            for child in value:
                refs(child)

    refs(result)
    manifest = read_ref(result["runtimeManifest"])
    assert all(
        manifest.get(k) == v
        for k, v in dict(
            spark=result["spark"], iceberg="1.11.0", kyuubi="1.10.2", ranger="2.8.0"
        ).items()
    ), "runtime pins differ"
    runtime_values = [read_ref(ref) for ref in result["runtimeObservations"]]
    runtimes = [value for value in runtime_values if "sparkUser" in value]
    assert set(result["subjects"].values()) == {
        value["sparkUser"] for value in runtimes
    }, "runtime subjects differ"
    catalogs = [value for value in runtime_values if "actualClass" in value]
    assert len(catalogs) == len(runtimes) and all(
        value["actualClass"] == result["catalogClass"] for value in catalogs
    ), "loaded catalog differs"
    for runtime in runtimes:
        assert (
            runtime["spark"] == result["spark"]
            and runtime["scala"] == "2.12.21"
            and runtime["java"].startswith("17.")
        ), "actual runtime differs"
        for cls, member, key, version in [
            (
                "org.apache.ranger.plugin.service.RangerBasePlugin",
                "META-INF/maven/org.apache.ranger/ranger-plugins-common/pom.properties",
                "version",
                "2.8.0",
            ),
            (
                "org.apache.iceberg.spark.SparkCatalog",
                "iceberg-build.properties",
                "git.build.version",
                "1.11.0",
            ),
            (
                "org.apache.kyuubi.plugin.spark.authz.ranger.RangerSparkExtension",
                "META-INF/maven/org.apache.kyuubi/kyuubi-spark-authz_2.12/pom.properties",
                "version",
                "1.10.2",
            ),
        ]:
            assert cls in runtime["origins"], "loaded provider missing"
            verify_jar_property(
                runtime["origins"][cls]["artifact"]["path"], member, key, version
            )
        for origin in runtime["origins"].values():
            reference = origin["artifact"]
            assert (
                file_ref(reference["path"]) == reference
                and manifest["jars"].get(reference["path"]) == reference["sha256"]
            ), "loaded provider differs"
    cases = result["cases"]
    specs = {s["name"]: s for s in case_specs()}
    assert len(cases) == len(specs) and {c["name"] for c in cases} == set(specs), (
        "case inventory"
    )
    states = [
        read_ref(ref)
        for ref in result["artifacts"]
        if all(key in read_ref(ref) for key in specs)
    ]
    assert len(states) == 2, "original before/after states absent"
    before_states, after_states = states
    adaptive = []
    for case in cases:
        assert (
            case["before"] == before_states[case["target"]]
            and case["after"] == after_states[case["target"]]
        ), "readback differs from original observations"
        spec = specs[case["name"]]
        assert all(case[k] == v for k, v in spec.items()), "case identity"
        identity = case["identity"]
        assert case["subject"] == result["subjects"][identity], "subject binding"
        assert case["target"] == case["name"], "target binding"
        assert typed_rows(case["before"]["rows"]) == typed_rows(ORIGINAL), (
            "original target rows"
        )
        verify_audit_slice(case, case["auditSinks"])
        download = case["policyDownload"]
        events = [
            json.loads(line)
            for line in Path(download["processHttp"]["path"]).read_text().splitlines()
        ]
        assert (
            verify_policy_http(
                events,
                download["rawPolicy"],
                result["service"],
                case["pluginPolicyVersion"],
            )
            == download["event"]
        ), "download event differs"
        assert read_ref(case["policySnapshot"]) == read_ref(download["rawPolicy"]), (
            "snapshot differs from downloaded policy"
        )
        requests = case["rangerRequests"]
        assert all(
            type(e["result"]) is int and e["result"] in (0, 1) for e in requests
        ), "audit decision type"
        assert all(e["repo"] == result["service"] for e in requests), (
            "audit service differs"
        )
        for callback in case["callbacks"]:
            if callback["failed"]:
                assert not callback["planNodes"], "failed callback materialized a plan"
            if callback["kind"] == "write-payload":
                assert (
                    "AppendData" in Path(callback["analyzedPlan"]["path"]).read_text()
                ), "callback is not an append command"
                final = Path(callback["finalPlan"]["path"]).read_text()
                for node in callback["planNodes"]:
                    if (
                        node["className"].endswith("AdaptiveSparkPlanExec")
                        and node.get("isFinalPlan") is True
                    ):
                        assert "AdaptiveSparkPlan isFinalPlan=true" in final, (
                            "adaptive tree not final"
                        )
                    if node["className"].endswith("ShuffleQueryStageExec"):
                        assert "ShuffleQueryStage" in final, "shuffle tree absent"
                    if "CoalescedPartitionSpec" in node.get("partitionSpecs", ""):
                        assert "AQEShuffleRead coalesced" in final, (
                            "coalesced tree absent"
                        )
        assert requests and all(e["reqUser"] == case["subject"] for e in requests), (
            "audit subject"
        )

        def demand(table, access, allowed):
            return any(
                (
                    e["resource"].split("/")[:2] == [result["database"], table]
                    if table == "source"
                    else e["resource"] == result["database"] + "/" + table
                )
                and e["access"] == "QUERY"
                and e["action"] == access
                and e["result"] == allowed
                for e in requests
            )

        if identity == "allowed":
            assert case["outcome"] == "allowed" and not case["error"], (
                "allowed request outcome"
            )
            assert demand("source", "select", 1) and demand(
                case["target"], "update", 1
            ), "source SELECT / target UPDATE"
            assert typed_rows(case["after"]["rows"]) == typed_rows(
                ORIGINAL + APPENDED
            ), "masked duplicate multiset"
            assert (
                case["after"]["snapshot"] != case["before"]["snapshot"]
                and case["after"]["snapshot"] is not None
            ), "snapshot did not advance"
            assert case["jobs"], "write job absent"
            adaptive.append(adaptive_payload(case))
        else:
            assert case["outcome"] == "denied", "denial outcome"
            assert (
                "Permission denied:" in (case["error"] or "")
                and "AccessControlException" in case["error"]
            ), "authorization error absent"
            table, access = (
                ("source", "select")
                if identity == "source-denied"
                else (case["target"], "update")
            )
            assert demand(table, access, 0), "actual denied access absent"
            assert typed_rows(case["after"]["rows"]) == typed_rows(ORIGINAL), (
                "denied write changed rows"
            )
            assert case["after"]["snapshot"] == case["before"]["snapshot"], (
                "denied write changed snapshot"
            )
    if "id-only" in result["subjects"]:
        assert len(result["supplementalCases"]) == 1, "injected-column probe absent"
        probe = result["supplementalCases"][0]
        assert probe["name"] == "id_only_probe"
        assert (
            probe["before"] == before_states["id_only_probe"]
            and probe["after"] == after_states["id_only_probe"]
        )
        verify_audit_slice(probe, probe["auditSinks"])
        verify_injected_column_probe(
            probe, result["database"], result["subjects"]["id-only"]
        )
    assert any(adaptive), "no actual materialized adaptive write payload"


def callback_plan(query, failed):
    """Never ask rejected callbacks to plan again; planning can repeat authorization."""
    if failed:
        return "Rejected callback; no plan materialization", None
    return str(query.analyzed()), query.executedPlan()


def verify_injected_column_probe(probe, database, subject):
    """The reference-only id projection is denied on the injected region demand."""
    assert (
        probe["outcome"] == "denied"
        and "Permission denied:" in probe["error"]
        and "AccessControlException" in probe["error"]
    )
    assert probe["before"] == probe["after"] and probe["before"]["rows"] == [[0]]
    requests = probe["rangerRequests"]
    assert requests and all(event["reqUser"] == subject for event in requests)
    for column, decision in [("id", 1), ("region", 0)]:
        assert any(
            e["resource"] == database + "/source/" + column
            and e["action"] == "select"
            and e["access"] == "QUERY"
            and e["result"] == decision
            for e in requests
        ), "injected-column actual demand differs"

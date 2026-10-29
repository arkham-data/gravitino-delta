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


DELTA_APPEND_ROWS = [[1, "MASKED", "allowed"], [3, "MASKED", "allowed"]]


def verify_delta_append(result: dict) -> None:
    """Check retained Delta cases without treating metadata AQE or errors as write proof.

    This validates each supplied case. The final matrix inventory separately establishes
    required frontend, maintenance, target-form and runtime coverage.
    """
    try:
        _verify_delta_append(result)
        if "runtime" in result:
            _verify_delta_runtime_cases(result)
    except (KeyError, TypeError, ValueError) as error:
        raise AssertionError("Missing or malformed Delta append evidence") from error


def _verify_delta_append(result):
    assert result["schemaVersion"] == 1
    assert result["spark"] in ("3.5.4", "3.5.6")
    assert result["delta"] in ("3.3.0", "3.3.2")
    subject = result["subject"]
    assert len({subject, result["metadataSubject"], result["verificationSubject"]}) == 3
    assert {"writer", "native-before", "native-after", "supervisor"} <= set(
        result["processExits"]
    )
    for descriptor in result["processExits"].values():
        original = read_ref(descriptor)
        assert original["exitCode"] == 0 and original["timedOut"] is False, (
            "Original process failed"
        )
    assert result["cases"] and len({c["name"] for c in result["cases"]}) == len(
        result["cases"]
    )
    for case in result["cases"]:
        assert case["api"] in ("insertInto", "saveAsTable")
        outcome = case["outcome"]
        assert outcome in (
            "committed",
            "duplicate",
            "source-denied",
            "target-denied",
            "denied-duplicate",
            "after-helper-revoked",
            "postcommit-failure",
        )
        left, right = case["callerBefore"], case["callerAfter"]
        assert left == right, "Writer connection, thread or caller settings changed"
        assert left["gatewayType"] == "py4j.clientserver.ClientServer"
        assert left["clientType"] == "py4j.clientserver.JavaClient"
        assert left["aqe"] == "true"
        assert all(
            type(left[key]) is int and left[key] > 0
            for key in ("pythonThread", "connectionId", "jvmThread")
        )
        assert len(left["socket"]) == len(left["peer"]) == 2
        before, after = read_ref(case["before"]), read_ref(case["after"])
        assert before["nativeId"] == after["nativeId"], "Native target identity changed"
        assert all(
            type(image["version"]) is int and image["version"] >= 0
            for image in (before, after)
        )

        def rows(value):
            assert all(
                len(row) == 3
                and type(row[0]) is int
                and all(type(v) is str for v in row[1:])
                for row in value
            )
            return Counter(tuple(row) for row in value)

        committed = outcome in ("committed", "postcommit-failure")
        assert after["version"] == before["version"] + int(committed), (
            "Unexpected native publication"
        )
        assert after["publications"] == before["publications"] + (
            [after["version"]] if committed else []
        ), "Duplicate or denied write published"
        assert rows(after["rows"]) == rows(
            before["rows"] + (DELTA_APPEND_ROWS if committed else [])
        ), "Committed row multiset differs"
        if outcome == "duplicate":
            assert before["version"] > 0 and before["publications"], (
                "Duplicate lacks original commit"
            )
        denied = outcome in (
            "source-denied",
            "target-denied",
            "denied-duplicate",
            "after-helper-revoked",
        )
        if denied:
            method = (
                "requireSnapshot" if outcome == "after-helper-revoked" else "authorize"
            )
            expected = (
                "Delta Ranger publication changed; reanalyze the query"
                if method == "requireSnapshot"
                else "Delta Ranger access denied"
            )
            assert any(
                f["className"] == "java.lang.SecurityException"
                and f["message"] == expected
                and f["stack"]
                and f["stack"][0].startswith(
                    "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerPolicyEngine."
                    + method
                    + "("
                )
                for f in case["failure"]
            ), "Generic failure is not authorization denial"
        elif outcome == "postcommit-failure":
            assert case["failure"], "Postcommit failure lacks original cause"
        else:
            assert case["failure"] == []
        if committed:
            assert any(
                cb["kind"] == "write-payload"
                and not cb["failed"]
                and cb["appendOwner"]
                and adaptive_payload({"callbacks": [cb]})
                for cb in case["callbacks"]
            ), "Native adaptive payload was not observed"

        def decision(table, access, allowed):
            return any(
                event["user"] == subject
                and event["accessType"] == access
                and event["policyType"] == 0
                and event["allowed"] is allowed
                and all(
                    event["resource"][key] == result[key]
                    for key in ("catalog", "database")
                )
                and set(event["resource"]) == {"catalog", "database", "table", "column"}
                and event["resource"]["table"] == table
                for event in case["rangerRequests"]
            )

        assert decision(case["source"], "select", outcome != "source-denied"), (
            "Missing source SELECT decision"
        )
        if outcome != "source-denied":
            assert decision(
                case["target"],
                "update",
                outcome not in ("target-denied", "denied-duplicate"),
            ), "Missing target UPDATE decision"
        _verify_delta_public_join(result, case)
        if outcome == "after-helper-revoked":
            _verify_delta_barrier(result, case)
        if "checkpoint" in case:
            checkpoint = read_ref(case["checkpoint"])
            assert (
                checkpoint["nativeId"] == after["nativeId"]
                and checkpoint["version"] == after["version"]
            )
            assert rows(checkpoint["rows"]) == rows(after["rows"]), (
                "Checkpoint reopen differs"
            )
            assert checkpoint["files"]
            for descriptor in checkpoint["files"]:
                assert file_ref(descriptor["path"]) == descriptor
        if "warm" in case:
            warm = read_ref(case["warm"])
            assert warm["order"] in ("target-first", "project-first")
            assert (
                rows(warm["targetRows"])
                == rows(warm["repeatedTargetRows"])
                == rows(after["rows"])
            ), "Warm target readback differs"
            assert Counter(tuple(row) for row in warm["dependentRows"]) == Counter(
                (row[0], row[2]) for row in after["rows"]
            ), "Warm project readback differs"


def _verify_delta_public_join(result, case):
    applied = read_ref(case["applied"])
    ranger = read_ref(case["rangerState"])
    downloaded = read_ref(applied["download"])
    stable = lambda value: {
        key: child for key, child in value.items() if key != "policyUpdateTime"
    }
    assert stable(downloaded) == stable(ranger["policies"]), (
        "Applied policy download differs"
    )
    assert applied["policy"] == downloaded["policyVersion"]
    assert applied["role"] == ranger["roles"]["roleVersion"]
    public = read_ref(applied["publicState"])
    assert public["appUser"]["name"] == result["subject"]
    assert public["metadataUser"]["name"] == result["metadataSubject"]
    assert public["appUser"]["roles"] == [public["appRole"]["name"]]
    role_download = read_ref(applied["roleDownload"])
    assert {k: v for k, v in role_download.items() if k != "roleUpdateTime"} == {
        k: v for k, v in ranger["roles"].items() if k != "roleUpdateTime"
    }, "Applied role download differs"
    matching_roles = [
        role
        for role in role_download["rangerRoles"]
        if role["name"] == public["appRole"]["rangerRole"]
    ]
    assert len(matching_roles) == 1
    assert matching_roles[0]["users"] == [{"name": result["subject"], "isAdmin": False}]
    assert not matching_roles[0].get("groups") and not matching_roles[0].get("roles")
    source_allowed = case["outcome"] != "source-denied"
    target_allowed = case["outcome"] not in (
        "target-denied",
        "denied-duplicate",
        "after-helper-revoked",
    )
    for table, privilege, allowed in [
        (case["source"], "SELECT_TABLE", source_allowed),
        (case["target"], "MODIFY_TABLE", target_allowed),
    ]:
        full = result["catalog"] + "." + result["database"] + "." + table
        objects = [
            entry
            for entry in public["appRole"]["securableObjects"]
            if entry["fullName"] == full
        ]
        assert len(objects) <= 1
        privileges = {
            (p["name"].upper(), p["condition"].upper())
            for entry in objects
            for p in entry["privileges"]
        }
        assert ((privilege, "ALLOW") in privileges) is allowed, (
            "Public privilege differs"
        )
        accesses = set()
        for policy in downloaded["policies"]:
            if policy.get("policyType", 0) != 0:
                continue
            resource = policy["resources"]
            if any(
                resource[key]["values"] != [result[key]]
                or resource[key].get("isExcludes", False)
                or resource[key].get("isRecursive", False)
                for key in ("catalog", "database")
            ) or resource["table"]["values"] != [table]:
                continue
            for item in policy["policyItems"]:
                if public["appRole"]["rangerRole"] in item["roles"]:
                    assert not item.get("users") and not item.get("groups")
                    accesses.update(
                        access["type"]
                        for access in item["accesses"]
                        if access["isAllowed"] is True
                    )
        assert (
            ("select" if privilege == "SELECT_TABLE" else "update") in accesses
        ) is allowed, "Downloaded access differs"
    transition = read_ref(case["publicTransition"])
    assert transition["beforePolicy"] < transition["afterPolicy"] == applied["policy"]
    request, response = (
        read_ref(transition["publicRequest"]),
        read_ref(transition["publicResponse"]),
    )
    assert request["method"] == "PUT" and response["code"] == 0
    privilege, grant = transition["privilege"], transition["grant"]
    assert privilege in ("SELECT_TABLE", "MODIFY_TABLE") and type(grant) is bool
    table = case["source"] if privilege == "SELECT_TABLE" else case["target"]
    expected = (
        "/api/metalakes/"
        + result["metalake"]
        + "/permissions/roles/"
        + public["appRole"]["name"]
        + "/table/"
        + result["catalog"]
        + "."
        + result["database"]
        + "."
        + table
        + ("/grant/" if grant else "/revoke/")
    )
    assert request["path"] == expected
    assert request["body"] == {
        "privileges": [{"name": privilege, "condition": "ALLOW"}]
    }
    assert grant is (source_allowed if privilege == "SELECT_TABLE" else target_allowed)
    callbacks = read_ref(transition["callbacks"])
    assert callbacks and any(event["method"] == "PUT" for event in callbacks)
    for event in callbacks:
        assert event["completion"] == "confirmed" and event["downstream"] == "sent"
        assert (
            event["started_ns"] <= event["completed_ns"]
            and len(event["response_sha256"]) == 64
        )
        assert "transport_error" not in event
        assert event["upstream_status"] in (
            (200, 204) if event["method"] != "GET" else (200, 400)
        )


def _verify_delta_barrier(result, case):
    barrier = read_ref(case["barrier"])
    held, armed = read_ref(barrier["held"]), read_ref(barrier["armed"])
    __import__(
        "cdf_live"
    )  # Installs the immutable historical fixture-helper import path.
    from evidence_checks import verify_phase_stack

    verify_phase_stack(held)
    assert (
        held["writerThreadId"] == armed["threadId"] == case["callerBefore"]["jvmThread"]
    )
    assert (
        armed["subject"] == armed["ugi"] == result["subject"] and armed["aqe"] is True
    )
    initial, applied = read_ref(barrier["initialApplied"]), read_ref(case["applied"])
    release, forwarded = read_ref(barrier["release"]), read_ref(barrier["forwarded"])
    assert (
        release["initialPolicy"]
        == initial["policy"]
        < release["policy"]
        == applied["policy"]
    )
    assert release["role"] == initial["role"] == applied["role"]
    assert release["downloadVersion"] == applied["policy"]
    assert release["responseSha256"] == applied["download"]["sha256"]
    assert release["publicStateSha256"] == applied["publicState"]["sha256"]
    assert release["rangerStateSha256"] == case["rangerState"]["sha256"]
    assert all(
        release[key] is True
        for key in (
            "publicRevokeComplete",
            "updateAbsent",
            "roleMatchesInitialDownload",
        )
    )
    assert (
        forwarded["runId"] == held["runId"]
        and forwarded["status"] == 200
        and forwarded["transparent"] is True
    )
    assert len(forwarded["responseSha256"]) == 64
    before = {entry["key"]: entry for entry in read_ref(barrier["rawBefore"])}
    generated = {entry["key"]: entry for entry in read_ref(barrier["rawHeld"])}
    assert all(generated.get(key) == entry for key, entry in before.items())
    added = set(generated) - set(before)
    assert added and all(
        "/adoption/" + case["target"] + "/" in key
        and "/_delta_log/" not in key
        and key.endswith(".parquet")
        for key in added
    ), "Held boundary lacks native generated files before commit"
    after = read_ref(case["after"])
    for key in added:
        artifact = after["objects"][key]
        assert artifact["sha256"] == generated[key]["sha256"]
        assert (
            file_ref(artifact["path"]) == artifact
            and Path(artifact["path"]).read_bytes()[:4] == b"PAR1"
        )
    assert any(
        cb["appendOwner"] and adaptive_payload({"callbacks": [cb]})
        for cb in case["callbacks"]
        if cb["kind"] == "write-payload" and not cb["failed"]
    ), "Post-helper refusal lacks its actual adaptive payload"
    if "runtime" in result:
        runtime = read_ref(result["runtime"])
        coordinator = "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerAppendCoordinator$"
        assert held["classSha256"] == runtime["providerClasses"][coordinator]
        assert (
            held["artifactSha256"]
            == runtime["jarHashes"][runtime["providers"][coordinator]]
        )


def _verify_delta_native_profile(profile, before, after):
    for field in ("metadata", "protocol", "schemaJson"):
        assert before[field] == after[field], "Native table definition changed"
    expected_partitions = ["region"] if profile == "partitioned" else []
    assert (
        before["partitionColumns"] == after["partitionColumns"] == expected_partitions
    )
    if profile not in ("cdf-enabled", "cdf-disabled"):
        return
    enabled = profile == "cdf-enabled"
    for image in (before, after):
        assert (
            image["native"]["properties"]["delta.enableChangeDataFeed"]
            == str(enabled).lower()
        )
    if not enabled:
        assert after["intervals"] == []
        return
    assert len(after["intervals"]) == 1
    interval = after["intervals"][0]
    # These fixtures change the CDF property at v1 before ADOPT; governed inserts start at v2.
    assert interval["startingVersion"] == 2
    assert interval["endingVersion"] == after["version"] >= 2
    assert interval["error"] is None
    assert Counter(tuple(row) for row in interval["rows"]) == Counter(
        tuple(row) + ("insert", version)
        for version in range(2, after["version"] + 1)
        for row in DELTA_APPEND_ROWS
    ), "Native CDF interval differs"


def _verify_delta_warm_state(warm, before, after, version):
    assert warm["freshDataFrames"] is True
    assert warm["order"] in ("target-first", "project-first")
    assert before["matchingOrder"] == (
        ["project", "target"]
        if warm["order"] == "target-first"
        else ["target", "project"]
    )
    assert before["aqe"] is after["aqe"] is True
    assert before["asyncStaleness"] == after["asyncStaleness"] == 0
    assert before["unrelated"] == after["unrelated"]
    assert before["roles"] == after["roles"] == ["target", "project"]
    assert len(before["entries"]) == len(after["entries"]) == 2
    assert after["oldBuffersUnloaded"] == [True, True]
    assert after["observedVersion"] == version
    for old, entry in zip(before["entries"], after["entries"]):
        assert old["loaded"] is True and old["owner"] is None
        assert old["key"] == entry["key"] and old["builder"] != entry["builder"]
        assert entry["loaded"] is False
        assert entry["owner"] and entry["ownerKey"] == entry["key"]
        assert entry["selectedVersion"] == version
        assert entry["selectedHead"] == after["observedHead"]
        assert entry["producerClass"] in (
            "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerRawProducer",
            "org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec",
        )


def _verify_delta_runtime_cases(result):
    runtime = read_ref(result["runtime"])
    from cdf_read_live_package import verify_runtime

    verify_runtime(runtime)
    assert (runtime["spark"], runtime["delta"]) == (result["spark"], result["delta"])
    assert runtime["application"] == "append-aqe"
    plan = read_ref(result["fixturePlan"])
    assert (
        plan["subject"] == result["subject"]
        and plan["metadataPrincipal"] == result["metadataSubject"]
        and plan["serviceAdmin"] == result["verificationSubject"]
    )
    assert (
        len(
            {
                plan["subject"],
                plan["metadataPrincipal"],
                plan["serviceAdmin"],
                plan["plugin"]["username"],
            }
        )
        == 4
    )
    ranger_provider = runtime["providers"][
        "org.apache.ranger.plugin.service.RangerBasePlugin"
    ]
    for artifact in (
        "ranger-plugins-common",
        "ranger-audit-core",
        "ranger-plugins-cred",
        "ranger-plugin-classloader",
        "ranger-authz-api",
    ):
        verify_jar_property(
            ranger_provider,
            "META-INF/maven/org.apache.ranger/" + artifact + "/pom.properties",
            "version",
            "2.8.0",
        )

    assert read_ref(result["persistenceBefore"]) == read_ref(result["persistenceAfter"])
    for case in result["cases"]:
        from live_common import validate_policy_boundary, source_resources

        ranger = read_ref(case["rangerState"])
        assert (
            ranger["policies"]["serviceName"]
            == ranger["roles"]["serviceName"]
            == plan["resourceService"]
        )
        validate_policy_boundary(plan, ranger["policies"]["policies"])
        source_policies = [
            policy
            for policy in ranger["policies"]["policies"]
            if policy["id"] in plan["sourcePolicyIds"]
        ]
        assert len(source_policies) == 2 and {
            policy["policyType"] for policy in source_policies
        } == {1, 2}
        assert [
            policy
            for policy in ranger["policies"]["policies"]
            if policy.get("policyType", 0) in (1, 2)
        ] == source_policies
        for policy in source_policies:
            kind = policy["policyType"]
            assert policy["isEnabled"] is True and policy[
                "resources"
            ] == source_resources(kind, plan)
            items = policy[
                "dataMaskPolicyItems" if kind == 1 else "rowFilterPolicyItems"
            ]
            assert len(items) == 1 and items[0]["roles"] == [plan["rangerAppRole"]]
            assert not items[0].get("users") and not items[0].get("groups")
            assert items[0]["accesses"] == [{"type": "select", "isAllowed": True}]
            if kind == 1:
                assert (
                    items[0]["dataMaskInfo"]["dataMaskType"] == "CUSTOM"
                    and items[0]["dataMaskInfo"]["valueExpr"] == "'MASKED'"
                )
            else:
                assert items[0]["rowFilterInfo"]["filterExpr"] == "region = 'allowed'"
        observation = read_ref(case["observation"])
        for key in ("rangerRequests", "callbacks", "failure"):
            assert observation[key] == case[key], "Actual JVM observation differs"
        caller = read_ref(case["callerEvidence"])
        assert (
            caller["before"] == case["callerBefore"]
            and caller["after"] == case["callerAfter"]
        )
        assert (
            caller["pinnedEnvironment"] == "true"
            and caller["before"]["javaServerType"] == "py4j.JavaServer"
        )
        assert (
            Path(caller["before"]["javaServerOrigin"]).resolve()
            == Path(runtime["providers"]["py4j.JavaServer"]).resolve()
        )
        python = read_ref(case["pythonRuntime"])
        assert (
            python["spark"] == result["spark"]
            and python["py4j"] == "0.10.9.7"
            and python["pinEnvironment"] == "true"
        )
        assert Path(python["executable"]).resolve() == Path(plan["python"]).resolve()
        assert set(python["providers"]) == {
            "py4j",
            "pyspark",
            "py4j.clientserver",
            "pyspark.java_gateway",
            "pyspark.sql.readwriter",
        }
        import hashlib
        import zipfile

        libraries = Path(runtime["sparkHome"]) / "python/lib"
        for name, provider in python["providers"].items():
            container = provider["container"]
            assert file_ref(container["path"]) == container
            assert provider["loader"] == "zipimport.zipimporter"
            expected = (
                libraries / "pyspark.zip"
                if name.startswith("pyspark")
                else next(libraries.glob("py4j*.zip"))
            )
            assert Path(container["path"]).resolve() == expected.resolve()
            with zipfile.ZipFile(container["path"]) as archive:
                assert (
                    hashlib.sha256(archive.read(provider["member"])).hexdigest()
                    == provider["sha256"]
                )
        assert caller["before"]["autoReset"] == "false"
        assert caller["before"]["nullDefaults"] == (
            "false" if case["profile"] == "no-default" else "true"
        )
        assert (
            caller["before"]["transactionAppId"]
            and int(caller["before"]["transactionVersion"]) >= 1
        )
        request = read_ref(case["request"])
        assert (
            request["frontend"] == case["api"]
            and request["target"] == case["target"]
            and request["source"] == case["source"]
        )
        origins = read_ref(case["origins"])
        assert set(origins) == set(runtime["providers"])
        for name, origin in origins.items():
            assert origin["jar"] == runtime["providers"][name]
            assert origin["jarSha256"] == runtime["jarHashes"][origin["jar"]]
            assert origin["classSha256"] == runtime["providerClasses"][name]
        profile = read_ref(case["runtimeProfile"])
        extensions = "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerSparkExtension,io.delta.sql.DeltaSparkSessionExtension"
        assert all(
            profile[key] == extensions
            for key in ("builderExtensions", "sessionExtensions", "sqlConfExtensions")
        )
        assert (
            profile["contextExtensions"]
            == "org.apache.gravitino.spark.connector.authorization.GravitinoAuthorizationSparkSessionExtensions,"
            + extensions
        )
        assert (
            profile["wrapperClass"]
            == "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerCatalogSpark35"
        )
        assert (
            "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerReadRule"
            in profile["resolution"]
        )
        assert (
            "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerAppendCheck"
            in profile["checks"]
        )
        for origin in profile["classOrigins"].values():
            assert (
                origin["sameLoader"] is True
                and runtime["jarHashes"][origin["jar"]] == origin["jarSha256"]
            )
        native_images = []
        target_images = []
        for side in ("before", "after"):
            image = read_ref(case[side])
            native = read_ref(image["nativeEvidence"])
            assert set(native["identity"].values()) == {result["verificationSubject"]}
            target = next(
                table for table in native["tables"] if table["name"] == case["target"]
            )
            target_images.append(target)
            assert image["version"] == target["version"] == target["reopenVersion"]
            assert image["rows"] == target["rows"] == target["reopenRows"]
            assert image["nativeId"] == target["native"]["id"]
            assert all(
                runtime["jarHashes"][entry["origin"]] == entry["sha256"]
                for entry in native["origins"].values()
            )
            commits = []
            for key, descriptor in image["objects"].items():
                assert file_ref(descriptor["path"]) == descriptor
                if (
                    "/adoption/" + case["target"] + "/_delta_log/" in key
                    and Path(key).suffix == ".json"
                    and Path(key).stem.isdecimal()
                ):
                    actions = [
                        json.loads(line)
                        for line in Path(descriptor["path"]).read_text().splitlines()
                        if line
                    ]
                    assert sum("commitInfo" in action for action in actions) == 1
                    commits.append(int(Path(key).stem))
            assert (
                sorted(commits)
                == image["publications"]
                == list(range(image["version"] + 1))
            )
            native_images.append(native)
        _verify_delta_native_profile(case["profile"], *target_images)
        from append_aqe_profiles import verify_registration_transition

        before_image, after_image = read_ref(case["before"]), read_ref(case["after"])
        verify_registration_transition(
            read_ref(before_image["registrations"]),
            read_ref(after_image["registrations"]),
            case["target"],
            before_image["version"],
            after_image["version"],
        )
        before_source = next(
            table for table in native_images[0]["tables"] if table["name"] == "source"
        )
        after_source = next(
            table for table in native_images[1]["tables"] if table["name"] == "source"
        )
        assert before_source == after_source and before_source["version"] == 0
        if "warm" in case:
            warm = read_ref(case["warm"])
            before, after = read_ref(warm["before"]), read_ref(warm["after"])
            _verify_delta_warm_state(
                warm, before, after, read_ref(case["after"])["version"]
            )
        if "checkpoint" in case:
            image = read_ref(case["after"])
            checkpoint = read_ref(case["checkpoint"])
            assert (
                len(checkpoint["files"]) >= 2
                and checkpoint["jsonTailVersion"] == image["version"] == 3
            )
            assert all(
                Path(ref["path"]).read_bytes()[:4] == b"PAR1"
                for ref in checkpoint["files"]
            )
            markers = [
                read_ref(ref)
                for key, ref in image["objects"].items()
                if key.endswith("/" + case["target"] + "/_delta_log/_last_checkpoint")
            ]
            assert (
                len(markers) == 1
                and markers[0]["version"] == 2
                and markers[0]["parts"] >= 2
            )
    if result["qualification"]:
        from append_aqe_profiles import case_specs

        specs = case_specs()
        assert result["caseInventory"] == [case["name"] for case in specs]
        assert len(result["cases"]) == sum(
            2 if case["duplicate"] else case["appendCount"] for case in specs
        )

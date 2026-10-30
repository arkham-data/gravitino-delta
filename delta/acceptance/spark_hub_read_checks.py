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

"""Independent qualification of six actual Spark Hub named-read cases.

verify_observations validates finite semantics using retained artifacts. Only
verify_result is the qualification gate: it additionally revalidates the original
runtime, package, imports and deployed security. Unit artifacts cannot qualify.
"""

from collections import Counter
import hashlib
import json
from pathlib import Path
import zipfile

from cdf_merge_live_checks import file_ref, read_ref, verify_adaptive_observation

CASE_SPECS = {
    "named-filter-mask": (
        "table-wide",
        "collect",
        "allowed",
        [[1, "east", "MASKED"], [3, "east", "MASKED"]],
        ["id", "region", "email"],
    ),
    "projected-count": ("id-only", "count", "allowed", [[2]], ["id"]),
    "column-denied": ("denied-email", "collect", "denied", None, ["email"]),
    "unknown-binding": ("table-wide", "collect", "rejected", None, []),
    "unpublished-registration": ("table-wide", "collect", "rejected", None, []),
    "fresh-action-revoke": ("id-only", "collect", "denied", None, ["id"]),
}
METHOD = "GravitinoDatasetCatalog.get_table_df_by_id"
SETTINGS = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.shuffle.partitions": "4",
    "spark.sql.adaptive.coalescePartitions.initialPartitionNum": "4",
    "spark.sql.autoBroadcastJoinThreshold": "-1",
    "spark.sql.adaptive.autoBroadcastJoinThreshold": "-1",
}
ROWS = [[1, "east", "a"], [2, "west", "b"], [3, "east", "c"]]


def row_multiset(rows):
    """Preserve JSON types as well as duplicate row multiplicity."""
    return (
        None
        if rows is None
        else Counter(json.dumps(row, sort_keys=True, allow_nan=False) for row in rows)
    )


def verify_policy(case, service):
    """Require the actual completed policy response to equal its applied snapshot."""
    policy = read_ref(case["policySnapshot"])
    applied = read_ref(case["appliedPolicy"])
    assert policy["serviceName"] == service, "policy service differs"
    assert (
        type(policy["policyVersion"]) is int and policy["policyVersion"] >= 0
    ), "policy version"
    assert (
        applied["policy"] == policy["policyVersion"] and applied["sameJvm"] is True
    ), "applied policy differs"
    downloaded = read_ref(applied["download"])
    assert (
        applied["download"]["sha256"] == applied["downloadSha256"]
    ), "policy download hash"
    event = applied["downloadEvent"]
    assert (
        event["downstreamResponseCompleted"] is True
        and event["downloadKind"] == "policy"
    ), "policy download incomplete"
    assert (
        event["responseBodyFile"] == applied["download"]["path"]
        and event["responseSha256"] == applied["downloadSha256"]
    ), "policy download event differs"

    def stable(value):
        return {key: item for key, item in value.items() if key != "policyUpdateTime"}

    assert stable(downloaded) == stable(policy), "policy download content differs"
    return policy["policyVersion"]


def verify_fixture_contract(case, result):
    """Check the fixture's actual downloaded grants, filter and mask definitions."""
    policies = read_ref(case["policySnapshot"])["policies"]
    rules = {rule["id"]: rule for rule in policies}
    selected = {
        name: rules[identifier] for name, identifier in result["policyIds"].items()
    }
    for name, rule in selected.items():
        resources = rule["resources"]
        assert resources["catalog"]["values"] == [result["catalog"]] and resources[
            "database"
        ]["values"] == [result["database"]], "fixture canonical resource differs"
        assert "source" in resources["table"]["values"] and all(
            resource.get("isExcludes") is False and resource.get("isRecursive") is False
            for resource in resources.values()
        ), "fixture resource scope differs"
        assert rule["isEnabled"] is (
            case.get("policyPhase") != "revoke-id" or name != "id-only"
        ), "fixture grant generation differs"
    grants = [
        ("id-only", "policyItems", ["id"], [result["subjects"]["id-only"]]),
        (
            "table-wide",
            "policyItems",
            ["*"],
            [result["subjects"]["table-wide"], result["subjects"]["denied-email"]],
        ),
        (
            "deny-email",
            "denyPolicyItems",
            ["email"],
            [result["subjects"]["denied-email"]],
        ),
    ]
    for name, field, columns, users in grants:
        rule = selected[name]
        assert (
            rule["policyType"] == 0 and rule["resources"]["column"]["values"] == columns
        ), "fixture SELECT resource differs"
        assert len(rule[field]) == 1 and set(rule[field][0]["users"]) == set(
            users
        ), "fixture SELECT identity differs"
        assert rule[field][0]["accesses"] == [
            {"type": "select", "isAllowed": True}
        ], "fixture SELECT grant differs"
    subject = result["subjects"][case["identityProfile"]]
    filters = [
        item
        for item in selected["filter"]["rowFilterPolicyItems"]
        if subject in item["users"]
    ]
    masks = [
        item
        for item in selected["mask"]["dataMaskPolicyItems"]
        if subject in item["users"]
    ]
    assert (
        selected["filter"]["policyType"] == 2
        and len(filters) == 1
        and filters[0]["rowFilterInfo"]["filterExpr"] == "region = 'east'"
    ), "fixture filter differs"
    assert (
        selected["mask"]["policyType"] == 1
        and selected["mask"]["resources"]["column"]["values"] == ["email"]
        and len(masks) == 1
        and (
            masks[0]["dataMaskInfo"].get("dataMaskType"),
            masks[0]["dataMaskInfo"].get("valueExpr"),
            masks[0]["dataMaskInfo"].get("conditionExpr"),
        )
        in [("CUSTOM", "'MASKED'", None), ("CUSTOM", "'MASKED'", "")]
    ), "fixture mask differs"


def verify_decisions(case, result, columns, allowed):
    """Attribute original SELECT decisions to the actual caller and canonical table."""
    permissions = []
    for request in case["rangerRequests"]:
        assert request["case"] == case["name"], "request case differs"
        assert (
            request["user"] == result["subjects"][case["identityProfile"]]
        ), "request subject differs"
        assert (
            type(request["policyType"]) is int and type(request["allowed"]) is bool
        ), "request decision types"
        resource = request["resource"]
        assert (resource["catalog"], resource["database"], resource["table"]) == (
            result["catalog"],
            result["database"],
            "source",
        ), "canonical request resource differs"
        if request["policyType"] == 0:
            assert (
                request["accessType"] == "select" and resource["column"] in columns
            ), "original SELECT demand differs"
            permissions.append(request)
    assert permissions, "actual SELECT absent"
    if allowed:
        assert all(
            request["allowed"] for request in permissions
        ), "allowed action has denied SELECT"
        assert {request["resource"]["column"] for request in permissions} == set(
            columns
        ), "SELECT columns absent"
    else:
        assert any(
            not request["allowed"] for request in permissions
        ), "denied SELECT absent"
        assert (
            "java.lang.SecurityException: Delta Ranger access denied"
            in case["error"]["message"]
        ), "actual permission denial absent"


def verify_observations(result: dict) -> None:
    """Validate exact finite outcomes; this pure layer cannot certify a runtime."""
    assert result["schemaVersion"] == 1, "schemaVersion"
    assert result["spark"] in ("3.5.4", "3.5.6") and result["delta"] in (
        "3.3.0",
        "3.3.2",
    ), "runtime pins"
    assert (
        result["rangerServerVersion"] == result["rangerClientVersion"] == "2.8.0"
    ), "Ranger pins"
    assert (result["engine"], result["catalogRoute"], result["storage"]) == (
        "delta",
        "gravitino",
        "minio",
    ), "engine route/storage"
    runtime = read_ref(result["runtimeManifest"])
    assert (runtime["spark"], runtime["delta"]) == (
        result["spark"],
        result["delta"],
    ), "runtime pair differs"
    assert (
        read_ref(result["deployedRanger"])["deployedRangerVersion"] == "2.8.0"
    ), "deployed Ranger differs"
    package = read_ref(result["sparkHubPackage"])
    assert package["sourceInputs"] == result["sourceInputs"], "packaged source differs"
    profiles = {"table-wide", "id-only", "denied-email"}
    assert set(result["processExits"]) == profiles | {
        "supervisor",
        "nativeSeed",
        "nativeReadback",
    }, "original exit inventory"
    for reference in result["processExits"].values():
        exit_record = read_ref(reference)
        assert (
            exit_record["exitCode"] == 0
            and not exit_record.get("terminated", False)
            and not exit_record.get("timedOut", False)
        ), "original process failed"
    assert (
        set(result["profiles"]) == set(result["subjects"]) == profiles
    ), "identity profile inventory"
    assert (
        len(
            set(result["subjects"].values())
            | {result["setupIdentity"], result["metadataPrincipal"]}
        )
        == 5
    ), "separate identities"
    for profile, references in result["profiles"].items():
        observed = read_ref(references["observed"])
        assert (
            observed["spark"],
            observed["scala"],
            observed["java"],
            observed["subject"],
        ) == (
            result["spark"],
            "2.12.21",
            "17",
            result["subjects"][profile],
        ), "actual runtime/identity differs"
        metadata = read_ref(references["metadata"])
        assert (
            metadata["principal"] == result["metadataPrincipal"]
            and metadata["successfulTargetGet"] is True
        ), "metadata identity differs"
        caller = read_ref(references["caller"])
        assert (
            caller["subject"] == observed["subject"]
            and caller["sessionReused"] is True
            and caller["nativeDataFrame"] is True
        ), "actual SparkClient reuse/native frame absent"
        assert (
            caller["pinnedConnection"] is True and caller["pinEnvironment"] == "true"
        ), "actual pinned connection absent"
        assert (
            caller["thread"]["subject"]
            == caller["thread"]["ugi"]
            == observed["subject"]
        ), "actual caller identity differs"
        assert caller["sourceInputs"] == result["sourceInputs"], "caller source differs"
        assert caller["python"] == result["livePython"] and caller[
            "pythonVersion"
        ].startswith("3.11."), "live interpreter differs"
        assert all(
            caller["settings"].get(key) == value for key, value in SETTINGS.items()
        ), "fixture startup settings differ"
    before, after = read_ref(result["nativeBefore"]), read_ref(result["nativeAfter"])
    assert before == after, "native source changed"
    assert (
        before["nativeIdentity"] == result["setupIdentity"]
        and before["spark"] == result["spark"]
    ), "native observation identity differs"
    source = before["tables"]["source"]
    assert source["version"] == 0 and row_multiset(source["rows"]) == row_multiset(
        ROWS
    ), "native source seed differs"
    registration = read_ref(result["registration"])
    assert registration["source"] == {
        "state": "PUBLISHED",
        "nativeId": source["id"],
        "nativeVersion": source["version"],
    }, "source publication differs"
    assert (
        registration["unpublished"]["state"] == "ADOPTING"
    ), "unpublished fixture differs"
    binding = result["binding"]
    assert (binding["catalog"], binding["schema"], binding["table"]) == (
        result["catalog"],
        result["database"],
        "source",
    ), "source binding differs"
    cases = result["cases"]
    assert len(cases) == len(CASE_SPECS) and {case["name"] for case in cases} == set(
        CASE_SPECS
    ), "case inventory"
    for case in cases:
        assert read_ref(case["artifact"]) == {
            key: value for key, value in case.items() if key != "artifact"
        }, "original case artifact differs"
        profile, api, outcome, rows, columns = CASE_SPECS[case["name"]]
        assert (
            case["identityProfile"],
            case["api"],
            case["outcome"],
            case["method"],
            case["actionRoute"],
        ) == (profile, api, outcome, METHOD, "native-pyspark"), "case contract differs"
        assert case["actualApi"] in (
            (None,)
            if outcome == "rejected"
            else (api,)
            if outcome == "allowed"
            else (None, api)
        ), "actual action invocation differs"
        assert (
            case["originalExit"] == result["processExits"][profile]
        ), "original caller exit differs"
        assert row_multiset(case["observedRows"]) == row_multiset(
            rows
        ), "exact row multiset differs"
        assert case["listenerQueuesDrained"] is True, "listener drain absent"
        for key in ("analyzedPlan", "finalPlan"):
            assert file_ref(case[key]["path"]) == case[key], "original plan changed"
        verify_policy(case, result["resourceService"])
        verify_fixture_contract(case, result)
        assert case["forbiddenCalls"] == [], "forbidden fallback invoked"
        caller = read_ref(result["profiles"][profile]["caller"])
        assert (
            case["threadBefore"] == case["threadAfter"] == caller["thread"]
        ), "actual caller thread changed"
        assert all(
            isinstance(address, list)
            and address[0] in ("127.0.0.1", "localhost", "::1")
            for address in case["pythonConnections"]
        ), "external Python service contacted"
        table_calls = [
            call for call in case["nativeCalls"] if call["method"] == "table"
        ]
        if case["name"] == "unknown-binding":
            assert not table_calls, "unknown binding reached Spark"
        else:
            assert len(table_calls) == (
                2 if case["name"] == "named-filter-mask" else 1
            ), "actual named Spark table call absent"
        if outcome == "allowed":
            assert any(
                call["method"] == api and call["module"] == "pyspark.sql.dataframe"
                for call in case["nativeCalls"]
            ), "actual native action method absent"
        expected_calls = 2 if case["name"] == "named-filter-mask" else 1
        assert len(case["adapterCalls"]) == expected_calls and all(
            call == {"method": METHOD, "datasetId": case["sourceBinding"]["datasetId"]}
            for call in case["adapterCalls"]
        ), "actual adapter boundary absent"
        before_requests, after_requests = (
            case["tableRequestsBefore"],
            case["tableRequestsAfter"],
        )
        assert (
            after_requests[: len(before_requests)] == before_requests
        ), "table request history changed"
        if case["name"] == "unknown-binding":
            assert (
                before_requests == after_requests
            ), "unknown binding requested a table"
            assert (
                case["error"]["class"] == "KeyError"
                and not case["rangerRequests"]
                and not case["originalDemands"]
            ), "unknown binding rejection differs"
            assert case["sourceBinding"]["datasetId"] not in {
                binding["datasetId"],
                result["unpublishedBinding"]["datasetId"],
            }, "unknown UUID substituted"
        elif case["name"] == "unpublished-registration":
            assert (
                case["sourceBinding"] == result["unpublishedBinding"]
            ), "unpublished source binding differs"
            assert (
                case["error"]["class"] != "KeyError"
                and "Published registration metadata is unavailable"
                in case["error"]["message"]
            ), "unpublished named rejection absent"
            assert any(
                request["method"] == "GET"
                and request["path"].endswith("/tables/unpublished")
                and request["status"] == 404
                for request in after_requests[len(before_requests) :]
            ), "unpublished named request did not return not-found"
        else:
            assert case["sourceBinding"] == binding, "case source binding differs"
            verify_decisions(case, result, columns, outcome == "allowed")
            demands = Counter(
                (d["catalog"], d["database"], d["table"], tuple(sorted(d["columns"])))
                for d in case["originalDemands"]
            )
            occurrences = (
                [columns, ["id"]] if case["name"] == "named-filter-mask" else [columns]
            )
            expected = Counter(
                (result["catalog"], result["database"], "source", tuple(sorted(demand)))
                for demand in occurrences
            )
            assert demands == expected, "original occurrence demand differs"
        if outcome == "allowed":
            assert (
                case["error"] is None
                and case["actualActionQueries"] > 0
                and case["callbacks"]
            ), "actual successful native action absent"
            assert all(
                c["case"] == case["name"]
                and c["caseSequence"] == case["caseSequence"]
                and c["failed"] is False
                for c in case["callbacks"]
            ), "actual callback attribution differs"
        else:
            assert (
                case["error"] and case.get("execution") is None
            ), "rejected action claimed a payload"
        if case["name"] == "named-filter-mask":
            assert (
                case["execution"]["plan"] == case["finalPlan"]
            ), "AQE plan is not case payload"
            assert case["execution"]["phase"] == "ordinary", "AQE payload phase differs"
            verify_adaptive_observation(case["execution"])
        if case["name"] == "fresh-action-revoke":
            assert (
                case["freshConstruction"] is True and case["policyPhase"] == "revoke-id"
            ), "fresh revoke action absent"
            precondition = read_ref(case["precondition"])
            assert (
                precondition["outcome"] == "allowed"
                and precondition["actualActionQueries"] > 0
                and precondition["freshConstruction"] is True
            ), "authorized revoke precondition absent"
            assert row_multiset(precondition["observedRows"]) == row_multiset(
                [[1], [3]]
            ), "revoke precondition rows differ"
            verify_decisions(
                dict(
                    precondition,
                    name=precondition.get("name", case["name"]),
                    identityProfile=profile,
                ),
                result,
                ["id"],
                True,
            )
            assert verify_policy(case, result["resourceService"]) > verify_policy(
                precondition, result["resourceService"]
            ), "revoke generation did not advance"


def verify_python_runtime(caller, package, runtime):
    """Bind loaded stock PySpark/Py4J ZIP members to normal installed probe bytes."""
    assert (
        caller["nativeSparkVersion"],
        caller["nativePy4jVersion"],
        caller["pinEnvironment"],
    ) == (runtime["spark"], "0.10.9.7", "true"), "actual native Python versions differ"
    libraries = Path(runtime["sparkHome"]) / "python/lib"
    py4j_archives = list(libraries.glob("py4j*.zip"))
    assert len(py4j_archives) == 1, "ambiguous stock Py4J provider"
    providers = caller["pythonProviders"]
    expected_names = {
        "py4j",
        "py4j.clientserver",
        "pyspark",
        "pyspark.java_gateway",
        "pyspark.sql.dataframe",
        "pyspark.sql.readwriter",
        "pyspark.sql.session",
        "pyspark.sql.functions",
    }
    assert (
        set(providers) == set(package["pythonProviders"]) == expected_names
    ), "actual Python provider inventory differs"
    for name, provider in providers.items():
        container = provider["container"]
        assert (
            file_ref(container["path"]) == container
        ), "actual Python provider container changed"
        expected = (
            libraries / "pyspark.zip"
            if name.startswith("pyspark")
            else py4j_archives[0]
        )
        assert (
            Path(container["path"]).resolve() == expected.resolve()
            and provider["loader"] == "zipimport.zipimporter"
        ), "actual Python provider origin differs"
        assert provider["path"].endswith(
            "/" + provider["member"]
        ), "actual Python member path differs"
        with zipfile.ZipFile(container["path"]) as archive:
            actual = hashlib.sha256(archive.read(provider["member"])).hexdigest()
        assert (
            actual == provider["sha256"] == package["pythonProviders"][name]["sha256"]
        ), "actual Python provider bytes differ"


def verify_process_provenance(result):
    """Join each original exit and capture to its role, command and output."""
    profiles = {"table-wide", "id-only", "denied-email"}
    roles = profiles | {"supervisor", "nativeSeed", "nativeReadback"}
    assert (
        set(result["processExits"]) == set(result["processCaptures"]) == roles
    ), "process role inventory"
    root = Path(result["nativeBefore"]["path"]).parent.parent.parent
    directories = {
        "supervisor": root.parent / (root.name + "-supervisor"),
        "nativeSeed": root / "fixture/native-seed/process",
        "nativeReadback": root / "native-readback/process",
        **{profile: root / profile / "process" for profile in profiles},
    }
    script_dir = Path(__file__).parent
    runtime = read_ref(result["runtimeManifest"])
    commands = {}
    for role, directory in directories.items():
        capture = result["processCaptures"][role]
        assert set(capture) == {
            "argv.json",
            "stdout",
            "stderr",
        }, "process capture inventory"
        for name, reference in capture.items():
            assert reference == file_ref(directory / name), "process capture differs"
        exit_ref = result["processExits"][role]
        assert exit_ref == file_ref(
            directory / "exit.json"
        ), "original process exit differs"
        record = read_ref(exit_ref)
        assert (
            record["exitCode"] == 0
            and not record.get("terminated", False)
            and not record.get("timedOut", False)
        ), "original process failed"
        commands[role] = read_ref(capture["argv.json"])
    assert set(result["nativeRequests"]) == {
        "nativeSeed",
        "nativeReadback",
    }, "native request inventory"
    native_requests = {}
    for role, mode, output_key in (
        ("nativeSeed", "seed", "nativeBefore"),
        ("nativeReadback", "verify", "nativeAfter"),
    ):
        directory = directories[role].parent
        request_path = directory / "request.private.json"
        assert result["nativeRequests"][role] == file_ref(
            request_path
        ), "native request changed"
        request = read_ref(result["nativeRequests"][role])
        native_requests[role] = {
            key: value
            for key, value in request.items()
            if key not in ("mode", "output")
        }
        assert result[output_key] == file_ref(
            directory / "result.json"
        ), "native output differs"
        assert (
            request["mode"] == mode
            and request["output"] == str(directory)
            and request["nativeIdentity"] == result["setupIdentity"]
            and (request["spark"], request["delta"])
            == (result["spark"], result["delta"])
        ), "native request differs"
        assert commands[role] == [
            runtime["sparkHome"] + "/bin/spark-submit",
            "--driver-memory",
            "2g",
            str(script_dir / "read_parity_pyspark.py"),
            str(request_path),
        ], "native command differs"
    assert (
        native_requests["nativeSeed"] == native_requests["nativeReadback"]
    ), "native readback request differs"
    for profile in profiles:
        assert result["profiles"][profile]["caller"] == file_ref(
            root / profile / "python-evidence/caller.json"
        ), "caller output differs"
        caller = read_ref(result["profiles"][profile]["caller"])
        request = caller["request"]
        assert request == file_ref(
            root / profile / "request/spark-hub-request.private.json"
        ), "caller request differs"
        caller_request = read_ref(request)
        assert (
            caller_request["identityProfile"] == profile
            and caller_request["pythonOutput"]
            == str(root / profile / "python-evidence")
            and caller_request["output"] == str(root / profile / "app-evidence")
        ), "caller role/output differs"
        assert commands[profile][0] == runtime[
            "sparkHome"
        ] + "/bin/spark-submit" and commands[profile][-2:] == [
            str(script_dir / "spark_hub_read_pyspark.py"),
            request["path"],
        ], "caller command differs"
    package = read_ref(result["sparkHubPackage"])
    inputs = result["supervisorInputs"]
    assert inputs == file_ref(inputs["path"]), "supervisor inputs changed"
    argv = commands["supervisor"]
    assert argv[1:] == [
        str(script_dir / "spark_hub_read_live.py"),
        "--worker",
        "--runtime",
        result["runtimeManifest"]["path"],
        "--spark-hub-root",
        package["root"],
        "--inputs",
        inputs["path"],
        "--out",
        str(root),
    ], "supervisor command differs"
    assert result["workerResult"] == file_ref(
        root / "result.worker.json"
    ), "worker output differs"
    worker = dict(result)
    for key in ("workerResult", "supervisorInputs"):
        worker.pop(key)
    worker["processCaptures"] = {
        key: value
        for key, value in result["processCaptures"].items()
        if key != "supervisor"
    }
    worker["processExits"] = {
        key: value
        for key, value in result["processExits"].items()
        if key != "supervisor"
    }
    assert read_ref(result["workerResult"]) == worker, "original worker result differs"


def verify_result(result: dict) -> None:
    """Full live gate: original processes, finite semantics, packages and security."""
    verify_observations(result)
    assert result.get("evidenceKind") == "actual-live", "not live qualification"
    verify_process_provenance(result)
    from cdf_read_live_package import verify_runtime
    from read_parity_checks import verify_jar_property
    from spark_hub_read_live import verify_spark_hub_package

    package = read_ref(result["sparkHubPackage"])
    verify_spark_hub_package(package)
    runtime = read_ref(result["runtimeManifest"])
    verify_runtime(runtime)
    ranger = "org.apache.ranger.plugin.service.RangerBasePlugin"
    assert ranger in runtime["providers"], "Ranger client provider absent"
    verify_jar_property(
        runtime["providers"][ranger],
        "META-INF/maven/org.apache.ranger/ranger-plugins-common/pom.properties",
        "version",
        "2.8.0",
    )
    for profile, references in result["profiles"].items():
        origins = read_ref(references["origins"])
        for name, path in runtime["providers"].items():
            assert origins[name] == {
                "jar": path,
                "jarSha256": runtime["jarHashes"][path],
                "classSha256": runtime["providerClasses"][name],
            }, "actual provider differs"
        security = read_ref(references["security"])
        assert (
            security["freshEmptyCache"] is True
            and security["mode"] == "empty"
            and set(security["changedPropertyNames"])
            == {
                "ranger.plugin.spark.policy.rest.url",
                "ranger.plugin.spark.policy.cache.dir",
            }
        ), "fresh actual security configuration absent"
        caller = read_ref(references["caller"])
        assert caller["package"] == result["sparkHubPackage"], "caller package differs"
        assert (
            caller["dependencies"] == package["dependencies"]
        ), "actual Python dependency differs"
        verify_python_runtime(caller, package, runtime)
        capture = result["processCaptures"][profile]
        for artifact in capture.values():
            assert (
                file_ref(artifact["path"]) == artifact
            ), "original process capture changed"
        argv = read_ref(capture["argv.json"])
        request = read_ref(caller["request"])
        assert argv[-1] == caller["request"]["path"] and argv[-2] == str(
            Path(__file__).parent / "spark_hub_read_pyspark.py"
        ), "original caller command differs"
        assert (
            Path(result["processExits"][profile]["path"]).parent
            == Path(capture["argv.json"]["path"]).parent
        ), "original caller exit substituted"
        assert (
            request["subject"] == result["subjects"][profile]
            and request["identityProfile"] == profile
            and request["sparkHubPackage"] == result["sparkHubPackage"]
        ), "original caller request differs"
        assert (
            caller["actualGatewayUri"] != caller["fixtureUpstreamUri"]
            and caller["fixtureUpstreamUri"] == request["fixtureUpstreamUri"]
        ), "actual gateway profile differs"
    assert read_ref(result["nativeObjectsBefore"]) == read_ref(
        result["nativeObjectsAfter"]
    ), "native object bytes changed"
    receipts = read_ref(result["publicationReceipts"])
    prefix = "delta.registration.receipt."
    native = read_ref(result["nativeBefore"])
    for table, binding in (
        ("source", result["binding"]),
        ("unpublished", result["unpublishedBinding"]),
    ):
        receipt = receipts[table]
        assert receipt[prefix + "state"] == (
            "PUBLISHED" if table == "source" else "ADOPTING"
        ), "original publication differs"
        assert (
            binding["datasetId"] == native["tables"][table]["id"]
        ), "exact UUID/name mapping differs"
        if table == "source":
            assert (
                receipt[prefix + "nativeId"] == binding["datasetId"]
                and int(receipt[prefix + "nativeVersion"]) == 0
            ), "original native publication differs"
    for case in result["cases"]:
        raw = read_ref(case["rawArtifact"])
        assert raw == {
            key: value
            for key, value in case.items()
            if key not in ("artifact", "rawArtifact", "originalExit")
        }, "original raw caller case differs"

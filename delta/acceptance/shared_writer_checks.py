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

"""Finite shared JDBC / actual job_wrapper evidence; attempted jobs never become retries."""
from collections import Counter
import json
import re

from external_jdbc_checks import PAIRS, LEAD, verify_identities, verify_process, verify_sql
from cdf_merge_live_checks import read_ref

PRODUCER = "8af43fa12c01394c32f1fef6196a3d384bd604e3"

COMMON = ("shared_create_append", "shared_existing_append", "shared_additive_append", "shared_merge_schema_false", "shared_update_denied")
EXTRA = ("shared_create_denied", "shared_alter_denied", "shared_metadata_denied", "shared_cdf_enable_append", "shared_late_target_revoke", "shared_postcommit_hook_failure", "shared_tracker_failure")
REQUIRED_CALLS = {
    ("src.shared.commands.job_runner", "job_wrapper"),
    ("src.shared.commands.job_runner", "wrapped_job"),
    ("src.shared.commands.job_runner", "_run_gravitino_job"),
    ("src.data_platform.sources.jdbc.v2.main", "run_source_jdbc_v2"),
    ("src.shared.services.v2.spark", "query_jdbc"),
    ("src.shared.services.v3.gravitino_jdbc", "read"),
    ("src.shared.commands.progress_tracker", "_run"),
    ("src.shared.services.v2.spark", "init_df_tracking"),
    ("src.shared.services.v3.gravitino_writer", "write_dataset"),
    ("src.shared.services.v3.gravitino_job", "open"),
}


def case_names(pair):
    assert tuple(pair) in PAIRS
    return COMMON + (EXTRA if tuple(pair) == LEAD else ())


def contract(name):
    """Original jobs, exact action invocations and phase outcomes frozen before execution."""
    assert name in COMMON + EXTRA
    create = name in ("shared_create_append", "shared_create_denied", "shared_late_target_revoke")
    cdf = name == "shared_cdf_enable_append"
    early = name in ("shared_merge_schema_false", "shared_create_denied", "shared_alter_denied", "shared_metadata_denied", "shared_update_denied")
    result = dict(create="not_attempted", cdf="not_attempted", data="not_attempted")
    if create:
        result["create"] = "unknown" if name == "shared_create_denied" else "confirmed"
    if cdf:
        result["cdf"] = "confirmed"
    if not early:
        result["data"] = "unknown" if name == "shared_late_target_revoke" else "confirmed"
    elif name in ("shared_alter_denied", "shared_update_denied"):
        result["data"] = "unknown"
    error = {
        "shared_merge_schema_false": "writer", "shared_create_denied": "writer", "shared_alter_denied": "writer",
        "shared_metadata_denied": "writer", "shared_update_denied": "writer", "shared_late_target_revoke": "writer",
        "shared_postcommit_hook_failure": "hook", "shared_tracker_failure": "tracker",
    }.get(name)
    calls = dict(source=1, sourceRead=2, remoteCount=1, trackerCount=1, writer=1, dataWriter=0 if name in ("shared_merge_schema_false", "shared_create_denied", "shared_metadata_denied") else 1)
    query = ("SELECT region, CAST(42 AS INTEGER) extra, CAST(secret AS TEXT) AS secret, id FROM source_schema.source"
             if name in ("shared_additive_append", "shared_merge_schema_false", "shared_alter_denied")
             else "SELECT id, CAST(secret AS TEXT) AS secret, region FROM source_schema.source")
    value = dict(job="initial", query=query, separateMetadataExpected=name not in ("shared_create_denied", "shared_metadata_denied"), phases=result, errorPhase=error, calls=calls,
                 sql=dict(schemaQueries=3, remoteCountQueries=1, payloadQueries=1 + int(not early), mutationQueries=0),
                 publications=(int(create and name != "shared_create_denied") + int(cdf) + int(not early and name != "shared_late_target_revoke")),
                 appendedRows=3 if not early and name != "shared_late_target_revoke" else 0)
    if name == "shared_additive_append":
        import copy
        following = copy.deepcopy(value)
        following["job"] = "matching_schema_next_append"
        return (value, following)
    return (value,)


def verify_inventory(cells):
    assert Counter((c["spark"], c["delta"]) for c in cells) == Counter(PAIRS)
    for cell in cells:
        assert Counter(cell["cases"]) == Counter(case_names((cell["spark"], cell["delta"])))


def verify_actions(name, index, calls):
    assert calls == contract(name)[index]["calls"], "Missing/extra/repeated action"


def verify_outcome(name, index, value):
    expected = contract(name)[index]
    assert value["phases"] == expected["phases"]
    assert value["errorPhase"] == expected["errorPhase"]
    assert value["attempted"] is True and value["retryEligible"] is False


def verify_caller(value):
    main, tracker = value["main"], value["tracker"]
    assert main["python"] != tracker["python"] and main["jvm"] != tracker["jvm"]
    assert main["session"] and main["ugi"]
    assert all(main[k] == tracker[k] for k in ("session", "ugi", "groups"))
    assert all(value[k] is True for k in ("joinedBeforeClose", "readerLiveDuringCount", "readerLiveUntilClose", "readerClosed"))
    assert value["sparkStopped"] is False
    assert value["remoteCount"] == value["trackerCount"] == 3


def verify_calls(calls):
    assert REQUIRED_CALLS <= {(c["module"], c["function"]) for c in calls}, "Actual wrapper call trace missing"


def verify_denial(name, failures):
    if name in ("shared_create_append", "shared_existing_append", "shared_additive_append", "shared_cdf_enable_append", "shared_postcommit_hook_failure", "shared_tracker_failure"):
        assert not failures
    elif name == "shared_merge_schema_false":
        assert not failures  # Local projection rejects before native writer invocation.
    elif name == "shared_metadata_denied":
        assert any(f["className"] == "java.lang.SecurityException" and f["message"] == "Delta metadata observation rejected" for f in failures)
        assert any("DeltaRangerTableMetadata" in line for f in failures for line in f.get("stack", []))
    elif name == "shared_late_target_revoke":
        from input_timestamp_checks import verify_timestamp_outcome
        verify_timestamp_outcome("after-helper-revoked", failures)
    else:
        assert any(f["className"] == "java.lang.SecurityException" and f["message"] == "Delta Ranger access denied" for f in failures)
        assert any("DeltaRangerPolicyEngine.authorize" in line for f in failures for line in f.get("stack", []))


def verify_barrier(name, value):
    if name != "shared_late_target_revoke":
        assert value is None
        return
    assert set(value) == {"held", "release", "forwarded", "rawHeld", "rawBefore"}
    barrier = {key: read_ref(reference) for key, reference in value.items()}
    assert barrier["forwarded"]["status"] == 200 and barrier["forwarded"]["transparent"] is True
    assert barrier["forwarded"]["runId"] == barrier["held"]["runId"]
    assert "publicRevokeComplete" not in barrier["release"]
    assert barrier["release"]["exactRangerDenyComplete"] is True
    assert barrier["release"]["unrelatedPoliciesUnchanged"] is True
    assert barrier["release"]["roleMatchesInitialDownload"] is True
    assert barrier["release"]["access"] == "update"
    assert barrier["release"]["policyPriority"] == 1
    assert barrier["release"]["policy"] > barrier["release"]["initialPolicy"]
    residue = {entry["key"] for entry in barrier["rawHeld"]} - {entry["key"] for entry in barrier["rawBefore"]}
    assert any(key.endswith(".parquet") for key in residue)
    assert not any(key.endswith(("/00000000000000000001.json", "/00000000000000000001.crc")) for key in residue)


def verify_public_record(value):
    text = json.dumps(value).lower()
    assert not any(token in text for token in ("password", "jdbc:postgresql:", "jdbcoptions", "credential", '"environment"', "fixtureplansha256", "originalsecuritysha256", "copiedsecuritysha256")), "Sensitive or credential-derived public evidence"


def verify_native(name, index, before, after):
    expected = contract(name)[index]
    assert before["checksums"] == before["publications"]
    assert after["checksums"] == after["publications"]
    assert after["version"] == before["version"] + expected["publications"]
    assert after["publications"] == list(range(after["version"] + 1))
    assert before["publications"] == list(range(before["version"] + 1))
    if before["version"] >= 0:
        assert before["nativeId"] == after["nativeId"]
    assert len(after["rows"]) == len(before["rows"]) + expected["appendedRows"]
    old = Counter(tuple(r[:3]) for r in before["rows"])
    new = Counter(tuple(r[:3]) for r in after["rows"])
    expected_rows = [(1, "alpha", "allowed"), (2, "beta", "blocked"), (3, "gamma", "allowed")] if expected["appendedRows"] else []
    assert new - old == Counter(expected_rows)
    assert not old - new
    if expected["appendedRows"]:
        assert after["properties"]["delta.enableChangeDataFeed"] == "true"
    if name == "shared_additive_append":
        assert after["schema"] == ["id", "secret", "region", "__arkham_loaded_at", "extra"]
        assert sum(row[-1] is None for row in after["rows"]) == 1
        assert sum(row[-1] == 42 for row in after["rows"]) == (index + 1) * 3
    if after["version"] >= 0:
        assert after["registeredVersion"] == after["version"]


def verify_visibility(name, requests):
    actor = [value for value in requests if value.get("identityKind") == "shared-application"
             and value["method"] == "GET" and value["path"].endswith("/tables/" + name)]
    assert actor and all(value["status"] > 0 for value in actor), "Application visibility was not forwarded"
    if name in ("shared_create_append", "shared_create_denied", "shared_late_target_revoke"):
        assert any(value["status"] == 404 for value in actor), "Authorized absence not observed"
    elif name != "shared_metadata_denied":
        assert any(value["status"] == 200 for value in actor)


def verify_case(record):
    name = record["name"]
    verify_public_record(record)
    assert len(record["jobs"]) == len(contract(name))
    verify_process(record["writerExit"])
    for index, job in enumerate(record["jobs"]):
        expected = contract(name)[index]
        assert job["job"] == expected["job"]
        observed = read_ref(job["caller"])
        assert observed["sourceQuery"] == expected["query"]
        verify_actions(name, index, observed["actionCounts"])
        verify_calls(observed["calls"])
        verify_caller(observed)
        verify_outcome(name, index, observed)
        verify_denial(name, read_ref(job["observation"])["failure"])
        verify_visibility(name, read_ref(job["namedRequests"]))
        assert verify_sql(job) == expected["sql"]
        verify_native(name, index, read_ref(job["before"]), read_ref(job["after"]))
        before_raw, after_raw = [read_ref(read_ref(job[key])["rawEvidence"]) for key in ("before", "after")]
        retained = {item["key"]: item for item in after_raw}
        assert all(retained.get(item["key"]) == item for item in before_raw), "Original native bytes changed"
        if name == "shared_create_denied":
            # Registration404 cannot establish absence of orphan native publications.
            assert before_raw == after_raw, "Denied CREATE mutated raw native objects"
    verify_barrier(name, record.get("barrier"))


def verify_producer(value, expected_commit, source_files, archive_path, archive_digest, process_id):
    assert value["producer"] == expected_commit and re.fullmatch("[0-9a-f]{40}", expected_commit)
    assert value["processId"] == process_id
    assert {module for module, _ in REQUIRED_CALLS} <= set(value["modules"])
    for module, source in value["modules"].items():
        assert source["container"] == dict(path=archive_path, sha256=archive_digest)
        assert source["member"] in source_files and source["sha256"] == source_files[source["member"]]
        assert source["path"] == archive_path + "/" + source["member"]
        assert module.startswith("src.")


def verify_column_persistence(before, after, catalog, cases):
    assert before and after
    for key, value in before.items():
        assert key in after and after[key]["id"] == value["id"]
        assert after[key]["version"] >= value["version"]
    for case in cases:
        last = read_ref(case["jobs"][-1]["after"])
        if last["version"] < 0:
            continue
        prefix = catalog + "." + case["name"] + "."
        columns = {key[len(prefix):]: value for key, value in after.items() if key.startswith(prefix)}
        assert set(columns) == set(last["schema"])
        assert len({value["id"] for value in columns.values()}) == len(columns)
        if case["name"] == "shared_additive_append":
            assert prefix + "extra" not in before and prefix + "extra" in after


def verify_native_image(image):
    from pathlib import Path
    import hashlib
    from registered_jdbc_checks import verify_artifact
    raw = read_ref(image["rawEvidence"])
    folder = Path(image["rawEvidence"]["path"]).parent / "raw"
    for entry in raw:
        verify_artifact(dict(path=str(folder / hashlib.sha256(entry["key"].encode()).hexdigest()), sha256=entry["sha256"]))
    if image["version"] < 0:
        assert image["rows"] == image["publications"] == image["checksums"] == []
        return
    native = read_ref(image["nativeEvidence"])["tables"]
    assert len(native) == 1
    table = native[0]
    assert image["version"] == table["version"] == table["reopenVersion"]
    assert image["rows"] == table["rows"] == table["reopenRows"]
    assert image["nativeId"] == table["native"]["id"]
    assert image["schema"] == [f["name"] for f in json.loads(table["schemaJson"])["fields"]]
    assert image["properties"] == table["native"]["properties"]
    prefix = table["location"].split("/", 3)[3].rstrip("/") + "/_delta_log/"
    for suffix, key in ((".json", "publications"), (".crc", "checksums")):
        derived = sorted(int(Path(e["key"]).stem) for e in raw if e["key"].startswith(prefix) and Path(e["key"]).suffix == suffix and Path(e["key"]).stem.isdecimal())
        assert image[key] == derived
    registration = read_ref(image["registration"])
    assert registration["status"] == 200
    properties = registration["body"]["table"]["properties"]
    assert properties["delta.registration.receipt.nativeId"] == image["nativeId"]
    assert int(properties["delta.registration.receipt.nativeVersion"]) == image["version"]
    assert set(image["commits"]) == {str(v) for v in image["publications"]}
    for reference in image["commits"].values():
        verify_artifact(reference)
        actions = [json.loads(line) for line in Path(reference["path"]).read_text().splitlines()]
        assert not any("remove" in action for action in actions)


def verify_segment_runtime(original, current):
    """Join retained test-helper revisions only when pinned production inputs and bytes agree."""
    from cdf_read_live_package import companion_sources, shared_sources
    assert (original["spark"], original["delta"]) == (current["spark"], current["delta"])
    assert companion_sources(original["sourceInputs"]) and shared_sources(original["sourceInputs"])
    assert companion_sources(original["sourceInputs"]) == companion_sources(current["sourceInputs"])
    assert shared_sources(original["sourceInputs"]) == shared_sources(current["sourceInputs"])
    for key in ("sharedArtifacts", "stockJarHashes", "changedRuntimeClasses", "pythonSourceInputs", "pythonZipMembers"):
        assert original[key] == current[key]
    assert original["jarHashes"][original["changedRuntime"]] == current["jarHashes"][current["changedRuntime"]]


def verify_origins(origins, runtime):
    assert set(origins) == set(runtime["providers"])
    for name, origin in origins.items():
        assert origin["jar"] == runtime["providers"][name]
        assert origin["jarSha256"] == runtime["jarHashes"][origin["jar"]]
        assert origin["classSha256"] == runtime["providerClasses"][name]


def verify_cell(result):
    from pathlib import Path
    import hashlib
    import zipfile
    from cdf_read_live_package import _verify_runtime_artifacts, sha
    from registered_jdbc_checks import verify_versions, verify_python_runtime
    from external_jdbc_checks import load_fixture_identity
    runtime = read_ref(result["runtime"])
    _verify_runtime_artifacts(runtime)
    verify_versions(runtime)
    pair = (result["spark"], result["delta"])
    assert pair == (runtime["spark"], runtime["delta"])
    assert result["caseInventory"] == list(case_names(pair)) == [c["name"] for c in result["cases"]]
    verify_process(result["supervisorExit"])
    verify_process(result["fixtureExit"])
    plan = load_fixture_identity(result)
    deployed = read_ref(result["deployedRanger"])
    assert deployed["associated"] is True and deployed["installedJarHashesRechecked"]
    assert sha(deployed["rootProof"]) == deployed["rootProofSha256"]
    source = read_ref(result["source"])
    assert source["writeDenied"] is True and source["upstreamRole"] == "extractor" and "f|t|f|f" in source["acl"]
    terminal = read_ref(result["sourceTerminal"])
    assert terminal["preservedData"] is True and terminal["state"]["Running"] is False and terminal["state"]["ExitCode"] == 0
    producer = read_ref(result["hubSource"])
    assert producer["commit"] == PRODUCER, "Wrong immutable Hub producer"
    from registered_jdbc_checks import verify_artifact
    verify_artifact(producer["archive"])
    with zipfile.ZipFile(producer["archive"]["path"]) as archive:
        assert producer["files"] == {name: hashlib.sha256(archive.read(name)).hexdigest() for name in archive.namelist()}
    for case in result["cases"]:
        case_runtime = read_ref(case.get("runtime", result["runtime"]))
        _verify_runtime_artifacts(case_runtime)
        verify_versions(case_runtime)
        verify_segment_runtime(case_runtime, runtime)
        verify_case(case)
        callers = [read_ref(job["caller"]) for job in case["jobs"]]
        pid = read_ref(case["writerExit"])["pid"]
        assert all(value["jvmProcessId"] == value["parentProcessId"] == pid for value in callers)
        assert len({value["processId"] for value in callers}) == 1
        assert len({value["readerIdentity"] for value in callers}) == len(callers)
        verify_producer(read_ref(case["hubProvenance"]), producer["commit"], producer["files"], producer["archive"]["path"], producer["archive"]["sha256"], callers[0]["processId"])
        verify_python_runtime(read_ref(case["pythonRuntime"]), case_runtime, plan)
        jvm = Path(case["origins"]["path"]).parent
        observed = json.loads((jvm.parent / "python/actual-runtime.json").read_text())
        verify_identities(dict(spark=observed["spark"], delta=observed["delta"], server=deployed["deployedRangerVersion"], client="2.8.0", java=observed["java"], scala=observed["scala"]))
        assert observed["subject"] == plan["subject"] and observed["jvmProcessId"] == pid
        from shared_writer_live import shared_settings
        assert observed["qualifiedSettings"] == shared_settings()
        if not contract(case["name"])[0]["separateMetadataExpected"]:
            expected = json.loads((jvm.parent / "python/expected-metadata-absence.json").read_text())
            assert expected["message"] == "requirement failed: Separately authenticated metadata lookup was not observed"
        else:
            actual = json.loads((jvm / "observed.json").read_text())
            verify_identities(dict(spark=actual["spark"], delta=runtime["delta"], server=deployed["deployedRangerVersion"], client="2.8.0", java=actual["java"], scala=actual["scala"]))
            assert actual["subject"] == plan["subject"]
        origins = read_ref(case["origins"])
        verify_origins(origins, case_runtime)
        for job in case["jobs"]:
            for key in ("before", "after"):
                verify_native_image(read_ref(job[key]))
    verify_column_persistence(read_ref(result["columnsBefore"]), read_ref(result["columnsAfter"]), plan["catalog"], result["cases"])
    return True

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

"""Serial public-grant append fixture runner. Original exits are checked externally.

The runner retains raw-result.json; after its original supervisor exit is captured,
join that exit in a fresh checked-result artifact and call verify_delta_append.
"""

import argparse
from collections import Counter
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import traceback

from append_aqe_fixture import native
from append_aqe_profiles import case_specs, settings, verify_registration_transition
from append_aqe_public import transition
from cdf_live import (
    Public,
    clean_environment,
    prepare_request,
    read,
    save,
    sha,
    verify_deployed,
)
from cdf_merge_live_checks import file_ref
from cdf_read_live_package import ROOT, verify_runtime
from live_common import OwnedObjects


def wait_for(path, process, seconds):
    deadline = time.monotonic() + seconds
    while not path.exists():
        assert process.poll() is None and time.monotonic() < deadline, (
            "Original process failed before required evidence; no replay"
        )
        time.sleep(0.025)


def start(argv, environment, output):
    output.mkdir(mode=0o700)
    save(
        output / "command.json",
        dict(
            argv=list(map(str, argv)),
            started=time.time(),
            environmentAllowlist={
                k: environment[k]
                for k in ("JAVA_HOME", "SPARK_HOME", "SPARK_USER", "HADOOP_USER_NAME")
                if k in environment
            },
        ),
    )
    log = (output / "process.private.log").open("wb")
    process = subprocess.Popen(
        list(map(str, argv)),
        env=environment,
        cwd=ROOT,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    save(output / "process.json", dict(pid=process.pid, started=time.time()))
    return process, log


def finish(process, log, output, seconds=30):
    timed = False
    try:
        code = process.wait(timeout=seconds)
    except subprocess.TimeoutExpired:
        timed = True
        process.terminate()
        try:
            code = process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            process.kill()
            code = process.wait(timeout=10)
    finally:
        log.close()
    save(
        output / "exit.json",
        dict(pid=process.pid, exitCode=code, timedOut=timed, finished=time.time()),
    )
    return code, timed


def observe(plan, runtime, table, output, intervals=None):
    output.mkdir(mode=0o700)
    objects = OwnedObjects(plan["primary"], plan["prefix"] + "/")
    before = objects.manifest(output / "raw-before")
    save(output / "raw-before.json", before)
    tables = [t for t in plan["tables"] if t["name"] in ("source", table)]
    image = native(plan, runtime, tables, "verify", output / "native", intervals)
    after = objects.manifest(output / "raw-after")
    assert before == after, "Independent readback changed object bytes"
    save(output / "raw-after.json", after)
    assert all(
        runtime["jarHashes"].get(entry["origin"]) == entry["sha256"]
        for entry in image["origins"].values()
    )
    target = next(item for item in image["tables"] if item["name"] == table)
    source = next(item for item in image["tables"] if item["name"] == "source")
    assert source["version"] == 0 and source["rows"] == [
        [1, "alpha", "allowed"],
        [2, "beta", "blocked"],
        [3, "gamma", "allowed"],
    ]
    assert (
        target["rows"] == target["reopenRows"]
        and target["version"] == target["reopenVersion"]
    )
    rest = Public(plan["gravitinoUrl"], plan["serviceAdmin"], output / "public")
    base = (
        "/api/metalakes/"
        + plan["metalake"]
        + "/catalogs/"
        + plan["catalog"]
        + "/schemas/"
        + plan["database"]
        + "/tables/"
    )
    registrations = {name: rest.call("GET", base + name) for name in ("source", table)}
    save(output / "registrations.private.json", registrations)
    from verify import verify_registration_native

    verify_registration_native(
        registrations, {item["name"]: item for item in image["tables"]}
    )
    prefix = next(t["prefix"] for t in tables if t["name"] == table) + "_delta_log/"
    commits = {}
    object_refs = {}
    for entry in before:
        key = entry["key"]
        retained = (
            output
            / "raw-before"
            / __import__("hashlib").sha256(key.encode()).hexdigest()
        )
        assert sha(retained) == entry["sha256"]
        object_refs[key] = file_ref(retained)
        if (
            key.startswith(prefix)
            and Path(key).suffix == ".json"
            and Path(key).stem.isdecimal()
        ):
            commits[int(Path(key).stem)] = [
                json.loads(line) for line in retained.read_text().splitlines() if line
            ]
    assert sorted(commits) == list(range(target["version"] + 1))
    normalized = dict(
        version=target["version"],
        rows=target["rows"],
        nativeId=target["native"]["id"],
        publications=sorted(commits),
        nativeEvidence=file_ref(output / "native/result.json"),
        nativeTable=table,
        registrations=file_ref(output / "registrations.private.json"),
        objects=object_refs,
    )
    save(output / "image.json", normalized)
    return dict(
        directory=str(output),
        image=file_ref(output / "image.json"),
        native=target,
        source=source,
        registrations=registrations,
        raw=before,
        commits=commits,
        objects=object_refs,
    )


def run_case(plan, runtime, spec, output, app):
    output.mkdir(mode=0o700)
    table = spec["target"]
    state, public_transition = transition(
        plan, table, "MODIFY_TABLE", True, output / "grant"
    )
    if spec["profile"] == "target-denied":
        state, public_transition = transition(
            plan, table, "MODIFY_TABLE", False, output / "entry-revoke"
        )
    if spec["profile"] == "source-denied":
        state, public_transition = transition(
            plan, "source", "SELECT_TABLE", False, output / "source-revoke"
        )
    before = observe(plan, runtime, table, output / "before")
    request = prepare_request(plan, runtime, state, output / "request")
    target = next(t for t in plan["tables"] if t["name"] == table)
    versions = [1, 1] if spec["duplicate"] else list(range(1, spec["appendCount"] + 1))
    request.update(
        frontend=spec["api"],
        caseName=spec["name"],
        target=table,
        output=str(output / "jvm"),
        pythonOutput=str(output / "python"),
        duplicate=spec["duplicate"],
        denyDuplicate=spec["profile"] == "denied-duplicate",
        warmOrder=spec["warmOrder"],
        barrier=spec["profile"] == "revoke",
        appendCount=spec["appendCount"],
        transactionVersions=versions,
        targetLocation=target["location"],
        deadlineSeconds=1200,
    )
    properties = Path(request["sparkConfigurationFile"])
    explicit = settings(spec, "append-aqe-" + plan["nonce"] + "-" + spec["name"])
    if spec["warmOrder"] != "none":
        deployment = {
            "version": "1",
            "profile": "raw-current-head-filter-project-v1",
            "table.count": "1",
            "table.0.metalake": plan["metalake"],
            "table.0.catalog": plan["catalog"],
            "table.0.database": plan["database"],
            "table.0.table": table,
        }
        path = output / "request/raw-maintenance.properties"
        path.write_text("".join(k + "=" + v + "\n" for k, v in deployment.items()))
        explicit["spark.gravitino.deltaRanger.rawCacheMaintenance.config"] = str(path)
        explicit["spark.sql.cache.serializer"] = (
            "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer"
        )
    lines = [
        line
        for line in properties.read_text().splitlines()
        if line.split("=", 1)[0] not in explicit
    ]
    properties.write_text(
        "\n".join(lines + [k + "=" + v for k, v in explicit.items()]) + "\n"
    )
    save(output / "request/request.private.json", request)
    env = clean_environment(plan)
    env.update(
        SPARK_HOME=runtime["sparkHome"],
        PYSPARK_PYTHON=plan["python"],
        SPARK_USER=plan["subject"],
        HADOOP_USER_NAME=plan["subject"],
        PYSPARK_PIN_THREAD="true",
    )
    argv = [
        runtime["sparkHome"] + "/bin/spark-submit",
        "--driver-memory",
        "3g",
        "--master",
        "local[2]",
        "--properties-file",
        str(properties),
        "--driver-class-path",
        request["driverSecurityDirectory"],
        str(app),
        str(output / "request/request.private.json"),
    ]
    process, log = start(argv, env, output / "writer")
    control = Path(request["controlDirectory"])
    records = []
    previous = before
    current = None
    try:
        if spec["profile"] == "revoke":
            wait_for(control / "held-ready.json", process, 180)
            held = read(output / "jvm/held.json")
            armed = read(output / "jvm" / (spec["name"] + "-first-writer-thread.json"))
            assert (
                held["writerThreadId"] == armed["threadId"]
                and armed["subject"] == armed["ugi"] == plan["subject"]
            )
            raw = OwnedObjects(plan["primary"], plan["prefix"] + "/").manifest(
                output / "raw-held"
            )
            save(output / "raw-held.json", raw)
            added = {entry["key"] for entry in raw} - {
                entry["key"] for entry in before["raw"]
            }
            assert any(
                key.endswith(".parquet") and "/_delta_log/" not in key for key in added
            )
            assert not any(
                key.endswith(
                    "/" + str(before["native"]["version"] + 1).zfill(20) + ".json"
                )
                for key in added
            )
            state, public_transition = transition(
                plan, table, "MODIFY_TABLE", False, output / "barrier-revoke"
            )
            state = dict(state, publicRevokeComplete=True, updateAbsent=True)
            save(output / "barrier-state.private.json", state)
            save(
                control / "revoke-complete.private.json",
                dict(stateEvidenceFile=str(output / "barrier-state.private.json")),
            )
        for index, version in enumerate(versions):
            attempt = (
                "first"
                if index == 0
                else ("duplicate" if spec["duplicate"] else "append" + str(index + 1))
            )
            name = spec["name"] + "-" + attempt
            if attempt == "duplicate" and request["denyDuplicate"]:
                wait_for(control / "deny-duplicate-ready.json", process, 180)
                state, public_transition = transition(
                    plan, table, "MODIFY_TABLE", False, output / "duplicate-revoke"
                )
                save(
                    control / "deny-duplicate-verified.json",
                    dict(
                        stateEvidenceFile=str(
                            Path(state["publicStateFile"]).parent
                            / "state-evidence.private.json"
                        )
                    ),
                )
            wait_for(control / (name + "-ready.json"), process, 240)
            intervals = (
                {
                    table: [
                        [
                            before["native"]["version"] + 1,
                            before["native"]["version"] + index + 1,
                        ]
                    ]
                }
                if spec["profile"] == "cdf-enabled"
                else None
            )
            current = observe(
                plan, runtime, table, output / ("after-" + attempt), intervals
            )
            denied = spec["profile"] in (
                "source-denied",
                "target-denied",
                "revoke",
            ) or (attempt == "duplicate" and request["denyDuplicate"])
            duplicate = attempt == "duplicate"
            expected = previous["native"]["version"] + int(not denied and not duplicate)
            assert current["native"]["version"] == expected
            expected_rows = previous["native"]["rows"] + (
                [[1, "MASKED", "allowed"], [3, "MASKED", "allowed"]]
                if not denied and not duplicate
                else []
            )
            assert Counter(map(tuple, current["native"]["rows"])) == Counter(
                map(tuple, expected_rows)
            )
            verify_registration_transition(
                previous["registrations"],
                current["registrations"],
                table,
                previous["native"]["version"],
                current["native"]["version"],
            )
            assert current["source"] == before["source"]
            assert (
                current["native"]["metadata"] == before["native"]["metadata"]
                and current["native"]["protocol"] == before["native"]["protocol"]
            )
            assert current["native"]["partitionColumns"] == (
                ["region"] if spec["profile"] == "partitioned" else []
            )
            if denied or duplicate:
                assert current["commits"] == previous["commits"]
                if spec["profile"] != "revoke":
                    assert current["raw"] == previous["raw"]
            else:
                actions = current["commits"][expected]
                assert any("add" in action for action in actions)
                assert all(
                    set(action) <= {"add", "commitInfo", "txn"} for action in actions
                )
            if spec["profile"] == "cdf-enabled":
                changes = current["native"]["intervals"]
                assert len(changes) == 1 and changes[0]["error"] is None
                assert Counter(tuple(row) for row in changes[0]["rows"]) == Counter(
                    (row[0], row[1], row[2], "insert", v)
                    for v in range(before["native"]["version"] + 1, expected + 1)
                    for row in [[1, "MASKED", "allowed"], [3, "MASKED", "allowed"]]
                )
            observation = read(output / "jvm" / (name + "-append.json"))
            caller = read(output / "python" / (name + "-caller.json"))
            applied_name = (
                "revoke"
                if spec["profile"] == "revoke"
                else (
                    "deny-duplicate"
                    if duplicate and request["denyDuplicate"]
                    else "initial"
                )
            )
            outcome = (
                "after-helper-revoked"
                if spec["profile"] == "revoke"
                else (
                    "denied-duplicate"
                    if denied and duplicate
                    else (
                        spec["profile"]
                        if denied
                        else ("duplicate" if duplicate else "committed")
                    )
                )
            )
            record = dict(
                name=name,
                api=spec["api"],
                profile=spec["profile"],
                outcome=outcome,
                source="source",
                target=table,
                callerBefore=caller["before"],
                callerAfter=caller["after"],
                before=previous["image"],
                after=current["image"],
                rangerRequests=observation["rangerRequests"],
                callbacks=observation["callbacks"],
                failure=observation["failure"],
                applied=file_ref(output / "jvm" / (applied_name + "-applied.json")),
                publicTransition=file_ref(public_transition),
                rangerState=file_ref(state["rangerStateFile"]),
                observation=file_ref(output / "jvm" / (name + "-append.json")),
                callerEvidence=file_ref(output / "python" / (name + "-caller.json")),
                origins=file_ref(output / "jvm/origins.json"),
                runtimeProfile=file_ref(output / "jvm/runtime-profile.json"),
                request=file_ref(output / "request/request.private.json"),
                pythonRuntime=file_ref(output / "python/python-runtime.json"),
            )
            if spec["warmOrder"] != "none":
                warm = dict(
                    read(output / "jvm" / (name + "-warm-rows.json")),
                    order=spec["warmOrder"],
                    before=file_ref(output / "jvm/warm-before.json"),
                    after=file_ref(output / "jvm" / (name + "-warm.json")),
                )
                save(output / (name + "-warm-result.json"), warm)
                record["warm"] = file_ref(output / (name + "-warm-result.json"))
            if spec["profile"] == "checkpoint" and index == len(versions) - 1:
                checkpoints = {
                    key: ref
                    for key, ref in current["objects"].items()
                    if "/" + table + "/_delta_log/" in key
                    and ".checkpoint." in key
                    and key.endswith(".parquet")
                }
                assert len(checkpoints) >= 2
                assert all(
                    Path(ref["path"]).read_bytes()[:4] == b"PAR1"
                    for ref in checkpoints.values()
                )
                checkpoint = dict(
                    version=current["native"]["reopenVersion"],
                    nativeId=current["native"]["native"]["id"],
                    rows=current["native"]["reopenRows"],
                    files=list(checkpoints.values()),
                    logicalFiles=list(checkpoints),
                    jsonTailVersion=expected,
                )
                save(output / "checkpoint-result.json", checkpoint)
                record["checkpoint"] = file_ref(output / "checkpoint-result.json")
            if spec["profile"] == "revoke":
                barrier = {
                    key: file_ref(output / "jvm" / file)
                    for key, file in [
                        ("held", "held.json"),
                        ("armed", name + "-writer-thread.json"),
                        ("release", "release-witness.json"),
                        ("forwarded", "forwarded.json"),
                        ("initialApplied", "initial-applied.json"),
                    ]
                }
                barrier["rawBefore"] = file_ref(output / "before/raw-before.json")
                barrier["rawHeld"] = file_ref(output / "raw-held.json")
                save(output / "barrier-result.json", barrier)
                record["barrier"] = file_ref(output / "barrier-result.json")
            records.append(record)
            save(output / (name + "-case.json"), record)
            previous = current
            save(
                control / (name + "-verified.json"),
                dict(checkpoint=name, verified=True),
            )
        code, timed = finish(process, log, output / "writer", 45)
        log = None
        assert code == 0 and not timed
    finally:
        if log is not None:
            finish(process, log, output / "writer", 5)
        if current is None or process.returncode:
            observe(plan, runtime, table, output / "failure-native-after")
    if spec["profile"] == "source-denied":
        transition(plan, "source", "SELECT_TABLE", True, output / "source-regrant")
    return records


def run(runtime_path, inputs_path, output, selected):
    output.mkdir(mode=0o700)
    runtime = read(runtime_path)
    runtime["manifestPath"] = str(runtime_path)
    verify_runtime(runtime)
    assert runtime["application"] == "append-aqe"
    inputs = read(inputs_path)
    association = verify_deployed(inputs, output / "deployed-ranger")
    inputs = dict(inputs, deployedRangerEvidence=str(association))
    save(output / "inputs.private.json", inputs)
    app = ROOT / "delta/acceptance/append_aqe_pyspark.py"
    save(
        output / "executed-inputs.json",
        dict(
            runtime=file_ref(runtime_path),
            app=file_ref(app),
            sourceInputDigest=runtime["sourceInputDigest"],
            gitHead=subprocess.check_output(
                ["git", "rev-parse", "HEAD"], text=True
            ).strip(),
            qualification=not selected,
        ),
    )
    fixture = output / "fixture"
    argv = [
        sys.executable,
        ROOT / "delta/acceptance/append_aqe_fixture.py",
        "--inputs",
        output / "inputs.private.json",
        "--package",
        runtime_path,
        "--freeze",
        runtime_path,
        "--out",
        fixture,
    ]
    process, log = start(argv, os.environ.copy(), output / "fixture-command")
    (output / "cases").mkdir(mode=0o700)
    plan = None
    all_records = []
    try:
        wait_for(fixture / "ready.json", process, 600)
        plan = read(fixture / "state.private.json")
        specs = [
            case for case in case_specs() if not selected or case["name"] in selected
        ]
        assert len(specs) == (len(set(selected)) if selected else 26)
        for spec in specs:
            records = run_case(
                plan, runtime, spec, output / "cases" / spec["name"], app
            )
            all_records.extend(records)
            save(
                output / (spec["name"] + "-completed.json"),
                dict(case=spec["name"], attempts=len(records), finished=time.time()),
            )
        rest = Public(
            plan["gravitinoUrl"], plan["serviceAdmin"], output / "persistence-before"
        )
        base = (
            "/api/metalakes/"
            + plan["metalake"]
            + "/catalogs/"
            + plan["catalog"]
            + "/schemas/"
            + plan["database"]
            + "/tables/"
        )
        before = {
            table["name"]: rest.call("GET", base + table["name"])
            for table in plan["tables"]
        }
        save(output / "registrations-before-restart.private.json", before)
        save(fixture / "restart-request.json", dict(ownedServerOnly=True))
        wait_for(fixture / "restart-complete.json", process, 180)
        rest = Public(
            plan["gravitinoUrl"], plan["serviceAdmin"], output / "persistence-after"
        )
        after = {
            table["name"]: rest.call("GET", base + table["name"])
            for table in plan["tables"]
        }
        assert before == after
        save(output / "registrations-after-restart.private.json", after)
    except BaseException:
        save(output / "failure.json", dict(trace=traceback.format_exc(), noReplay=True))
        raise
    finally:
        if fixture.exists():
            save(
                fixture / "stop-request.json",
                dict(stopOwnedServer=True, preserveDataAndEvidence=True),
            )
        code, timed = finish(process, log, output / "fixture-command", 60)
        assert code == 0 and not timed
    exits = {}
    for path in output.rglob("exit.json"):
        exits[str(path.relative_to(output))] = file_ref(path)
    first = specs[0]["name"]
    exits["writer"] = file_ref(output / "cases" / first / "writer/exit.json")
    exits["native-before"] = file_ref(
        output / "cases" / first / "before/native/process/exit.json"
    )
    exits["native-after"] = file_ref(
        output / "cases" / first / "after-first/native/process/exit.json"
    )
    result = dict(
        schemaVersion=1,
        spark=runtime["spark"],
        delta=runtime["delta"],
        subject=plan["subject"],
        metadataSubject=plan["metadataPrincipal"],
        verificationSubject=plan["serviceAdmin"],
        metalake=plan["metalake"],
        catalog=plan["catalog"],
        database=plan["database"],
        runtime=file_ref(runtime_path),
        fixturePlan=file_ref(output / "fixture/state.private.json"),
        processExits=exits,
        cases=all_records,
        caseInventory=[case["name"] for case in specs],
        qualification=not selected,
        persistenceBefore=file_ref(
            output / "registrations-before-restart.private.json"
        ),
        persistenceAfter=file_ref(output / "registrations-after-restart.private.json"),
    )
    save(output / "raw-result.json", result)


def main():
    parser = argparse.ArgumentParser()
    for name in ("runtime", "inputs", "out"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--case", action="append", default=[])
    args = parser.parse_args()
    os.umask(0o077)
    run(
        Path(args.runtime).resolve(),
        Path(args.inputs).resolve(),
        Path(args.out).resolve(),
        args.case,
    )


if __name__ == "__main__":
    main()

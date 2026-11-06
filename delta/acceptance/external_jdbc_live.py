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

"""Finite external PostgreSQL service qualification; original attempts are never retried."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

from cdf_live import ROOT, JAVA, read, save, sha
from cdf_merge_live_checks import file_ref
from registered_jdbc_live import OwnedPostgres
from external_jdbc_checks import LEAD, case_names, action_names, postgres_counts


def case_specs(pair, selected=None):
    inventory = list(case_names(pair))
    if selected is not None:
        assert selected and len(set(selected)) == len(selected) and set(selected) <= set(inventory)
        inventory = list(selected)
    profiles = {"external_partitioned_save_as_table": "partitioned", "external_target_update_denied": "target-denied",
                "external_named_source_denied": "source-denied", "external_scope_closed_before_publish": "close",
                "external_target_revoked_before_publish": "revoke"}
    return [dict(name=name, target=name, api="saveAsTable" if name == "external_partitioned_save_as_table" else "insertInto",
                 profile=profiles.get(name, "plain"), duplicate=False, appendCount=1, inputTimestamp=True,
                 externalJdbc=True, warmOrder="target-first" if name == "external_mixed_governed" else "none") for name in inventory]


def verify_capacity(available, pair=None, bounded=False):
    """Keep the default reserve; only the final two approved cells use the measured budget."""
    if bounded:
        assert tuple(pair) in (("3.5.4", "3.3.2"), ("3.5.6", "3.3.0"))
    reserve = (5 if bounded else 8) * 1024**3
    assert available > reserve, "Available disk is below the explicit stage reserve"
    return reserve


def readiness(output, pair=None, bounded=False):
    """Retain bounded local capacity and service state without container environment."""
    output = Path(output)
    available = shutil.disk_usage(ROOT).free
    reserve = verify_capacity(available, pair, bounded)
    capacity = dict(minimumAvailableBytes=reserve, profile="default-8GiB")
    if bounded:
        decision = ROOT/".superpowers/sdd/2026-11-05-delta-external-jdbc/task-4-capacity-decision.json"
        assert read(decision)["minimumAvailableBytes"] == reserve
        capacity.update(profile="task4-final-two-pairs-5GiB", pair=list(pair), decision=file_ref(decision))
    raw = subprocess.check_output(["docker", "ps", "--format", "{{json .}}"], text=True)
    services = [json.loads(line) for line in raw.splitlines()]
    required = ["delta-live-20261010-ranger-admin-1", "delta-live-20261010-minio-1"]
    selected = [dict(name=s["Names"], image=s["Image"], status=s["Status"], ports=s["Ports"]) for s in services if s["Names"] in required]
    assert sorted(s["name"] for s in selected) == sorted(required)
    identities = {}
    for name in required:
        identities[name] = json.loads(subprocess.check_output(["docker", "inspect", name, "--format", '{"id":{{json .Id}},"image":{{json .Image}},"state":{{json .State}},"ports":{{json .NetworkSettings.Ports}}}'], text=True))
        assert identities[name]["state"]["Running"] is True
    version = subprocess.check_output(["docker", "exec", "delta-live-20261010-minio-1", "minio", "--version"], text=True)
    save(output, dict(availableBytes=available, services=selected, identities=identities, minioVersion=version, observed=time.time(), capacity=capacity))


class ExternalPostgres(OwnedPostgres):
    """Same isolated read-only principal, with external action traffic classification."""
    def __init__(self, output, owner="external-jdbc-task4"):
        super().__init__(output, owner=owner)
        self.storage = "volume"

    def observe(self, output):
        stdout, _ = self.command(["docker", "logs", self.name])
        raw = self.output / ("command-" + str(self.sequence).zfill(3))
        value = dict(postgres_counts(stdout + (raw/"stderr.private").read_text()),
                     logStdout=file_ref(raw/"stdout.private"), logStderr=file_ref(raw/"stderr.private"))
        save(output, value)
        return value


def external_python_path(hub, facade, dependencies):
    assert Path(dependencies).is_dir()
    return os.pathsep.join([str(hub), str(facade), str(dependencies), str(ROOT/"delta/acceptance")])


def run_case(plan, runtime, spec, postgres, output, hub, bounded_capacity=False):
    from append_aqe_live import start, finish, wait_for, observe
    from append_aqe_public import transition
    from append_aqe_profiles import settings, verify_registration_transition
    from cdf_live import prepare_request, clean_environment
    from live_common import OwnedObjects
    from external_jdbc_checks import verify_case

    output.mkdir(mode=0o700)
    readiness(output/"readiness.json", (runtime["spark"], runtime["delta"]), bounded_capacity)
    state, granted = transition(plan, spec["target"], "MODIFY_TABLE", True, output/"target-grant")
    transitions = [file_ref(granted)]
    if spec["profile"] in ("target-denied", "source-denied"):
        table, privilege = (spec["target"], "MODIFY_TABLE") if spec["profile"] == "target-denied" else ("source", "SELECT_TABLE")
        state, changed = transition(plan, table, privilege, False, output/"denial")
        transitions.append(file_ref(changed))
    before = observe(plan, runtime, spec["target"], output/"before")
    request = prepare_request(plan, runtime, state, output/"request")
    target = next(t for t in plan["tables"] if t["name"] == spec["target"])
    request.update(frontend=spec["api"], caseName=spec["name"], target=spec["target"], output=str(output/"jvm"),
                   pythonOutput=str(output/"python"), duplicate=False, denyDuplicate=False, appendCount=1,
                   warmOrder=spec["warmOrder"], barrier=spec["profile"] in ("close", "revoke"),
                   closeExternalScope=spec["profile"] == "close", transactionVersions=[1], targetLocation=target["location"],
                   deadlineSeconds=1200, inputTimestamp=True, externalJdbc=True, revokeSource=False,
                   jdbcConnectionFile=plan["jdbcConnectionFile"], hubSource=hub["path"],
                   deltaSourceName=plan["catalog"]+"."+plan["database"]+".source")
    properties = Path(request["sparkConfigurationFile"])
    explicit = settings(spec, "external-"+plan["nonce"]+"-"+spec["name"])
    explicit.update({"spark.gravitino.enableDeltaRangerExternalJdbcSupport": "true",
                     "spark.redaction.regex": "(?i)secret|password|token|access[.]?key|credential|jdbc-url|jdbc-user"})
    if spec["warmOrder"] != "none":
        path = output/"request/raw-maintenance.properties"
        values = {"version": "1", "profile": "raw-current-head-filter-project-v1", "table.count": "1",
                  "table.0.metalake": plan["metalake"], "table.0.catalog": plan["catalog"],
                  "table.0.database": plan["database"], "table.0.table": spec["target"]}
        path.write_text("".join(k+"="+v+"\n" for k,v in values.items()))
        explicit["spark.gravitino.deltaRanger.rawCacheMaintenance.config"] = str(path)
        explicit["spark.sql.cache.serializer"] = "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer"
    lines = [line for line in properties.read_text().splitlines() if line.split("=",1)[0] not in explicit]
    properties.write_text("\n".join(lines+[k+"="+v for k,v in explicit.items()])+"\n")
    save(output/"request/request.private.json", request)
    env = clean_environment(plan)
    env.update(SPARK_HOME=runtime["sparkHome"], PYSPARK_PYTHON=plan["python"], SPARK_USER=plan["subject"], HADOOP_USER_NAME=plan["subject"],
               PYSPARK_PIN_THREAD="true", JAVA_TOOL_OPTIONS="-XX:ActiveProcessorCount=2 -XX:MaxMetaspaceSize=768m",
               ENV="test", IS_LOCAL="false", KAFKA_BROKERS="localhost:9092", KAFKA_TOPIC="unused",
               PYTHONPATH=external_python_path(hub["path"], runtime["pythonZip"], Path(hub["path"]).parent/"pytest-tools"))
    pg_before = postgres.observe(output/"postgres-before.json")
    save(output/"attempt.json", dict(attempted=True, outcome="unknown", retryEligible=False, target=spec["target"]))
    argv = [runtime["sparkHome"]+"/bin/spark-submit", "--driver-memory", "3g", "--master", "local[2]",
            "--properties-file", str(properties), "--driver-class-path", request["driverSecurityDirectory"],
            str(ROOT/"delta/acceptance/append_aqe_pyspark.py"), str(output/"request/request.private.json")]
    process, log = start(argv, env, output/"writer")
    control = Path(request["controlDirectory"])
    records, observed = [], None
    try:
        if request["barrier"]:
            wait_for(control/"held-ready.json", process, 240)
            held = OwnedObjects(plan["primary"], plan["prefix"]+"/").manifest(output/"raw-held")
            save(output/"raw-held.json", held)
            added = {e["key"] for e in held} - {e["key"] for e in before["raw"]}
            assert any(k.endswith(".parquet") for k in added) and not any(k.endswith("/00000000000000000001.json") for k in added)
            if spec["profile"] == "revoke":
                state, changed = transition(plan, spec["target"], "MODIFY_TABLE", False, output/"late-target-revoke")
                transitions.append(file_ref(changed))
                state = dict(state, publicRevokeComplete=True, updateAbsent=True)
            save(output/"barrier-state.private.json", state)
            save(control/"revoke-complete.private.json", dict(stateEvidenceFile=str(output/"barrier-state.private.json")))
        previous, previous_path = pg_before, output/"postgres-before.json"
        for action in action_names(spec["name"]):
            name = spec["name"]+"-"+action
            wait_for(control/(name+"-ready.json"), process, 450)
            path = output/("postgres-"+action+".json")
            current = postgres.observe(path)
            observation = file_ref(output/"jvm"/(name+"-append.json"))
            caller = file_ref(output/"python"/(name+"-caller.json"))
            records.append(dict(action=action, postgresBefore=file_ref(previous_path), postgresAfter=file_ref(path),
                                observation=observation, caller=caller,
                                counts={k: current[k]-previous[k] for k in ("schemaQueries", "remoteCountQueries", "payloadQueries", "mutationQueries")}))
            previous, previous_path = current, path
            if action == "append":
                observed = observe(plan, runtime, spec["target"], output/"after")
            if spec["name"] == "external_source_advances" and action == "count_before":
                rows, _ = postgres.sql("INSERT INTO source_schema.source (id, secret, region) VALUES (4,'delta','allowed'); SELECT count(*) FROM source_schema.source;")
                assert rows.splitlines()[-1] == "4"
                save(output/"source-advanced.json", dict(rows=4, action=action, command=file_ref(postgres.output/("command-"+str(postgres.sequence).zfill(3))/"exit.json")))
            save(control/(name+"-verified.json"), dict(checkpoint=name, verified=True))
        code, timed = finish(process, log, output/"writer", 60)
        log = None
        assert code == 0 and not timed
    finally:
        if log is not None:
            finish(process, log, output/"writer", 5)
            observe(plan, runtime, spec["target"], output/"failure-native-after")
            postgres.observe(output/"postgres-failure.json")
    record = dict(name=spec["name"], api=spec["api"], before=before["image"], after=observed["image"], actions=records,
                  target=dict(catalog=plan["catalog"], database=plan["database"], table=spec["target"]), transitions=transitions,
                  request=file_ref(output/"request/request.private.json"), origins=file_ref(output/"jvm/origins.json"),
                  runtimeProfile=file_ref(output/"jvm/runtime-profile.json"), pythonRuntime=file_ref(output/"python/python-runtime.json"),
                  facade=file_ref(output/"python/facade-provider.json"), declaredActions=file_ref(output/"python/declared-actions.json"),
                  writerExit=file_ref(output/"writer/exit.json"), attempted=True, retryEligible=False,
                  outcome="committed" if observed["native"]["version"] == 1 else "denied")
    if request["barrier"]:
        record["barrier"] = {key: file_ref(output/path) for key,path in {"held":"jvm/held.json", "release":"jvm/release-witness.json", "forwarded":"jvm/forwarded.json", "rawHeld":"raw-held.json", "rawBefore":"before/raw-before.json"}.items()}
    if spec["warmOrder"] != "none":
        record["warm"] = file_ref(output/"jvm"/(spec["name"]+"-append-warm-rows.json"))
    if spec["name"] == "external_unissued_rejected":
        record["runtimeObserved"] = file_ref(output/"python/runtime-before-close.json")
        record["metadataAbsentClose"] = file_ref(output/"python/expected-metadata-absence.json")
    if spec["name"] == "external_hub_tracker":
        record["hubCaller"] = file_ref(output/"python/hub-caller.json")
        record["hubProvenance"] = file_ref(output/"python/hub-provenance.json")
    if spec["name"] == "external_source_advances":
        record["advancement"] = file_ref(output/"source-advanced.json")
    save(output/"original-case.json", record)
    verify_case(record)
    verify_registration_transition(before["registrations"], observed["registrations"], spec["target"], before["native"]["version"], observed["native"]["version"])
    assert before["source"] == observed["source"]
    if spec["profile"] == "source-denied":
        transition(plan, "source", "SELECT_TABLE", True, output/"source-restore")
    return record


def run(runtime_path, inputs_path, output, hub_path, selected=None, bounded_capacity=False):
    from append_aqe_live import start, finish, wait_for
    from cdf_live import verify_deployed
    from cdf_read_live_package import verify_runtime
    output.mkdir(mode=0o700)
    runtime = read(runtime_path)
    readiness(output/"readiness.json", (runtime["spark"], runtime["delta"]), bounded_capacity)
    runtime["manifestPath"] = str(runtime_path)
    verify_runtime(runtime)
    inputs = read(inputs_path)
    association = verify_deployed(inputs, output/"deployed-ranger", safe_inspection=True)
    pair = (runtime["spark"], runtime["delta"])
    specs = case_specs(pair, selected)
    inputs = dict(inputs, deployedRangerEvidence=str(association), externalJdbcPair=list(pair), externalJdbcCases=[s["name"] for s in specs])
    inputs["pythonRuntime"] = str(ROOT/".superpowers/sdd/2026-10-29-spark-hub-catalog-foundation/runtime-python"/(pair[0]+"-"+pair[1])/"venv/bin/python")
    assert Path(inputs["pythonRuntime"]).is_file()
    hub = file_ref(hub_path)
    postgres = ExternalPostgres(output/"postgres", owner="external-jdbc-task4")
    process, log, records = None, None, []
    fixture = output/"fixture"
    try:
        inputs["jdbcConnectionFile"] = str(postgres.start())
        save(output/"inputs.private.json", inputs)
        process, log = start([sys.executable, str(ROOT/"delta/acceptance/append_aqe_fixture.py"), "--inputs", str(output/"inputs.private.json"),
                              "--package", str(runtime_path), "--freeze", str(runtime_path), "--out", str(fixture)], os.environ.copy(), output/"fixture-command")
        wait_for(fixture/"ready.json", process, 600)
        plan = read(fixture/"state.private.json")
        from external_jdbc_checks import fixture_identity
        save(output/"fixture-identity.json", fixture_identity(plan))
        (output/"cases").mkdir(mode=0o700)
        for spec in specs:
            records.append(run_case(plan, runtime, spec, postgres, output/"cases"/spec["name"], hub, bounded_capacity))
            save(output/(spec["name"]+"-completed.json"), dict(case=spec["name"], finished=time.time()))
    finally:
        if fixture.exists():
            save(fixture/"stop-request.json", dict(stopOwnedServer=True, preserveDataAndEvidence=True))
        if process is not None:
            code, timed = finish(process, log, output/"fixture-command", 60)
            save(output/"fixture-terminal.json", dict(exitCode=code, timedOut=timed))
        postgres.close()
    assert code == 0 and not timed
    save(output/"raw-result.json", dict(schemaVersion=1, spark=pair[0], delta=pair[1], caseInventory=[s["name"] for s in specs], cases=records,
         runtime=file_ref(runtime_path), fixtureIdentity=file_ref(output/"fixture-identity.json"), source=file_ref(output/"postgres/source.json"),
         sourceTerminal=file_ref(output/"postgres/terminal.json"), fixtureExit=file_ref(output/"fixture-command/exit.json"), deployedRanger=file_ref(association), hubSource=hub))


def main():
    parser = argparse.ArgumentParser()
    for key in ("runtime", "inputs", "out", "hub"):
        parser.add_argument("--"+key, required=True)
    parser.add_argument("--cases")
    parser.add_argument("--bounded-capacity", action="store_true")
    args = parser.parse_args()
    os.umask(0o077)
    run(Path(args.runtime).resolve(), Path(args.inputs).resolve(), Path(args.out).resolve(), Path(args.hub).resolve(), args.cases.split(",") if args.cases else None, args.bounded_capacity)


if __name__ == "__main__":
    main()

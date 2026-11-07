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

"""Bounded actual shared-writer service fixture and original-process supervisor."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import threading
import time
from uuid import uuid4

from shared_writer_checks import case_names, contract, LEAD


def fixture_tables(plan, pair):
    """Keep CREATE targets absent; only existing targets receive independent native seeds."""
    from append_aqe_profiles import fixture_tables as original_tables
    source = original_tables(plan)[0]
    source["seed"] = True
    result = [source]
    for name in case_names(pair):
        seed = name not in ("shared_create_append", "shared_create_denied", "shared_late_target_revoke")
        prefix = plan["prefix"] + "/adoption/" + name + "/"
        result.append(dict(name=name, seed=seed, rows=[[0, "sentinel", "seed", None]],
                           schema="id LONG, secret STRING, region STRING, __arkham_loaded_at TIMESTAMP",
                           cdf=name != "shared_cdf_enable_append", properties={"delta.compatibility.symlinkFormatManifest.enabled": "false", "delta.enableChangeDataFeed": str(name != "shared_cdf_enable_append").lower()},
                           prefix=prefix, location="s3a://" + plan["primary"]["bucket"] + "/" + prefix.rstrip("/")))
    return result


def role_objects(objects, create_schema):
    """Combine privileges on one existing schema object without modifying metadata-role input."""
    import copy
    result = copy.deepcopy(objects)
    assert len({(item["type"], item["fullName"]) for item in result}) == len(result)
    if create_schema is not None:
        schemas = [item for item in result if item["type"] == "SCHEMA" and item["fullName"] == create_schema]
        assert len(schemas) == 1
        assert not any(p["name"] == "CREATE_TABLE" for p in schemas[0]["privileges"])
        schemas[0]["privileges"].append(dict(name="CREATE_TABLE", condition="ALLOW"))
    return result


def startup_settings(defaults, inherited):
    """Apply model defaults before explicit trusted fixture settings, before Spark exists."""
    return {**defaults, **inherited, **shared_settings()}


def shared_settings():
    """Freeze normal Hub settings before Spark creation, including its AQE=true defaults."""
    return {
        "spark.gravitino.enableDeltaSupport": "true",
        "spark.gravitino.enableDeltaRangerSupport": "true",
        "spark.gravitino.enableDeltaRangerExternalJdbcSupport": "true",
        "spark.sql.extensions": "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerSparkExtension,io.delta.sql.DeltaSparkSessionExtension",
        "spark.databricks.delta.write.txnVersion.autoReset.enabled": "false",
        "spark.databricks.delta.optimizeWrite.enabled": "false",
        "spark.databricks.delta.autoCompact.enabled": "false",
        "spark.databricks.delta.properties.defaults.compatibility.symlinkFormatManifest.enabled": "false",
        "spark.databricks.delta.schema.autoMerge.enabled": "false",
        "spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols": "-1",
        "spark.databricks.delta.constraints.allowUnenforcedNotNull.enabled": "true",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128m",
        "spark.sql.adaptive.coalescePartitions.initialPartitionNum": "2000",
        "spark.sql.adaptive.coalescePartitions.minPartitionSize": "16m",
        "spark.sql.adaptive.coalescePartitions.parallelismFirst": "false",
        "spark.sql.sources.partitionOverwriteMode": "static",
        "spark.redaction.regex": "(?i)secret|password|token|access[.]?key|credential|jdbc-url|jdbc-user",
    }


def run_child(request_path):
    """Execute the immutable Hub package with passive profiling and bounded timing barriers."""
    from dataclasses import asdict
    from pyspark import SparkContext
    from pyspark.sql import SparkSession
    from py4j.protocol import Py4JJavaError
    from append_aqe_pyspark import python_runtime_evidence
    from append_aqe_profiles import module_provenance
    from external_jdbc_pyspark import hub_payload
    from src.data_platform.sources.jdbc.v2.main import run_source_jdbc_v2
    from src.data_platform.sources.jdbc.v2.schema import JdbcExtractionSchema
    from src.shared.commands.job_runner import CoreException, job_wrapper
    from src.shared.commands.progress_tracker import ProgressTracker
    from src.shared.schemas.v2.base_jobs import ComputeProperties
    from src.shared.schemas.v3.gravitino import DatasetBinding, GravitinoRuntimeProfile
    from src.shared.services.v3.gravitino_job import GravitinoJobRuntime, GravitinoJobError
    from src.shared.services.v3.gravitino_writer import GravitinoWriteError

    request = json.loads(Path(request_path).read_text())
    output = Path(request["pythonOutput"])
    output.mkdir(mode=0o700)

    def save(name, value):
        with (output / (name + ".json")).open("x") as stream:
            json.dump(value, stream, indent=2)

    from cdf_merge_live_checks import file_ref
    acceptance_source = dict(entryPoint=file_ref(Path(__file__).resolve()),
                             checker=file_ref(Path(sys.modules["shared_writer_checks"].__file__).resolve()))
    assert acceptance_source == request["acceptanceSourceExpected"]
    save("acceptance-source", acceptance_source)

    # Model defaults are loaded from the immutable producer before Spark is created.
    props_path = Path(request["sparkConfigurationFile"])
    props = dict(line.split("=", 1) for line in props_path.read_text().splitlines() if line)
    compute = {key: str(value) for key, value in ComputeProperties().model_dump(by_alias=True).items() if value is not None}
    props = startup_settings(compute, props)
    assert "spark.databricks.delta.snapshotPartitions" not in props
    assert not any(key in props for key in ("spark.databricks.delta.write.txnAppId", "spark.databricks.delta.write.txnVersion"))
    props_path.write_text("".join(key + "=" + value + "\n" for key, value in props.items()))
    save("python-runtime", python_runtime_evidence(request["expectedSparkVersion"]))
    SparkContext._ensure_initialized()
    jvm, gateway = SparkContext._jvm, SparkContext._gateway
    bridge = jvm.org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerLiveAppendAqe(str(request_path))
    jsc = jvm.org.apache.spark.api.java.JavaSparkContext.fromSparkContext(bridge.session().sparkContext())
    spark = SparkSession(SparkContext(gateway=gateway, jsc=jsc), bridge.session())
    jvm.org.apache.spark.sql.SparkSession.setActiveSession(spark._jsparkSession)
    save("actual-runtime", dict(spark=spark.version, delta=request["expectedDeltaVersion"],
         java=jvm.java.lang.System.getProperty("java.specification.version"), scala=jvm.scala.util.Properties.versionNumberString(),
         subject=jvm.org.apache.hadoop.security.UserGroupInformation.getCurrentUser().getShortUserName(),
         qualifiedSettings={key: (spark.conf.get(key, None) if key.startswith("spark.sql.") else spark.sparkContext.getConf().get(key, None)) for key in shared_settings()},
         jvmProcessId=int(jvm.java.lang.management.ManagementFactory.getRuntimeMXBean().getName().split("@")[0])))
    conf = {key: value for key, value in props.items() if key in compute or key in shared_settings() or key in (
        "spark.sql.extensions", "spark.gravitino.enableDeltaSupport", "spark.gravitino.enableDeltaRangerSupport")}
    runtime = GravitinoJobRuntime(GravitinoRuntimeProfile(spark.version, request["expectedDeltaVersion"], conf),
        [DatasetBinding(uuid4(), request["catalog"], request["database"], request["target"])], 1200)
    payload = hub_payload(json.loads(Path(request["jdbcConnectionFile"]).read_text()))
    payload.output_dataset.id = runtime.bindings[0].dataset_id
    payload.execution_properties.overwrite_schema = False
    payload.execution_properties.merge_schema = request["caseName"] != "shared_merge_schema_false"
    payload.job.query = contract(request["caseName"])[0]["query"]
    readers, trackers = [], []
    previous_profile, previous_thread_profile = sys.getprofile(), threading.getprofile()
    original_error_telemetry = CoreException._send_error_event
    original_metrics_telemetry = ProgressTracker._send_telemetry
    # Disclosed external error-reporting boundary; no native/auth/source replacement.
    CoreException._send_error_event = lambda *args, **kwargs: None
    observations = []
    current = {}
    counted, written = threading.Event(), threading.Event()

    def identity(frame):
        return dict(python=threading.get_ident(), jvm=jvm.java.lang.Thread.currentThread().getId(),
                    session=frame._jdf.sparkSession().sessionUUID(),
                    ugi=str(jvm.org.apache.hadoop.security.UserGroupInformation.getCurrentUser()),
                    groups=sorted(jvm.org.apache.hadoop.security.UserGroupInformation.getCurrentUser().getGroupNames()))

    def telemetry_failure(self):
        assert written.wait(180), "Writer did not reach its original outcome"
        assert current.get("phases", {}).get("data") == "confirmed"
        raise RuntimeError("shared fixture external metrics telemetry failure")

    def profile(frame, event, value):
        module, name = frame.f_globals.get("__name__", ""), frame.f_code.co_name
        if event == "call" and module.startswith("src."):
            current["calls"].append(dict(module=module, function=name, python=threading.get_ident()))
            key = {("src.data_platform.sources.jdbc.v2.main", "run_source_jdbc_v2"): "source",
                   ("src.shared.services.v3.gravitino_jdbc", "read"): "sourceRead",
                   ("src.shared.services.v2.spark", "init_df_tracking"): "trackerCount",
                   ("src.shared.services.v3.gravitino_writer", "write_dataset"): "writer"}.get((module, name))
            if key:
                current["actionCounts"][key] += 1
        if event == "call" and name == "collect" and module == "pyspark.sql.dataframe" and frame.f_back.f_code.co_name == "run_source_jdbc_v2":
            current["actionCounts"]["remoteCount"] += 1
        if event == "call" and name == "saveAsTable" and module == "pyspark.sql.readwriter":
            current["actionCounts"]["dataWriter"] += 1
            if request["caseName"] == "shared_late_target_revoke":
                assert bridge.armCurrentThread(request["target"], 120) == current["main"]["jvm"]
        if event == "return" and name == "__init__" and module == "src.shared.services.v3.gravitino_jdbc":
            readers.append(frame.f_locals["self"])
        if event == "return" and name == "__init__" and module == "src.shared.commands.progress_tracker":
            trackers.append(frame.f_locals["self"])
        if event == "return" and name == "run_source_jdbc_v2" and value is not None:
            current["main"] = identity(value)
            current["sourceSchema"] = value.schema.jsonValue()
            current["remoteCount"] = trackers[-1].total_records_to_read
        if module == "src.shared.services.v2.spark" and name == "init_df_tracking":
            if event == "call":
                current["tracker"] = identity(frame.f_locals["df"])
                current["readerLiveDuringCount"] = not readers[-1]._closed
            elif event == "return" and value is not None:
                current["trackerCount"] = value
                counted.set()
        if event == "call" and module == "src.shared.commands.progress_tracker" and name == "update_metrics":
            metrics = frame.f_locals["metrics"]
            if request["caseName"] == "shared_tracker_failure" and metrics.total_records_read is not None:
                metrics._send_telemetry = True
        if module == "src.shared.services.v3.gravitino_writer" and name == "write_dataset":
            if event == "call":
                assert counted.wait(120), "Real tracker count did not complete"
            elif event == "return":
                if value is not None:
                    current["phases"] = {key: getattr(value, key) for key in ("create", "cdf", "data")}
                written.set()
        if event == "call" and name == "close" and module == "src.shared.services.v3.gravitino_jdbc":
            current["joinedBeforeClose"] = trackers[-1]._thread is not None and not trackers[-1]._thread.is_alive()
            current["readerLiveUntilClose"] = not readers[-1]._closed

    def hook(_payload, _frame):
        current["hookCalls"] += 1
        if request["caseName"] == "shared_postcommit_hook_failure":
            raise RuntimeError("shared fixture postcommit hook failure")

    try:
        if request["caseName"] == "shared_tracker_failure":
            ProgressTracker._send_telemetry = telemetry_failure
        for index, job in enumerate(contract(request["caseName"])):
            current = dict(job=job["job"], sourceQuery=payload.job.query, calls=[], actionCounts={key: 0 for key in job["calls"]},
                           phases=dict(create="not_attempted", cdf="not_attempted", data="not_attempted"),
                           errorPhase=None, attempted=True, retryEligible=False, hookCalls=0)
            counted.clear()
            written.clear()
            name = request["caseName"] + "-" + job["job"]
            bridge.begin(name)
            bridge.bindWriter()
            sys.setprofile(profile)
            threading.setprofile(profile)
            error = None
            try:
                job_wrapper(JdbcExtractionSchema, "shared-live-qualification", payload, hook,
                            gravitino_runtime=runtime)(run_source_jdbc_v2)()
            except CoreException as failure:
                governed = failure.__cause__
                assert isinstance(governed, GravitinoJobError)
                current["errorPhase"] = governed.phase
                current["phases"] = {key: getattr(governed.result, key) for key in ("create", "cdf", "data")}
                cause = governed.cause
                if isinstance(cause, GravitinoWriteError):
                    current["writerErrorPhase"] = cause.phase
                    cause = cause.cause
                current["causeClass"] = type(cause).__module__ + "." + type(cause).__name__
                if isinstance(cause, Py4JJavaError):
                    error = cause.java_exception
                else:
                    assert type(cause) in (RuntimeError, ValueError)
            finally:
                sys.setprofile(previous_profile)
                threading.setprofile(previous_thread_profile)
                written.set()
            current["readerClosed"] = readers[-1]._closed
            current["sparkStopped"] = spark.sparkContext._jsc.sc().isStopped()
            current["readerIdentity"] = id(readers[-1])
            current["processId"] = os.getpid()
            current["parentProcessId"] = os.getppid()
            current["jvmProcessId"] = int(jvm.java.lang.management.ManagementFactory.getRuntimeMXBean().getName().split("@")[0])
            save(name + "-caller", current)
            bridge.finishAppend(name, error)
            observations.append(current)
            control = Path(request["controlDirectory"])
            (control / (name + "-ready.json")).write_text(json.dumps(dict(ready=True)))
            deadline = time.monotonic() + 240
            while not (control / (name + "-verified.json")).exists():
                assert time.monotonic() < deadline, "Independent native verification timed out; no replay"
                time.sleep(0.025)
        assert len({item["readerIdentity"] for item in observations}) == len(observations)
        modules = {name: module_provenance(module) for name, module in list(sys.modules.items())
                   if name.startswith("src.") and getattr(module, "__file__", "").endswith(".py")}
        assert all(value["container"]["path"] == request["hubSource"] for value in modules.values())
        save("hub-provenance", dict(modules=modules, producer=request["hubProducer"], processId=os.getpid()))
    finally:
        sys.setprofile(previous_profile)
        threading.setprofile(previous_thread_profile)
        CoreException._send_error_event = original_error_telemetry
        ProgressTracker._send_telemetry = original_metrics_telemetry
        if not contract(request["caseName"])[0]["separateMetadataExpected"]:
            from pyspark.errors.exceptions.captured import IllegalArgumentException
            try:
                bridge.close()
            except IllegalArgumentException as error:
                assert str(error) == "requirement failed: Separately authenticated metadata lookup was not observed"
                save("expected-metadata-absence", dict(className=type(error).__name__, message=str(error)))
        else:
            bridge.close()


def verify_owner(owner, subject):
    """Require the exact canonical public owner DTO for this application subject."""
    assert owner == {"name": subject, "type": "user"}


def snapshot(plan, output):
    """Read real grants/downloads and verify owner authority only for actual owned entities."""
    from cdf_live import Public, Ranger, save
    from urllib.parse import quote
    output.mkdir(mode=0o700)
    rest, ranger = Public(plan["gravitinoUrl"], plan["serviceAdmin"], output / "public"), Ranger(plan, output / "ranger")
    base = "/api/metalakes/" + plan["metalake"]
    public = {key: rest.call("GET", base + path)["role" if "Role" in key else "user"] for key, path in (
        ("appRole", "/roles/" + plan["appRole"]), ("metadataRole", "/roles/" + plan["metaRole"]),
        ("appUser", "/users/" + plan["subject"]), ("metadataUser", "/users/" + plan["metadataPrincipal"]))}
    policies = ranger.call("GET", "/service/plugins/secure/policies/download/" + plan["resourceService"] + "?lastKnownVersion=-1&pluginId=shared-supervisor", download=True)
    roles = ranger.call("GET", "/service/roles/secure/download/" + plan["resourceService"] + "?lastKnownRoleVersion=-1&pluginId=shared-supervisor", download=True)
    assert policies["serviceName"] == plan["resourceService"]
    owned = {}
    role_users = {r["name"]: {user["name"] for user in r.get("users", [])} for r in roles.get("rangerRoles", [])}
    for policy in policies["policies"]:
        if policy.get("policyType", 0) != 0:
            continue
        assert policy["service"] == plan["resourceService"]
        resources = policy["resources"]
        assert all(not value.get("isExcludes", False) and not value.get("isRecursive", False) for value in resources.values())
        for item in policy.get("policyItems", []):
            actors = set(item.get("users", [])) | set().union(*(role_users.get(role, set()) for role in item.get("roles", [])))
            if plan["subject"] not in actors and plan["rangerAppRole"] not in item.get("roles", []):
                continue
            accesses = {a["type"] for a in item["accesses"] if a["isAllowed"]}
            assert resources["catalog"]["values"] == [plan["catalog"]]
            assert resources["database"]["values"] == [plan["database"]]
            assert resources["column"]["values"] == ["*"]
            target = resources["table"]["values"]
            if target == ["*"]:
                assert accesses <= {"create"}, "Unqualified application wildcard authority"
            else:
                assert len(target) == 1 and target[0] in {t["name"] for t in plan["tables"]}
                if "all" in accesses:
                    owner = rest.call("GET", base + "/owners/table/" + quote(plan["catalog"] + "." + plan["database"] + "." + target[0], safe=""))["owner"]
                    verify_owner(owner, plan["subject"])
                    assert "gravitino-managed" in policy.get("policyLabels", [])
                    owned[target[0]] = owner
    public["appRole"]["rangerRole"] = plan["rangerAppRole"]
    save(output / "public-state.private.json", public)
    save(output / "ranger-state.private.json", dict(policies=policies, roles=roles))
    save(output / "owners.json", owned)
    state = dict(publicStateFile=str(output / "public-state.private.json"), rangerStateFile=str(output / "ranger-state.private.json"),
                 policy=policies["policyVersion"], role=roles["roleVersion"])
    save(output / "state-evidence.private.json", state)
    return state


def has_exact_table_privilege(role, target, privilege):
    """Recognize a retained public grant without issuing the mutation a second time."""
    objects = [item for item in role["securableObjects"] if item["type"] == "table" and item["fullName"] == target]
    assert len(objects) <= 1
    return bool(objects) and dict(name=privilege.lower(), condition="allow") in objects[0]["privileges"]


def deny_policy(plan, name, access):
    """One exact Ranger-authored override deny, distinct from managed normal-priority allow."""
    assert access in ("create", "alter", "update")
    return dict(service=plan["resourceService"], name="shared_exact_" + name + "_" + access,
                policyType=0, policyPriority=1, isEnabled=True, isAuditEnabled=True,
                resources={key: dict(values=[value], isExcludes=False, isRecursive=False) for key, value in
                           dict(catalog=plan["catalog"], database=plan["database"], table=name, column="*").items()},
                denyPolicyItems=[dict(roles=[plan["rangerAppRole"]], delegateAdmin=False, accesses=[dict(type=access, isAllowed=True)])])


def verify_deny_policy(policy, expected):
    """Verify canonical Ranger JSON without admitting extra subjects, authority or conditions."""
    assert all(policy.get(key) == value for key, value in expected.items())
    assert all(not policy.get(key) for key in ("policyItems", "allowExceptions", "denyExceptions", "dataMaskPolicyItems", "rowFilterPolicyItems", "conditions", "additionalResources", "policyLabels", "validitySchedules"))
    assert policy.get("isDenyAllElse", False) is False


def policy_setup(plan, name, output, late=False):
    """One exact local-fixture change; deny policies do not pretend to be public pushdown."""
    from cdf_live import Public, Ranger, save, read
    from urllib.parse import quote
    output.mkdir(mode=0o700)
    before = snapshot(plan, output / "before")
    base = "/api/metalakes/" + plan["metalake"]
    rest = Public(plan["gravitinoUrl"], plan["serviceAdmin"], output / "public")
    absent = name in ("shared_create_append", "shared_create_denied", "shared_late_target_revoke")
    retained_grant = False
    if not late and not absent:
        privilege, grant = ("SELECT_TABLE", False) if name == "shared_metadata_denied" else ("MODIFY_TABLE", True)
        target = plan["catalog"] + "." + plan["database"] + "." + name
        retained_grant = grant and has_exact_table_privilege(read(before["publicStateFile"])["appRole"], target, privilege)
        if not retained_grant:
            rest.call("PUT", base + "/permissions/roles/" + quote(plan["appRole"], safe="") + "/table/" +
                      quote(target, safe="") + ("/grant/" if grant else "/revoke/"),
                      {"privileges": [{"name": privilege, "condition": "ALLOW"}]})
    access = "update" if late else {"shared_update_denied": "update", "shared_alter_denied": "alter", "shared_create_denied": "create"}.get(name)
    policy = None
    retained_policy = False
    if access:
        ranger = Ranger(plan, output / "ranger")
        body = deny_policy(plan, name, access)
        existing = [value for value in read(before["rangerStateFile"])["policies"]["policies"] if value["name"] == body["name"]]
        assert len(existing) <= 1
        retained_policy = bool(existing)
        if retained_policy:
            verify_deny_policy(existing[0], body)
            created = existing[0]
        else:
            created = ranger.call("POST", "/service/public/v2/api/policy", body)
        policy = ranger.call("GET", "/service/public/v2/api/policy/" + str(created["id"]))
        verify_deny_policy(policy, body)
        save(output / "exact-deny-policy.json", policy)
    after = snapshot(plan, output / "after")
    if policy:
        old = {p["id"]: p for p in read(before["rangerStateFile"])["policies"]["policies"]}
        new = {p["id"]: p for p in read(after["rangerStateFile"])["policies"]["policies"]}
        if retained_policy:
            assert old == new
        elif late or absent:
            assert old == {key: value for key, value in new.items() if key != policy["id"]}
        assert new[policy["id"]]["denyPolicyItems"] == policy["denyPolicyItems"]
    save(output / "transition.json", dict(before=before, after=after, exactDenyPolicyId=policy["id"] if policy else None,
                                         publicPushdownClaim=False if policy else True, target=name, access=access, publicGrantAlreadyPresent=retained_grant, exactDenyAlreadyPresent=retained_policy))
    if late:
        after = dict(after, beforeRangerStateFile=before["rangerStateFile"], exactDenyPolicyId=policy["id"], applicationRole=plan["rangerAppRole"])
    return after


def observe(plan, runtime, target, output):
    """Independent raw Delta readback and ordinary Gravitino metadata, including absence."""
    from cdf_live import save, sha
    from create_live import registration
    from append_aqe_fixture import native
    from s3_objects import OwnedObjects
    from cdf_merge_live_checks import file_ref
    output.mkdir(mode=0o700)
    raw = OwnedObjects(plan["primary"], plan["prefix"] + "/").manifest(output / "raw")
    save(output / "raw.json", raw)
    registered = registration(plan, target, plan["serviceAdmin"], output / "registration.json")
    if registered["status"] == 404:
        value = dict(version=-1, publications=[], nativeId=None, rows=[], schema=[], properties={}, checksums=[], columnIds=[])
    else:
        assert registered["status"] == 200
        table = registered["body"]["table"]
        properties = table["properties"]
        location = properties["delta.registration.receipt.location"]
        descriptor = dict(next(t for t in plan["tables"] if t["name"] == target), location=location)
        image = native(plan, runtime, [descriptor], "verify", output / "native")
        actual = image["tables"][0]
        assert actual["rows"] == actual["reopenRows"] and actual["version"] == actual["reopenVersion"]
        assert all(runtime["jarHashes"].get(value["origin"]) == value["sha256"] for value in image["origins"].values())
        prefix = location.split("/", 3)[3].rstrip("/") + "/_delta_log/"
        publications, checksums, commits = [], [], {}
        for entry in raw:
            if not entry["key"].startswith(prefix):
                continue
            path = Path(entry["key"])
            if path.stem.isdecimal() and path.suffix in (".json", ".crc"):
                (publications if path.suffix == ".json" else checksums).append(int(path.stem))
                retained = output / "raw" / hashlib.sha256(entry["key"].encode()).hexdigest()
                assert sha(retained) == entry["sha256"]
                if path.suffix == ".json":
                    commits[str(int(path.stem))] = file_ref(retained)
        fields = json.loads(actual["schemaJson"])["fields"]
        assert [c["name"] for c in table["columns"]] == [f["name"] for f in fields]
        assert properties["delta.registration.receipt.nativeId"] == actual["native"]["id"]
        value = dict(version=actual["version"], publications=sorted(publications), checksums=sorted(checksums),
                     nativeId=actual["native"]["id"], rows=actual["rows"], schema=[f["name"] for f in fields],
                     properties=actual["native"]["properties"], registeredVersion=int(properties["delta.registration.receipt.nativeVersion"]),
                     nativeEvidence=file_ref(output / "native/result.json"), registration=file_ref(output / "registration.json"), commits=commits)
    after = OwnedObjects(plan["primary"], plan["prefix"] + "/").manifest()
    assert raw == after, "Readback mutated native bytes"
    value["rawEvidence"] = file_ref(output / "raw.json")
    save(output / "image.json", value)
    return value


def run_case(plan, runtime, name, postgres, output, hub):
    from cdf_live import prepare_request, clean_environment, save, read
    from cdf_merge_live_checks import file_ref
    from append_aqe_live import start, finish, wait_for
    from external_jdbc_live import external_python_path
    from s3_objects import OwnedObjects
    from shared_writer_checks import verify_actions, verify_outcome, verify_denial, verify_native, verify_visibility
    output.mkdir(mode=0o700)
    state = policy_setup(plan, name, output / "policy")
    before = observe(plan, runtime, name, output / "before")
    request = prepare_request(plan, runtime, state, output / "request")
    request.update(frontend="saveAsTable", caseName=name, target=name, output=str(output / "jvm"), pythonOutput=str(output / "python"),
                   externalJdbc=True, sharedWriter=True, warmOrder="none", barrier=name == "shared_late_target_revoke", closeExternalScope=False,
                   jdbcConnectionFile=plan["jdbcConnectionFile"], hubSource=hub["archive"]["path"], hubProducer=hub["commit"], deadlineSeconds=1200,
                   acceptanceSourceExpected=dict(entryPoint=file_ref(Path(__file__).resolve()), checker=file_ref(Path(sys.modules["shared_writer_checks"].__file__).resolve())))
    properties = Path(request["sparkConfigurationFile"])
    lines = [line for line in properties.read_text().splitlines() if not line.startswith("spark.databricks.delta.snapshotPartitions=")]
    properties.write_text("\n".join(lines) + "\n")
    save(output / "request/request.private.json", request)
    env = clean_environment(plan)
    root = Path(__file__).resolve().parents[2]
    dependencies = root / ".superpowers/sdd/2026-11-05-delta-external-jdbc/pytest-tools"
    env.update(SPARK_HOME=runtime["sparkHome"], PYSPARK_PYTHON=plan["python"], SPARK_USER=plan["subject"], HADOOP_USER_NAME=plan["subject"],
               PYSPARK_PIN_THREAD="true", JAVA_TOOL_OPTIONS="-XX:ActiveProcessorCount=2 -XX:MaxMetaspaceSize=768m",
               ENV="test", IS_LOCAL="false", KAFKA_BROKERS="localhost:9092", KAFKA_TOPIC="unused",
               PYTHONPATH=external_python_path(hub["archive"]["path"], runtime["pythonZip"], dependencies))
    pg_before = postgres.observe(output / "postgres-before.json")
    save(output / "attempt.json", dict(attempted=True, retryEligible=False, outcome="unknown", contract=contract(name)))
    argv = [runtime["sparkHome"] + "/bin/spark-submit", "--driver-memory", "3g", "--master", "local[2]", "--properties-file", str(properties),
            "--driver-class-path", request["driverSecurityDirectory"], str(Path(__file__).resolve()), "--child", str(output / "request/request.private.json")]
    process, log = start(argv, env, output / "writer")
    jobs = []
    try:
        control = Path(request["controlDirectory"])
        if request["barrier"]:
            wait_for(control / "held-ready.json", process, 300)
            held = OwnedObjects(plan["primary"], plan["prefix"] + "/").manifest(output / "raw-held")
            save(output / "raw-held.json", held)
            state = policy_setup(plan, name, output / "late-revoke", late=True)
            state = dict(state, exactRangerDenyComplete=True)
            save(output / "barrier-state.private.json", state)
            save(control / "revoke-complete.private.json", dict(stateEvidenceFile=str(output / "barrier-state.private.json")))
        previous_path = output / "postgres-before.json"
        previous, before_path = pg_before, output / "before/image.json"
        for index, job in enumerate(contract(name)):
            label = name + "-" + job["job"]
            wait_for(control / (label + "-ready.json"), process, 450)
            path = output / ("postgres-" + job["job"] + ".json")
            current = postgres.observe(path)
            after = observe(plan, runtime, name, output / ("after-" + job["job"]))
            caller_path = output / "python" / (label + "-caller.json")
            observation_path = output / "jvm" / (label + "-append.json")
            record = dict(job=job["job"], postgresBefore=file_ref(previous_path), postgresAfter=file_ref(path),
                          counts={key: current[key] - previous[key] for key in job["sql"]}, caller=file_ref(caller_path),
                          observation=file_ref(observation_path), namedRequests=file_ref(output / "jvm" / (label + "-named-requests.json")), before=file_ref(before_path), after=file_ref(output / ("after-" + job["job"]) / "image.json"))
            save(output / ("original-" + job["job"] + ".json"), record)
            caller = read(caller_path)
            verify_actions(name, index, caller["actionCounts"])
            verify_outcome(name, index, caller)
            verify_denial(name, read(observation_path)["failure"])
            verify_visibility(name, read(record["namedRequests"]["path"]))
            verify_native(name, index, before, after)
            assert record["counts"] == job["sql"], "Original source actions differ; no replay"
            jobs.append(record)
            save(control / (label + "-verified.json"), dict(checkpoint=label, verified=True))
            previous, previous_path, before, before_path = current, path, after, Path(record["after"]["path"])
        code, timed = finish(process, log, output / "writer", 60)
        log = None
        assert code == 0 and not timed
    finally:
        if log is not None:
            finish(process, log, output / "writer", 5)
            observe(plan, runtime, name, output / "failure-native-after")
            postgres.observe(output / "postgres-failure.json")
    record = dict(name=name, jobs=jobs, runtime=file_ref(runtime["manifestPath"]), writerExit=file_ref(output / "writer/exit.json"),
                  acceptanceSource=file_ref(output / "python/acceptance-source.json"), hubProvenance=file_ref(output / "python/hub-provenance.json"), origins=file_ref(output / "jvm/origins.json"),
                  runtimeProfile=file_ref(output / "jvm/runtime-profile.json"), pythonRuntime=file_ref(output / "python/python-runtime.json"))
    if request["barrier"]:
        record["barrier"] = {key: file_ref(output / path) for key, path in {
            "held": "jvm/held.json", "release": "jvm/release-witness.json", "forwarded": "jvm/forwarded.json",
            "rawHeld": "raw-held.json", "rawBefore": "before/raw.json"}.items()}
    save(output / "original-case.json", record)
    return record


def run(runtime_path, inputs_path, output, hub_path):
    from cdf_live import ROOT, read, save, verify_deployed
    from cdf_merge_live_checks import file_ref
    from cdf_read_live_package import verify_runtime
    from append_aqe_live import start, finish, wait_for
    from external_jdbc_live import ExternalPostgres, readiness
    output.mkdir(mode=0o700)
    runtime = read(runtime_path)
    runtime["manifestPath"] = str(runtime_path)
    verify_runtime(runtime)
    pair = (runtime["spark"], runtime["delta"])
    readiness(output / "readiness.json", pair)
    inputs = read(inputs_path)
    association = verify_deployed(inputs, output / "deployed-ranger", safe_inspection=True)
    inputs = dict(inputs, deployedRangerEvidence=str(association), sharedWriterPair=list(pair))
    inputs["pythonRuntime"] = str(ROOT / ".superpowers/sdd/2026-10-29-spark-hub-catalog-foundation/runtime-python" / (pair[0] + "-" + pair[1]) / "venv/bin/python")
    hub = read(hub_path)
    save(output / "frozen-inventory.json", dict(pair=pair, cases={name: contract(name) for name in case_names(pair)}, hubProducer=hub["commit"], qualifiedSettings=shared_settings()))
    postgres = ExternalPostgres(output / "postgres", owner="shared-writer-task5")
    process, log, cases = None, None, []
    fixture = output / "fixture"
    try:
        inputs["jdbcConnectionFile"] = str(postgres.start())
        save(output / "inputs.private.json", inputs)
        process, log = start([sys.executable, str(ROOT / "delta/acceptance/append_aqe_fixture.py"), "--inputs", str(output / "inputs.private.json"),
                              "--package", str(runtime_path), "--freeze", str(runtime_path), "--out", str(fixture)], os.environ.copy(), output / "fixture-command")
        wait_for(fixture / "ready.json", process, 600)
        plan = read(fixture / "state.private.json")
        (output / "cases").mkdir(mode=0o700)
        for name in case_names(pair):
            cases.append(run_case(plan, runtime, name, postgres, output / "cases" / name, hub))
            save(output / (name + "-completed.json"), dict(case=name, finished=time.time()))
    finally:
        if fixture.exists():
            save(fixture / "stop-request.json", dict(stopOwnedServer=True, preserveDataAndEvidence=True))
        if process is not None:
            code, timed = finish(process, log, output / "fixture-command", 60)
            save(output / "fixture-terminal.json", dict(exitCode=code, timedOut=timed))
        postgres.close()
    assert code == 0 and not timed
    from external_jdbc_checks import fixture_identity
    save(output / "fixture-identity.json", fixture_identity(plan))
    save(output / "raw-result.json", dict(spark=pair[0], delta=pair[1], caseInventory=list(case_names(pair)), cases=cases, runtime=file_ref(runtime_path),
         fixtureIdentity=file_ref(output / "fixture-identity.json"),
         columnsBefore=file_ref(fixture / "server/columns-shared-before.json"), columnsAfter=file_ref(fixture / "server/columns-shared-after.json"),
         fixtureExit=file_ref(output / "fixture-command/exit.json"), sourceTerminal=file_ref(output / "postgres/terminal.json"),
         source=file_ref(output / "postgres/source.json"), deployedRanger=file_ref(association), hubSource=file_ref(hub_path)))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--child")
    for key in ("runtime", "inputs", "out", "hub"):
        parser.add_argument("--" + key)
    args = parser.parse_args()
    os.umask(0o077)
    if args.child:
        run_child(Path(args.child).resolve())
    else:
        assert all((args.runtime, args.inputs, args.out, args.hub))
        run(*(Path(value).resolve() for value in (args.runtime, args.inputs, args.out, args.hub)))


if __name__ == "__main__":
    main()

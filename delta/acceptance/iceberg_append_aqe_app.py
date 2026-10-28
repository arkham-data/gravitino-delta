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

"""Evidence-only stock Iceberg / Kyuubi Spark runtime probe."""

import json
import os
from pathlib import Path
import shlex
import sys
import time
import threading
from read_parity_pyspark import ref, clear_job_group
from append_aqe_checks import case_specs, SCHEMA, callback_plan
from pyspark.sql import SparkSession

request = json.loads(Path(sys.argv[1]).read_text())
out = Path(request["output"])


def save(name, value):
    (out / name).write_text(json.dumps(value, indent=2, sort_keys=True))


def envfile(path):
    return {
        k: shlex.split(v)[0]
        for k, v in (
            line.split("=", 1)
            for line in Path(path).read_text().splitlines()
            if line and not line.startswith("#")
        )
    }


creds = envfile(request["credentials"])
builder = (
    SparkSession.builder.appName("Iceberg governed append comparator")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.parallelismFirst", "false")
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "67108864")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .config("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
    .config("spark.hadoop.fs.s3a.endpoint", request["endpoint"])
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.access.key", creds["MINIO_ROOT_USER"])
    .config("spark.hadoop.fs.s3a.secret.key", creds["MINIO_ROOT_PASSWORD"])
    .config("spark.ui.enabled", "false")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")
)
if request.get("wrapper"):
    builder = (
        builder.config(
            "spark.plugins",
            "org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin",
        )
        .config("spark.sql.gravitino.uri", request["gravitinoUrl"])
        .config("spark.sql.gravitino.metalake", request["metalake"])
        .config("spark.sql.gravitino.enableIcebergSupport", "true")
        .config("spark.sql.gravitino.iceberg.rest-routing-enabled", "false")
    )
else:
    builder = (
        builder.config(
            "spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog"
        )
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config("spark.sql.catalog.iceberg.warehouse", request["warehouse"])
        .config("spark.sql.catalog.iceberg.cache-enabled", "false")
    )
if request["mode"] == "application":
    builder = builder.config(
        "spark.sql.extensions",
        "org.apache.kyuubi.plugin.spark.authz.ranger.RangerSparkExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
else:
    builder = builder.config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
spark = builder.getOrCreate()
assert spark.version == request["spark"]
assert os.environ.get("TZ") == "UTC"
jvm = spark.sparkContext._jvm
loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()
classes = [
    "org.apache.iceberg.spark.SparkCatalog",
    "org.apache.kyuubi.plugin.spark.authz.ranger.RangerSparkExtension",
    "org.apache.ranger.plugin.service.RangerBasePlugin",
    "org.apache.ranger.audit.provider.AuditProviderFactory",
    "org.apache.spark.sql.SparkSession",
    "scala.Option",
]
if request.get("wrapper"):
    classes += [
        "org.apache.gravitino.spark.connector.iceberg.GravitinoIcebergCatalogSpark35",
        "org.apache.gravitino.spark.connector.iceberg.GravitinoIcebergCatalog",
        "org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin",
    ]
origins = {}
for name in classes:
    cls = loader.loadClass(name)
    origins[name] = {
        "source": str(cls.getProtectionDomain().getCodeSource().getLocation()),
        "implementationVersion": cls.getPackage().getImplementationVersion(),
        "artifact": ref(
            Path(
                str(cls.getProtectionDomain().getCodeSource().getLocation().getPath())
            ).resolve()
        ),
    }
save(
    "runtime.json",
    {
        "spark": spark.version,
        "scala": jvm.scala.util.Properties.versionNumberString(),
        "java": jvm.java.lang.System.getProperty("java.version"),
        "sparkUser": spark.sparkContext.sparkUser(),
        "ugi": str(
            jvm.org.apache.hadoop.security.UserGroupInformation.getCurrentUser()
        ),
        "origins": origins,
        "mode": request["mode"],
        "warehouse": request["warehouse"],
    },
)

catalog = spark._jsparkSession.sessionState().catalogManager().catalog("iceberg")
save(
    "catalog.json",
    {
        "actualClass": str(catalog.getClass().getName()),
        "extensions": spark.conf.get("spark.sql.extensions"),
    },
)
ns = "iceberg." + request["database"]


def state(table):
    rows = [list(row) for row in spark.table(ns + "." + table).collect()]
    snapshots = spark.sql(
        "SELECT snapshot_id FROM "
        + ns
        + "."
        + table
        + ".snapshots ORDER BY committed_at DESC"
    ).collect()
    return {"rows": rows, "snapshot": int(snapshots[0][0]) if snapshots else None}


if request["mode"] == "seed":
    spark.sql("CREATE NAMESPACE " + ns)
    spark.sql("CREATE TABLE " + ns + ".source (" + SCHEMA + ") USING iceberg")
    spark.sql(
        "INSERT INTO "
        + ns
        + ".source VALUES (1,'east','z',true),(1,'east','z',true),(2,'west','a',true),(3,'north','hidden',false)"
    )
    states = {}
    for spec in case_specs():
        table = spec["name"]
        partition = " PARTITIONED BY (region)" if spec["form"] == "partitioned" else ""
        spark.sql(
            "CREATE TABLE "
            + ns
            + "."
            + table
            + " ("
            + SCHEMA
            + ") USING iceberg"
            + partition
            + " TBLPROPERTIES ('write.distribution-mode'='hash')"
        )
        spark.sql(
            "INSERT INTO " + ns + "." + table + " VALUES (0,'seed','original',true)"
        )
        states[table] = state(table)
    spark.sql("CREATE TABLE " + ns + ".id_only_probe (id BIGINT) USING iceberg")
    spark.sql("INSERT INTO " + ns + ".id_only_probe VALUES (0)")
    states["id_only_probe"] = state("id_only_probe")
    save("states.json", states)
    spark.stop()
    sys.exit(0)
if request["mode"] == "verify":
    save(
        "states.json",
        {
            spec["name"]: state(spec["name"])
            for spec in case_specs() + [{"name": "id_only_probe"}]
        },
    )
    spark.stop()
    sys.exit(0)

from pyspark.java_gateway import ensure_callback_server_started

ensure_callback_server_started(spark.sparkContext._gateway)
plugin = getattr(
    getattr(jvm.org.apache.kyuubi.plugin.spark.authz.ranger, "SparkRangerAdminPlugin$"),
    "MODULE$",
)
deadline = time.monotonic() + 90
while int(plugin.getPoliciesVersion()) < request["minPolicy"]:
    assert time.monotonic() < deadline, "Initial policy download did not converge"
    time.sleep(0.1)
audit_provider = plugin.getAuditProviderFactory().getAuditProvider()


class Listener:
    class Java:
        implements = ["org.apache.spark.sql.util.QueryExecutionListener"]

    def record(self, function, query, failed):
        if bridge.active:
            with bridge.lock:
                bridge.queries.append(query)
                bridge.callbacks.append(
                    {
                        "case": bridge.active,
                        "caseSequence": bridge.sequence,
                        "function": function,
                        "queryId": int(query.id()),
                        "failed": failed,
                    }
                )

    def onSuccess(self, function, query, nanos):
        self.record(function, query, False)

    def onFailure(self, function, query, error):
        self.record(function, query, True)


class Bridge:
    def __init__(self):
        self.active = ""
        self.sequence = 0
        self.lock = threading.Lock()
        self.requests = []
        self.queries = []
        self.callbacks = []
        self.audit_start = 0

    def drain(self):
        spark.sparkContext._jsc.sc().listenerBus().waitUntilEmpty(30000)
        total = int(audit_provider.getTotalCount())
        deadline = time.monotonic() + 30
        while True:
            events = []
            for path in sorted(Path(request["auditDirectory"]).glob("*.log")):
                raw = path.read_bytes()
                if raw and not raw.endswith(b"\n"):
                    break
                events.extend(
                    json.loads(line) for line in raw.decode().splitlines() if line
                )
            else:
                if len(events) == total:
                    assert (
                        int(audit_provider.getTotalFailedCount())
                        == int(audit_provider.getTotalDeferredCount())
                        == 0
                    ), "Ranger audit queue lost/deferred events"
                    self.audit_bytes = b"".join(
                        path.read_bytes()
                        for path in sorted(
                            Path(request["auditDirectory"]).glob("*.log")
                        )
                    )
                    assert len(list(Path(request["auditDirectory"]).iterdir())) <= 1, (
                        "Unexpected audit rotation"
                    )
                    return events
                assert len(events) <= total, (
                    "Unexpected audit events outside this isolated process"
                )
            assert time.monotonic() < deadline, (
                "Ranger audit sink did not flush submitted event count"
            )
            time.sleep(0.01)

    def begin(self, name):
        events = self.drain()
        with self.lock:
            self.audit_start = len(events)
            self.audit_start_bytes = len(self.audit_bytes)
            self.requests = []
            self.queries = []
            self.callbacks = []
            self.sequence += 1
            self.active = name
        spark.sparkContext.setJobGroup(name, name)

    def phase(self, name):
        self.drain()
        self.active = ""
        save(name + "-ready.json", {"ready": True})
        gate = out / (name + "-verified.json")
        deadline = time.monotonic() + 180
        while not gate.exists():
            assert time.monotonic() < deadline, "Policy phase gate timed out"
            time.sleep(0.05)
        state = json.loads(gate.read_text())
        while int(plugin.getPoliciesVersion()) < state["policyVersion"]:
            assert time.monotonic() < deadline, "Policy application did not converge"
            time.sleep(0.1)
        save(
            name + "-applied.json",
            {
                "policy": int(plugin.getPoliciesVersion()),
                "policySnapshot": state["policySnapshot"],
            },
        )

    def finish(self, name, allowed):
        events = self.drain()
        assert self.active == name
        self.active = ""
        selected_events = events[self.audit_start :]
        audit_path = out / (name + "-audit.raw.jsonl")
        audit_path.write_bytes(self.audit_bytes[self.audit_start_bytes :])
        callbacks = []
        with self.lock:
            for index, (callback, query) in enumerate(
                zip(self.callbacks, self.queries)
            ):
                callback = dict(callback)
                analyzed, physical = callback_plan(query, callback["failed"])
                final = (
                    str(physical)
                    if physical is not None
                    else "Rejected before physical payload"
                )
                prefix = name + "-callback-" + str(index)
                analyzed_path = out / (prefix + "-analyzed.txt")
                analyzed_path.write_text(analyzed)
                final_path = out / (prefix + "-final.txt")
                final_path.write_text(final)
                callback.update(
                    analyzedPlan=ref(analyzed_path),
                    finalPlan=ref(final_path),
                    kind="write-payload" if "AppendData" in analyzed else "metadata",
                    planNodes=plan_nodes(physical) if physical is not None else [],
                )
                callbacks.append(callback)
        jobs = list(spark.sparkContext.statusTracker().getJobIdsForGroup(name))
        job_details = []
        for job_id in jobs:
            info = spark.sparkContext.statusTracker().getJobInfo(job_id)
            job_details.append(
                {
                    "jobId": job_id,
                    "stageIds": list(info.stageIds),
                    "status": info.status,
                }
            )
        clear_job_group(spark)
        return {
            "rangerRequests": [
                dict(event, observationKind="audited-permission-event")
                for event in selected_events
            ],
            "auditEvidence": ref(audit_path),
            "auditRange": {
                "firstEvent": self.audit_start,
                "endEvent": len(events),
                "firstByte": self.audit_start_bytes,
                "endByte": len(self.audit_bytes),
                "submittedTotal": int(audit_provider.getTotalCount()),
                "failedTotal": int(audit_provider.getTotalFailedCount()),
                "deferredTotal": int(audit_provider.getTotalDeferredCount()),
            },
            "callbacks": callbacks,
            "jobs": job_details,
            "pluginPolicyVersion": int(plugin.getPoliciesVersion()),
            "listenerQueuesDrained": True,
            "unobservedEvaluations": ["mask requests", "row-filter requests"],
        }

    def close(self):
        try:
            self.drain()
            self.active = ""
            spark._jsparkSession.listenerManager().unregister(listener)
        finally:
            spark.stop()


def plan_nodes(plan):
    result = []
    seen = set()

    def visit(node):
        ident = int(jvm.java.lang.System.identityHashCode(node))
        if ident in seen:
            return
        seen.add(ident)
        cls = str(node.getClass().getName())
        rec = {"className": cls, "nodeName": str(node.nodeName()), "id": ident}
        if cls.endswith("AdaptiveSparkPlanExec"):
            rec["isFinalPlan"] = bool(node.isFinalPlan())
            visit(node.executedPlan())
        if cls.endswith("ShuffleQueryStageExec") or cls.endswith(
            "BroadcastQueryStageExec"
        ):
            rec["stageId"] = int(node.id())
            rec["materialized"] = bool(node.isMaterialized())
            visit(node.plan())
        if cls.endswith("AQEShuffleReadExec"):
            rec["partitionSpecs"] = str(node.partitionSpecs())
            rec["partitions"] = int(node.partitionSpecs().size())
        result.append(rec)
        if cls.endswith("CommandResultExec"):
            visit(node.commandPhysicalPlan())
        iterator = node.children().iterator()
        while iterator.hasNext():
            visit(iterator.next())

    visit(plan)
    return result


bridge = Bridge()
listener = Listener()
spark._jsparkSession.listenerManager().register(listener)
cases = []
try:
    for spec in case_specs() + [
        dict(name="id_only_probe", api="writeTo", form="plain", identity="id-only")
    ]:
        if spec["identity"] != request["identityProfile"]:
            continue
        name = spec["name"]
        target = ns + "." + name
        bridge.begin(name)
        error = None
        try:
            # A qualified source join supplies an exchange even for unpartitioned targets.
            query = (
                "SELECT s.id,s.region,s.email,s.visible FROM "
                + ns
                + ".source s JOIN (SELECT id AS join_id FROM range(1,4)) keys ON s.id=keys.join_id"
            )
            if spec["identity"] == "id-only":
                query = "SELECT id FROM " + ns + ".source"
            if spec["api"] == "sql":
                spark.sql("INSERT INTO " + target + " " + query).collect()
            else:
                frame = spark.sql(query)
                if spec["api"] == "insertInto":
                    frame.write.mode("append").insertInto(target)
                elif spec["api"] == "saveAsTable":
                    frame.write.mode("append").saveAsTable(target)
                else:
                    frame.writeTo(target).append()
        except Exception as exc:
            error = str(exc)
        observation = bridge.finish(name, error is None)
        observation.update(
            spec,
            subject=spark.sparkContext.sparkUser(),
            target=name,
            outcome="allowed"
            if error is None
            else (
                "denied"
                if "AccessControlException" in error and "Permission denied:" in error
                else "unqualified"
            ),
            error=error,
        )
        cases.append(observation)
        save("cases.json", cases)
finally:
    bridge.close()
save("success.json", {"completed": True, "cases": len(cases)})

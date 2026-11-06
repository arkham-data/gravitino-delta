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

"""Real external-input actions inside the existing downloaded-policy observer."""
import json
import os
from pathlib import Path
import threading
import sys
import time
from uuid import uuid4


def hub_payload(connection):
    """Construct the bounded PostgreSQL snapshot payload using private credentials."""
    from urllib.parse import urlsplit
    from src.data_platform.sources.jdbc.v2.schema import JdbcExtractionSchema
    endpoint = urlsplit(connection["endpoint"][5:])
    return JdbcExtractionSchema.model_validate({
        "job": {"query": "SELECT id, secret, region FROM source_schema.source", "source": {
            "id": str(uuid4()), "name": "owned-live-external", "platform": {"id": 1, "name": "PostgreSQL"},
            "structure": {"id": 1, "name": "table"}, "location": "source_schema.source", "credentials": {
                "host": endpoint.hostname, "port": endpoint.port, "database": endpoint.path[1:],
                "username": connection["user"], "password": connection["password"]}}},
        "output_dataset": {"id": str(uuid4()), "name": "existing-native-target"},
        "user_properties": {"group_id": str(uuid4()), "target_bucket": "s3://unused"},
        "write_strategy": {"write_target_type": "append", "read_source_type": "snapshot"},
        "execution_properties": {"execution_method_id": 1, **{key: str(uuid4()) for key in (
            "cluster_id", "pipeline_version_id", "pipeline_step_id", "pipeline_execution_id",
            "pipeline_step_execution_id", "pipeline_id")}}})


def arm_publication_barrier(bridge, target, thread):
    """Use the existing gateway's bounded deadline on the pinned writer connection."""
    assert bridge.armCurrentThread(target, 120) == thread


def close_external_bridge(spark, bridge, request, save):
    """Retain actual early-denial identity and narrowly classify absent metadata at close."""
    if request["caseName"] != "external_unissued_rejected":
        bridge.close()
        return
    from pyspark.errors.exceptions.captured import IllegalArgumentException
    from cdf_merge_live_checks import file_ref
    from external_jdbc_checks import verify_early_unissued_close
    jvm = spark._sc._jvm
    identity = dict(spark=spark.version, java=jvm.java.lang.System.getProperty("java.specification.version"),
                    scala=jvm.scala.util.Properties.versionNumberString(),
                    subject=jvm.org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerQuerySubject.current(spark._jsparkSession).user(),
                    jvmTimezone=jvm.java.util.TimeZone.getDefault().getID(), origin="executing-jvm-before-close",
                    jvmPid=int(jvm.java.lang.management.ManagementFactory.getRuntimeMXBean().getName().split("@")[0]))
    save("runtime-before-close", identity)
    try:
        bridge.close()
    except IllegalArgumentException as error:
        if str(error) != "requirement failed: Separately authenticated metadata lookup was not observed":
            raise
        output = Path(request["pythonOutput"]).parent
        read = lambda path: json.loads(path.read_text())
        checkpoint = Path(request["controlDirectory"])/(request["caseName"]+"-append-verified.json")
        before, after = [read(output/(part+"/image.json")) for part in ("before", "after")]
        old, new = [read(output/name) for name in ("postgres-before.json", "postgres-append.json")]
        counts = {k:new[k]-old[k] for k in ("schemaQueries","remoteCountQueries","payloadQueries","mutationQueries")}
        verify_early_unissued_close(request["caseName"], str(error), read(checkpoint),
                                   read(output/("jvm/"+request["caseName"]+"-append-append.json")), before, after, counts, identity)
        save("expected-metadata-absence", dict(className=type(error).__module__+"."+type(error).__name__, message=str(error),
                                              checkpoint=file_ref(checkpoint), runtimeObserved=file_ref(output/"python/runtime-before-close.json")))


def run_external(spark, bridge, request, save, connection, checkpoint):
    """Invoke stock named writers and the installed Hub caller, without replay."""
    from gravitino_delta import GravitinoJdbcInput
    import gravitino_delta.inputs as facade
    from append_aqe_profiles import module_provenance
    from py4j.protocol import Py4JJavaError
    from pyspark.sql import functions as F
    from external_jdbc_checks import action_names

    jvm = spark._sc._jvm
    name = request["caseName"]
    bridge.begin(name + "-setup")
    bridge.bindWriter()
    if request["warmOrder"] != "none":
        bridge.warmTarget(request["targetLocation"], request["warmOrder"])
    private = json.loads(Path(request["jdbcConnectionFile"]).read_text())
    options = dict(url=private["endpoint"], driver=private["driver"], user=private["user"], password=private["password"],
                   dbtable="(SELECT id, secret, region FROM source_schema.source) external_input")
    if name == "external_partitioned_save_as_table":
        options.update(partitionColumn="id", lowerBound="1", upperBound="4", numPartitions="2", fetchsize="2")
    last_configuration = None
    def configuration_boundary(label):
        nonlocal last_configuration
        explicit = spark._jsparkSession.conf().getAll().iterator()
        effective = spark._jsparkSession.sessionState().conf().getAllDefinedConfs().iterator()
        current = {}
        for kind, iterator in (("explicit", explicit), ("effective", effective)):
            values = {}
            while iterator.hasNext():
                item = iterator.next()
                values[str(item._1())] = str(item._2())
            current[kind] = values
        changes = {kind: sorted(k for k in current[kind].keys() | last_configuration[kind].keys()
                                if current[kind].get(k) != last_configuration[kind].get(k))
                   for kind in current} if last_configuration is not None else {kind: [] for kind in current}
        save("configuration-"+label, dict(boundary=label, changedKeys=changes))
        last_configuration = current
    configuration_boundary("before-open")
    reader, client, tracker, frame = None, None, None, None
    identities = {}
    calls = []
    previous_profile = sys.getprofile()
    previous_thread_profile = threading.getprofile()
    def profile(frame, event, _argument):
        if event == "call":
            module = frame.f_globals.get("__name__", "")
            function = frame.f_code.co_name
            if (module, function) in {
                ("src.data_platform.sources.jdbc.v2.main", "run_source_jdbc_v2"),
                ("src.shared.services.v2.spark", "query_jdbc"),
                ("src.shared.services.v3.gravitino_jdbc", "read"),
                ("src.shared.commands.progress_tracker", "_run"),
                ("src.shared.services.v2.spark", "init_df_tracking"),
            }:
                calls.append(dict(module=module, function=function, pythonThread=threading.get_ident()))
    save("facade-provider", module_provenance(facade))
    if name == "external_hub_tracker":
        from src.shared.services.v3.gravitino_jdbc import GravitinoJdbcReader
        from src.shared.services.v2.spark import SparkClient
        from src.shared.schemas.v2.base_jobs import ComputeProperties
        from src.shared.commands.progress_tracker import ProgressTracker
        from src.data_platform.sources.jdbc.v2.main import run_source_jdbc_v2
        sys.setprofile(profile)
        threading.setprofile(profile)
        reader = GravitinoJdbcReader(lifetime_seconds=1200)
        client = SparkClient("external-live-hub", ComputeProperties(auto_compact_enabled=False, delta_optimize_write=False), external_jdbc_reader=reader)
        assert client.session._jsparkSession == spark._jsparkSession
        tracker = ProgressTracker(uuid4(), "external-live-tracker", interval_seconds=0.01)
    else:
        reader = GravitinoJdbcInput.open(spark, lifetime_seconds=1200)
    configuration_boundary("after-open")
    if request["closeExternalScope"]:
        bridge.bindExternalScope(reader._jinput)

    def identity():
        ugi = jvm.org.apache.hadoop.security.UserGroupInformation.getCurrentUser()
        return dict(python=threading.get_ident(), jvm=jvm.java.lang.Thread.currentThread().getId(),
                    session=spark._jsparkSession.sessionUUID(), ugi=str(ugi), groups=sorted(ugi.getGroupNames()))

    def execute(action, operation):
        label = name + "-" + action
        bridge.begin(label)
        thread = bridge.bindWriter()
        configuration_boundary(action+"-before")
        before = connection()
        assert before["jvmThread"] == thread
        error, value = None, None
        try:
            value = operation()
        except Py4JJavaError as failure:
            error = failure.java_exception
        configuration_boundary(action+"-after")
        after = connection()
        assert before == after
        save(label + "-caller", dict(before=before, after=after, count=value if isinstance(value, int) else None,
                                    pinnedEnvironment=os.environ.get("PYSPARK_PIN_THREAD")))
        bridge.finishAppend(label, error)
        if action == "append" and request["warmOrder"] != "none":
            bridge.observeWarm(label)
        checkpoint(label)
        return value

    try:
        if name == "external_hub_tracker":
            identities["main"] = identity()
            def remote_count():
                nonlocal frame
                frame = run_source_jdbc_v2(client, hub_payload(private), tracker, None)
                return tracker.total_records_to_read
            assert execute("remote_count", remote_count) == 3
            errors = []
            old_hook = threading.excepthook
            threading.excepthook = lambda args: errors.append(type(args.exc_value).__name__)
            completed = threading.Event()
            def tracked():
                identities["tracker"] = identity()
                count = execute("tracker_count", lambda: client.init_df_tracking(frame))
                completed.set()
                return count
            try:
                tracker.begin_read_tracker(tracked)
                assert completed.wait(300) and not errors, "Actual ProgressTracker callback did not complete"
                tracker.stop_read_tracker()
                assert tracker.total_records_read == 3
            finally:
                threading.excepthook = old_hook
            from spark_hub_read_pyspark import import_inventory
            inventory = import_inventory(request["hubSource"])
            save("hub-provenance", inventory)
            save("hub-caller", dict(modules=list(inventory["importPaths"]), remoteCount=tracker.total_records_to_read,
                                    trackerCount=tracker.total_records_read, identities=identities, calls=calls))
        else:
            if name != "external_unissued_rejected":
                frame = reader.read(options)
        configuration_boundary("after-source")
        if name == "external_source_advances":
            assert execute("count_before", frame.count) == 3

        def append():
            source = (spark.read.format("jdbc").options(**options).load()
                      if name == "external_unissued_rejected" else frame)
            if name in ("external_mixed_governed", "external_named_source_denied"):
                left = source.alias("l")
                right = spark.table(request["deltaSourceName"]).alias("r")
                source = left.join(right, left["id"] == right["id"], "inner").select("l.id", "r.secret", "l.region")
            source = source.withColumn("__arkham_loaded_at", F.current_timestamp()).withColumn("second_time", F.current_timestamp())
            if request["barrier"]:
                arm_publication_barrier(bridge, request["target"], connection()["jvmThread"])
            target = request["catalog"] + "." + request["database"] + "." + request["target"]
            if request["frontend"] == "insertInto":
                source.write.mode("append").insertInto(target)
            else:
                source.write.format("delta").mode("append").saveAsTable(target)
        execute("append", append)
        if name == "external_source_advances":
            assert execute("count_after", frame.count) == 4
        save("declared-actions", list(action_names(name)))
    finally:
        sys.setprofile(previous_profile)
        threading.setprofile(previous_thread_profile)
        if tracker is not None and hasattr(tracker, "_thread"):
            tracker.stop_read_tracker()
        reader.close()

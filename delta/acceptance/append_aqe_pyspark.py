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

"""Actual classic PySpark append caller; the JVM bridge only observes and controls."""

import json
import importlib
import py4j
import pyspark
import os
from pathlib import Path
import sys
import threading
import time
import traceback

from py4j.protocol import Py4JJavaError
from pyspark import SparkContext
from pyspark.sql import SparkSession
from append_aqe_profiles import module_provenance


def python_runtime_evidence(expected_spark):
    """Inspect the pinned Python providers without starting a gateway or Spark session."""
    assert pyspark.__version__ == expected_spark and py4j.__version__ == "0.10.9.7"
    modules = [py4j, pyspark] + [
        importlib.import_module(name)
        for name in (
            "py4j.clientserver",
            "pyspark.java_gateway",
            "pyspark.sql.readwriter",
        )
    ]
    return dict(
        python=sys.version,
        executable=sys.executable,
        py4j=py4j.__version__,
        spark=pyspark.__version__,
        providers={module.__name__: module_provenance(module) for module in modules},
        pinEnvironment=os.environ.get("PYSPARK_PIN_THREAD"),
    )


def main():
    request_path = Path(sys.argv[1]).resolve()
    request = json.loads(request_path.read_text())
    output = Path(request["pythonOutput"])
    output.mkdir(mode=0o700)

    def save(name, value):
        path = output / (name + ".json")
        assert not path.exists(), "Original observation already exists"
        path.write_text(json.dumps(value, indent=2) + "\n")

    save("python-runtime", python_runtime_evidence(request["expectedSparkVersion"]))
    SparkContext._ensure_initialized()
    gateway = SparkContext._gateway
    jvm = SparkContext._jvm
    bridge = (
        jvm.org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerLiveAppendAqe(
            str(request_path)
        )
    )
    jsc = jvm.org.apache.spark.api.java.JavaSparkContext.fromSparkContext(
        bridge.session().sparkContext()
    )
    context = SparkContext(gateway=gateway, jsc=jsc)
    spark = SparkSession(context, bridge.session())
    failures = []

    def connection():
        current = gateway._gateway_client.get_thread_connection()
        return dict(
            gatewayType=type(gateway).__module__ + "." + type(gateway).__name__,
            clientType=type(gateway._gateway_client).__module__
            + "."
            + type(gateway._gateway_client).__name__,
            pythonThread=threading.get_ident(),
            connectionId=id(current),
            socket=current.socket.getsockname(),
            peer=current.socket.getpeername(),
            jvmThread=jvm.java.lang.Thread.currentThread().getId(),
            aqe=spark.conf.get("spark.sql.adaptive.enabled"),
            javaServerType=gateway.java_gateway_server.getClass().getName(),
            javaServerOrigin=gateway.java_gateway_server.getClass()
            .getProtectionDomain()
            .getCodeSource()
            .getLocation()
            .getPath(),
            transactionAppId=spark.conf.get("spark.databricks.delta.write.txnAppId"),
            transactionVersion=spark.conf.get(
                "spark.databricks.delta.write.txnVersion"
            ),
            autoReset=spark.conf.get(
                "spark.databricks.delta.write.txnVersion.autoReset.enabled"
            ),
            nullDefaults=spark.conf.get(
                "spark.sql.defaultColumn.useNullsForMissingDefaultValues"
            ),
        )

    def checkpoint(name):
        control = Path(request["controlDirectory"])
        ready, release = (
            control / (name + "-ready.json"),
            control / (name + "-verified.json"),
        )
        assert not ready.exists()
        ready.write_text(json.dumps(dict(checkpoint=name, ready=True)))
        deadline = time.monotonic() + 180
        while not release.exists() and time.monotonic() < deadline:
            time.sleep(0.025)
        assert release.exists(), "Independent native checkpoint timed out"
        value = json.loads(release.read_text())
        assert value == dict(checkpoint=name, verified=True)

    def writer():
        try:
            api = request["frontend"]
            assert api in ("insertInto", "saveAsTable")
            versions = request["transactionVersions"]
            assert versions == (
                [1, 1]
                if request["duplicate"]
                else list(range(1, request["appendCount"] + 1))
            )
            for index, version in enumerate(versions):
                attempt = (
                    "first"
                    if index == 0
                    else (
                        "duplicate"
                        if request["duplicate"]
                        else "append" + str(index + 1)
                    )
                )
                spark.conf.set("spark.databricks.delta.write.txnVersion", str(version))
                name = request["caseName"] + "-" + attempt
                if attempt == "duplicate" and request["denyDuplicate"]:
                    bridge.phase("deny-duplicate")
                bridge.begin(name)
                thread_id = bridge.bindWriter()
                before = connection()
                assert before["jvmThread"] == thread_id and before["aqe"] == "true"
                if request["warmOrder"] != "none" and attempt == "first":
                    bridge.warmTarget(request["targetLocation"], request["warmOrder"])
                source_name = (
                    request["catalog"]
                    + "."
                    + request["database"]
                    + "."
                    + request["source"]
                )
                target_name = (
                    request["catalog"]
                    + "."
                    + request["database"]
                    + "."
                    + request["target"]
                )
                error = None
                try:
                    left = spark.table(source_name).alias("l")
                    right = spark.table(source_name).alias("r")
                    source = left.join(right, left["id"] == right["id"]).select(
                        "l.id", "l.secret", "l.region"
                    )
                    if request["barrier"]:
                        assert (
                            bridge.armCurrentThread(request["target"], 120) == thread_id
                        )
                    # These are the actual named native Python DataFrameWriter operations.
                    if api == "insertInto":
                        source.write.mode("append").insertInto(target_name)
                    else:
                        source.write.mode("append").saveAsTable(target_name)
                except Py4JJavaError as failure:
                    error = failure.java_exception
                    save(
                        name + "-python-failure",
                        dict(
                            errorClass=type(failure).__name__,
                            trace=traceback.format_exc(),
                            noReplay=True,
                        ),
                    )
                after = connection()
                save(
                    name + "-caller",
                    dict(
                        before=before,
                        after=after,
                        pinnedEnvironment=os.environ.get("PYSPARK_PIN_THREAD"),
                    ),
                )
                assert before == after, (
                    "Connection/thread ambiguity invalidates the writer proof"
                )
                observation = bridge.finishAppend(name, error)
                save(name + "-observation", dict(path=observation))
                if request["warmOrder"] != "none":
                    bridge.observeWarm(name)
                checkpoint(name)
        except BaseException:
            failures.append(traceback.format_exc())

    try:
        thread = threading.Thread(target=writer, name="native-python-append-writer")
        thread.start()
        thread.join(request["deadlineSeconds"])
        assert not thread.is_alive(), "Original writer did not finish; no replay"
        if failures:
            save("unexpected-failure", dict(trace=failures, noReplay=True))
            raise RuntimeError(
                "Original Python writer failed; preserve native committed state"
            )
    finally:
        bridge.close()


if __name__ == "__main__":
    main()

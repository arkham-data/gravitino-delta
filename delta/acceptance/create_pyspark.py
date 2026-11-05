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

"""Real classic PySpark CREATE caller using the existing downloaded-policy session."""

import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import sys
import threading
import traceback

import gravitino_delta
import gravitino_delta.tables
from gravitino_delta import GravitinoDeltaTable
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from append_aqe_profiles import module_provenance
from append_aqe_pyspark import python_runtime_evidence
from create_live_checks import verify_invalid_create


def main():
    request_path = Path(sys.argv[1]).resolve()
    request = json.loads(request_path.read_text())
    output = Path(request["pythonOutput"])
    output.mkdir(mode=0o700)

    def save(name, value):
        with (output / (name + ".json")).open("x") as stream:
            json.dump(value, stream, indent=2)

    runtime = python_runtime_evidence(request["expectedSparkVersion"])
    modules = {
        m.__name__: module_provenance(m)
        for m in (gravitino_delta, gravitino_delta.tables)
    }
    archive = request["pythonZip"]
    for value in modules.values():
        assert value["container"]["path"] == archive, (
            "Loaded checkout instead of packaged API"
        )
    wheels = {
        name: importlib.metadata.version(name)
        for name in ("pyspark", "delta-spark", "py4j")
    }
    save("python-runtime", dict(runtime, modules=modules, wheelVersions=wheels))
    assert os.environ.get("PYSPARK_PIN_THREAD") == "true"
    SparkContext._ensure_initialized()
    gateway, jvm = SparkContext._gateway, SparkContext._jvm
    bridge = jvm.org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerDownloadedQuery.openPythonSession(
        str(request_path), request["driverOutput"]
    )
    primary = None
    try:
        jsc = jvm.org.apache.spark.api.java.JavaSparkContext.fromSparkContext(
            bridge.session().sparkContext()
        )
        context = SparkContext(gateway=gateway, jsc=jsc)
        spark = SparkSession(context, bridge.session())

        def connection():
            current = gateway._gateway_client.get_thread_connection()
            return dict(
                gatewayType=type(gateway).__module__ + "." + type(gateway).__name__,
                pythonThread=threading.get_ident(),
                connectionId=id(current),
                socket=current.socket.getsockname(),
                peer=current.socket.getpeername(),
                jvmThread=jvm.java.lang.Thread.currentThread().getId(),
            )

        before = connection()
        failure = None
        try:
            operation = request["operation"]
            if operation["kind"] == "read":
                spark.table(operation["name"]).collect()
            else:
                GravitinoDeltaTable.create(
                    spark,
                    operation["name"],
                    StructType.fromJson(operation["schema"]),
                    partition_by=operation["partitionColumns"],
                    properties=operation["properties"],
                    comment=operation["comment"],
                    if_not_exists=operation["ifNotExists"],
                )
        except BaseException as error:
            failure = getattr(error, "java_exception", None)
            if failure is None:
                failure = getattr(error, "_origin", None)
            save(
                "python-failure",
                dict(
                    errorClass=type(error).__name__,
                    trace=traceback.format_exc(),
                    hasJavaCause=failure is not None,
                    noReplay=True,
                ),
            )
            if failure is None:
                # A Python failure is never a successful native API return.
                failure = jvm.java.lang.RuntimeException(
                    "Python-side failure: " + str(error)
                )
        after = connection()
        bridge.complete(request["commandId"], failure)
        actual = dict(
            frontend="pyspark",
            spark=request["expectedSparkVersion"],
            delta=request["expectedDeltaVersion"],
            fixtureSha256=request["fixtureSha256"],
            pythonZipSha256=hashlib.sha256(Path(archive).read_bytes()).hexdigest(),
            modules={name: value["sha256"] for name, value in modules.items()},
            pythonExecutable=sys.executable,
            wheelVersions=wheels,
            pinned=before["gatewayType"] == "py4j.clientserver.ClientServer",
            sameThread=before == after,
        )
        save("caller", dict(before=before, after=after, provenance=actual))
        assert actual["pinned"] and actual["sameThread"], (
            "Caller thread/connection changed"
        )
        result = json.loads(
            Path(request["driverOutput"], request["commandId"] + ".json").read_text()
        )
        if request["expected"] in ("published", "noop"):
            assert failure is None, "Original Python operation failed; no replay"
        elif request["expected"] in ("denied", "read_denied"):
            assert result["authorizationDenied"], "Expected actual authorization denial"
        elif request["expected"] == "invalid":
            verify_invalid_create(result)
        else:
            assert failure is not None, "Expected declaration/conflict refusal"
    except BaseException as error:
        primary = error
        raise
    finally:
        try:
            bridge.close()
        except BaseException:
            save("cleanup-failure", dict(trace=traceback.format_exc(), noReplay=True))
            if primary is None:
                raise


if __name__ == "__main__":
    main()

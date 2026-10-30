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

"""Fixed stock-Delta native scenarios; no Gravitino or AWS client construction."""

import json
import os
from pathlib import Path
import sys
from uuid import UUID, uuid4

from spark_hub_write_native import ref, save
from spark_hub_write_native_checks import CASES, native_outcomes, require
from s3_objects import OwnedObjects
from harness import credentials


def main() -> None:
    request = json.loads(Path(sys.argv[1]).read_text())
    output = Path(request["output"])
    source = Path(request["sparkHubRoot"])
    sys.path.insert(0, str(source))
    # Normal helper imports, including all eager dependencies; never instantiate DeltaClient.
    from src.shared.schemas.v2.base_jobs import (
        ComputeProperties,
        ExecutionProperties,
        TableProperties,
        WriteStrategy,
    )
    from src.shared.services.v2.delta import DeltaClient
    from spark_hub_read_pyspark import import_inventory
    from delta.tables import DeltaTable
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import current_timestamp
    import pyspark

    access, secret = credentials(request["primary"]["credentialsFile"])
    compute = ComputeProperties().model_dump(by_alias=True)
    settings = {
        key: compute[key]
        for key in (
            "spark.databricks.delta.optimizeWrite.enabled",
            "spark.databricks.delta.autoCompact.enabled",
            "spark.databricks.delta.properties.defaults.compatibility.symlinkFormatManifest.enabled",
        )
    }
    settings.update(
        {
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.ui.enabled": "false",
            "spark.driver.host": "127.0.0.1",
            "spark.driver.bindAddress": "127.0.0.1",
            "spark.sql.shuffle.partitions": "2",
            "spark.databricks.delta.snapshotPartitions": "2",
            "spark.sql.adaptive.enabled": "false",
            "spark.databricks.delta.schema.autoMerge.enabled": "false",
            "spark.sql.sources.partitionOverwriteMode": "static",
            "spark.databricks.delta.autoCompact.minNumFiles": "3",
            "spark.databricks.delta.autoCompact.maxFileSize": "1048576",
            "spark.databricks.delta.autoCompact.minFileSize": "524288",
            "spark.databricks.delta.optimizeWrite.binSize": "1",
            "spark.databricks.delta.optimizeWrite.numShuffleBlocks": "2",
        }
    )
    builder = SparkSession.builder.master("local[2]").appName(
        "spark-hub-native-write-characterization"
    )
    for key, value in settings.items():
        builder = builder.config(key, value)
    for key, value in {
        "endpoint": request["primary"]["endpoint"],
        "endpoint.region": request["primary"]["region"],
        "path.style.access": "true",
        "connection.ssl.enabled": "false",
        "aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "access.key": access,
        "secret.key": secret,
    }.items():
        builder = builder.config("spark.hadoop.fs.s3a." + key, value)
    builder = builder.config(
        "spark.redaction.regex",
        "(?i)secret|password|token|access[.]?key|s3-access-key-id",
    )
    spark = builder.getOrCreate()
    try:
        jvm = spark._jvm
        identity = {
            "sparkUser": spark.sparkContext._jsc.sc().sparkUser(),
            "ugi": str(
                jvm.org.apache.hadoop.security.UserGroupInformation.getCurrentUser().getShortUserName()
            ),
            "SPARK_USER": os.environ.get("SPARK_USER"),
            "HADOOP_USER_NAME": os.environ.get("HADOOP_USER_NAME"),
        }
        require(
            set(identity.values()) == {request["nativeIdentity"]},
            "Native fixture identity differs",
        )
        require(
            spark.version == pyspark.__version__ == request["pins"]["spark"],
            "JVM/PySpark Spark pins differ",
        )
        require(
            str(jvm.java.lang.System.getProperty("java.specification.version")) == "17",
            "Java pin differs",
        )
        require(
            str(jvm.scala.util.Properties.versionNumberString()) == "2.12.21",
            "Scala pin differs",
        )
        require(
            spark.conf.get("spark.sql.extensions")
            == "io.delta.sql.DeltaSparkSessionExtension",
            "Native-only extension required",
        )
        origins = {}
        loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()
        for name in (
            "org.apache.spark.sql.SparkSession",
            "org.apache.spark.sql.delta.DeltaLog",
            "io.delta.storage.LogStore",
            "scala.Predef$",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        ):
            cls = loader.loadClass(name)
            path = Path(
                str(cls.getProtectionDomain().getCodeSource().getLocation().getPath())
            ).resolve()
            artifact = ref(path)
            require(
                request["runtime"]["jarHashes"].get(str(path)) == artifact["sha256"],
                "JVM origin not in verified runtime",
            )
            origins[name] = artifact
        require(
            Path(origins["org.apache.spark.sql.delta.DeltaLog"]["path"]).name
            == "delta-spark_2.12-3.3.2.jar",
            "Delta pin differs",
        )
        require(
            Path(origins["org.apache.spark.sql.SparkSession"]["path"]).name
            == "spark-sql_2.12-3.5.6.jar",
            "Spark origin differs",
        )
        require(
            Path(origins["scala.Predef$"]["path"]).name == "scala-library-2.12.21.jar",
            "Scala origin differs",
        )
        properties = TableProperties().to_delta_options()
        cases = []
        steps_counter = 0

        def frame(rows=None, schema="id LONG, region STRING", empty=False):
            return spark.createDataFrame(
                [] if empty else (rows or [(2, "new")]), schema
            ).withColumn("__arkham_loaded_at", current_timestamp())

        def options(method=1, merge=True, overwrite=False):
            fixed = UUID("9a91a5ef-842e-4a3e-92ef-c0b581a29d99")
            execution = ExecutionProperties(
                execution_method_id=method,
                cluster_id=fixed,
                pipeline_version_id=fixed,
                pipeline_step_id=fixed,
                pipeline_execution_id=fixed,
                pipeline_step_execution_id=fixed,
                pipeline_id=fixed,
                merge_schema=merge,
                overwrite_schema=overwrite,
                user_metadata="native-characterization",
            )
            raw = execution.to_df_properties()
            return raw, DeltaClient._resolve_write_options(raw, execution)

        def effective():
            return {key: spark.conf.get(key) for key in settings}

        def observe(name, prefix, location):
            directory = output / name
            directory.mkdir(mode=0o700)
            owned = OwnedObjects(request["primary"], prefix)
            objects = owned.manifest()
            logs = []
            actions = []
            for item in objects:
                filename = item["key"].removeprefix(prefix.rstrip("/") + "/")
                if (
                    filename.startswith("_delta_log/")
                    and filename.endswith(".json")
                    and filename.split("/")[-1][:-5].isdigit()
                ):
                    version = int(filename.split("/")[-1][:-5])
                    path = directory / (str(version).zfill(20) + ".json")
                    with path.open("xb") as stream:
                        stream.write(owned.request("GET", item["key"]))
                    logs.append({"version": version, "artifact": ref(path)})
                    actions.extend(
                        dict(json.loads(line), version=version)
                        for line in path.read_text().splitlines()
                        if line
                    )
            metadata = [a["metaData"] for a in actions if "metaData" in a]
            protocol = [a["protocol"] for a in actions if "protocol" in a]
            rows = []
            state = {
                "readback": True,
                "exists": bool(metadata),
                "uuid": metadata[-1]["id"] if metadata else None,
                "version": max((log["version"] for log in logs), default=-1),
                "schema": json.loads(metadata[-1]["schemaString"]) if metadata else {},
                "properties": metadata[-1]["configuration"] if metadata else {},
                "protocol": protocol[-1] if protocol else {},
                "actions": actions,
                "logs": logs,
                "manifest": [
                    item
                    for item in objects
                    if "/_symlink_format_manifest/" in item["key"]
                ],
            }
            if metadata:
                try:
                    rows = sorted(
                        (
                            r.asDict(recursive=True)
                            for r in spark.read.format("delta").load(location).collect()
                        ),
                        key=lambda r: json.dumps(r, default=str, sort_keys=True),
                    )
                    # JSON representation is the retained native row multiset, timestamps included.
                    rows = json.loads(json.dumps(rows, default=str))
                except Exception as error:
                    state["readback"] = False
                    state["readbackException"] = {
                        "class": type(error).__name__,
                        "message": str(error),
                    }
            row_path = directory / "rows.json"
            save(row_path, rows)
            state.update(rows=rows, rowEvidence=ref(row_path))
            save(directory / "state.json", state)
            return state

        def step(label, prefix, location, action, executed=("data",)):
            nonlocal steps_counter
            steps_counter += 1
            name = str(steps_counter).zfill(3) + "-" + label
            before = observe(name + "-before", prefix, location)
            settings_before = effective()
            exception = None
            try:
                action()
            except Exception as error:
                exception = {
                    "class": type(error).__name__,
                    "errorClass": (
                        error.getErrorClass()
                        if hasattr(error, "getErrorClass")
                        else None
                    ),
                    "message": str(error),
                }
            after = observe(name + "-after", prefix, location)
            return {
                "id": label,
                "before": before,
                "after": after,
                "exception": exception,
                "executedPhases": list(executed),
                "outcomes": native_outcomes(before, after, executed),
                "settingsBefore": settings_before,
                "settingsAfter": effective(),
            }

        def write(df, location, actual, mode="append", partitioned=False):
            strategy = WriteStrategy(write_target_type=mode)
            writer = (
                df.write.format("delta")
                .options(**actual)
                .mode(strategy.write_target_type)
            )
            if partitioned:
                writer = writer.partitionBy("region")
            writer.save(location)

        def seed(prefix, location, partitioned=False, count=1):
            for index in range(count):
                write(
                    frame([(index, "old-only" if index == 0 else "old")]),
                    location,
                    properties,
                    "append",
                    partitioned,
                )

        for name in CASES:
            prefix = request["prefix"].rstrip("/") + "/" + name
            location = "s3a://" + request["primary"]["bucket"] + "/" + prefix
            require(
                not OwnedObjects(request["primary"], prefix).manifest(),
                "Absent-only candidate/independent case requires fresh target",
            )
            raw, actual = options()
            extra = {
                "location": location,
                "rawOptions": raw,
                "actualOptions": actual,
                "mode": "append",
                "expectedUserMetadata": str(actual["userMetadata"]),
            }
            steps = []
            if name == "legacy-create":
                df = frame()

                def create_empty():
                    table = (
                        DeltaTable.create(spark)
                        .location(location)
                        .addColumns(df.schema)
                    )
                    for key, value in properties.items():
                        table = table.property(key, value)
                    table.execute()

                steps.append(
                    step(name + "-empty", prefix, location, create_empty, ("metadata",))
                )
                result = step(
                    name, prefix, location, lambda: write(df, location, actual)
                )
            elif name in ("stage-create", "absent-create-or-replace"):

                def staged():
                    require(
                        not OwnedObjects(request["primary"], prefix).manifest(),
                        "Absent-only candidate refuses pre-existing target",
                    )
                    table_name = "spark_catalog.default.m2_" + uuid4().hex
                    writer = (
                        frame()
                        .writeTo(table_name)
                        .using("delta")
                        .options(**actual)
                        .tableProperty("location", location)
                    )
                    for key, value in properties.items():
                        writer = writer.tableProperty(key, value)
                    if name == "stage-create":
                        writer.create()
                    else:
                        writer.createOrReplace()

                result = step(name, prefix, location, staged, ("data", "metadata"))
            elif name == "ordinary-hooks":
                # All ordinary defaults remain enabled while enough independent tiny writes
                # accumulate to exceed minNumFiles=3. No fallback OPTIMIZE is issued.
                for index in range(6):
                    steps.append(
                        step(
                            name + "-tiny-" + str(index),
                            prefix,
                            location,
                            lambda index=index: write(
                                frame([(index, "tiny")]),
                                location,
                                {**actual, **properties},
                            ),
                            ("data", "maintenance"),
                        )
                    )
                result = dict(steps[-1], before=steps[0]["before"], id=name)
                result["outcomes"] = native_outcomes(
                    result["before"], result["after"], ("data", "maintenance")
                )
                extra["thresholds"] = {
                    "minNumFiles": 3,
                    "maxFileSize": 1048576,
                    "minFileSize": 524288,
                    "tinyWrites": 6,
                    "optimizeWriteBinSizeMiB": 1,
                }
            else:
                partitioned = name == "overwrite-partitioned"
                steps.append(
                    step(
                        name + "-seed",
                        prefix,
                        location,
                        lambda: seed(
                            prefix, location, partitioned, 2 if partitioned else 1
                        ),
                        ("data", "metadata"),
                    )
                )
                if name in ("append", "append-evolve"):
                    df = (
                        frame(
                            [(2, "new", "evolved")],
                            "id LONG, region STRING, extra STRING",
                        )
                        if name.endswith("evolve")
                        else frame()
                    )
                    result = step(
                        name, prefix, location, lambda: write(df, location, actual)
                    )
                elif name in (
                    "append-explicit-false",
                    "overwrite-explicit-false",
                    "auto-merge-precedence",
                ):
                    raw, actual = options(merge=False, overwrite=False)
                    mode = "overwrite" if name.startswith("overwrite") else "append"

                    def incompatible():
                        previous = spark.conf.get(
                            "spark.databricks.delta.schema.autoMerge.enabled"
                        )
                        try:
                            if name == "auto-merge-precedence":
                                spark.conf.set(
                                    "spark.databricks.delta.schema.autoMerge.enabled",
                                    "true",
                                )
                            write(
                                frame(
                                    [(2, "new", "incompatible")],
                                    "id LONG, region STRING, extra STRING",
                                ),
                                location,
                                actual,
                                mode,
                            )
                        finally:
                            spark.conf.set(
                                "spark.databricks.delta.schema.autoMerge.enabled",
                                previous,
                            )

                    result = step(name, prefix, location, incompatible)
                    extra.update(rawOptions=raw, actualOptions=actual, mode=mode)
                    if name == "auto-merge-precedence":
                        extra["settingDuringWrite"] = {
                            "spark.databricks.delta.schema.autoMerge.enabled": "true"
                        }
                elif name.startswith("overwrite"):
                    raw, actual = options(method=7, merge=False, overwrite=False)
                    df = (
                        frame(empty=True)
                        if name == "overwrite-empty"
                        else frame(
                            [(8, "new", "replacement")],
                            "id LONG, region STRING, replacement STRING",
                        )
                    )
                    result = step(
                        name,
                        prefix,
                        location,
                        lambda: write(df, location, actual, "overwrite", partitioned),
                    )
                    extra.update(rawOptions=raw, actualOptions=actual, mode="overwrite")
                elif name == "refresh-append":
                    refreshes = []
                    for method in (7, 8):
                        raw, actual = options(
                            method=method, merge=False, overwrite=False
                        )
                        strategy = WriteStrategy(write_target_type="append")
                        refreshes.append(
                            {
                                "method": method,
                                "raw": raw,
                                "actual": actual,
                                "mode": strategy.write_target_type.value,
                            }
                        )
                        steps.append(
                            step(
                                name + "-" + str(method),
                                prefix,
                                location,
                                lambda actual=actual: write(
                                    frame(),
                                    location,
                                    actual,
                                    strategy.write_target_type,
                                ),
                            )
                        )
                    result = dict(steps[-1], before=steps[1]["before"], id=name)
                    result["outcomes"] = native_outcomes(
                        result["before"], result["after"], ("data",)
                    )
                    extra["refreshOptions"] = refreshes
                elif name.startswith("duplicate-"):
                    txn = {
                        **actual,
                        "txnAppId": "native-" + uuid4().hex,
                        "txnVersion": "9",
                    }
                    df = frame().cache()
                    df.count()
                    steps.append(
                        step(
                            name + "-first-intent",
                            prefix,
                            location,
                            lambda: write(df, location, txn),
                        )
                    )
                    changed = name == "duplicate-changed"
                    repeated_options = (
                        {**txn, "userMetadata": {"changedIntent": True}}
                        if changed
                        else txn
                    )
                    result = step(
                        name,
                        prefix,
                        location,
                        lambda: write(
                            frame([(999, "changed")]) if changed else df,
                            location,
                            repeated_options,
                        ),
                    )
                    df.unpersist()
                    extra.update(
                        intentChanged=changed,
                        actualOptions=repeated_options,
                        originalIntentOptions=txn,
                    )
                else:
                    raise AssertionError("Unimplemented fixed case " + name)
            result.update(extra, steps=steps)
            # Ordinary phase status is explicit even when it did not run.
            result["phaseStatus"] = {
                phase: (
                    "attempted-or-native-effect"
                    if phase in result["executedPhases"]
                    or result["outcomes"][phase] == "committed"
                    else "not-executed"
                )
                for phase in result["outcomes"]
            }
            path = output / (name + ".json")
            save(path, result)
            result["evidence"] = ref(path)
            cases.append(result)
        inventory = import_inventory(source)
        # Inventory is a new observation for the actual sink helpers, never the read package's old receipt.
        save(output / "imports.json", inventory)
        dependencies = []
        for relative, digest in inventory["sourceInputs"].items():
            dependencies.append({"path": str(source / relative), "sha256": digest})
        for dependency in inventory["dependencies"].values():
            dependencies.extend(
                {"path": path, "sha256": digest}
                for path, digest in dependency["files"].items()
            )
        save(
            output / "observations.json",
            {
                "cases": cases,
                "identity": identity,
                "origins": origins,
                "pythonVersion": inventory["pythonVersion"],
                "pythonProviders": inventory["pythonProviders"],
                "importInventory": ref(output / "imports.json"),
                "importArtifacts": dependencies,
                "effectiveSettings": effective(),
                "tableDefaults": properties,
            },
        )
        expected_rejections = {
            "stage-create",
            "absent-create-or-replace",
            "append-explicit-false",
            "overwrite-explicit-false",
            "auto-merge-precedence",
        }
        unexpected = [
            case["id"]
            for case in cases
            if (case["exception"] is not None and case["id"] not in expected_rejections)
            or not case["after"].get("readback")
            or any(
                step["exception"] is not None
                for step in case["steps"]
                if step["id"] not in {"refresh-append-7", "refresh-append-8"}
            )
        ]
        require(
            not unexpected, "Unexpected/incomplete native execution: " + str(unexpected)
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

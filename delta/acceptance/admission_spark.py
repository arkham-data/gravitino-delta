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

"""Focused native replacement admission regression; launched by admission_run.py."""

import json
from pathlib import Path
import sys
import time
import uuid

from pyspark.sql import SparkSession

from harness import write_json
from process_cases import RECEIPT, command, identity, submit
from run import Rest


def main():
    case = json.loads(Path(sys.argv[1]).read_text())
    output = Path(sys.argv[2])
    spark = SparkSession.builder.appName(
        "Delta replacement base admission"
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    rest = Rest(case["uri"], case["user"])
    base = "/api/metalakes/" + case["metalake"]
    catalog = case["catalogs"][0]
    table_path = base + "/catalogs/" + catalog + "/schemas/shared/tables/"
    records = []

    def qualified(name):
        return f"`{catalog}`.`shared`.`{name}`"

    def public(name):
        return rest.call("GET", table_path + name)["table"]

    def snapshot(name, location):
        history = spark.sql(f"DESCRIBE HISTORY delta.`{location}`").first()
        detail = spark.sql(f"DESCRIBE DETAIL delta.`{location}`").first()
        rows = [
            row.asDict(recursive=True)
            for row in spark.read.format("delta").load(location).orderBy("id").collect()
        ]
        return {
            "version": history.version,
            "uuid": detail.id,
            "rows": rows,
            "table": public(name),
        }

    def save():
        write_json(output, {"records": records})

    loader = spark._jvm.java.lang.Thread.currentThread().getContextClassLoader()
    origins = {}
    for name in (
        "org.apache.spark.sql.SparkSession",
        "org.apache.spark.sql.delta.DeltaLog",
        "org.apache.gravitino.spark.connector.delta.GravitinoDeltaCatalogSpark35",
        "org.apache.hadoop.util.VersionInfo",
    ):
        origins[name] = str(
            loader.loadClass(name).getProtectionDomain().getCodeSource().getLocation()
        )
    records.append(
        {
            "origins": origins,
            "spark": spark.version,
            "delta": case["delta"],
            "hadoop": str(spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion()),
            "scala": str(spark._jvm.scala.util.Properties.versionNumberString()),
        }
    )
    save()
    assert spark.version == case["spark"]
    assert ("-" + case["delta"] + ".jar") in origins[
        "org.apache.spark.sql.delta.DeltaLog"
    ]
    assert records[0]["hadoop"] == "3.3.4"
    assert records[0]["scala"] == "2.12.18"
    try:
        names = (
            ["none_schema_explicit"]
            if case["mode"] == "red"
            else [
                "none_schema_explicit",
                "none_query_default",
                "none_schema_default",
                "none_query_explicit",
                "none_direct",
                "semantic_adopted",
            ]
        )
        for name in names:
            if name == "semantic_adopted":
                location = case["adoptionLocation"]
                spark.sql(
                    f"CREATE TABLE delta.`{location}` (id BIGINT, generated BIGINT GENERATED ALWAYS AS (id + 1)) USING delta TBLPROPERTIES ('delta.columnMapping.mode'='name')"
                ).collect()
                spark.sql(f"INSERT INTO delta.`{location}` (id) VALUES (17)").collect()
                adopting = submit(
                    rest,
                    base,
                    catalog,
                    name,
                    command(
                        "ADOPT",
                        token=str(uuid.uuid4()),
                        location=location,
                        runId="retired-native",
                        reference="completed-native-seed",
                    ),
                )
                submit(
                    rest,
                    base,
                    catalog,
                    name,
                    command("PUBLISH", expected=identity(adopting)),
                )
            else:
                spark.sql(
                    f"CREATE TABLE {qualified(name)} USING delta TBLPROPERTIES ('delta.columnMapping.mode'='none') AS SELECT CAST(17 AS BIGINT) id"
                ).collect()
                location = public(name)["properties"][RECEIPT + "location"]
            before = snapshot(name, location)
            record = {"name": name, "location": location, "before": before}
            records.append(record)
            save()
            # The orchestrator archives all original objects before releasing replacement.
            gate = output.parent / (name + "-ready")
            gate.write_text(location)
            deadline = time.monotonic() + 120
            while not (output.parent / (name + "-continue")).exists():
                if time.monotonic() > deadline:
                    raise TimeoutError("Object archive gate was not released")
                time.sleep(0.1)
            default = "default" in name
            spark.conf.set(
                "spark.databricks.delta.properties.defaults.columnMapping.mode",
                "name" if default else "none",
            )
            props = (
                "" if default else " TBLPROPERTIES ('delta.columnMapping.mode'='name')"
            )
            statement = f"REPLACE TABLE {qualified(name)}" + (
                " USING delta" + props + " AS SELECT CAST(99 AS BIGINT) id"
                if "query" in name
                else " (id BIGINT) USING delta" + props
            )
            try:
                if name == "none_direct":
                    schema = {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "id",
                                "type": "long",
                                "nullable": True,
                                "metadata": {},
                            }
                        ],
                    }
                    payload = command(
                        "BEGIN_REPLACE",
                        expected=identity(before["table"]["properties"]),
                        replacementToken=str(uuid.uuid4()),
                        runId=str(uuid.uuid4()),
                        schemaContract={
                            "kind": "SPARK_35_DELTA_33_NAME_MAPPING_V1",
                            "operationMode": "SCHEMA_ONLY",
                            "requiredSchema": schema,
                            "columnMappingMode": "name",
                            "featurePolicy": "BASIC_NAME_MAPPING_V1",
                        },
                    )
                    payload["version"] = 2
                    submit(rest, base, catalog, name, payload)
                else:
                    spark.sql(statement).collect()
            except Exception as error:
                record["rejection"] = str(error)
                if hasattr(error, "result"):
                    record["response"] = error.result
                if case["mode"] != "red":
                    expected = (
                        "INVALID_SCHEMA_CONTRACT"
                        if name == "none_direct"
                        else "DELTA_REGISTRATION_STATUS_NEEDED"
                    )
                    assert expected in str(error) + json.dumps(
                        getattr(error, "result", {})
                    ), "Unexpected public admission failure diagnostic"
                if case["mode"] == "red":
                    # Keep the actual failing driver exit and original publication exception.
                    raise
            else:
                raise AssertionError("Unsupported replacement was admitted")
            finally:
                spark.conf.set(
                    "spark.databricks.delta.properties.defaults.columnMapping.mode",
                    "none",
                )
                # Raw native path remains observable even if publication was quarantined.
                record["nativeAfter"] = {
                    "version": spark.sql(f"DESCRIBE HISTORY delta.`{location}`")
                    .first()
                    .version,
                    "uuid": spark.sql(f"DESCRIBE DETAIL delta.`{location}`").first().id,
                    "rows": [
                        r.asDict()
                        for r in spark.read.format("delta").load(location).collect()
                    ],
                }
                save()
            after = snapshot(name, location)
            record["after"] = after
            assert (
                before == after
            ), "Rejected replacement changed native/public registration state"
            assert spark.sql(f"SELECT id FROM {qualified(name)}").first().id == 17
            record["unchanged"] = True
            save()
        if case["mode"] != "red":
            name = "mapped_positive"
            spark.sql(
                f"CREATE TABLE {qualified(name)} USING delta TBLPROPERTIES ('delta.columnMapping.mode'='name') AS SELECT CAST(17 AS BIGINT) id"
            ).collect()
            prior = public(name)["properties"]
            spark.sql(
                f"REPLACE TABLE {qualified(name)} USING delta TBLPROPERTIES ('delta.columnMapping.mode'='name') AS SELECT CAST(99 AS BIGINT) id"
            ).collect()
            after = public(name)["properties"]
            assert after[RECEIPT + "id"] == prior[RECEIPT + "id"]
            assert (
                int(after[RECEIPT + "generation"])
                == int(prior[RECEIPT + "generation"]) + 1
            )
            assert spark.sql(f"SELECT id FROM {qualified(name)}").first().id == 99
            records.append(
                {"name": name, "before": prior, "after": after, "supported": True}
            )
            save()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

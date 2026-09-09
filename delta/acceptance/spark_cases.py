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

"""SQL/client assertions executed only as a real spark-submit application."""

import base64
import hashlib
import json
from pathlib import Path
import sys
import urllib.request
import datetime

from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession


def main():
    case = json.loads(Path(sys.argv[1]).read_text())
    output = Path(sys.argv[2])
    spark = SparkSession.builder.appName(
        "Gravitino packaged Delta acceptance"
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    jvm = spark._jvm
    loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()
    checks = []

    def check(label, condition, detail=None):
        if not condition:
            raise AssertionError(label)
        checks.append(
            {
                "check": label,
                "kind": "sql-event" if label == "sql" else "assertion",
                "detail": detail,
            }
        )
        output.write_text(json.dumps({"checks": checks}, indent=2, default=str) + "\n")

    def origin(name):
        return str(
            loader.loadClass(name).getProtectionDomain().getCodeSource().getLocation()
        )

    def qualified(catalog, name):
        return ".".join(
            "`" + part.replace("`", "``") + "`" for part in (catalog, "shared", name)
        )

    def sql(statement):
        result = spark.sql(statement).collect()
        check("sql", True, statement)
        return result

    def table(catalog, name):
        namespace = spark.sparkContext._gateway.new_array(jvm.java.lang.String, 1)
        namespace[0] = "shared"
        identifier = jvm.org.apache.spark.sql.connector.catalog.Identifier.of(
            namespace, name
        )
        return (
            spark._jsparkSession.sessionState()
            .catalogManager()
            .catalog(catalog)
            .loadTable(identifier)
        )

    def request(path):
        auth = base64.b64encode((case["user"] + ":").encode()).decode()
        req = urllib.request.Request(
            case["uri"] + "/api/metalakes/" + case["metalake"] + path,
            headers={"Authorization": "Basic " + auth},
        )
        with urllib.request.urlopen(req) as response:
            return json.load(response)

    def metadata(catalog, name):
        auth = base64.b64encode((case["user"] + ":").encode()).decode()
        uri = (
            case["uri"]
            + "/api/metalakes/"
            + case["metalake"]
            + "/catalogs/"
            + catalog
            + "/schemas/shared/tables/"
            + name
        )
        request = urllib.request.Request(
            uri, headers={"Authorization": "Basic " + auth}
        )
        with urllib.request.urlopen(request) as response:
            return json.load(response)["table"]

    try:
        versions = {
            "spark": spark.version,
            "hadoop": str(jvm.org.apache.hadoop.util.VersionInfo.getVersion()),
            "scala": str(jvm.scala.util.Properties.versionNumberString()),
            "java": str(jvm.java.lang.System.getProperty("java.version")),
            "sparkOrigin": origin("org.apache.spark.sql.SparkSession"),
            "deltaOrigin": origin("org.apache.spark.sql.delta.DeltaLog"),
            "adapterOrigin": origin(
                "org.apache.gravitino.spark.connector.delta.GravitinoDeltaCatalogSpark35"
            ),
            "hadoopOrigin": origin("org.apache.hadoop.util.VersionInfo"),
            "s3Origin": origin("org.apache.hadoop.fs.s3a.S3AFileSystem"),
        }
        check(
            "runtime versions",
            versions["spark"] == case["spark"]
            and versions["hadoop"] == "3.3.4"
            and versions["scala"] == "2.12.18",
            versions,
        )
        check(
            "selected native Delta artifact",
            ("-" + case["delta"] + ".jar") in versions["deltaOrigin"],
        )
        try:
            loader.loadClass("io.delta.kernel.Table")
        except Py4JJavaError as error:
            check(
                "no driver Kernel dependency",
                error.java_exception.getClass().getName()
                == "java.lang.ClassNotFoundException",
            )
        else:
            raise AssertionError("Kernel unexpectedly available to driver")
        if case["mode"] == "disabled":
            for catalog in case["catalogs"]:
                check(
                    "disabled flag omits Delta catalog discovery",
                    spark.conf.get("spark.sql.catalog." + catalog, None) is None,
                )
            check(
                "disabled flag omits native Delta extension",
                "io.delta.sql.DeltaSparkSessionExtension"
                not in spark.conf.get("spark.sql.extensions", ""),
            )
            return
        for catalog in case["catalogs"]:
            check(
                "flag discovery",
                spark.conf.get("spark.sql.catalog." + catalog)
                == "org.apache.gravitino.spark.connector.delta.GravitinoDeltaCatalogSpark35",
                catalog,
            )
        check(
            "installed SQL extensions",
            True,
            {
                "extensions": spark.conf.get("spark.sql.extensions"),
                "parser": str(
                    spark._jsparkSession.sessionState().sqlParser().getClass().getName()
                ),
            },
        )
        if case["mode"] == "diagnostics":
            for statement in (
                "DESCRIBE HISTORY delta.`/nonexistent`",
                "DESCRIBE HISTORY x.y.z",
                "DESCRIBE HISTORY a_3_5_4_3_3_0.shared.history",
                "DESCRIBE HISTORY `a_3_5_4_3_3_0`.`shared`.`history`",
            ):
                plan = str(
                    spark._jsparkSession.sessionState().sqlParser().parsePlan(statement)
                )
                check("parser plan", True, {"sql": statement, "plan": plan})
            return
        a = case["catalogs"][0]
        if case["mode"] == "reject-identity":
            try:
                table(a, case["table"])
            except Exception as error:
                check(
                    "changed native identity cannot return named table",
                    "TABLE_OR_VIEW_NOT_FOUND" in str(error),
                )
            else:
                raise AssertionError("Changed native identity returned a named table")
            return
        if case["mode"] == "single-create":
            sql(
                f"CREATE TABLE {qualified(a, case['table'])} USING delta AS SELECT CAST(17 AS BIGINT) id"
            )
            return
        if case["mode"] == "fault-create":
            # Do not catch this exception: the original nonzero driver exit is evidence.
            sql(
                f"CREATE TABLE {qualified(a, case['table'])} USING delta AS SELECT CAST(17 AS BIGINT) id"
            )
            raise AssertionError("Armed transport fault did not fail the driver")
        if case["mode"] == "recovery":
            from pyspark.java_gateway import ensure_callback_server_started

            ensure_callback_server_started(spark.sparkContext._gateway)
            catalog = jvm.org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager.get().getGravitinoCatalogInfo(
                a
            )

            class Supplier:
                class Java:
                    implements = ["java.util.function.Supplier"]

                def get(self):
                    return catalog

            supplier = Supplier()
            adapter = jvm.org.apache.gravitino.spark.connector.delta
            registration = jvm.org.apache.gravitino.catalog.lakehouse.delta.registration
            client = adapter.DeltaRegistrationClient(supplier, supplier)
            empty_strings = spark.sparkContext._gateway.new_array(
                jvm.java.lang.String, 0
            )
            journal_path = jvm.java.nio.file.Paths.get(
                str(Path(case["journals"]) / a), empty_strings
            )
            journal = adapter.DeltaRegistrationJournal.durable(journal_path)
            recovery = adapter.DeltaRegistrationRecovery(client, journal)
            entry_path = jvm.java.nio.file.Paths.get(case["entry"], empty_strings)
            entry = adapter.DeltaRegistrationJournal.read(entry_path)
            observed = recovery.status(entry)
            check(
                "STATUS observed after drained driver and server restart",
                True,
                dict(observed.toProperties()),
            )
            # STATUS writes a new immutable snapshot. Read it back; never use stale entry evidence.
            reread = adapter.DeltaRegistrationJournal.read(entry_path)
            check("journal reread binds learned receipt", reread.receipt().isPresent())
            command = (
                registration.DeltaRegistrationCommand.abort(observed.identity())
                if case["action"] == "ABORT"
                else registration.DeltaRegistrationCommand.publish(observed.identity())
            )
            try:
                recovery.execute(reread, command, False)
            except Exception as error:
                check(
                    "STATUS alone cannot authorize request replay",
                    "request-drain" in str(error),
                )
            else:
                raise AssertionError("Missing drain accepted")
            if case["action"] != "STATUS":
                if case.get("handoff"):
                    handoff = registration.DeltaRegistrationCommand.handoff(
                        observed.identity(), observed.runId(), case["drainReference"]
                    )
                    handed = recovery.execute(reread, handoff, True)
                    check(
                        "explicit authenticated handoff",
                        True,
                        dict(handed.toProperties()),
                    )
                completed = recovery.execute(reread, command, True)
                check(
                    "explicit recovery " + case["action"],
                    str(completed.state()) == case["expectedState"],
                    dict(completed.toProperties()),
                )
            if case["expectedState"] == "PUBLISHED":
                check(
                    "restarted named table reads committed data",
                    sql(f"SELECT id FROM {qualified(a, case['table'])}")[0][0] == 17,
                )
            return
        if case["mode"] == "native-adoption":
            spark.createDataFrame([(case["value"],)], "id long").write.format(
                "delta"
            ).save(case["location"])
            check(
                "native writer completed and will drain before adoption",
                True,
                case["location"],
            )
            return
        if case["mode"] == "read":
            check(
                "fresh driver reads profile",
                sql(f"SELECT id FROM {qualified(a, case['table'])}")[0][0]
                == case["value"],
            )
            return
        a, b = case["catalogs"]
        for catalog, value in ((a, 1), (b, 101)):
            name = qualified(catalog, "same")
            if not case.get("seeded"):
                sql(
                    f"CREATE TABLE {name} USING delta AS SELECT CAST({value} AS BIGINT) id"
                )
            check(
                "native DeltaTableV2",
                table(catalog, "same").getClass().getName()
                == "org.apache.spark.sql.delta.catalog.DeltaTableV2",
            )
            check("separate catalog data", sql(f"SELECT id FROM {name}")[0][0] == value)
            info = metadata(catalog, "same")
            check(
                "published receipt",
                info["properties"]["delta.registration.receipt.state"] == "PUBLISHED",
                info,
            )
        check(
            "separate physical storage",
            metadata(a, "same")["properties"]["delta.registration.receipt.location"]
            != metadata(b, "same")["properties"]["delta.registration.receipt.location"],
        )
        if case["mode"] == "matrix-seed":
            return
        name = qualified(a, "same")
        owner_name = a + ".shared.same"
        before = metadata(a, "same")
        owner = request("/owners/table/" + owner_name)["owner"]
        check("creator owner", owner["name"] == case["user"], owner)
        location = before["properties"]["delta.registration.receipt.location"]
        sql(f"INSERT INTO {name} VALUES (2), (3)")
        check("S3 append", sql(f"SELECT count(*) FROM {name}")[0][0] == 3)
        sql(f"UPDATE {name} SET id = 20 WHERE id = 2")
        sql(f"DELETE FROM {name} WHERE id = 3")
        sql(
            f"MERGE INTO {name} t USING (SELECT CAST(20 AS BIGINT) id UNION ALL SELECT CAST(30 AS BIGINT)) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET id = s.id + 1 WHEN NOT MATCHED THEN INSERT *"
        )
        check(
            "native DML rows",
            sorted(row[0] for row in sql(f"SELECT id FROM {name}")) == [1, 21, 30],
        )
        check(
            "time travel initial CTAS",
            sql(f"SELECT id FROM {name} VERSION AS OF 0")[0][0] == 1,
        )
        sql(f"INSERT OVERWRITE {name} SELECT CAST(7 AS BIGINT) id")
        check("native overwrite", sql(f"SELECT id FROM {name}")[0][0] == 7)
        # Native schema evolution through the ordinary path writer; named load refreshes metadata.
        spark.createDataFrame([(8, "new")], "id long, extra string").write.format(
            "delta"
        ).mode("append").option("mergeSchema", "true").save(location)
        refreshed = metadata(a, "same")
        check(
            "metadata refresh after native evolution",
            [c["name"] for c in refreshed["columns"]] == ["id", "extra"],
            refreshed,
        )
        check(
            "registration identity stable",
            before["properties"]["delta.registration.receipt.id"]
            == refreshed["properties"]["delta.registration.receipt.id"],
        )
        check(
            "existing column projection retained",
            before["columns"][0] == refreshed["columns"][0],
        )
        # Public ColumnDTO omits internal IDs; the outer runner inspects persisted version history read-only after shutdown.
        check(
            "owner stable after refresh",
            request("/owners/table/" + owner_name)["owner"] == owner,
        )
        check(
            "named load refreshed native schema",
            table(a, "same").schema().fieldNames()[1] == "extra",
        )
        lifecycle = qualified(a, "lifecycle")
        sql(f"CREATE TABLE {lifecycle} (id BIGINT) USING delta")
        sql(f"REPLACE TABLE {lifecycle} (id BIGINT, text STRING) USING delta")
        sql(f"CREATE OR REPLACE TABLE {lifecycle} (id BIGINT) USING delta")
        sql(f"REPLACE TABLE {lifecycle} USING delta AS SELECT CAST(4 AS BIGINT) id")
        sql(
            f"CREATE OR REPLACE TABLE {lifecycle} USING delta AS SELECT CAST(5 AS BIGINT) id"
        )
        check("replacement rows", sql(f"SELECT id FROM {lifecycle}")[0][0] == 5)
        frame = spark.createDataFrame([(10,)], "id long")
        writer_name = qualified(a, "writer_v2")
        frame.writeTo(writer_name).using("delta").create()
        frame.writeTo(writer_name).using("delta").replace()
        frame.writeTo(writer_name).using("delta").createOrReplace()
        frame.writeTo(writer_name).append()
        check(
            "DataFrameWriterV2 create replace createOrReplace append",
            sql(f"SELECT count(*) FROM {writer_name}")[0][0] == 2,
        )
        for mapping in ("none", "name"):
            nested = qualified(a, "nested_" + mapping)
            sql(
                f"CREATE TABLE {nested} (id BIGINT NOT NULL, nested STRUCT<label: STRING, items: ARRAY<STRUCT<value: INT>>>, lookup MAP<STRING, STRUCT<value: INT>>) USING delta TBLPROPERTIES ('delta.columnMapping.mode'='{mapping}')"
            )
            sql(
                f"INSERT INTO {nested} VALUES (1, named_struct('label','ok','items',array(named_struct('value',2))), map('x',named_struct('value',3)))"
            )
            check(
                "nested mapped data " + mapping,
                sql(f'SELECT nested.items[0].value, lookup["x"].value FROM {nested}')[0]
                == (2, 3),
            )
            try:
                spark.sql(f"INSERT INTO {nested} (id) VALUES (NULL)").collect()
            except Exception as error:
                check(
                    "NOT NULL enforcement " + mapping,
                    "NOT NULL" in str(error) or "NOT_NULL" in str(error),
                    type(error).__name__,
                )
            else:
                raise AssertionError("NOT NULL constraint accepted NULL")
        # Retain actual native history beyond the reader's bounded JSON fallback.
        history_name = qualified(a, "history")
        sql(
            f"CREATE TABLE {history_name} USING delta TBLPROPERTIES ('delta.checkpointInterval'='10') AS SELECT CAST(0 AS BIGINT) id"
        )
        initial_history = metadata(a, "history")
        history_location = initial_history["properties"][
            "delta.registration.receipt.location"
        ]
        for value in range(1, 42):
            spark.range(value, value + 1).coalesce(1).write.format("delta").mode(
                "append"
            ).save(history_location)
        # Provider jumps directly from version 0 to 41, beyond the JSON fallback bound.
        history = metadata(a, "history")
        checkpoint_path = jvm.org.apache.hadoop.fs.Path(
            history_location + "/_delta_log/_last_checkpoint"
        )
        fs = checkpoint_path.getFileSystem(spark._jsc.hadoopConfiguration())
        stream = fs.open(checkpoint_path)
        try:
            checkpoint = json.loads(
                str(jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8"))
            )
        finally:
            stream.close()
        check(
            "checkpoint beyond 32 JSON commits", checkpoint["version"] >= 40, checkpoint
        )
        check(
            "provider reads checkpoint and later commits",
            int(history["properties"]["delta.registration.receipt.nativeVersion"])
            == 41,
        )
        check("history rows", sql(f"SELECT count(*) FROM {history_name}")[0][0] == 42)
        check(
            "history evidence",
            True,
            [
                row.asDict(recursive=True)
                for row in sql(f"DESCRIBE HISTORY {history_name}")
            ],
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

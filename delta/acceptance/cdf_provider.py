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

"""Compose the CDF Spark command with one fresh isolated provider and stopped-store checks."""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import subprocess
import struct
import uuid

from run import Server, Rest, catalog_properties, clean_environment
from harness import credentials, write_json

ROOT = Path(__file__).resolve().parents[2]


def configuration_digest(configuration):
    digest = hashlib.sha256(b"gravitino.delta.configuration.v1")
    for key, value in sorted(configuration.items()):
        for text in (key, value):
            encoded = text.encode("utf-8")
            digest.update(struct.pack(">i", len(encoded)))
            digest.update(encoded)
    return "sha256-v1:" + digest.hexdigest()


def store_rows(server, output, name, query):
    assert server.process is None
    destination = output / (name + ".csv")
    quote = lambda value: "'" + str(value).replace("'", "''") + "'"
    sql = "CALL CSVWRITE(" + quote(destination) + "," + quote(query) + ")"
    command = [server.inputs["javaHome"] + "/bin/java", "-cp", str(next((server.home / "libs").glob("h2-*.jar"))),
               "org.h2.tools.Shell", "-url", "jdbc:h2:file:" + str(server.home / "data/store") + ";MODE=MYSQL;ACCESS_MODE_DATA=r",
               "-user", "gravitino", "-password", "gravitino", "-sql", sql]
    result = subprocess.run(command, capture_output=True, text=True, timeout=30)
    (output / (name + ".log")).write_text(result.stdout + result.stderr)
    assert result.returncode == 0 and "Error:" not in result.stdout
    with destination.open() as stream:
        return list(csv.DictReader(stream))


def verify_store(server, output, records_output=None, table_name="t", column_label="cdf", record_names=("baseline", "enabled", "disabled", "recovered")):
    assert table_name in ("t", "cdf_target")
    output.mkdir(exist_ok=True)
    records_output = records_output or output
    rows = store_rows(server, output, "table-history", "SELECT t.table_id,t.schema_id,t.catalog_id,t.current_version,t.audit_info,v.version,v.properties FROM table_meta t JOIN table_version_info v ON t.table_id=v.table_id WHERE t.table_name='" + table_name + "' AND t.deleted_at=0 ORDER BY v.version")
    states = [json.loads(json.loads(row["PROPERTIES"])["delta.registration.internal.state"]) for row in rows]
    published = [state for state in states if state.get("state") == "PUBLISHED"]
    assert published
    records = [json.loads((records_output / (name + ".json")).read_text()) for name in record_names]
    for record in records:
        matching = [state for state in published if state["observation"]["version"] == record["nativeVersion"]]
        assert matching, "Missing persisted native observation"
        observation = matching[-1]["observation"]
        assert observation["uuid"] == record["nativeId"]
        assert json.loads(observation["schema"]) == json.loads(record["nativeSchema"])
        safe_keys = {"delta.columnMapping.mode", "delta.columnMapping.maxColumnId", "delta.appendOnly", "delta.enableChangeDataFeed", "delta.enableDeletionVectors"}
        assert observation["safe"] == {key: value for key, value in record["nativeConfiguration"].items() if key in safe_keys}
        assert observation["digest"] == configuration_digest(record["nativeConfiguration"])
        assert observation["location"] == record["receipt"]["delta.registration.receipt.location"]
        for key in ("id", "generation", "token"):
            assert str(matching[-1][key]) == record["receipt"]["delta.registration.receipt." + key]
        protocol = record["nativeProtocol"]["protocol"]
        assert observation["protocol"] == {"reader": protocol["minReaderVersion"], "writer": protocol["minWriterVersion"],
            "readerFeatures": sorted(protocol.get("readerFeatures", [])), "writerFeatures": sorted(protocol.get("writerFeatures", []))}
    assert len({(row["TABLE_ID"], row["SCHEMA_ID"], row["CATALOG_ID"], row["AUDIT_INFO"]) for row in rows}) == 1
    assert len({(state["id"], state["generation"], state["token"], state["location"]) for state in states}) == 1
    owners = store_rows(server, output, "owner", "SELECT o.owner_id,o.owner_type,o.current_version,u.user_name FROM owner_meta o JOIN table_meta t ON t.table_id=o.metadata_object_id JOIN user_meta u ON u.user_id=o.owner_id WHERE t.table_name='" + table_name + "' AND o.metadata_object_type='TABLE' AND o.deleted_at=0 AND t.deleted_at=0")
    assert len(owners) == 1 and owners[0]["USER_NAME"] == records[0]["owner"]
    assert owners[0]["OWNER_TYPE"] == records[0]["ownerType"]
    columns = server.column_snapshot(column_label)
    columns = {key: value for key, value in columns.items() if "." + table_name + "." in key}
    assert len(columns) == 3 and len({value["id"] for value in columns.values()}) == 3
    history = store_rows(server, output, "column-history", "SELECT c.column_id,c.column_name,c.column_type,c.column_nullable,c.column_position,c.table_version,c.column_op_type FROM table_column_version_info c JOIN table_meta t ON t.table_id=c.table_id WHERE t.table_name='" + table_name + "' AND t.deleted_at=0 AND c.deleted_at=0 ORDER BY c.table_version,c.column_position")
    schema = {(row["COLUMN_ID"], row["COLUMN_NAME"], row["COLUMN_TYPE"], row["COLUMN_NULLABLE"], row["COLUMN_POSITION"]) for row in history}
    assert len(schema) == 3 and all(row["COLUMN_OP_TYPE"] != "3" for row in history)
    write_json(output / "persisted-verified.json", {"nativeVersions": [r["nativeVersion"] for r in records], "tableId": rows[0]["TABLE_ID"], "columns": columns, "owner": owners[0]})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--inputs", required=True)
    parser.add_argument("--package", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    os.umask(0o077)
    inputs = json.loads(Path(args.inputs).read_text())
    package = json.loads(Path(args.package).read_text())
    output = Path(args.out).resolve()
    output.mkdir(mode=0o700)
    evidence = output / "evidence"
    evidence.mkdir()
    prefix = "cdf-composition-" + uuid.uuid4().hex
    server = Server(Path(package["assemblyManifest"]).parent / "server", output, inputs)
    try:
        server.start()
        rest = Rest(server.uri, inputs["serviceAdmin"])
        base = "/api/metalakes/lake"
        rest.call("POST", "/api/metalakes", {"name": "lake", "properties": {}})
        rest.call("POST", base + "/catalogs", {"name": "a", "type": "RELATIONAL", "provider": "lakehouse-delta", "properties": catalog_properties(inputs, "primary", prefix)})
        rest.call("POST", base + "/catalogs/a/schemas", {"name": "db", "properties": {}})
        objects = [{"type": "CATALOG", "fullName": "a", "privileges": [{"name": "USE_CATALOG", "condition": "ALLOW"}]},
                   {"type": "SCHEMA", "fullName": "a.db", "privileges": [{"name": "USE_SCHEMA", "condition": "ALLOW"}]}]
        for role, principal in (("app", inputs["subject"]), ("metadata", inputs["metadataPrincipal"])):
            rest.call("POST", base + "/users", {"name": principal})
            rest.call("POST", base + "/roles", {"name": role, "properties": {}, "securableObjects": objects})
            rest.call("PUT", base + "/permissions/users/" + principal + "/grant/", {"roleNames": [role]})
        storage = inputs["primary"]
        access, secret = credentials(storage["credentialsFile"])
        options = {"spark.hadoop.fs.s3a.endpoint": storage["endpoint"], "spark.hadoop.fs.s3a.endpoint.region": storage["region"],
            "spark.hadoop.fs.s3a.path.style.access": "true", "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.access.key": access, "spark.hadoop.fs.s3a.secret.key": secret,
            "spark.redaction.regex": "(?i)secret|password|token|access[.]?key|s3-access-key-id"}
        fixture = {"uri": server.uri, "output": str(evidence), "location": "s3a://" + storage["bucket"] + "/" + prefix + "/adoption/t",
            "serviceAdmin": inputs["serviceAdmin"], "subject": inputs["subject"], "metadataPrincipal": inputs["metadataPrincipal"], "sparkOptions": options}
        fixture_path = output / "fixture.private.json"
        write_json(fixture_path, fixture)
        command = ["./gradlew", "-I", "delta/acceptance/cdf_provider.gradle", ":spark-connector:spark-delta-ranger-3.5:test",
            "--tests", "*TestDeltaRangerCdfProviderComposition", "-PcdfProviderFixture=" + str(fixture_path),
            "-PdeltaSparkVersion=" + package["spark"], "-PdeltaVersion=" + package["delta"], "-PskipITs", "-PskipDockerTests=false",
            "--offline", "--max-workers=2", "--no-daemon", "-Dorg.gradle.jvmargs=-Xmx1536m", "--console=plain"]
        write_json(output / "test-command.json", command)
        environment = clean_environment(inputs)
        environment.update(SPARK_USER=inputs["subject"], HADOOP_USER_NAME=inputs["subject"])
        with (output / "test.log").open("wb") as log:
            result = subprocess.run(command, cwd=ROOT, env=environment, stdout=log, stderr=subprocess.STDOUT)
        write_json(output / "test-exit.json", {"exitCode": result.returncode})
        assert result.returncode == 0, "Original test failed; evidence retained; do not replay mutation"
        server.stop()
        verify_store(server, evidence)
        server.start()
        reloaded = rest.call("GET", base + "/catalogs/a/schemas/db/tables/t")
        recovered = json.loads((evidence / "recovered.json").read_text())
        assert all(reloaded["table"]["properties"].get(k) == v for k, v in recovered["receipt"].items())
        server.stop()
        verify_store(server, evidence / "restarted-store", evidence)
        write_json(evidence / "restarted-readonly.json", {"sameReceipt": True, "nativeVersion": recovered["nativeVersion"]})
    finally:
        server.stop()


if __name__ == "__main__":
    main()

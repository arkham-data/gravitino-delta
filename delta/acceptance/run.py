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

"""Run fresh packaged Gravitino and Spark JVMs against existing owned S3 endpoints."""

import argparse
import base64
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

from process_cases import (
    adoption_and_isolation,
    authority_and_quarantine,
    recovery_cases,
    verify_pair_objects,
)
from harness import RecordingProxy, credentials, owned_directory, sha256, write_json

ROOT = Path(__file__).resolve().parents[2]

COLUMN_SNAPSHOT_QUERY = (
    "SELECT a.catalog_name,t.table_name,c.column_name,c.column_id,c.table_version "
    "FROM table_column_version_info c JOIN table_meta t ON t.table_id=c.table_id "
    "JOIN catalog_meta a ON a.catalog_id=c.catalog_id "
    "WHERE c.deleted_at=0 AND t.deleted_at=0 AND a.deleted_at=0 "
    "AND c.column_op_type<>3 AND c.table_version=("
    "SELECT MAX(v.table_version) FROM table_column_version_info v "
    "WHERE v.table_id=c.table_id AND v.column_id=c.column_id "
    "AND v.deleted_at=0 AND v.table_version<=t.current_version) "
    "ORDER BY a.catalog_name,t.table_name,c.column_name"
)


class Rest:
    def __init__(self, uri, user):
        self.uri = uri
        self.user = user

    def call(self, method, path, body=None, expected=(200,)):
        data = None if body is None else json.dumps(body).encode()
        auth = base64.b64encode((self.user + ":").encode()).decode()
        request = urllib.request.Request(
            self.uri + path,
            data=data,
            method=method,
            headers={
                "Authorization": "Basic " + auth,
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=180) as response:
                status, raw = response.status, response.read()
        except urllib.error.HTTPError as error:
            status, raw = error.code, error.read()
        result = json.loads(raw) if raw else {}
        if status not in expected:
            # Response messages may quote storage configuration; keep them private.
            raise RestError(status, result)
        return result


class RestError(RuntimeError):
    def __init__(self, status, result):
        super().__init__("REST status " + str(status))
        self.status = status
        self.result = result


def clean_environment(inputs):
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("GRAVITINO_", "SPARK_", "PYSPARK_", "HADOOP_", "AWS_"))
        and key
        not in (
            "CLASSPATH",
            "JAVA_TOOL_OPTIONS",
            "_JAVA_OPTIONS",
            "JDK_JAVA_OPTIONS",
            "JAVA_OPTS",
            "JAVA_RUNNER",
            "PYTHONPATH",
        )
    }
    env.update(
        JAVA_HOME=inputs["javaHome"],
        SPARK_LOCAL_IP="127.0.0.1",
        SPARK_USER=inputs["serviceAdmin"],
        HADOOP_USER_NAME=inputs["serviceAdmin"],
    )
    return env


class Server:
    def __init__(self, package, output, inputs):
        self.home = output / "server"
        shutil.copytree(package, self.home, ignore=shutil.ignore_patterns("spark-jars"))
        self.inputs = inputs
        self.process = None
        self.starts = 0
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        self.uri = f"http://127.0.0.1:{self.port}"
        for child in ("data", "logs"):
            (self.home / child).mkdir()
        config = {
            "gravitino.server.webserver.host": "127.0.0.1",
            "gravitino.server.webserver.httpPort": str(self.port),
            "gravitino.entity.store": "relational",
            "gravitino.entity.store.relational": "JDBCBackend",
            "gravitino.entity.store.relational.jdbcUrl": f"jdbc:h2:file:{self.home}/data/store;DB_CLOSE_DELAY=0;MODE=MYSQL;AUTO_SERVER=FALSE",
            "gravitino.entity.store.relational.storagePath": str(self.home / "data"),
            "gravitino.entity.store.relational.jdbcDriver": "org.h2.Driver",
            "gravitino.entity.store.relational.jdbcUser": "gravitino",
            "gravitino.entity.store.relational.jdbcPassword": "gravitino",
            "gravitino.catalog.classloader.isolated": "true",
            "gravitino.catalog.classloader.sharing.enabled": "true",
            "gravitino.authorization.enable": "true",
            "gravitino.authenticators": "simple",
            "gravitino.authorization.serviceAdmins": inputs["serviceAdmin"],
            "gravitino.auxService.names": "",
            "gravitino.cache.enabled": "false",
        }
        with (self.home / "conf/log4j2.properties").open("a") as logger:
            logger.write(
                "\nlogger.catalog_lifecycle.name = org.apache.gravitino.catalog.CatalogManager\nlogger.catalog_lifecycle.level = debug\nlogger.pool_lifecycle.name = org.apache.gravitino.utils.ClassLoaderPool\nlogger.pool_lifecycle.level = debug\n"
            )
        (self.home / "conf/gravitino.conf").write_text(
            "".join(f"{k} = {v}\n" for k, v in config.items())
        )

    def start(self):
        self.starts += 1
        env = clean_environment(self.inputs)
        env.update(
            GRAVITINO_HOME=str(self.home),
            GRAVITINO_CONF_DIR=str(self.home / "conf"),
            GRAVITINO_LOG_DIR=str(self.home / "logs"),
            GRAVITINO_MEM="-Xms256m -Xmx1024m",
            GRAVITINO_DEBUG_OPTS=f"-Xlog:class+load=info:file={self.home}/class-load-{self.starts}.log:time,level,tags",
        )
        self.log = (self.home / f"launch-{self.starts}.log").open("wb")
        command = [str(self.home / "bin/gravitino.sh"), "run"]
        write_json(self.home / f"command-{self.starts}.json", command)
        self.process = subprocess.Popen(
            command, env=env, cwd=self.home, stdout=self.log, stderr=subprocess.STDOUT
        )
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(
                    "Packaged server exited during startup; inspect private launch log"
                )
            try:
                Rest(self.uri, self.inputs["serviceAdmin"]).call(
                    "GET", "/api/health/ready"
                )
                return
            except (OSError, RestError):
                time.sleep(0.25)
        raise RuntimeError("Packaged server readiness timed out")

    def stop(self):
        if self.process is not None:
            if self.process.poll() is None:
                self.process.terminate()
                try:
                    self.process.wait(timeout=40)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=10)
            write_json(
                self.home / f"exit-{self.starts}.json",
                {"pid": self.process.pid, "exit": self.process.returncode},
            )
            self.log.close()
            self.process = None

    def column_snapshot(self, label):
        if self.process is not None:
            raise RuntimeError("Column inspection requires stopped owned server")
        jar = next((self.home / "libs").glob("h2-*.jar"))
        command = [
            self.inputs["javaHome"] + "/bin/java",
            "-cp",
            str(jar),
            "org.h2.tools.Shell",
            "-url",
            "jdbc:h2:file:"
            + str(self.home / "data/store")
            + ";MODE=MYSQL;ACCESS_MODE_DATA=r",
            "-user",
            "gravitino",
            "-password",
            "gravitino",
            "-sql",
            COLUMN_SNAPSHOT_QUERY,
        ]
        result = subprocess.run(command, capture_output=True, text=True, timeout=30)
        (self.home / ("columns-" + label + ".log")).write_text(
            result.stdout + result.stderr
        )
        write_json(
            self.home / ("columns-" + label + "-exit.json"), {"exit": result.returncode}
        )
        if result.returncode or "Error:" in result.stdout:
            raise RuntimeError("Read-only H2 column inspection failed")
        rows = {}
        for line in result.stdout.splitlines():
            parts = [part.strip() for part in line.split("|")]
            if len(parts) == 5 and parts[3].isdigit():
                key = ".".join(parts[:3])
                if key in rows:
                    raise AssertionError("Ambiguous persisted column identity rows")
                rows[key] = {
                    "id": int(parts[3]),
                    "version": int(parts[4]),
                }
        if not rows:
            raise AssertionError("No persisted column IDs observed")
        write_json(self.home / ("columns-" + label + ".json"), rows)
        return rows


def catalog_properties(inputs, profile, prefix):
    storage = inputs[profile]
    access, secret = credentials(storage["credentialsFile"])
    location = f"s3a://{storage['bucket']}/{prefix}"
    return {
        "delta.warehouse": location + "/warehouse",
        "delta.adoption-roots": json.dumps([location + "/adoption"]),
        "delta.recovery-principals": json.dumps([inputs["serviceAdmin"]]),
        "delta.storage-credential-mode": "static",
        "s3-access-key-id": access,
        "s3-secret-access-key": secret,
        "s3-endpoint": storage["endpoint"],
        "s3-region": storage["region"],
        "s3-path-style-access": "true",
    }


def prepare(inputs, output, delta, runtime=None):
    package = output / ("package-" + delta)
    command = [
        "./gradlew",
        "-I",
        "delta/acceptance/prepare.gradle",
        "prepareDeltaAcceptance",
        "-PdeltaAcceptanceOutput=" + str(package),
        "-PdeltaKernelVersion=3.3.2",
        "-PdeltaVersion=" + delta,
        "-PdeltaSparkVersion=3.5.4",
        "-PskipITs",
        "--console=plain",
    ]
    if runtime is not None:
        command.append("-PdeltaAcceptanceRuntime=" + str(runtime))
    write_json(output / ("prepare-" + delta + "-command.json"), command)
    with (output / ("prepare-" + delta + ".log")).open("wb") as log:
        result = subprocess.run(
            command,
            cwd=ROOT,
            env=clean_environment(inputs),
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    write_json(
        output / ("prepare-" + delta + "-exit.json"), {"exit": result.returncode}
    )
    if result.returncode:
        raise RuntimeError("Production package preparation failed")
    return package


def run_spark(inputs, package, environment, case, directory):
    directory.mkdir(mode=0o700)
    case_path = directory / "case.json"
    write_json(case_path, case)
    storage = inputs[case.get("profile", "primary")]
    access, secret = credentials(storage["credentialsFile"])
    settings = {
        "spark.master": "local[2]",
        "spark.driver.memory": "1g",
        "spark.ui.enabled": "false",
        "spark.sql.shuffle.partitions": "2",
        "spark.default.parallelism": "2",
        "spark.databricks.delta.snapshotPartitions": "2",
        "spark.plugins": "org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin",
        "spark.sql.gravitino.uri": case["uri"],
        "spark.sql.gravitino.metalake": case["metalake"],
        "spark.gravitino.enableDeltaSupport": str(
            case.get("deltaEnabled", True)
        ).lower(),
        "spark.databricks.delta.catalog.update.enabled": "false",
        "spark.hadoop.fs.s3a.endpoint": storage["endpoint"],
        "spark.hadoop.fs.s3a.endpoint.region": storage["region"],
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.hadoop.fs.s3a.access.key": access,
        "spark.hadoop.fs.s3a.secret.key": secret,
        "spark.redaction.regex": "(?i)secret|password|token|access[.]?key|s3-access-key-id",
    }
    Path(case["journals"]).mkdir(mode=0o700, parents=True, exist_ok=True)
    for catalog in case["catalogs"]:
        settings[f"spark.sql.catalog.{catalog}.journal-directory"] = str(
            Path(case["journals"]) / catalog
        )
    properties = directory / "spark.properties"
    properties.write_text("".join(f"{k} {v}\n" for k, v in settings.items()))
    os.chmod(properties, 0o600)
    jars = sorted(
        path
        for path in (package / "spark-jars").glob("*.jar")
        if not path.name.startswith("gravitino-")
    ) + [Path(inputs["_runtime"])]
    write_json(
        directory / "jar-hashes.json",
        {
            str(path): sha256(path)
            for path in jars
            + sorted((Path(environment["spark_home"]) / "jars").glob("*.jar"))
        },
    )
    env = clean_environment(inputs)
    env.update(
        SPARK_USER=case["user"],
        HADOOP_USER_NAME=case["user"],
        SPARK_HOME=environment["spark_home"],
        PYSPARK_PYTHON=environment["venv"] + "/bin/python",
        PYSPARK_DRIVER_PYTHON=environment["venv"] + "/bin/python",
    )
    command = [
        environment["spark_submit"],
        "--properties-file",
        str(properties),
        "--jars",
        ",".join(map(str, jars)),
        str(Path(inputs["_harness"]) / "spark_cases.py"),
        str(case_path),
        str(directory / "result.json"),
    ]
    write_json(directory / "command.json", command)
    with (directory / "process.log").open("wb") as log:
        process = subprocess.Popen(
            command, cwd=directory, env=env, stdout=log, stderr=subprocess.STDOUT
        )
        try:
            code = process.wait(timeout=1200)
        except subprocess.TimeoutExpired:
            process.kill()
            code = process.wait(timeout=30)
        write_json(directory / "exit.json", {"exit": code, "pid": process.pid})
    return code


def run(inputs, output):
    if inputs.get("serverKernelVersion") != "3.3.2":
        raise ValueError("This matrix requires server Kernel 3.3.2")
    if (
        inputs["primary"]["endpoint"] == inputs["secondary"]["endpoint"]
        or inputs["primary"]["bucket"] != inputs["secondary"]["bucket"]
    ):
        raise ValueError(
            "Isolation requires different endpoints with the same S3 authority"
        )
    if "packages" in inputs:
        packages = {version: Path(path) for version, path in inputs["packages"].items()}
    else:
        first = prepare(inputs, output, "3.3.0")
        runtime = next((first / "spark-jars").glob("gravitino-*.jar"))
        packages = {"3.3.0": first, "3.3.2": prepare(inputs, output, "3.3.2", runtime)}
    canonical_runtime = next((packages["3.3.0"] / "spark-jars").glob("gravitino-*.jar"))
    inputs["_runtime"] = str(canonical_runtime)
    write_json(
        output / "selected-runtime.json",
        {
            "path": str(canonical_runtime),
            "sha256": sha256(canonical_runtime),
            "target_pairs": [
                "3.5.4/3.3.0",
                "3.5.4/3.3.2",
                "3.5.6/3.3.0",
                "3.5.6/3.3.2",
            ],
        },
    )
    inventories = {}
    for version, package in packages.items():
        inventories[version] = {
            str(path.relative_to(package)): sha256(path)
            for path in sorted(package.rglob("*.jar"))
        }
    write_json(output / "artifacts.json", inventories)
    environments = json.loads(Path(inputs["sparkEnvironmentsFile"]).read_text())[
        "environments"
    ]
    if len(environments) != 2 or {entry["spark"] for entry in environments} != {
        "3.5.4",
        "3.5.6",
    }:
        raise ValueError("Provide exactly Spark 3.5.4 and 3.5.6 environments")
    server = Server(packages["3.3.0"], output, inputs)
    try:
        server.start()
        rest = Rest(server.uri, inputs["serviceAdmin"])
        metalake = "packaged_" + uuid.uuid4().hex[:12]
        base = "/api/metalakes/" + metalake
        rest.call(
            "POST",
            "/api/metalakes",
            {
                "name": metalake,
                "comment": "Owned packaged acceptance",
                "properties": {},
            },
        )
        with RecordingProxy(server.uri, output / "transport.jsonl") as proxy:
            for environment in [] if inputs.get("processOnly") else environments:
                for delta in ("3.3.0", "3.3.2"):
                    pair = environment["spark"] + "-" + delta
                    catalogs = [
                        "a_" + pair.replace(".", "_").replace("-", "_"),
                        "b_" + pair.replace(".", "_").replace("-", "_"),
                    ]
                    for catalog in catalogs:
                        props = catalog_properties(
                            inputs,
                            "primary",
                            inputs["prefix"] + "/" + output.name + "/" + catalog,
                        )
                        rest.call(
                            "POST",
                            base + "/catalogs",
                            {
                                "name": catalog,
                                "type": "RELATIONAL",
                                "provider": "lakehouse-delta",
                                "comment": "Owned acceptance",
                                "properties": props,
                            },
                        )
                        rest.call(
                            "POST",
                            base + "/catalogs/" + catalog + "/schemas",
                            {"name": "shared", "comment": "Owned", "properties": {}},
                        )
                    case = {
                        "mode": "matrix",
                        "uri": f"http://127.0.0.1:{proxy.port}",
                        "metalake": metalake,
                        "catalogs": catalogs,
                        "journals": str(output / "journals"),
                        "spark": environment["spark"],
                        "delta": delta,
                        "user": inputs["serviceAdmin"],
                    }
                    disabled = dict(case, mode="disabled", deltaEnabled=False)
                    if run_spark(
                        inputs,
                        packages[delta],
                        environment,
                        disabled,
                        output / (pair + "-disabled"),
                    ):
                        raise RuntimeError("Disabled discovery process failed: " + pair)
                    seed = dict(case, mode="matrix-seed")
                    seed_code = run_spark(
                        inputs,
                        packages[delta],
                        environment,
                        seed,
                        output / (pair + "-seed"),
                    )
                    if seed_code:
                        raise RuntimeError("Spark matrix seed failed: " + pair)
                    server.stop()
                    columns_before = server.column_snapshot(pair + "-before")
                    server.start()
                    case["seeded"] = True
                    code = run_spark(
                        inputs, packages[delta], environment, case, output / pair
                    )
                    if code:
                        raise RuntimeError("Spark pair failed: " + pair)
                    verify_pair_objects(inputs, output, rest, base, case)
                    server.stop()
                    columns_after = server.column_snapshot(pair + "-after")
                    for catalog in catalogs:
                        key = catalog + ".same.id"
                        if columns_before[key]["id"] != columns_after[key]["id"]:
                            raise AssertionError(
                                "Persisted existing column identity changed"
                            )
                    extra_key = catalogs[0] + ".same.extra"
                    if (
                        columns_after[extra_key]["id"]
                        == columns_before[catalogs[0] + ".same.id"]["id"]
                    ):
                        raise AssertionError("New column reused existing identity")
                    write_json(
                        output / ("column-identity-" + pair + ".json"),
                        {"before": columns_before, "after": columns_after},
                    )
                    server.start()
                    if inputs.get("representativeOnly"):
                        return

            def create_catalog(catalog, profile, prefix):
                props = catalog_properties(inputs, profile, prefix)
                rest.call(
                    "POST",
                    base + "/catalogs",
                    {
                        "name": catalog,
                        "type": "RELATIONAL",
                        "provider": "lakehouse-delta",
                        "comment": "Owned acceptance",
                        "properties": props,
                    },
                )
                rest.call(
                    "POST",
                    base + "/catalogs/" + catalog + "/schemas",
                    {"name": "shared", "comment": "Owned", "properties": {}},
                )

            create_catalog(
                "recovery",
                "primary",
                inputs["prefix"] + "/" + output.name + "/recovery",
            )
            process_case = {
                "uri": f"http://127.0.0.1:{proxy.port}",
                "metalake": metalake,
                "catalogs": ["recovery"],
                "journals": str(output / "journals"),
                "spark": environments[0]["spark"],
                "delta": "3.3.0",
                "user": inputs["serviceAdmin"],
                "serverLog": str(server.home / "logs/gravitino-server.log"),
            }

            def launch(next_case, directory):
                return run_spark(
                    inputs, packages["3.3.0"], environments[0], next_case, directory
                )

            if inputs.get("diagnosticOnly"):
                code = launch(
                    dict(process_case, mode="diagnostics"), output / "diagnostics"
                )
                if code:
                    raise AssertionError("Diagnostic process failed")
                return
            if "recovery" in inputs.get(
                "processStages", ["recovery", "isolation", "authority"]
            ):
                recovery_cases(
                    inputs, output, server, proxy, rest, base, process_case, launch
                )
            if "isolation" in inputs.get(
                "processStages", ["recovery", "isolation", "authority"]
            ):
                adoption_and_isolation(
                    inputs, output, rest, base, process_case, launch, create_catalog
                )
            if "authority" in inputs.get(
                "processStages", ["recovery", "isolation", "authority"]
            ):
                authority_and_quarantine(
                    inputs, output, rest, base, process_case, launch, create_catalog
                )
    finally:
        server.stop()


def verify_server_origins(output):
    logs = [
        line
        for path in (output / "server").glob("class-load-*.log")
        for line in path.read_text().splitlines()
    ]
    expected = {
        "org.apache.gravitino.catalog.lakehouse.delta.DeltaCatalog": "gravitino-catalog-lakehouse-delta-2.0.0-SNAPSHOT.jar",
        "io.delta.kernel.Table": "delta-kernel-api-3.3.2.jar",
        "io.delta.kernel.defaults.engine.DefaultEngine": "delta-kernel-defaults-3.3.2.jar",
        "org.apache.hadoop.util.VersionInfo": "hadoop-common-3.3.6.jar",
    }
    observations = {}
    for name, jar in expected.items():
        matches = [line for line in logs if " " + name + " source: " in line]
        if not matches or any(
            "/server/catalogs/lakehouse-delta/libs/" + jar not in line
            for line in matches
        ):
            raise AssertionError("Unexpected packaged provider origin for " + name)
        observations[name] = {
            "class_load_events": matches,
            "sha256": sha256(output / "server/catalogs/lakehouse-delta/libs" / jar),
        }
    if any(
        "[class,load] org.apache.spark." in line
        or "NativeRegistrationTestSupport" in line
        or "MiniGravitino" in line
        for line in logs
    ):
        raise AssertionError("Server loaded Spark or fixture classes")
    write_json(output / "server-origins.json", observations)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    output = owned_directory(args.output.resolve())
    inputs = json.loads(args.inputs.read_text())
    shutil.copytree(
        ROOT / "delta/acceptance",
        output / "harness",
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    inputs["_harness"] = str(output / "harness")
    write_json(
        output / "source-revision.json",
        {
            "head": subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
            ).strip(),
            "note": "Exact executed file contents are independently bound by source-freeze.json",
        },
    )
    write_json(
        output / "source-freeze.json",
        {
            str(path.relative_to(ROOT)): sha256(path)
            for folder in (
                "delta/acceptance",
                "spark-connector/v3.5/spark-delta/src/main",
                "spark-connector/spark-common/src/main",
                "catalogs/catalog-lakehouse-delta/src/main",
            )
            for path in sorted((ROOT / folder).rglob("*"))
            if path.is_file() and "__pycache__" not in str(path)
        },
    )
    try:
        run(inputs, output)
        if not inputs.get("diagnosticOnly"):
            verify_server_origins(output)
    except Exception as error:
        record = {"error": type(error).__name__, "message": str(error)}
        if isinstance(error, RestError):
            record["private_response"] = error.result
        write_json(output / "failure.json", record)
        print("Acceptance failed; evidence: " + str(output), file=sys.stderr)
        return 1
    write_json(
        output / "completed.json",
        {
            "status": "passed",
            "scope": {
                key: inputs.get(key)
                for key in (
                    "representativeOnly",
                    "processOnly",
                    "processStages",
                    "diagnosticOnly",
                )
            },
            "driver_exits": {
                str(path.relative_to(output)): json.loads(path.read_text())
                for path in sorted(output.glob("*/exit.json"))
            },
        },
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

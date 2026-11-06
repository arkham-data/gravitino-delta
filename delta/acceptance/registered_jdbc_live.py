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

"""Owned PostgreSQL fixtures and exact registered-source live qualification.

Secrets remain in private inputs. Every command has original lifecycle receipts;
no known or uncertain committed action is replayed.
"""
import argparse
import copy
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import sys
import time
import uuid
from urllib.parse import quote

from cdf_live import ROOT, JAVA, Public, Ranger, read, save, sha, launch
from cdf_merge_live_checks import file_ref
from registered_jdbc_checks import LEAD, case_names

IMAGE = "sha256:33f923b05f64ca54ac4401c01126a6b92afe839a0aa0a52bc5aeb5cc958e5f20"


def case_specs(pair):
    """Expose exactly eight lead or three remaining targets, each attempted once."""
    profiles = {"jdbc_insert_into": "plain", "jdbc_source_select_denied": "source-denied",
                "jdbc_target_update_denied": "target-denied", "jdbc_mixed_save_as_table": "plain",
                "jdbc_column_composition": "columns", "jdbc_source_revoked_before_publish": "revoke",
                "jdbc_config_changed_before_publish": "config", "jdbc_view_rejected": "view"}
    return [dict(name=name, api="saveAsTable" if name == "jdbc_mixed_save_as_table" else "insertInto",
                 profile=profiles[name], target=name, duplicate=False, appendCount=1,
                 warmOrder="target-first" if name == "jdbc_mixed_save_as_table" else "none",
                 inputTimestamp=True, registeredJdbc=True,
                 revokeSource=name == "jdbc_source_revoked_before_publish") for name in case_names(pair)]


class OwnedPostgres:
    """One new cluster with retained on-disk state and a SELECT-only extraction role."""
    def __init__(self, output):
        self.output = Path(output)
        self.output.mkdir(mode=0o700)
        self.name = "registered-jdbc-live-" + uuid.uuid4().hex[:12]
        self.database = "extraction_" + uuid.uuid4().hex[:8]
        self.schema = "source_schema"
        self.password = secrets.token_hex(24)
        self.second_password = secrets.token_hex(24)
        self.started = False
        self.sequence = 0

    def command(self, argv, stdin=None, allow_failure=False):
        self.sequence += 1
        out = self.output / ("command-" + str(self.sequence).zfill(3))
        out.mkdir(mode=0o700)
        save(out / "command.json", dict(argv=argv, started=time.time(), stdinPrivate=stdin is not None))
        with (out / "stdout.private").open("wb") as stdout, (out / "stderr.private").open("wb") as stderr:
            process = subprocess.Popen(argv, stdin=subprocess.PIPE if stdin is not None else subprocess.DEVNULL,
                                       stdout=stdout, stderr=stderr)
            save(out / "process.json", dict(pid=process.pid, started=time.time()))
            timed = False
            try:
                process.communicate(None if stdin is None else stdin.encode(), timeout=60)
            except subprocess.TimeoutExpired:
                timed = True
                process.kill()
                process.communicate()
        save(out / "exit.json", dict(pid=process.pid, exitCode=process.returncode, timedOut=timed, finished=time.time()))
        assert not timed and (allow_failure or process.returncode == 0), "Owned PostgreSQL command failed; private evidence retained"
        return (out / "stdout.private").read_text().strip(), process.returncode

    def sql(self, sql, user="postgres", allow_failure=False):
        return self.command(["docker", "exec", "-i", self.name, "psql", "-X", "-A", "-t",
                             "-v", "ON_ERROR_STOP=1", "-U", user, "-d", self.database], sql, allow_failure)

    def start(self):
        image, _ = self.command(["docker", "image", "inspect", "postgres:16", "--format", "{{.Id}}"])
        assert image == IMAGE
        env = self.output / "postgres.env"
        env.write_text("POSTGRES_PASSWORD=" + secrets.token_hex(24) + "\nPOSTGRES_DB=" + self.database + "\n")
        env.chmod(0o600)
        data = self.output / "data"
        data.mkdir(mode=0o700)
        container, _ = self.command(["docker", "run", "--detach", "--name", self.name,
            "--label", "gravitino.test.owner=registered-jdbc-task3", "--publish", "127.0.0.1::5432",
            "--env-file", str(env), "--mount", "type=bind,source=" + str(data) + ",target=/var/lib/postgresql/data", IMAGE])
        self.started = True
        port, _ = self.command(["docker", "port", self.name, "5432/tcp"])
        assert re.fullmatch(r"127\.0\.0\.1:[0-9]+", port)
        deadline = time.monotonic() + 50
        while True:
            _, code = self.command(["docker", "exec", self.name, "pg_isready", "-h", "127.0.0.1", "-U", "postgres", "-d", self.database], allow_failure=True)
            if code == 0:
                break
            assert time.monotonic() < deadline, "Owned PostgreSQL readiness timed out"
            time.sleep(0.2)
        setup = "CREATE SCHEMA source_schema; CREATE TABLE source_schema.source (id BIGINT, secret TEXT, region TEXT); "
        setup += "INSERT INTO source_schema.source VALUES (1,'alpha','allowed'),(2,'beta','blocked'),(3,'gamma','allowed'); "
        setup += "CREATE VIEW source_schema.source_view AS SELECT * FROM source_schema.source; "
        setup += "REVOKE CREATE ON SCHEMA public FROM PUBLIC; REVOKE ALL ON DATABASE " + self.database + " FROM PUBLIC; "
        for role, password in [("extractor", self.password), ("extractor_rotated", self.second_password)]:
            setup += "CREATE ROLE " + role + " LOGIN PASSWORD '" + password + "' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT; "
            setup += "GRANT CONNECT ON DATABASE " + self.database + " TO " + role + "; GRANT USAGE ON SCHEMA source_schema TO " + role + "; "
            setup += "GRANT SELECT ON ALL TABLES IN SCHEMA source_schema TO " + role + "; "
        self.sql(setup)
        version, _ = self.sql("SHOW server_version;")
        kinds, _ = self.sql("SELECT table_name,table_type FROM information_schema.tables WHERE table_schema='source_schema' ORDER BY table_name;")
        assert kinds.splitlines() == ["source|BASE TABLE", "source_view|VIEW"]
        acl, _ = self.sql("SELECT rolname,rolsuper,rolcreatedb,rolcreaterole,rolinherit FROM pg_roles WHERE rolname LIKE 'extractor%'; SELECT has_schema_privilege('extractor','source_schema','CREATE'),has_table_privilege('extractor','source_schema.source','SELECT'),has_table_privilege('extractor','source_schema.source','INSERT'),has_database_privilege('extractor',current_database(),'CREATE');")
        _, denied = self.sql("INSERT INTO source_schema.source VALUES (9,'forbidden','allowed');", "extractor", True)
        assert denied != 0 and "f|t|f|f" in acl
        self.sql("ALTER SYSTEM SET log_line_prefix = '%m [%p] %u@%d '; ALTER SYSTEM SET log_statement = 'all'; SELECT pg_reload_conf();")
        self.descriptor = dict(catalog="jdbc_source", isolationCatalog="jdbc_isolation", schema=self.schema, table="source",
                              view="source_view", database=self.database, provider="jdbc-postgresql", driver="org.postgresql.Driver",
                              driverVersion="42.7.11", serverVersion=version, imageId=image, containerId=container,
                              kind="TABLE", endpoint="jdbc:postgresql://" + port + "/" + self.database)
        save(self.output / "source.json", dict(self.descriptor, upstreamRole="extractor", acl=acl, writeDenied=True, physicalKinds=kinds))
        save(self.output / "connection.private.json", dict(self.descriptor, user="extractor", password=self.password,
                                                          rotatedUser="extractor_rotated", rotatedPassword=self.second_password))
        return self.output / "connection.private.json"

    def observe(self, output):
        stdout, _ = self.command(["docker", "logs", self.name])
        raw = self.output / ("command-" + str(self.sequence).zfill(3))
        text = stdout + (raw / "stderr.private").read_text()
        from registered_jdbc_checks import postgres_counts
        result = dict(postgres_counts(text), logStdout=file_ref(raw/"stdout.private"), logStderr=file_ref(raw/"stderr.private"))
        save(output, result)
        return result

    def close(self):
        if self.started:
            self.command(["docker", "stop", "--time", "20", self.name])
            state, _ = self.command(["docker", "inspect", self.name, "--format", "{{json .State}}"])
            save(self.output/"terminal.json", dict(container=self.name, state=json.loads(state), preservedData=True))
            self.started = False


def register_sources(plan, inputs, rest, auth_properties, output):
    """Register two distinct catalogs over the same owned physical PostgreSQL source."""
    source = read(inputs["jdbcConnectionFile"])
    plan["registeredJdbc"] = True
    plan["jdbcSource"] = {key: value for key, value in source.items() if key not in ("password", "rotatedPassword")}
    plan["jdbcConnectionFile"] = inputs["jdbcConnectionFile"]
    plan["policyCatalogs"] = [plan["catalog"], source["catalog"], source["isolationCatalog"]]
    base = "/api/metalakes/" + plan["metalake"]
    for catalog in (source["catalog"], source["isolationCatalog"]):
        props = {key: value for key, value in auth_properties.items() if key.startswith("authorization")}
        props.update({"authorization.ranger.delta.catalog": catalog, "jdbc-url": source["endpoint"],
                      "jdbc-database": source["database"], "jdbc-driver": source["driver"],
                      "jdbc-user": source["user"], "jdbc-password": source["password"]})
        rest.call("POST", base + "/catalogs", dict(name=catalog, type="RELATIONAL", provider="jdbc-postgresql", properties=props))
        prefix = base + "/catalogs/" + catalog + "/schemas/" + source["schema"]
        rest.call("GET", prefix)
        for table in (source["table"], source["view"]):
            rest.call("GET", prefix + "/tables/" + table)


def source_objects(plan, obj):
    """Visibility plus genuine metadata/SELECT grants, subsequently changed publicly."""
    source = plan["jdbcSource"]
    result = []
    for catalog in (source["catalog"], source["isolationCatalog"]):
        result.extend([obj("CATALOG", catalog, "USE_CATALOG"), obj("SCHEMA", catalog + "." + source["schema"], "USE_SCHEMA")])
        result.extend(obj("TABLE", catalog + "." + source["schema"] + "." + table, "SELECT_TABLE") for table in (source["table"], source["view"]))
    return result


def source_policies(plan, create):
    """Ranger-authored mask/filter policies are separate from public SELECT grants."""
    source = plan["jdbcSource"]
    ids = []
    for catalog in (source["catalog"], source["isolationCatalog"]):
        for kind in (1, 2):
            resources = {key: dict(values=[value], isExcludes=False, isRecursive=False)
                         for key, value in dict(catalog=catalog, database=source["schema"], table=source["table"]).items()}
            if kind == 1:
                resources["column"] = dict(values=["secret"], isExcludes=False, isRecursive=False)
            item = dict(roles=[plan["rangerAppRole"]], users=[], groups=[], accesses=[dict(type="select", isAllowed=True)])
            item["dataMaskInfo" if kind == 1 else "rowFilterInfo"] = (
                dict(dataMaskType="CUSTOM", valueExpr="'MASKED'" if catalog == source["catalog"] else "'ISOLATED'")
                if kind == 1 else dict(filterExpr="region = 'allowed'" if catalog == source["catalog"] else "id = 2"))
            policy = dict(service=plan["resourceService"], name=catalog + "_" + str(kind) + "_" + plan["nonce"],
                          policyType=kind, isEnabled=True, isAuditEnabled=True, resources=resources)
            policy["dataMaskPolicyItems" if kind == 1 else "rowFilterPolicyItems"] = [item]
            ids.append(create("jdbc-source-policy", "/service/public/v2/api/policy", policy)["id"])
    plan["jdbcPolicyIds"] = ids


def validate_boundary(plan, policies):
    """Admit only the explicitly owned three catalogs and exact source/target resources."""
    source = plan["jdbcSource"]
    allowed = {(plan["catalog"], plan["database"], table["name"]) for table in plan["tables"]}
    allowed |= {(catalog, source["schema"], table) for catalog in (source["catalog"], source["isolationCatalog"])
                for table in (source["table"], source["view"])}
    labels = {}
    for policy in policies:
        if policy.get("policyType", 0) != 0:
            continue
        for item in policy.get("policyItems", []):
            assert not set(item.get("users", [])).intersection({plan["subject"], plan["metadataPrincipal"]})
            assert not item.get("groups")
            if plan["rangerAppRole"] not in item.get("roles", []):
                continue
            resources = policy["resources"]
            assert set(resources) == {"catalog", "database", "table", "column"}
            identity = tuple(resources[key]["values"][0] for key in ("catalog", "database", "table"))
            assert identity in allowed and all(len(resource["values"]) == 1 for resource in resources.values())
            assert all(not r.get("isExcludes", False) and not r.get("isRecursive", False) for r in resources.values())
            if policy["id"] in plan.get("columnPolicyIds", []):
                assert identity == (source["catalog"], source["schema"], source["table"])
                assert resources["column"]["values"] == ["id"]
            else:
                assert resources["column"]["values"] == ["*"]
                owned = tuple(sorted(policy["policyLabels"]))
                assert len(owned) == 3 and "gravitino-managed" in owned
                assert sum(value.startswith("gravitino-metalake-id:") for value in owned) == 1
                assert sum(value.startswith("gravitino-catalog-id:") for value in owned) == 1
                assert labels.setdefault(identity[0], owned) == owned


def build_base(output, stock_home):
    """Build and archive current server/base inputs once; never relabel a predecessor."""
    from cdf_read_live_package import capture_sources, canonical_digest, classes, archive_entries
    from cdf_live_package import provider_inventory
    from prepare import PROVIDERS

    output.mkdir(mode=0o700)
    before = capture_sources(ROOT)
    save(output / "source-before.json", dict(files=before, digest=canonical_digest(before)))
    assembly = output / "assembly"
    invocation = uuid.uuid4().hex
    argv = ["./gradlew", "-I", "delta/acceptance/append_aqe_live.gradle", "-I", "delta/acceptance/registered_jdbc_base.gradle",
            "preparePublicAppendPackage", "-PpublicAppendOutput=" + str(assembly), "-PpublicAppendInvocation=" + invocation,
            "-PpublicAppendSourceCapture=" + str(output / "source-before.json"), "-PdeltaSparkVersion=3.5.6", "-PdeltaVersion=3.3.2",
            "-PskipITs", "-PskipDockerTests=false", "--offline", "--max-workers=1", "--console=plain", "-Dorg.gradle.jvmargs=-Xmx2g"]
    launch(argv, dict(os.environ, JAVA_HOME=JAVA, JAVA_TOOL_OPTIONS="-XX:ActiveProcessorCount=2 -XX:MaxMetaspaceSize=768m"), output / "build", timeout=1200)
    after = capture_sources(ROOT)
    save(output / "source-after.json", dict(files=after, digest=canonical_digest(after)))
    assert before == after, "Source moved during fresh shared build"
    built = read(assembly / "assembly.json")
    assert built["sourceInputs"] == before and built["buildInvocation"] == invocation
    assert all(sha(path) == digest for path, digest in built["artifactFiles"].items())
    home = output / "spark-home"
    home.mkdir()
    (home / "jars").mkdir()
    for child in stock_home.iterdir():
        if child.name != "jars":
            (home / child.name).symlink_to(child, target_is_directory=child.is_dir())
    omit = {"hadoop-client-api-3.3.4.jar", "hadoop-client-runtime-3.3.4.jar", "hadoop-yarn-server-web-proxy-3.3.4.jar",
            "guava-14.0.1.jar", "scala-library-2.12.18.jar", "scala-reflect-2.12.18.jar", "scala-compiler-2.12.18.jar"}
    stock = {p.name: p for p in (stock_home / "jars").glob("*.jar")}
    assert omit <= stock.keys()
    selected = {p.name: p for folder in ("native", "production", "app") for p in (assembly / folder).glob("*.jar")}
    assert not (stock.keys() - omit) & selected.keys()
    jars = dict({name: path for name, path in stock.items() if name not in omit}, **selected)
    for name, path in jars.items():
        (home / "jars" / name).symlink_to(path.resolve())
    required = ["org.postgresql.Driver", "org.apache.gravitino.spark.connector.jdbc.GravitinoJdbcCatalog",
                "org.apache.gravitino.spark.connector.jdbc.SparkJdbcTable",
                "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog",
                "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScan",
                "org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD"]
    all_classes = {name for path in selected.values() for name in classes(path)}
    required += [name[:-6].replace("/", ".") for name in all_classes
                 if any(stem in name for stem in ("DeltaRangerPostgreSqlCatalogSpark35", "DeltaRangerJdbc", "DeltaRangerInputSources", "GravitinoPostgreSqlCatalogSpark35"))
                 and "$anon" not in name and "$$" not in name]
    providers = provider_inventory(list(jars.values()), sorted(set(PROVIDERS + required)))
    import hashlib
    import zipfile
    from collections import Counter
    retained = {}
    for path in assembly.rglob("*.jar"):
        with zipfile.ZipFile(path) as archive:
            counts = Counter(archive.namelist())
            assert {name for name, count in counts.items() if count > 1} <= {"META-INF/NOTICE", "META-INF/LICENSE"}, "Unexpected duplicate package member"
            members = [dict(index=index, name=entry.filename, sha256=hashlib.sha256(archive.read(entry)).hexdigest())
                       for index, entry in enumerate(archive.infolist())]
        retained[str(path)] = dict(sha256=sha(path), members=members)
    save(output / "archive-members.json", retained)
    receipt = dict(invocation=invocation, command=file_ref(output / "build/command.json"),
                   process=file_ref(output / "build/process.json"), exit=file_ref(output / "build/exit.json"),
                   before=file_ref(output / "source-before.json"), after=file_ref(output / "source-after.json"),
                   archives=file_ref(output / "archive-members.json"), assembly=file_ref(assembly / "assembly.json"))
    save(output / "build-receipt.json", receipt)
    manifest = dict(spark="3.5.6", delta="3.3.2", sourceInputs=before, sourceInputDigest=canonical_digest(before),
                    stockSparkHome=str(stock_home), stockJarHashes={name: sha(path) for name, path in stock.items()},
                    sparkHome=str(home), jarHashes={str(p.resolve()): sha(p) for p in jars.values()},
                    providers=providers, app=str(assembly / "app/public-grants-live-append.jar"),
                    assemblyManifest=str(assembly / "assembly.json"), assemblySha256=sha(assembly / "assembly.json"),
                    buildReceipt=file_ref(output / "build-receipt.json"), scope="Fresh current shared server/base with explicit stock PostgreSQL driver")
    save(output / "manifest.json", manifest)
    return output / "manifest.json"


def source_descriptor(plan, isolation=False, view=False):
    source = plan["jdbcSource"]
    return dict(catalog=source["isolationCatalog"] if isolation else source["catalog"],
                database=source["schema"], table=source["view"] if view else source["table"])


def column_policy(plan, output, create=True):
    """Grant only id to the application, with broad metadata access kept separate."""
    ranger = Ranger(plan, output)
    if not create:
        for identifier in plan["columnPolicyIds"]:
            path = "/service/public/v2/api/policy/" + str(identifier)
            policy = ranger.call("GET", path)
            policy.update(isEnabled=False, policyItems=[])
            ranger.call("PUT", path, policy)
        plan["columnPolicyIds"] = []
        return
    descriptor = dict(source_descriptor(plan), column="id")
    policy = dict(service=plan["resourceService"], name="jdbc_id_only_" + plan["nonce"], policyType=0,
                  isEnabled=True, isAuditEnabled=True,
                  resources={key: dict(values=[value], isExcludes=False, isRecursive=False) for key, value in descriptor.items()},
                  policyItems=[dict(roles=[plan["rangerAppRole"]], users=[], groups=[], accesses=[dict(type="select", isAllowed=True)], delegateAdmin=False)])
    created = ranger.call("POST", "/service/public/v2/api/policy", policy)
    plan["columnPolicyIds"] = [created["id"]]


def run_case(plan, runtime, spec, postgres, output):
    """One native Python process and fresh target; preserve any partial publication."""
    from append_aqe_live import start, finish, wait_for, observe
    from append_aqe_public import snapshot, transition
    from append_aqe_profiles import settings, verify_registration_transition
    from cdf_live import prepare_request, clean_environment
    from live_common import OwnedObjects
    from registered_jdbc_checks import verify_case

    output.mkdir(mode=0o700)
    descriptor = source_descriptor(plan)
    # Every pair explicitly proves source SELECT pushdown via public revoke then grant.
    _, first_transition = transition(plan, descriptor, "SELECT_TABLE", False, output / "source-initial-revoke")
    state, source_grant = transition(plan, descriptor, "SELECT_TABLE", True, output / "source-grant")
    state, target_grant = transition(plan, spec["target"], "MODIFY_TABLE", True, output / "target-grant")
    transitions = [file_ref(first_transition), file_ref(source_grant), file_ref(target_grant)]
    if spec["profile"] in ("source-denied", "columns"):
        state, change = transition(plan, descriptor, "SELECT_TABLE", False, output / "source-denial")
        transitions.append(file_ref(change))
    if spec["profile"] == "target-denied":
        state, change = transition(plan, spec["target"], "MODIFY_TABLE", False, output / "target-denial")
        transitions.append(file_ref(change))
    if spec["profile"] == "columns":
        column_policy(plan, output / "column-policy")
        state = snapshot(plan, output / "columns-state")
    before = observe(plan, runtime, spec["target"], output / "before")
    request = prepare_request(plan, runtime, state, output / "request")
    target = next(t for t in plan["tables"] if t["name"] == spec["target"])
    source = plan["jdbcSource"]
    request.update(frontend=spec["api"], caseName=spec["name"], target=spec["target"],
                   output=str(output / "jvm"), pythonOutput=str(output / "python"), duplicate=False, denyDuplicate=False,
                   warmOrder=spec["warmOrder"], barrier=spec["profile"] in ("revoke", "config"), appendCount=1,
                   transactionVersions=[1], targetLocation=target["location"], deadlineSeconds=1200,
                   inputTimestamp=True, revokeSource=spec["revokeSource"], registeredJdbc=True,
                   configChange=spec["profile"] == "config", columnComposition=spec["profile"] == "columns",
                   mixedJdbc=spec["name"] == "jdbc_mixed_save_as_table", viewControl=spec["profile"] == "view", sourceCatalog=source["catalog"],
                   sourceSchema=source["schema"], sourceTable=source["view"] if spec["profile"] == "view" else source["table"],
                   deltaSourceName=plan["catalog"] + "." + plan["database"] + ".source")
    properties = Path(request["sparkConfigurationFile"])
    explicit = settings(spec, "jdbc-" + plan["nonce"] + "-" + spec["name"])
    explicit["spark.gravitino.enableDeltaRangerJdbcSupport"] = "true"
    explicit["spark.redaction.regex"] = "(?i)secret|password|token|access[.]?key|credential|jdbc-url|jdbc-user"
    if spec["warmOrder"] != "none":
        path = output / "request/raw-maintenance.properties"
        values = {"version": "1", "profile": "raw-current-head-filter-project-v1", "table.count": "1",
                  "table.0.metalake": plan["metalake"], "table.0.catalog": plan["catalog"],
                  "table.0.database": plan["database"], "table.0.table": spec["target"]}
        path.write_text("".join(k + "=" + v + "\n" for k, v in values.items()))
        explicit["spark.gravitino.deltaRanger.rawCacheMaintenance.config"] = str(path)
        explicit["spark.sql.cache.serializer"] = "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer"
    lines = [line for line in properties.read_text().splitlines() if line.split("=", 1)[0] not in explicit]
    properties.write_text("\n".join(lines + [k + "=" + v for k, v in explicit.items()]) + "\n")
    save(output / "request/request.private.json", request)
    env = clean_environment(plan)
    env.update(SPARK_HOME=runtime["sparkHome"], PYSPARK_PYTHON=plan["python"], SPARK_USER=plan["subject"],
               HADOOP_USER_NAME=plan["subject"], PYSPARK_PIN_THREAD="true", JAVA_TOOL_OPTIONS="-XX:ActiveProcessorCount=2 -XX:MaxMetaspaceSize=768m")
    pg_before = postgres.observe(output / "postgres-before.json")
    argv = [runtime["sparkHome"] + "/bin/spark-submit", "--driver-memory", "3g", "--master", "local[2]",
            "--properties-file", str(properties), "--driver-class-path", request["driverSecurityDirectory"],
            str(ROOT / "delta/acceptance/append_aqe_pyspark.py"), str(output / "request/request.private.json")]
    process, log = start(argv, env, output / "writer")
    control = Path(request["controlDirectory"])
    records = []
    observed = None
    try:
        if request["barrier"]:
            wait_for(control / "held-ready.json", process, 180)
            held = OwnedObjects(plan["primary"], plan["prefix"] + "/").manifest(output / "raw-held")
            save(output / "raw-held.json", held)
            added = {entry["key"] for entry in held} - {entry["key"] for entry in before["raw"]}
            assert any(key.endswith(".parquet") for key in added)
            assert not any(key.endswith("/00000000000000000001.json") for key in added)
            if spec["profile"] == "revoke":
                state, change = transition(plan, descriptor, "SELECT_TABLE", False, output / "late-source-revoke")
                transitions.append(file_ref(change))
                state = dict(state, publicRevokeComplete=True, sourceSelectAbsent=True)
            else:
                connection = read(plan["jdbcConnectionFile"])
                rest = Public(plan["gravitinoUrl"], plan["serviceAdmin"], output / "config-change")
                catalog_path = "/api/metalakes/" + plan["metalake"] + "/catalogs/" + source["catalog"]
                previous = rest.call("GET", catalog_path)
                rest.call("PUT", catalog_path, dict(updates=[{"@type": "setProperty", "property": "jdbc-user", "value": connection["rotatedUser"]},
                                                            {"@type": "setProperty", "property": "jdbc-password", "value": connection["rotatedPassword"]}]))
                current = rest.call("GET", catalog_path)
                assert previous != current and current["catalog"]["properties"]["jdbc-user"] == connection["rotatedUser"]
                healthy, _ = postgres.sql("SELECT 1;", connection["rotatedUser"])
                assert healthy == "1"
                state = dict(initialPolicy=state["policy"], policy=state["policy"], role=state["role"], publicMetadataChanged=True, upstreamHealthy=True)
            save(output / "barrier-state.private.json", state)
            save(control / "revoke-complete.private.json", dict(stateEvidenceFile=str(output / "barrier-state.private.json")))
        actions = (("id_only", "hidden_predicate", "pruned_original_demand") if request["columnComposition"] else
                   (("physical_control", "first") if request["viewControl"] else ("first",)))
        previous_pg = pg_before
        previous_pg_path = output / "postgres-before.json"
        for action in actions:
            name = spec["name"] + "-" + action
            wait_for(control / (name + "-ready.json"), process, 300)
            pg_after = postgres.observe(output / ("postgres-" + action + ".json"))
            observation = read(output / "jvm" / (name + "-append.json"))
            caller = read(output / "python" / (name + "-caller.json"))
            records.append(dict(action=action, postgresBefore=file_ref(previous_pg_path), postgresAfter=file_ref(output / ("postgres-" + action + ".json")), observation=file_ref(output / "jvm" / (name + "-append.json")),
                                caller=file_ref(output / "python" / (name + "-caller.json")),
                                payloadQueries=pg_after["payloadQueries"] - previous_pg["payloadQueries"],
                                metadataQueries=pg_after["metadataQueries"] - previous_pg["metadataQueries"],
                                failures=observation["failure"], requests=observation["rangerRequests"], rows=caller.get("rows")))
            previous_pg = pg_after
            previous_pg_path = output / ("postgres-" + action + ".json")
            # Independent native readback happens before releasing the original caller.
            observed = observe(plan, runtime, spec["target"], output / ("after-" + action))
            save(control / (name + "-verified.json"), dict(checkpoint=name, verified=True))
        code, timed = finish(process, log, output / "writer", 60)
        log = None
        assert code == 0 and not timed
    finally:
        if log is not None:
            finish(process, log, output / "writer", 5)
            observe(plan, runtime, spec["target"], output / "failure-native-after")
            postgres.observe(output / "postgres-failure.json")
    record = dict(name=spec["name"], api=spec["api"], source=source_descriptor(plan, view=spec["profile"] == "view"),
                  target=dict(catalog=plan["catalog"], database=plan["database"], table=spec["target"]),
                  before=before["image"], after=observed["image"], actions=records, transitions=transitions,
                  request=file_ref(output / "request/request.private.json"), origins=file_ref(output / "jvm/origins.json"),
                  runtimeProfile=file_ref(output / "jvm/runtime-profile.json"), pythonRuntime=file_ref(output / "python/python-runtime.json"),
                  wrapper=file_ref(output / "jvm/jdbc-wrapper.json"), applied=file_ref(output / "jvm/initial-applied.json"),
                  writerExit=file_ref(output / "writer/exit.json"), postgresBefore=file_ref(output / "postgres-before.json"),
                  postgresAfter=file_ref(output / ("postgres-" + actions[-1] + ".json")))
    if request["barrier"]:
        record["barrier"] = {key: file_ref(output / path) for key, path in {
            "held": "jvm/held.json", "release": "jvm/release-witness.json", "forwarded": "jvm/forwarded.json", "rawHeld": "raw-held.json", "rawBefore": "before/raw-before.json"}.items()}
    if request["mixedJdbc"]:
        record["warm"] = file_ref(output / "jvm" / (spec["name"] + "-first-warm-rows.json"))
    save(output / "original-case.json", record)
    verify_case(record)
    verify_registration_transition(before["registrations"], observed["registrations"], spec["target"], before["native"]["version"], observed["native"]["version"])
    assert before["source"] == observed["source"]
    if spec["profile"] == "columns":
        column_policy(plan, output / "column-policy-remove", False)
    if spec["profile"] in ("source-denied", "columns", "revoke"):
        transition(plan, descriptor, "SELECT_TABLE", True, output / "source-restore")
    return record


def run(runtime_path, inputs_path, output):
    """Run only this pair's explicit inventory and close only owned children."""
    from append_aqe_live import start, finish, wait_for
    from cdf_live import verify_deployed
    from cdf_read_live_package import verify_runtime
    from registered_jdbc_checks import verify_case

    output.mkdir(mode=0o700)
    runtime = read(runtime_path)
    runtime["manifestPath"] = str(runtime_path)
    verify_runtime(runtime)
    inputs = read(inputs_path)
    association = verify_deployed(inputs, output / "deployed-ranger")
    pair = (runtime["spark"], runtime["delta"])
    specs = case_specs(pair)
    inputs = dict(inputs, deployedRangerEvidence=str(association), registeredJdbcPair=list(pair))
    inputs["pythonRuntime"] = str(ROOT / ".superpowers/sdd/2026-10-29-spark-hub-catalog-foundation/runtime-python" / (pair[0] + "-" + pair[1]) / "venv/bin/python")
    assert Path(inputs["pythonRuntime"]).is_file()
    postgres = OwnedPostgres(output / "postgres")
    process, log = None, None
    records = []
    fixture = output / "fixture"
    try:
        inputs["jdbcConnectionFile"] = str(postgres.start())
        save(output / "inputs.private.json", inputs)
        argv = [sys.executable, str(ROOT / "delta/acceptance/append_aqe_fixture.py"), "--inputs", str(output / "inputs.private.json"),
                "--package", str(runtime_path), "--freeze", str(runtime_path), "--out", str(fixture)]
        process, log = start(argv, os.environ.copy(), output / "fixture-command")
        wait_for(fixture / "ready.json", process, 600)
        plan = read(fixture / "state.private.json")
        (output / "cases").mkdir(mode=0o700)
        for spec in specs:
            record = run_case(plan, runtime, spec, postgres, output / "cases" / spec["name"])
            records.append(record)
            save(output / (spec["name"] + "-completed.json"), dict(case=spec["name"], finished=time.time()))
    finally:
        if fixture.exists():
            save(fixture / "stop-request.json", dict(stopOwnedServer=True, preserveDataAndEvidence=True))
        if process is not None:
            code, timed = finish(process, log, output / "fixture-command", 60)
            save(output / "fixture-terminal.json", dict(exitCode=code, timedOut=timed))
        postgres.close()
    assert code == 0 and not timed
    assert [case["name"] for case in records] == list(case_names(pair))
    for record in records:
        verify_case(record)
    result = dict(schemaVersion=1, spark=pair[0], delta=pair[1], cases=records, caseInventory=list(case_names(pair)),
                  runtime=file_ref(runtime_path), fixturePlan=file_ref(fixture / "state.private.json"),
                  source=file_ref(output / "postgres/source.json"), sourceTerminal=file_ref(output / "postgres/terminal.json"),
                  fixtureExit=file_ref(output / "fixture-command/exit.json"), deployedRanger=file_ref(association))
    save(output / "raw-result.json", result)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["base", "run"])
    for key in ("out", "stock", "runtime", "inputs"):
        parser.add_argument("--" + key, required=key == "out")
    args = parser.parse_args()
    os.umask(0o077)
    if args.mode == "base":
        build_base(Path(args.out).resolve(), Path(args.stock).resolve())
    else:
        run(Path(args.runtime).resolve(), Path(args.inputs).resolve(), Path(args.out).resolve())


if __name__ == "__main__":
    main()

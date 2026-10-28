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

"""Pinned native/wrapped Iceberg comparator with isolated Hive-service resources."""

import argparse
import json
import os
from pathlib import Path
import secrets
import shutil
import sys
import uuid
import xml.etree.ElementTree as ET

from cdf_live import (
    ROOT,
    JAVA,
    Ranger,
    Public,
    Server,
    RecordingProxy,
    envfile,
    read,
    save,
    sha,
    verify_deployed,
)
from read_parity_live import start, finish
from cdf_merge_live_checks import file_ref
from read_parity_checks import (
    verify_audit_slice,
    verify_policy_http,
    verify_jar_property,
)
from package_binding import capture_sources, canonical_digest

from iceberg_read_parity import runtime, artifact, wrapper_dependencies
from append_aqe_checks import case_specs, verify_iceberg_append


def _run_worker(spark, route, inputs_path, out):
    os.umask(0o077)
    out.mkdir(mode=0o700)
    inputs = read(inputs_path)
    endpoints = read(inputs["endpoints"])
    admin = envfile(inputs["rangerAdminFile"])
    association = verify_deployed(inputs, out / "deployed-ranger")
    harness = out / "executed-harness"
    harness.mkdir()
    harness_files = [
        "iceberg_append_aqe.py",
        "iceberg_append_aqe_app.py",
        "append_aqe_checks.py",
        "iceberg_read_parity.py",
        "read_parity_pyspark.py",
        "cdf_live.py",
        "read_parity_live.py",
        "read_parity_checks.py",
    ]
    for name in harness_files:
        shutil.copyfile(ROOT / "delta/acceptance" / name, harness / name)
    harness_refs = [file_ref(harness / name) for name in harness_files]
    sources = capture_sources(ROOT)
    save(
        out / "source-inputs.json",
        {"files": sources, "digest": canonical_digest(sources)},
    )
    nonce = uuid.uuid4().hex[:12]
    subjects = {
        name: "ice_append_" + name.replace("-", "_") + "_" + nonce
        for name in ["allowed", "source-denied", "target-denied", "id-only"]
    }
    setup = "ice_setup_" + nonce
    service = "ice_append_" + nonce
    database = "ice_append_db_" + nonce
    plan = {
        "rangerUrl": endpoints["ranger-admin"],
        "rangerAdmin": {
            "username": admin["RANGER_ADMIN_USER"],
            "password": admin["RANGER_ADMIN_PASSWORD"],
        },
        "plugin": {
            "username": "ice_append_download_" + nonce,
            "password": "Aa1!" + secrets.token_urlsafe(20),
        },
    }
    save(
        out / "state.private.json",
        dict(plan, subjects=subjects, setup=setup, service=service, database=database),
    )
    ranger = Ranger(plan, out / "ranger-trace")
    for user, password in [(plan["plugin"]["username"], plan["plugin"]["password"])] + [
        (user, "Aa1!" + secrets.token_urlsafe(20)) for user in subjects.values()
    ]:
        ranger.call(
            "POST",
            "/service/xusers/secure/users",
            {
                "name": user,
                "firstName": "Iceberg",
                "lastName": "AppendFixture",
                "password": password,
                "emailAddress": user + "@fixture.invalid",
                "userRoleList": ["ROLE_USER"],
                "status": 1,
                "userSource": 0,
                "isVisible": 1,
            },
        )
    definition = ranger.call("GET", "/service/public/v2/api/servicedef/name/hive")
    save(out / "service-definition.json", definition)
    created = ranger.call(
        "POST",
        "/service/public/v2/api/service",
        {
            "name": service,
            "type": "hive",
            "isEnabled": True,
            "configs": {
                "username": plan["plugin"]["username"],
                "password": plan["plugin"]["password"],
                "jdbc.driverClassName": "org.apache.hive.jdbc.HiveDriver",
                "jdbc.url": "jdbc:hive2://localhost:10000",
                "create.default.policy.per.hierarchy": "false",
                "policy.download.auth.users": plan["plugin"]["username"],
            },
        },
    )
    save(
        out / "service.json",
        {
            "id": created["id"],
            "name": service,
            "database": database,
            "subjects": subjects,
            "setup": setup,
        },
    )
    policies = []

    def policy(label, kind, tables, columns, users, access="select", info=None):
        resources = {
            key: {"values": values, "isExcludes": False, "isRecursive": False}
            for key, values in [
                ("database", [database]),
                ("table", tables),
                ("column", columns),
            ]
        }
        if kind == 2:
            resources.pop("column")
        item = {
            "users": users,
            "groups": [],
            "roles": [],
            "accesses": [{"type": access, "isAllowed": True}],
        }
        field = {0: "policyItems", 1: "dataMaskPolicyItems", 2: "rowFilterPolicyItems"}[
            kind
        ]
        if info:
            item.update(info)
        body = {
            "service": service,
            "name": label + "_" + nonce,
            "policyType": kind,
            "isEnabled": True,
            "isAuditEnabled": True,
            "resources": resources,
            "policyLabels": ["append-aqe-fixture-authored"],
            field: [item],
        }
        policies.append(ranger.call("POST", "/service/public/v2/api/policy", body))

    tables = [s["name"] for s in case_specs()]
    policy(
        "source_select",
        0,
        ["source"],
        ["*"],
        [subjects["allowed"], subjects["target-denied"]],
    )
    policy(
        "target_update",
        0,
        tables,
        ["*"],
        [subjects["allowed"], subjects["source-denied"]],
        access="update",
    )
    policy(
        "source_visible",
        2,
        ["source"],
        [],
        list(subjects.values()),
        info={"rowFilterInfo": {"filterExpr": "region <> 'north'"}},
    )
    policy(
        "source_mask",
        1,
        ["source"],
        ["email"],
        list(subjects.values()),
        info={"dataMaskInfo": {"dataMaskType": "CUSTOM", "valueExpr": "'M'"}},
    )
    policy(
        "target_read_filter",
        2,
        tables,
        [],
        list(subjects.values()),
        info={"rowFilterInfo": {"filterExpr": "id < 0"}},
    )
    policy(
        "target_read_mask",
        1,
        tables,
        ["email"],
        list(subjects.values()),
        info={"dataMaskInfo": {"dataMaskType": "CUSTOM", "valueExpr": "'TARGET'"}},
    )
    policy("id_only", 0, ["source"], ["id"], [subjects["id-only"]])
    policy(
        "id_only_update",
        0,
        ["id_only_probe"],
        ["*"],
        [subjects["id-only"]],
        access="update",
    )
    save(out / "fixture-policies.json", policies)
    security = out / "security"
    security.mkdir(mode=0o700)
    cache = out / "policy-cache"
    cache.mkdir()
    proxy = RecordingProxy(plan["rangerUrl"], out / "plugin-http.jsonl")
    proxy.__enter__()
    properties = {
        "ranger.plugin.spark.service.name": service,
        "ranger.plugin.spark.policy.rest.url": "http://127.0.0.1:" + str(proxy.port),
        "ranger.plugin.spark.policy.rest.client.username": plan["plugin"]["username"],
        "ranger.plugin.spark.policy.rest.client.password": plan["plugin"]["password"],
        "ranger.plugin.spark.policy.cache.dir": str(cache),
        "ranger.plugin.spark.policy.pollIntervalMs": "500",
        "ranger.plugin.spark.policy.source.impl": "org.apache.ranger.admin.client.RangerAdminRESTClient",
        "ranger.plugin.spark.policy.rest.client.connection.timeoutMs": "3000",
        "ranger.plugin.spark.policy.rest.client.read.timeoutMs": "3000",
        "ranger.plugin.spark.policy.rest.client.cookie.enabled": "false",
        "ranger.plugin.spark.supports.policy.deltas": "false",
        "ranger.plugin.spark.enable.implicit.userstore.enricher": "false",
    }
    xml = ET.Element("configuration")
    for key, value in properties.items():
        prop = ET.SubElement(xml, "property")
        ET.SubElement(prop, "name").text = key
        ET.SubElement(prop, "value").text = value
    ET.indent(xml)
    ET.ElementTree(xml).write(
        security / "ranger-spark-security.xml", encoding="utf-8", xml_declaration=True
    )
    (security / "ranger-spark-audit.xml").write_text(
        "<configuration><property><name>xasecure.audit.is.enabled</name><value>false</value></property></configuration>"
    )
    home = runtime(spark, out)
    server = None
    server_proxy = None
    wrapper_config = {}
    warehouse = "s3a://delta-fixtures/iceberg-append-parity-" + nonce
    wire_policies = {}

    def snapshot(name):
        result = ranger.call(
            "GET",
            "/service/plugins/secure/policies/download/"
            + service
            + "?lastKnownVersion=-1&pluginId=iceberg-append-supervisor",
            download=True,
        )
        path = out / (name + "-policies.json")
        save(path, result)
        wire_policies[str(path)] = file_ref(
            ranger.out / (f"{ranger.count:04d}.response.private.bin")
        )
        return result, file_ref(path)

    try:
        if route == "gravitino":
            assembly = Path(inputs["sharedAssemblyManifest"])
            assert assembly.is_file(), "Fresh shared assembly manifest required"
            server = Server(
                assembly.parent / "server",
                out,
                {"serviceAdmin": setup, "javaHome": JAVA},
            )
            conf = server.home / "conf/gravitino.conf"
            conf.write_text(
                conf.read_text().replace(
                    "gravitino.authorization.enable = true",
                    "gravitino.authorization.enable = false",
                )
            )
            catalog_home = server.home / "catalogs/lakehouse-iceberg"
            shutil.copytree(
                ROOT / "catalogs/catalog-lakehouse-iceberg/build/libs",
                catalog_home / "libs",
            )
            for args in [
                ("org.apache.hadoop", "hadoop-aws", "3.3.6"),
                ("com.amazonaws", "aws-java-sdk-bundle", "1.12.367"),
            ]:
                jar = artifact(*args)
                shutil.copyfile(jar, catalog_home / "libs" / jar.name)
            (catalog_home / "conf").mkdir()
            (catalog_home / "conf/lakehouse-iceberg.conf").write_text(
                "# Isolated stock HadoopCatalog comparator backend.\n"
            )
            credentials = envfile(inputs["minioCredentialsFile"])
            hadoop = {
                "fs.s3a.endpoint": endpoints["minio"],
                "fs.s3a.path.style.access": "true",
                "fs.s3a.connection.ssl.enabled": "false",
                "fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                "fs.s3a.access.key": credentials["MINIO_ROOT_USER"],
                "fs.s3a.secret.key": credentials["MINIO_ROOT_PASSWORD"],
            }
            core = ET.Element("configuration")
            for key, value in hadoop.items():
                prop = ET.SubElement(core, "property")
                ET.SubElement(prop, "name").text = key
                ET.SubElement(prop, "value").text = value
            ET.ElementTree(core).write(
                server.home / "conf/core-site.xml",
                encoding="utf-8",
                xml_declaration=True,
            )
            extras = wrapper_dependencies()
            supplemental = out / "supplemental"
            supplemental.mkdir()
            for jar in extras:
                retained = supplemental / jar.name
                shutil.copyfile(jar, retained)
                (home / "jars" / jar.name).symlink_to(retained)
            save(
                out / "wrapper-artifacts.json",
                {str(p): sha(p) for p in supplemental.iterdir()},
            )
            server.start()
            rest = Public(server.uri, setup, out / "public-trace")
            metalake = "ice_append_lake_" + nonce
            rest.call(
                "POST",
                "/api/metalakes",
                {
                    "name": metalake,
                    "comment": "Ordinary Iceberg read comparator",
                    "properties": {},
                },
            )
            props = {
                "catalog-backend": "custom",
                "catalog-backend-impl": "org.apache.iceberg.hadoop.HadoopCatalog",
                "catalog-backend-name": "iceberg",
                "uri": "file:///unused-hadoop-catalog-uri",
                "warehouse": warehouse,
                "table-metadata-cache-impl": "",
                "io-impl": "org.apache.iceberg.hadoop.HadoopFileIO",
            }
            rest.call(
                "POST",
                "/api/metalakes/" + metalake + "/catalogs",
                {
                    "name": "iceberg",
                    "type": "RELATIONAL",
                    "provider": "lakehouse-iceberg",
                    "properties": props,
                },
            )
            server_proxy = RecordingProxy(server.uri, out / "gravitino-http.jsonl")
            server_proxy.__enter__()
            wrapper_config = {
                "wrapper": True,
                "gravitinoUrl": "http://127.0.0.1:" + str(server_proxy.port),
                "metalake": metalake,
            }
            save(
                out / "wrapper-registration.json",
                dict(
                    wrapper_config,
                    provider="lakehouse-iceberg",
                    properties=props,
                    metadataAuthorizationEnabled=False,
                    sharedAssembly=file_ref(assembly),
                ),
            )
        python = str(
            ROOT / (".superpowers/runtime-20260914/spark-" + spark + "/bin/python")
        )
        environment = dict(
            os.environ,
            JAVA_HOME=JAVA,
            TZ="UTC",
            SPARK_HOME=str(home),
            SPARK_LOCAL_IP="127.0.0.1",
            PYSPARK_PYTHON=python,
        )

        def launch(mode, subject, profile=None):
            target = out / mode
            target.mkdir()
            initial, reference = snapshot(mode + "-initial")
            proxy.wait_drained(30)
            first_byte = proxy.events.stat().st_size if proxy.events.exists() else 0
            proxy.disarm()
            request = {
                "mode": mode if mode in ("seed", "verify") else "application",
                "spark": spark,
                "output": str(target),
                "pythonOutput": str(target / "python-evidence"),
                "credentials": inputs["minioCredentialsFile"],
                "warehouse": warehouse,
                "endpoint": endpoints["minio"],
                "database": database,
                "identityProfile": profile,
                "minPolicy": initial["policyVersion"],
                "initialPolicySnapshot": reference,
                "pluginHttpFirstByte": first_byte,
            }
            if mode not in ("seed", "verify"):
                request.update(wrapper_config)
            audit = target / "audit"
            audit.mkdir()
            request["auditDirectory"] = str(audit)
            local_security = target / "security"
            shutil.copytree(security, local_security)
            local_cache = target / "policy-cache"
            local_cache.mkdir()
            security_xml = ET.parse(local_security / "ranger-spark-security.xml")
            for prop in security_xml.getroot().findall("property"):
                if prop.findtext("name") == "ranger.plugin.spark.policy.cache.dir":
                    prop.find("value").text = str(local_cache)
            security_xml.write(
                local_security / "ranger-spark-security.xml",
                encoding="utf-8",
                xml_declaration=True,
            )
            audit_values = {
                "xasecure.audit.is.enabled": "true",
                "xasecure.audit.destination.file": "true",
                "xasecure.audit.destination.file.dir": str(audit),
                "xasecure.audit.destination.file.filename.format": "permission.log",
                "xasecure.audit.destination.file.file.rollover.sec": "86400",
                "xasecure.audit.destination.file.queue": "none",
                "xasecure.audit.provider.summary.enabled": "false",
            }
            audit_xml = ET.Element("configuration")
            for key, value in audit_values.items():
                prop = ET.SubElement(audit_xml, "property")
                ET.SubElement(prop, "name").text = key
                ET.SubElement(prop, "value").text = value
            ET.indent(audit_xml)
            ET.ElementTree(audit_xml).write(
                local_security / "ranger-spark-audit.xml",
                encoding="utf-8",
                xml_declaration=True,
            )
            save(target / "request.private.json", request)
            argv = [
                str(home / "bin/spark-submit"),
                "--master",
                "local[2]",
                "--driver-memory",
                "2g",
                "--driver-class-path",
                str(local_security),
                ROOT / "delta/acceptance/iceberg_append_aqe_app.py",
                target / "request.private.json",
            ]
            return start(
                argv,
                dict(environment, SPARK_USER=subject, HADOOP_USER_NAME=subject),
                target / "process",
            ), reference

        (process, stdout, stderr), _ = launch("seed", setup)
        try:
            process.wait(timeout=300)
        finally:
            code = finish(
                process, stdout, stderr, out / "seed/process", process.poll() is None
            )
        assert code == 0, "Original Iceberg seed failed"
        if server is not None:
            for table in ["source", "id_only_probe"] + [
                s["name"] for s in case_specs()
            ]:
                rest.call(
                    "GET",
                    "/api/metalakes/"
                    + metalake
                    + "/catalogs/iceberg/schemas/"
                    + database
                    + "/tables/"
                    + table,
                )
        cases = []
        exits = {"seed": file_ref(out / "seed/process/exit.json")}
        for profile in subjects:
            (process, stdout, stderr), initial_ref = launch(
                profile, subjects[profile], profile
            )
            try:
                process.wait(timeout=600)
            finally:
                code = finish(
                    process,
                    stdout,
                    stderr,
                    out / profile / "process",
                    process.poll() is None,
                )
            exits[profile] = file_ref(out / profile / "process/exit.json")
            proxy.wait_drained(30)
            request = read(out / profile / "request.private.json")
            selected_http = proxy.events.read_bytes()[request["pluginHttpFirstByte"] :]
            http_slice = out / profile / "plugin-http.jsonl"
            http_slice.write_bytes(selected_http)
            events = [json.loads(line) for line in selected_http.splitlines()]
            proxy.disarm()
            if (out / profile / "cases.json").exists():
                for case in read(out / profile / "cases.json"):
                    wire = wire_policies[initial_ref["path"]]
                    case["policySnapshot"] = initial_ref
                    case["policyDownload"] = {
                        "event": verify_policy_http(
                            events, wire, service, case["pluginPolicyVersion"]
                        ),
                        "rawPolicy": wire,
                        "processHttp": file_ref(http_slice),
                    }
                    case["auditSinks"] = [
                        file_ref(path)
                        for path in (out / profile / "audit").glob("*.log")
                    ]
                    verify_audit_slice(case, case["auditSinks"])
                    cases.append(case)
        (process, stdout, stderr), _ = launch("verify", setup)
        try:
            process.wait(timeout=600)
        finally:
            code = finish(
                process, stdout, stderr, out / "verify/process", process.poll() is None
            )
        exits["verify"] = file_ref(out / "verify/process/exit.json")
        before = read(out / "seed/states.json")
        after = read(out / "verify/states.json")
        for case in cases:
            case["before"] = before[case["target"]]
            case["after"] = after[case["target"]]
        artifacts = {
            str(path.resolve()): sha(path) for path in (home / "jars").iterdir()
        }
        runtime_refs = []
        for profile in subjects:
            observed = read(out / profile / "runtime.json")
            assert observed["spark"] == spark and observed["scala"] == "2.12.21"
            assert observed["sparkUser"] == subjects[profile]
            assert observed["java"].startswith("17.")
            for cls, member, key, version in [
                (
                    "org.apache.ranger.plugin.service.RangerBasePlugin",
                    "META-INF/maven/org.apache.ranger/ranger-plugins-common/pom.properties",
                    "version",
                    "2.8.0",
                ),
                (
                    "org.apache.iceberg.spark.SparkCatalog",
                    "iceberg-build.properties",
                    "git.build.version",
                    "1.11.0",
                ),
                (
                    "org.apache.kyuubi.plugin.spark.authz.ranger.RangerSparkExtension",
                    "META-INF/maven/org.apache.kyuubi/kyuubi-spark-authz_2.12/pom.properties",
                    "version",
                    "1.10.2",
                ),
            ]:
                verify_jar_property(
                    observed["origins"][cls]["artifact"]["path"], member, key, version
                )
            for origin in observed["origins"].values():
                assert (
                    artifacts[origin["artifact"]["path"]]
                    == origin["artifact"]["sha256"]
                )
            actual = read(out / profile / "catalog.json")["actualClass"]
            assert actual == (
                "org.apache.iceberg.spark.SparkCatalog"
                if route == "native"
                else "org.apache.gravitino.spark.connector.iceberg.GravitinoIcebergCatalogSpark35"
            )
            runtime_refs.extend(
                [
                    file_ref(out / profile / "runtime.json"),
                    file_ref(out / profile / "catalog.json"),
                ]
            )
        save(
            out / "runtime-manifest.json",
            {
                "spark": spark,
                "iceberg": "1.11.0",
                "kyuubi": "1.10.2",
                "ranger": "2.8.0",
                "jars": artifacts,
            },
        )
        assert all(
            sha(ROOT / "delta/acceptance" / name) == sha(harness / name)
            for name in harness_files
        ), "Harness changed during comparator"
        assert capture_sources(ROOT) == sources, "Source changed during comparator"
        save(
            out / "observations.json",
            {
                "schemaVersion": 1,
                "spark": spark,
                "engine": "iceberg",
                "catalogRoute": route,
                "catalogClass": actual,
                "verificationSubject": setup,
                "subjects": subjects,
                "service": service,
                "runtimeManifest": file_ref(out / "runtime-manifest.json"),
                "runtimeObservations": runtime_refs,
                "deployedRanger": file_ref(association),
                "pluginHttp": file_ref(proxy.events),
                "cases": [c for c in cases if c["identity"] != "id-only"],
                "supplementalCases": [c for c in cases if c["identity"] == "id-only"],
                "processExits": exits,
                "database": database,
                "executedHarness": harness_refs,
                "artifacts": [
                    file_ref(out / "source-inputs.json"),
                    file_ref(out / "fixture-policies.json"),
                    file_ref(out / "seed/states.json"),
                    file_ref(out / "verify/states.json"),
                ],
                "deploymentExclusions": [
                    "native seeding and verification",
                    "metadata authorization disabled for wrapper",
                    "fixture-authored policies; no public grant propagation",
                ],
            },
        )
    finally:
        proxy.__exit__(None, None, None)
        if server_proxy is not None:
            server_proxy.__exit__(None, None, None)
        if server is not None:
            server.stop()


def run(spark: str, route: str, inputs_path: Path, out: Path) -> None:
    """Run one fresh cell and publish its immutable result after the original worker exits."""
    output = out.resolve()
    inputs_path = inputs_path.resolve()
    assert not output.exists(), "Fresh comparator directory required"
    output.parent.mkdir(parents=True, exist_ok=True)
    directory = output.parent / (output.name + "-supervisor")
    argv = [
        sys.executable,
        Path(__file__).resolve(),
        "--worker",
        "--spark",
        spark,
        "--catalog",
        route,
        "--inputs",
        inputs_path,
        "--out",
        output,
    ]
    process, stdout, stderr = start(argv, dict(os.environ), directory)
    try:
        process.wait(timeout=3600)
    finally:
        code = finish(process, stdout, stderr, directory, process.poll() is None)
    assert code == 0, "Original comparator supervisor failed"
    result = read(output / "observations.json")
    result["processExits"]["supervisor"] = file_ref(directory / "exit.json")
    try:
        verify_iceberg_append(result)
        result["qualification"] = "qualified"
    except AssertionError as exc:
        result["qualification"] = "unqualified"
        result["qualificationFailure"] = str(exc)
    assert not (output / "result.json").exists(), "Immutable result already exists"
    with (output / "result.json").open("x") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
    print(
        "Append comparator", spark, route, result["qualification"], len(result["cases"])
    )
    if result["qualification"] != "qualified":
        sys.exit(2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--spark", required=True, choices=["3.5.4", "3.5.6"])
    parser.add_argument("--catalog", required=True, choices=["native", "gravitino"])
    parser.add_argument("--inputs", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--worker", action="store_true")
    args = parser.parse_args()
    runner = _run_worker if args.worker else run
    runner(
        args.spark, args.catalog, Path(args.inputs).resolve(), Path(args.out).resolve()
    )


if __name__ == "__main__":
    main()

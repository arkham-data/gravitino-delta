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

"""Provision one owned public-grant append matrix fixture; preserve every partial state."""

import argparse
import json
import hashlib
import os
from pathlib import Path
import re
import secrets
import shutil
import time
import uuid
import xml.etree.ElementTree as ET
from urllib.parse import quote
import sys

sys.path.insert(
    0,
    str(
        Path.cwd()
        / ".superpowers/sdd/2026-10-14-delta-native-governed-append/public-grants-live-harness"
    ),
)
from live_common import (
    JAVA,
    Public,
    RECEIPT,
    ROOT,
    Ranger,
    RecordingProxy,
    Server,
    catalog_properties,
    clean_environment,
    command,
    envfile,
    identity,
    save,
    sha,
    source_resources,
    submit,
)


def native(plan, runtime, tables, mode, out, intervals=None):
    from cdf_live import launch, read

    out = Path(out)
    out.mkdir(mode=0o700)
    data = {
        "primary": plan["primary"],
        "spark": runtime["spark"],
        "delta": runtime["delta"],
        "tables": tables,
        "mode": mode,
        "output": str(out),
        "seedAppId": plan["seedAppId"],
        "nativeIdentity": plan["serviceAdmin"],
        "verifyIntervals": intervals or {},
    }
    save(out / "inputs.private.json", data)
    env = clean_environment(plan)
    env.update(
        SPARK_HOME=runtime["sparkHome"],
        PYSPARK_PYTHON=plan["python"],
        SPARK_USER=plan["serviceAdmin"],
        HADOOP_USER_NAME=plan["serviceAdmin"],
    )
    launch(
        [
            runtime["sparkHome"] + "/bin/spark-submit",
            "--driver-memory",
            "2g",
            "--master",
            "local[2]",
            str(ROOT / "delta/acceptance/append_aqe_native.py"),
            str(out / "inputs.private.json"),
        ],
        env,
        out / "process",
        timeout=480,
    )
    return read(out / "result.json")


def run():
    p = argparse.ArgumentParser()
    p.add_argument("--inputs", required=True)
    p.add_argument("--package", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--freeze", required=True)
    a = p.parse_args()
    os.umask(0o077)
    inputs = json.loads(Path(a.inputs).read_text())
    runtime = json.loads(Path(a.package).read_text())
    # Exact modern immutable package gate; no historical runtime relabeling.
    from cdf_read_live_package import verify_runtime

    verify_runtime(runtime)
    smoke = Path(a.package).parent
    association = Path(inputs["deployedRangerEvidence"])
    deployed = json.loads(association.read_text())
    assert (
        deployed["associated"] is True and deployed["deployedRangerVersion"] == "2.8.0"
    )
    for path, h in runtime["jarHashes"].items():
        assert sha(path) == h
    out = Path(a.out).resolve()
    out.mkdir(mode=0o700)
    save(
        out / "lifecycle-start.json",
        {
            "pid": os.getpid(),
            "started": time.time(),
            "runtimeManifest": str(Path(a.package).resolve()),
            "runtimeManifestSha256": sha(a.package),
        },
    )
    for child in ["private", "public-trace", "ranger-trace"]:
        (out / child).mkdir(mode=0o700)
    nonce = uuid.uuid4().hex[:12]
    endpoints = json.loads(Path(inputs["endpoints"]).read_text())
    assert (
        hashlib.sha256(endpoints["ranger-admin"].encode()).hexdigest()
        == deployed["endpointSha256"]
    )
    admin = envfile(inputs["rangerAdminFile"])
    plan = {
        "nonce": nonce,
        "subject": "append_app_" + nonce,
        "metadataPrincipal": "append_meta_" + nonce,
        "serviceAdmin": "append_setup_" + nonce,
        "metalake": "append_lake_" + nonce,
        "catalog": "append_catalog",
        "database": "shared",
        "resourceService": "append_service_" + nonce,
        "resourceDefinition": "append_def_" + nonce,
        "appRole": "append_query_" + nonce,
        "metaRole": "append_metadata_" + nonce,
        "prefix": "public-append-" + nonce,
        "seedAppId": "public-append-seed-" + nonce,
        "javaHome": JAVA,
        "python": str(
            ROOT
            / ".superpowers/runtime-20260914"
            / ("spark-" + runtime["spark"])
            / "bin/python"
        ),
        "primary": {
            "endpoint": endpoints["minio"],
            "region": "us-east-1",
            "bucket": "delta-fixtures",
            "credentialsFile": inputs["minioCredentialsFile"],
        },
        "rangerUrl": endpoints["ranger-admin"],
        "rangerAdmin": {
            "username": admin["RANGER_ADMIN_USER"],
            "password": admin["RANGER_ADMIN_PASSWORD"],
        },
        "plugin": {
            "username": "append_download_" + nonce,
            "password": "Aa1!" + secrets.token_urlsafe(24),
        },
        "runtimeManifest": str(Path(a.package).resolve()),
        "sourceFreeze": str(Path(a.freeze).resolve()),
        "sourceFreezeSha256": sha(a.freeze),
        "smokeEvidence": str(smoke.resolve()),
    }
    from append_aqe_profiles import fixture_tables

    tables = fixture_tables(plan)
    names = [table["name"] for table in tables]
    plan["tables"] = tables
    plan["deployedRangerEvidence"] = str(association.resolve())
    plan["deployedRangerEvidenceSha256"] = sha(association)
    save(out / "state.private.json", plan)
    ranger = Ranger(plan, out / "ranger-trace")
    owned = []

    def create(kind, path, body):
        value = ranger.call("POST", path, body)
        owned.append({"kind": kind, "id": value["id"], "name": body["name"]})
        save(out / "ranger-owned.json", owned)
        return value

    # No existing service, definition, user or native prefix is repurposed.
    create(
        "downloader",
        "/service/xusers/secure/users",
        {
            "name": plan["plugin"]["username"],
            "firstName": "Append",
            "lastName": "Fixture",
            "password": plan["plugin"]["password"],
            "emailAddress": plan["plugin"]["username"] + "@fixture.invalid",
            "userRoleList": ["ROLE_USER"],
            "status": 1,
            "userSource": 0,
            "isVisible": 1,
        },
    )
    definition = json.loads(
        (
            ROOT
            / "authorizations/authorization-ranger-delta/src/main/resources/ranger-delta-servicedef.json"
        ).read_text()
    )
    definition["name"] = plan["resourceDefinition"]
    create("definition", "/service/public/v2/api/servicedef", definition)
    create(
        "service",
        "/service/public/v2/api/service",
        {
            "name": plan["resourceService"],
            "type": plan["resourceDefinition"],
            "isEnabled": True,
            "configs": {
                "create.default.policy.per.hierarchy": "false",
                "policy.download.auth.users": plan["plugin"]["username"],
                "ranger.plugin.spark.gravitino.metalake": plan["metalake"],
            },
        },
    )
    callbacks = RecordingProxy(plan["rangerUrl"], out / "callbacks.private.jsonl")
    callbacks.__enter__()
    plan["callbackUri"] = "http://127.0.0.1:" + str(callbacks.port)
    server = Server(Path(runtime["assemblyManifest"]).parent / "server", out, plan)
    try:
        server.start()
        plan["gravitinoUrl"] = server.uri
        plan["serverPid"] = server.process.pid
        save(out / "state.private.json", plan)
        rest = Public(server.uri, plan["serviceAdmin"], out / "public-trace")
        base = "/api/metalakes/" + plan["metalake"]
        rest.call(
            "POST",
            "/api/metalakes",
            {
                "name": plan["metalake"],
                "comment": "Owned packaged public append acceptance",
                "properties": {},
            },
        )
        props = catalog_properties(plan, "primary", plan["prefix"])
        props.update(
            {
                "authorization-provider": "ranger-delta",
                "authorization.ranger.admin.url": plan["callbackUri"],
                "authorization.ranger.auth.type": "simple",
                "authorization.ranger.username": plan["rangerAdmin"]["username"],
                "authorization.ranger.password": plan["rangerAdmin"]["password"],
                "authorization.ranger.service.name": plan["resourceService"],
                "authorization.ranger.service.type": plan["resourceDefinition"],
                "authorization.ranger.service.create-if-absent": "false",
                "authorization.ranger.delta.metalake": plan["metalake"],
                "authorization.ranger.delta.catalog": plan["catalog"],
            }
        )
        rest.call(
            "POST",
            base + "/catalogs",
            {
                "name": plan["catalog"],
                "type": "RELATIONAL",
                "provider": "lakehouse-delta",
                "comment": "Owned",
                "properties": props,
            },
        )
        rest.call(
            "POST",
            base + "/catalogs/" + plan["catalog"] + "/schemas",
            {"name": plan["database"], "comment": "Owned", "properties": {}},
        )
        native(plan, runtime, tables, "seed", out / "native-seed")
        receipts = {}
        for table in tables:
            receipt = submit(
                rest,
                base,
                plan["catalog"],
                table["name"],
                command(
                    "ADOPT",
                    token=str(uuid.uuid4()),
                    location=table["location"],
                    runId=str(uuid.uuid4()),
                    reference="owned-native-seed-exit-zero",
                ),
            )
            receipts[table["name"]] = receipt
            save(out / "receipts.private.json", receipts)
            receipt = submit(
                rest,
                base,
                plan["catalog"],
                table["name"],
                command("PUBLISH", expected=identity(receipt)),
            )
            assert receipt[RECEIPT + "state"] == "PUBLISHED"
            receipts[table["name"]] = receipt
            save(out / "receipts.private.json", receipts)

        def obj(kind, name, priv):
            return {
                "type": kind,
                "fullName": name,
                "privileges": [{"name": priv, "condition": "ALLOW"}],
            }

        objects = [
            obj("CATALOG", plan["catalog"], "USE_CATALOG"),
            obj("SCHEMA", plan["catalog"] + "." + plan["database"], "USE_SCHEMA"),
        ]
        objects += [
            obj(
                "TABLE",
                plan["catalog"] + "." + plan["database"] + "." + t["name"],
                "SELECT_TABLE",
            )
            for t in tables
        ]
        for user in [plan["subject"], plan["metadataPrincipal"]]:
            rest.call("POST", base + "/users", {"name": user})
        for role, user in [
            (plan["appRole"], plan["subject"]),
            (plan["metaRole"], plan["metadataPrincipal"]),
        ]:
            rest.call(
                "POST",
                base + "/roles",
                {"name": role, "properties": {}, "securableObjects": objects},
            )
            rest.call(
                "PUT",
                base + "/permissions/users/" + quote(user, safe="") + "/grant/",
                {"roleNames": [role]},
            )
        roles = ranger.call(
            "GET",
            "/service/roles/roles?serviceName="
            + quote(plan["resourceService"], safe=""),
        )
        # Read every original admin-role page; serviceName does not limit this endpoint.
        start = len(roles["roles"])
        total = roles["totalCount"]
        while start < total:
            page = ranger.call(
                "GET",
                "/service/roles/roles?serviceName="
                + quote(plan["resourceService"], safe="")
                + "&startIndex="
                + str(start)
                + "&pageSize=200",
            )
            assert (
                page["startIndex"] == start
                and page["totalCount"] == total
                and page["roles"]
            )
            roles["roles"].extend(page["roles"])
            start += len(page["roles"])
        assert len(roles["roles"]) == total
        matches = [
            r
            for r in roles["roles"]
            if re.fullmatch(
                "GRAVITINO_METALAKE_[0-9]+_ROLE_" + re.escape(plan["appRole"]),
                r["name"],
            )
        ]
        assert len(matches) == 1
        app_role = matches[0]
        plan["rangerAppRole"] = app_role["name"]
        assert app_role.get("users") == [{"name": plan["subject"], "isAdmin": False}]
        assert not app_role.get("groups") and not app_role.get("roles")
        resources = source_resources(2, plan)
        mask_resources = source_resources(1, plan)
        mask = create(
            "mask",
            "/service/public/v2/api/policy",
            {
                "service": plan["resourceService"],
                "name": "source_mask_" + nonce,
                "policyType": 1,
                "isEnabled": True,
                "isAuditEnabled": True,
                "resources": mask_resources,
                "dataMaskPolicyItems": [
                    {
                        "roles": [app_role["name"]],
                        "users": [],
                        "groups": [],
                        "accesses": [{"type": "select", "isAllowed": True}],
                        "dataMaskInfo": {
                            "dataMaskType": "CUSTOM",
                            "valueExpr": "'MASKED'",
                        },
                    }
                ],
            },
        )
        filtering = create(
            "filter",
            "/service/public/v2/api/policy",
            {
                "service": plan["resourceService"],
                "name": "source_filter_" + nonce,
                "policyType": 2,
                "isEnabled": True,
                "isAuditEnabled": True,
                "resources": resources,
                "rowFilterPolicyItems": [
                    {
                        "roles": [app_role["name"]],
                        "users": [],
                        "groups": [],
                        "accesses": [{"type": "select", "isAllowed": True}],
                        "rowFilterInfo": {"filterExpr": "region = 'allowed'"},
                    }
                ],
            },
        )
        plan["sourcePolicyIds"] = [mask["id"], filtering["id"]]
        security = out / "private/source-security"
        security.mkdir()
        (out / "private/source-cache").mkdir()
        values = {
            "ranger.plugin.spark.service.name": plan["resourceService"],
            "ranger.plugin.spark.policy.rest.url": plan["rangerUrl"],
            "ranger.plugin.spark.policy.rest.client.username": plan["plugin"][
                "username"
            ],
            "ranger.plugin.spark.policy.rest.client.password": plan["plugin"][
                "password"
            ],
            "ranger.plugin.spark.policy.cache.dir": str(out / "private/source-cache"),
            "ranger.plugin.spark.policy.pollIntervalMs": "500",
            "ranger.plugin.spark.policy.rest.client.connection.timeoutMs": "3000",
            "ranger.plugin.spark.policy.rest.client.read.timeoutMs": "3000",
            "ranger.plugin.spark.policy.rest.client.max.retry.attempts": "0",
            "ranger.plugin.spark.policy.rest.client.cookie.enabled": "false",
            "ranger.plugin.spark.supports.policy.deltas": "false",
        }
        xml = ET.Element("configuration")
        for k, v in values.items():
            n = ET.SubElement(xml, "property")
            ET.SubElement(n, "name").text = k
            ET.SubElement(n, "value").text = v
        ET.indent(xml)
        ET.ElementTree(xml).write(
            security / "ranger-spark-security.xml",
            encoding="utf-8",
            xml_declaration=True,
        )
        shutil.copyfile(
            Path(inputs["sourceSecurityDirectory"]) / "log4j2.xml",
            security / "log4j2.xml",
        )
        (out / "private/metadata.properties").write_text(
            "auth-type=simple\nprincipal="
            + plan["metadataPrincipal"]
            + "\nlifetime-seconds=3600\n"
        )
        plan["sourceSecurityDirectory"] = str(security)
        plan["fixtureDirectory"] = str(out)
        save(out / "state.private.json", plan)
        save(
            out / "ready.json",
            {
                "ready": True,
                "serverPid": server.process.pid,
                "subject": plan["subject"],
                "metadataPrincipal": plan["metadataPrincipal"],
                "serviceAdmin": plan["serviceAdmin"],
                "tables": names,
            },
        )
        print(
            "Fresh public-grant fixture ready; owned server remains active", flush=True
        )
        deadline = time.monotonic() + 7200
        while not (out / "stop-request.json").exists():
            if server.process.poll() is not None:
                raise RuntimeError("Owned server exited")
            if time.monotonic() > deadline:
                raise RuntimeError("Owned fixture supervisor deadline")
            if (out / "restart-request.json").exists() and not (
                out / "restart-complete.json"
            ).exists():
                old_pid = server.process.pid
                server.stop()
                server.column_snapshot("append-before-restart")
                server.start()
                plan["serverPid"] = server.process.pid
                save(
                    out / "restart-complete.json",
                    {
                        "previousServerPid": old_pid,
                        "serverPid": server.process.pid,
                        "sameUri": server.uri == plan["gravitinoUrl"],
                    },
                )
            time.sleep(0.25)
    except BaseException as error:
        save(
            out / "failure.json",
            {"class": type(error).__name__, "noReplay": True, "noCleanup": True},
        )
        raise RuntimeError(
            "Fresh fixture failed; private partial state retained; no replay"
        ) from None
    finally:
        process = server.process
        try:
            server.stop()
        finally:
            try:
                callbacks.wait_drained(20)
            finally:
                callbacks.__exit__()
        if process is None or process.poll() is None:
            raise RuntimeError("Owned server terminal state is unknown")
        save(
            out / "lifecycle-exit.json",
            {
                "pid": os.getpid(),
                "finished": time.time(),
                "serverPid": process.pid,
                "serverExit": process.returncode,
                "stopRequested": (out / "stop-request.json").exists(),
                "failed": (out / "failure.json").exists(),
            },
        )


if __name__ == "__main__":
    run()

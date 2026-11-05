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

"""Owned schema CREATE qualification using the existing downloaded SQL driver.

Every mutation is issued once. Failure evidence survives; targets are never replayed.
"""

import argparse
import hashlib
import http.client
import http.server
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import shutil
import subprocess
import threading
import time
from urllib.parse import urlsplit, unquote
import xml.etree.ElementTree as ET
import zipfile

from cdf_live import (
    ROOT,
    JAVA,
    Public,
    Ranger,
    Server,
    OwnedObjects,
    envfile,
    save,
    sha,
    read,
    catalog_properties,
    prepare_request,
    clean_environment,
    verify_deployed,
    RECEIPT,
)
from run import Rest, RestError
from create_live_checks import (
    verify_case,
    verify_hashes,
    verify_native,
    verify_frontend,
    verify_registration,
)

FIXTURE = "spark-connector/v3.5/spark-delta/src/test/resources/native-create/spark-hub-options.json"
ORDINARY_FIXTURE = FIXTURE.replace("options.json", "ordinary-schema.json")
ORDINARY_CASES = ["no_grant", "allowed", "revoked", "specific_deny", "invalid_nested"]
HELPERS = {
    "DeltaRangerDownloadedQuery",
    "DeltaRangerDownloadedCommand",
    "DeltaRangerDownloadedSecurity",
    "DeltaRangerCallbackGateway",
    "DeltaRangerRetainedQueries",
    "DeltaRangerRetainedAction",
    "DeltaRangerStartupCache",
    "DeltaRangerStartupProbe",
}
CASES = [
    "no_grant",
    "other_schema",
    "allowed",
    "revoked",
    "direct_removed",
    "group_allowed",
    "group_removed",
    "specific_deny",
    "wildcard_allowed",
]


def validate_inputs(inputs):
    required = {
        "endpoints",
        "rangerAdminFile",
        "minioCredentialsFile",
        "rangerRootProof",
        "javaHome",
        "sourceSecurityDirectory",
        "sparkHomes",
        "python",
        "fixtureResource",
        "fixtureSha256",
        "identities",
        "targets",
        "requiredDownloads",
        "profile",
    }
    ordinary = inputs.get("fixtureResource") == ORDINARY_FIXTURE
    if ordinary:
        required.update({"frontend", "pythonRuntime"})
        assert set(inputs) == required, "Unknown/missing ordinary fixture input"
        assert inputs["frontend"] in ("sql", "pyspark")
        assert Path(inputs["pythonRuntime"]).is_file()
    cases = ORDINARY_CASES if ordinary else CASES
    assert set(inputs) == required, "Unknown/missing fixture input"
    identities = inputs["identities"]
    assert set(identities) == {
        "nonce",
        "subject",
        "groupSubject",
        "group",
        "metadataPrincipal",
        "serviceAdmin",
        "metalake",
        "catalog",
        "database",
        "otherDatabase",
        "resourceService",
        "resourceDefinition",
        "appRole",
        "otherRole",
        "visibilityRole",
        "prefix",
    }
    assert all(
        isinstance(v, str) and re.fullmatch("[a-z][a-z0-9_]{2,80}", v)
        for v in identities.values()
    )
    assert (
        len(
            {
                identities[k]
                for k in [
                    "subject",
                    "groupSubject",
                    "metadataPrincipal",
                    "serviceAdmin",
                ]
            }
        )
        == 4
    )
    assert identities["database"] != identities["otherDatabase"]
    assert inputs["profile"] in ("full", "repeat")
    assert set(inputs["targets"]) == set(cases)
    assert all(
        re.fullmatch("[a-z][a-z0-9_]{2,80}", v) for v in inputs["targets"].values()
    )
    assert len(set(inputs["targets"].values())) == len(cases)
    assert set(inputs["requiredDownloads"]) == {"policy", "role"}
    assert all(type(v) is int and v >= 0 for v in inputs["requiredDownloads"].values())
    assert set(inputs["sparkHomes"]) == {"3.5.4", "3.5.6"}, (
        "Unknown/missing stock runtime input"
    )
    assert all(Path(path).is_dir() for path in inputs["sparkHomes"].values())
    assert (Path(inputs["sourceSecurityDirectory"]) / "log4j2.xml").is_file()
    assert inputs["javaHome"] == JAVA
    assert (
        inputs["fixtureResource"] in (FIXTURE, ORDINARY_FIXTURE)
        and sha(ROOT / inputs["fixtureResource"]) == inputs["fixtureSha256"]
    )
    assert set(read(inputs["endpoints"])) == {"ranger-admin", "minio"}
    assert all(
        urlsplit(v).hostname in ("127.0.0.1", "localhost")
        for v in read(inputs["endpoints"]).values()
    )
    for key in ["rangerAdminFile", "minioCredentialsFile", "rangerRootProof", "python"]:
        assert Path(inputs[key]).is_file(), "Missing prerequisite: " + key
    return inputs


def validate_runtime(path):
    runtime = read(path)
    assert runtime["spark"] in ("3.5.4", "3.5.6") and runtime["delta"] in (
        "3.3.0",
        "3.3.2",
    )
    assert runtime["javaHome"] == JAVA and runtime["executionMode"] == "local[2]"
    verify_hashes(runtime["jarHashes"], {p: sha(p) for p in runtime["jarHashes"]})
    assert sha(runtime["assemblyManifest"]) == runtime["assemblySha256"]
    assert sha(runtime["buildReceipt"]) == runtime["buildReceiptSha256"]
    assembly, build = read(runtime["assemblyManifest"]), read(runtime["buildReceipt"])
    assert read(build["exit"])["exitCode"] == 0
    assert assembly["buildInvocation"] == build["invocation"]
    for when in ["before", "after"]:
        assert sha(build[when]) == build[when + "Sha256"]
        assert read(build[when])["files"] == assembly["sourceInputs"]
    # Unchanged-production reuse is allowed only after this fresh assembly. Never
    # remove the predecessor source match to reuse an obsolete base/plugin.
    sources = {
        p: d
        for p, d in assembly["sourceInputs"].items()
        if "/src/main/" in p
        or p.endswith((".gradle", ".gradle.kts"))
        or Path(p).stem in HELPERS
        or p.startswith(("gradle/", "buildSrc/", "delta/acceptance/"))
        or "/spark-delta-ranger/python/" in p
        or p in (FIXTURE, ORDINARY_FIXTURE)
        or p in ("gradlew", "gradle.properties")
    }
    verify_hashes(sources, {p: sha(ROOT / p) for p in sources})
    verify_hashes(
        assembly["artifactFiles"], {p: sha(p) for p in assembly["artifactFiles"]}
    )
    for entries in assembly["runtimeInputs"].values():
        for entry in entries:
            assert (
                sha(entry.get("capturedProjectArtifact", entry["source"]))
                == entry["sha256"]
            )
    jars = list(runtime["jarHashes"])
    inventory = {name: [] for name in runtime["providers"]}
    for jar in jars:
        with zipfile.ZipFile(jar) as archive:
            names = set(archive.namelist())
        for name in inventory:
            if name.replace(".", "/") + ".class" in names:
                inventory[name].append(str(Path(jar).resolve()))
    assert all(
        paths == [str(Path(runtime["providers"][name]).resolve())]
        for name, paths in inventory.items()
    )
    with zipfile.ZipFile(runtime["app"]) as archive:
        classes = {
            n: hashlib.sha256(archive.read(n)).hexdigest()
            for n in archive.namelist()
            if n.endswith(".class")
        }
    verify_hashes(runtime["thinClasses"], classes)
    assert {
        Path(n).name.split("$")[0].removesuffix(".class") for n in classes
    } == HELPERS
    assert len(runtime["rangerProvenance"]) == 6 and all(
        "version=2.8.0" in v for v in runtime["rangerProvenance"].values()
    )
    for artifact in ["delta-spark_2.12", "delta-storage"]:
        assert (
            sum(
                Path(p).name == artifact + "-" + runtime["delta"] + ".jar" for p in jars
            )
            == 1
        )
    if "pythonZip" in runtime:
        assert sha(runtime["pythonZip"]) == runtime["pythonZipSha256"]
        with zipfile.ZipFile(runtime["pythonZip"]) as archive:
            members = {
                n: hashlib.sha256(archive.read(n)).hexdigest()
                for n in archive.namelist()
                if n.endswith(".py")
            }
        prefix = "spark-connector/v3.5/spark-delta-ranger/python/"
        assert members == {
            p[len(prefix) :]: digest
            for p, digest in assembly["sourceInputs"].items()
            if p.startswith(prefix + "gravitino_delta/") and p.endswith(".py")
        }
        assert runtime["pythonModules"] == {
            n.removesuffix(".py").replace("/", ".").removesuffix(".__init__"): digest
            for n, digest in members.items()
        }
    runtime["manifestPath"] = str(path)
    return runtime


class WireRecorder:
    """Private byte recorder; forwards once, without fault injection or retries."""

    def __init__(self, upstream, directory, port=0):
        self.directory = Path(directory)
        self.directory.mkdir(mode=0o700)
        self.upstream = urlsplit(upstream)
        self.lock = threading.Lock()
        self.counter = 0
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def forward(self):
                with owner.lock:
                    owner.counter += 1
                    sequence = owner.counter
                prefix = owner.directory / str(sequence)
                body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                prefix.with_suffix(".request.bin").write_bytes(body)
                event = {
                    "method": self.command,
                    "path": self.path,
                    "started": time.time(),
                    "completed": False,
                    "request": str(prefix.with_suffix(".request.bin")),
                    "requestSha256": sha(prefix.with_suffix(".request.bin")),
                }
                connection = http.client.HTTPConnection(
                    owner.upstream.hostname, owner.upstream.port, timeout=180
                )
                try:
                    headers = {
                        k: v
                        for k, v in self.headers.items()
                        if k.lower() not in ("host", "connection", "content-length")
                    }
                    connection.request(self.command, self.path, body, headers)
                    response = connection.getresponse()
                    raw = response.read()
                    target = prefix.with_suffix(".response.bin")
                    target.write_bytes(raw)
                    event.update(
                        status=response.status,
                        response=str(target),
                        responseSha256=sha(target),
                        completed=True,
                    )
                    self.send_response(response.status)
                    for key, value in response.getheaders():
                        if key.lower() not in (
                            "connection",
                            "transfer-encoding",
                            "content-length",
                        ):
                            self.send_header(key, value)
                    self.send_header("Content-Length", str(len(raw)))
                    self.end_headers()
                    self.wfile.write(raw)
                    event["delivered"] = True
                finally:
                    connection.close()
                    event["finished"] = time.time()
                    save(prefix.with_suffix(".json"), event)

            do_GET = do_POST = do_PUT = do_DELETE = forward

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", port), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.uri = "http://127.0.0.1:" + str(self.server.server_port)

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def events(self):
        return [
            read(p)
            for p in sorted(self.directory.glob("*.json"), key=lambda p: int(p.stem))
        ]


def provision(runtime, inputs, output, handles):
    output.mkdir(mode=0o700)
    (output / "private").mkdir()
    endpoints = read(inputs["endpoints"])
    admin = envfile(inputs["rangerAdminFile"])
    plan = dict(
        inputs["identities"],
        javaHome=JAVA,
        python=inputs["python"],
        fixtureDirectory=str(output),
        rangerUrl=endpoints["ranger-admin"],
        primary={
            "endpoint": endpoints["minio"],
            "region": "us-east-1",
            "bucket": "delta-fixtures",
            "credentialsFile": inputs["minioCredentialsFile"],
        },
        rangerAdmin={
            "username": admin["RANGER_ADMIN_USER"],
            "password": admin["RANGER_ADMIN_PASSWORD"],
        },
        plugin={
            "username": "create_download_" + inputs["identities"]["nonce"],
            "password": "Aa1!" + secrets.token_urlsafe(24),
        },
    )
    save(output / "state.private.json", plan)
    ranger = Ranger(plan, output / "ranger-trace")
    owned = []

    def create(kind, path, body):
        value = ranger.call("POST", path, body)
        owned.append({"kind": kind, "id": value["id"], "name": body["name"]})
        save(output / "ranger-owned.json", owned)
        return value

    create(
        "downloader",
        "/service/xusers/secure/users",
        {
            "name": plan["plugin"]["username"],
            "firstName": "Create",
            "lastName": "Fixture",
            "password": plan["plugin"]["password"],
            "emailAddress": plan["plugin"]["username"] + "@fixture.invalid",
            "userRoleList": ["ROLE_USER"],
            "status": 1,
            "userSource": 0,
            "isVisible": 1,
        },
    )
    definition = read(
        ROOT
        / "authorizations/authorization-ranger-delta/src/main/resources/ranger-delta-servicedef.json"
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
    ranger_wire = WireRecorder(plan["rangerUrl"], output / "ranger-wire")
    handles["rangerWire"] = ranger_wire
    server = Server(Path(runtime["assemblyManifest"]).parent / "server", output, plan)
    handles["server"] = server
    server.start()
    wire = WireRecorder(server.uri, output / "public-wire")
    handles["publicWire"] = wire
    plan["gravitinoUrl"] = wire.uri
    rest = Public(wire.uri, plan["serviceAdmin"], output / "public-trace")
    base = "/api/metalakes/" + plan["metalake"]
    rest.call(
        "POST",
        "/api/metalakes",
        {
            "name": plan["metalake"],
            "comment": "Owned schema CREATE qualification",
            "properties": {},
        },
    )
    props = catalog_properties(plan, "primary", plan["prefix"])
    props.update(
        {
            "authorization-provider": "ranger-delta",
            "authorization.ranger.admin.url": ranger_wire.uri,
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
            "properties": props,
        },
    )
    for database in [plan["database"], plan["otherDatabase"]]:
        rest.call(
            "POST",
            base + "/catalogs/" + plan["catalog"] + "/schemas",
            {"name": database, "properties": {}},
        )
    for user in [plan["subject"], plan["groupSubject"], plan["metadataPrincipal"]]:
        rest.call("POST", base + "/users", {"name": user})
    rest.call("POST", base + "/groups", {"name": plan["group"]})
    # Isolated fixture catalog ownership supplies metadata reads for future names.
    rest.call(
        "PUT",
        base + "/owners/catalog/" + plan["catalog"],
        {"name": plan["metadataPrincipal"], "type": "USER"},
    )

    def obj(kind, name, privileges):
        return {
            "type": kind,
            "fullName": name,
            "privileges": [{"name": v, "condition": "ALLOW"} for v in privileges],
        }

    use = [obj("CATALOG", plan["catalog"], ["USE_CATALOG"])] + [
        obj("SCHEMA", plan["catalog"] + "." + d, ["USE_SCHEMA"])
        for d in [plan["database"], plan["otherDatabase"]]
    ]
    for role, objects in [
        (plan["visibilityRole"], use),
        (
            plan["appRole"],
            [obj("SCHEMA", plan["catalog"] + "." + plan["database"], ["CREATE_TABLE"])],
        ),
        (
            plan["otherRole"],
            [
                obj(
                    "SCHEMA",
                    plan["catalog"] + "." + plan["otherDatabase"],
                    ["CREATE_TABLE"],
                )
            ],
        ),
    ]:
        rest.call(
            "POST",
            base + "/roles",
            {"name": role, "properties": {}, "securableObjects": objects},
        )
    for user in [plan["subject"], plan["groupSubject"]]:
        rest.call(
            "PUT",
            base + "/permissions/users/" + user + "/grant/",
            {"roleNames": [plan["visibilityRole"]]},
        )
    security = output / "private/source-security"
    security.mkdir()
    (output / "private/source-cache").mkdir()
    values = {
        "ranger.plugin.spark.service.name": plan["resourceService"],
        "ranger.plugin.spark.policy.rest.url": ranger_wire.uri,
        "ranger.plugin.spark.policy.rest.client.username": plan["plugin"]["username"],
        "ranger.plugin.spark.policy.rest.client.password": plan["plugin"]["password"],
        "ranger.plugin.spark.policy.cache.dir": str(output / "private/source-cache"),
        "ranger.plugin.spark.policy.pollIntervalMs": "500",
        "ranger.plugin.spark.policy.rest.client.connection.timeoutMs": "3000",
        "ranger.plugin.spark.policy.rest.client.read.timeoutMs": "3000",
        "ranger.plugin.spark.policy.rest.client.max.retry.attempts": "0",
        "ranger.plugin.spark.policy.rest.client.cookie.enabled": "false",
        "ranger.plugin.spark.supports.policy.deltas": "false",
    }
    xml = ET.Element("configuration")
    for key, value in values.items():
        node = ET.SubElement(xml, "property")
        ET.SubElement(node, "name").text = key
        ET.SubElement(node, "value").text = value
    ET.indent(xml)
    ET.ElementTree(xml).write(
        security / "ranger-spark-security.xml", encoding="utf-8", xml_declaration=True
    )
    shutil.copyfile(
        Path(inputs["sourceSecurityDirectory"]) / "log4j2.xml", security / "log4j2.xml"
    )
    (output / "private/metadata.properties").write_text(
        "auth-type=simple\nprincipal="
        + plan["metadataPrincipal"]
        + "\nlifetime-seconds=3600\n"
    )
    plan["sourceSecurityDirectory"] = str(security)
    save(output / "state.private.json", plan)
    return plan


def snapshot(plan, directory, required):
    directory.mkdir(mode=0o700)
    rest = Public(
        plan["gravitinoUrl"], plan["serviceAdmin"], directory / "public-trace"
    )
    ranger = Ranger(plan, directory / "ranger-trace")
    base = "/api/metalakes/" + plan["metalake"]
    public = {
        key: rest.call("GET", base + "/" + kind + "/" + plan[key])
        for key, kind in [
            ("subject", "users"),
            ("groupSubject", "users"),
            ("group", "groups"),
            ("appRole", "roles"),
            ("otherRole", "roles"),
        ]
    }
    public["schemaOwner"] = rest.call(
        "GET", base + "/owners/schema/" + plan["catalog"] + "." + plan["database"]
    )
    public["catalogOwner"] = rest.call(
        "GET", base + "/owners/catalog/" + plan["catalog"]
    )
    save(directory / "public.json", public)
    policies = ranger.call(
        "GET",
        "/service/plugins/secure/policies/download/"
        + plan["resourceService"]
        + "?lastKnownVersion=-1&pluginId=create-supervisor",
        download=True,
    )
    roles = ranger.call(
        "GET",
        "/service/roles/secure/download/"
        + plan["resourceService"]
        + "?lastKnownRoleVersion=-1&pluginId=create-supervisor",
        download=True,
    )
    save(directory / "policies.json", policies)
    save(directory / "roles.json", roles)
    assert policies["serviceName"] == roles["serviceName"] == plan["resourceService"]
    assert (
        policies["policyVersion"] >= required["policy"]
        and roles["roleVersion"] >= required["role"]
    )
    assert public["catalogOwner"]["owner"]["name"] == plan["metadataPrincipal"]
    assert public["schemaOwner"]["owner"]["name"] not in [
        plan["subject"],
        plan["groupSubject"],
    ]
    for key in ["appRole", "otherRole"]:
        assert all(
            p["name"].upper() == "CREATE_TABLE" and p["condition"].upper() == "ALLOW"
            for o in public[key]["role"]["securableObjects"]
            for p in o["privileges"]
        )
    return {
        "policy": policies["policyVersion"],
        "role": roles["roleVersion"],
        "policies": policies,
        "roles": roles,
        "publicStateFile": str(directory / "public.json"),
        "rangerStateFile": str(directory / "policies.json"),
    }


def registration(plan, name, principal, directory):
    path = (
        "/api/metalakes/"
        + plan["metalake"]
        + "/catalogs/"
        + plan["catalog"]
        + "/schemas/"
        + plan["database"]
        + "/tables/"
        + name
    )
    try:
        body = Rest(plan["gravitinoUrl"], principal).call("GET", path)
        status = 200
    except RestError as error:
        body, status = error.result, error.status
    result = {"status": status, "body": body, "principal": principal, "path": path}
    save(directory, result)
    return result


def receipt(properties):
    result = {
        k[len(RECEIPT) :]: v for k, v in properties.items() if k.startswith(RECEIPT)
    }
    if "marker" in result:
        result["marker"] = json.loads(result["marker"])
    return result


def downloaded(state, driver, wire):
    initial = read(driver / "initial-downloads.json")
    ready = read(driver / "ready.json")
    events = [
        json.loads(line)
        for line in (driver / "downloads.jsonl").read_text().splitlines()
    ]
    result = {}
    for kind, field in [("policy", "policies"), ("role", "roles")]:
        selected = [
            e
            for e in events
            if e.get("attempt") == initial[kind + "Attempt"] and "finished" in e
        ]
        assert len(selected) == 1
        event = selected[0]
        assert (
            event["downloadKind"] == kind
            and event["upstreamStatus"] == 200
            and event["downstreamResponseCompleted"] is True
        )
        matches = [
            e
            for e in wire.events()
            if e.get("responseSha256") == event["responseSha256"] and e.get("completed")
        ]
        assert matches, "Completed body not retained"
        document = read(matches[0]["response"])

        def stable(value):
            return {k: v for k, v in value.items() if k != "queryTimeMS"}

        assert stable(document) == stable(state[field]), (
            "Applied model differs from public grant phase"
        )
        result[kind] = {
            "completed": True,
            "version": initial[kind + "Version"],
            "expectedVersion": state[kind],
            "bodySha256": event["responseSha256"],
            "expectedBodySha256": sha(matches[0]["response"]),
            "attempt": initial[kind + "Attempt"],
            "body": matches[0]["response"],
        }
        assert ready["versions"][kind] == state[kind]
    return result


def fixture_settings(inherited, fixture):
    """Restore the native snapshot default for the ordinary Hub profile only."""
    settings = dict(inherited)
    if "schema" in fixture:
        settings.pop("spark.databricks.delta.snapshotPartitions", None)
    settings.update(fixture["settings"])
    return settings


def run_phase(
    plan,
    runtime,
    inputs,
    handles,
    name,
    allowed,
    output,
    group=False,
    group_rest_denied=False,
    expected=None,
    target_name=None,
):
    output.mkdir(mode=0o700)
    expected = expected or (
        "group_rest_denied"
        if group_rest_denied
        else ("published" if allowed else "denied")
    )
    existing = expected in ("noop", "duplicate", "read_denied")
    target = inputs["targets"][target_name or name]
    state = snapshot(plan, output / "state", inputs["requiredDownloads"])
    actor = plan["groupSubject"] if group else plan["subject"]
    if group:
        assert (
            plan["appRole"]
            not in read(state["publicStateFile"])["groupSubject"]["user"]["roles"]
        )
    objects = OwnedObjects(plan["primary"], plan["prefix"] + "/")
    before_objects = objects.manifest(output / "before-objects")
    save(output / "before-objects.json", before_objects)
    before = registration(
        plan, target, plan["metadataPrincipal"], output / "before-registration.json"
    )
    assert before["status"] == (200 if existing else 404), (
        "Metadata identity must distinguish absent names before mutation"
    )
    actor_before = registration(plan, target, actor, output / "before-actor-read.json")
    assert actor_before["status"] in ((200, 403) if existing else (403, 404))
    request_plan = dict(plan, subject=actor)
    request = prepare_request(request_plan, runtime, state, output / "request")
    request["catalogs"] = [plan["catalog"]]
    fixture = read(ROOT / inputs["fixtureResource"])
    request["observedSettingNames"] = sorted(fixture["settings"])
    request["nativeTuningNames"] = [
        "spark.databricks.delta." + key
        for key in [
            "snapshotPartitions",
            "autoCompact.minNumFiles",
            "autoCompact.maxFileSize",
            "autoCompact.minFileSize",
            "optimizeWrite.binSize",
            "optimizeWrite.numShuffleBlocks",
        ]
    ]
    properties = Path(request["sparkConfigurationFile"])
    lines = dict(line.split("=", 1) for line in properties.read_text().splitlines())
    lines = fixture_settings(lines, fixture)
    if group:
        lines["spark.hadoop.hadoop.user.group.static.mapping.overrides"] = (
            actor + "=" + plan["group"]
        )
    lines["spark.driver.extraJavaOptions"] += " -XX:ActiveProcessorCount=2"
    properties.write_text("".join(k + "=" + v + "\n" for k, v in sorted(lines.items())))
    save(output / "effective-settings.json", {k: lines[k] for k in fixture["settings"]})
    save(output / "request/request.private.json", request)
    declaration = dict(fixture)
    if expected == "noop":
        declaration = dict(
            fixture,
            schema={
                "type": "struct",
                "fields": [
                    {
                        "name": "different",
                        "type": "string",
                        "nullable": True,
                        "metadata": {},
                    }
                ],
            },
            sqlColumns="different STRING",
            partitionColumns=[],
            comment="different declaration",
            tableProperties=dict(
                fixture["tableProperties"],
                **{"delta.deletedFileRetentionDuration": "interval 2 days"},
            ),
        )
    elif expected == "invalid":
        declaration = dict(
            fixture,
            schema={
                "type": "struct",
                "fields": [
                    {
                        "name": "nested",
                        "type": {
                            "type": "struct",
                            "fields": [
                                {
                                    "name": "x",
                                    "type": "long",
                                    "nullable": True,
                                    "metadata": {},
                                }
                            ],
                        },
                        "nullable": True,
                        "metadata": {},
                    }
                ],
            },
            sqlColumns="nested STRUCT<x:BIGINT>",
            partitionColumns=[],
        )
    full_name = ".".join([plan["catalog"], plan["database"], target])
    sql = "CREATE TABLE " + ("IF NOT EXISTS " if expected == "noop" else "") + full_name
    sql += " (" + declaration.get("sqlColumns", "id BIGINT") + ") USING delta"
    if declaration.get("partitionColumns"):
        sql += " PARTITIONED BY (" + ", ".join(declaration["partitionColumns"]) + ")"
    if "comment" in declaration:
        sql += " COMMENT " + json.dumps(declaration["comment"], ensure_ascii=False)
    sql += (
        " TBLPROPERTIES ("
        + ", ".join(
            "'" + k + "'='" + v + "'" for k, v in declaration["tableProperties"].items()
        )
        + ")"
    )
    if expected == "read_denied":
        sql = "SELECT * FROM " + full_name
    command = dict(id=name, sql=sql, minPolicy=state["policy"], minRole=state["role"])
    command.update(
        {"denied": True}
        if expected in ("denied", "read_denied")
        else {"expectedRows": []}
    )
    save(output / "command.json", command)
    save(output / "declaration.json", declaration)
    frontend = inputs.get("frontend", "sql")
    request.update(
        commandId=name,
        expected=expected,
        operation=dict(
            kind="read" if expected == "read_denied" else "create",
            name=full_name,
            schema=declaration.get("schema"),
            partitionColumns=declaration.get("partitionColumns", []),
            properties=declaration["tableProperties"],
            comment=declaration.get("comment"),
            ifNotExists=expected == "noop",
        ),
        driverOutput=str(output / "driver"),
        pythonOutput=str(output / "python"),
        fixtureSha256=inputs["fixtureSha256"],
    )
    common_args = [
        "--driver-memory",
        "1536m",
        "--master",
        "local[2]",
        "--properties-file",
        str(properties),
        "--driver-class-path",
        request["driverSecurityDirectory"],
    ]
    argv = (
        [runtime["sparkHome"] + "/bin/spark-submit"]
        + common_args
        + [
            "--class",
            "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerDownloadedQuery",
            runtime["app"],
            str(output / "request/request.private.json"),
            str(output / "driver"),
        ]
    )
    if frontend == "pyspark":
        request["pythonZip"] = runtime["pythonZip"]
        argv = [
            inputs["pythonRuntime"],
            str(ROOT / "delta/acceptance/create_pyspark.py"),
            str(output / "request/request.private.json"),
        ]
    save(output / "request/request.private.json", request)
    environment = clean_environment(plan)
    environment.update(
        SPARK_HOME=runtime["sparkHome"], SPARK_USER=actor, HADOOP_USER_NAME=actor
    )
    if frontend == "pyspark":
        environment.update(
            PYSPARK_PIN_THREAD="true",
            PYSPARK_PYTHON=inputs["pythonRuntime"],
            PYSPARK_DRIVER_PYTHON=inputs["pythonRuntime"],
            PYTHONPATH=runtime["pythonZip"],
            PYSPARK_SUBMIT_ARGS=shlex.join(common_args + ["pyspark-shell"]),
        )
    save(
        output / "process-command.json",
        {"argv": argv, "JAVA_HOME": JAVA, "subject": actor, "started": time.time()},
    )
    start_wire = handles["publicWire"].counter
    with (output / "process.private.log").open("wb") as log:
        process = subprocess.Popen(
            argv,
            env=environment,
            cwd=ROOT,
            stdin=subprocess.PIPE,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        save(output / "process.json", {"pid": process.pid, "started": time.time()})
        try:
            process.communicate(
                (json.dumps(command) + '\n{"close":true}\n').encode(), timeout=300
            )
        except subprocess.TimeoutExpired:
            process.terminate()
            process.wait(timeout=30)
    save(
        output / "exit.json",
        {"exitCode": process.returncode, "finished": time.time(), "pid": process.pid},
    )
    # Observe original effects before checking success or considering a fresh fixture.
    after_objects = objects.manifest(output / "after-objects")
    save(output / "after-objects.json", after_objects)
    after = registration(
        plan, target, plan["metadataPrincipal"], output / "after-registration.json"
    )
    old = {v["key"]: v for v in before_objects}
    new = [v for v in after_objects if v["key"] not in old]
    assert all(v == old[v["key"]] for v in after_objects if v["key"] in old), (
        "Existing owned bytes changed"
    )
    wire_events = handles["publicWire"].events()[start_wire:]
    save(output / "public-events.json", wire_events)
    operations = []
    public_requests = []
    for event in wire_events:
        if event["method"] not in ("POST", "PUT"):
            continue
        body = read(event["request"])
        encoded = body.get("properties", {}).get("delta.registration.command")
        for update in body.get("updates", []):
            if update.get("property") == "delta.registration.command":
                encoded = update["value"]
        if encoded:
            operation = json.loads(encoded)["operation"]
            operations.append(operation)
            public_requests.append(
                {
                    "operation": operation,
                    "status": event.get("status"),
                    "completed": event["completed"],
                }
            )
    result = {
        "expected": expected,
        "command": command,
        "before": {
            "registration": before,
            "objects": before_objects if existing else [],
        },
        "after": {"registration": after, "objects": after_objects if existing else new},
        "exitCode": process.returncode,
        "fixture": fixture,
        "settings": read(output / "effective-settings.json"),
        "nativeOutcome": "NOT_COMMITTED",
        "publicationOutcome": "NOT_ATTEMPTED",
        "statusNeeded": False,
        "publicOperations": operations,
        "publicRequests": public_requests,
    }
    logs = [
        v for v in new if v["key"].endswith("/_delta_log/00000000000000000000.json")
    ]
    if logs:
        assert len(logs) == 1
        log0 = (
            output
            / "after-objects"
            / hashlib.sha256(logs[0]["key"].encode()).hexdigest()
        )
        actions = [json.loads(line) for line in log0.read_text().splitlines()]
        metadata = [a["metaData"] for a in actions if "metaData" in a]
        assert len(metadata) == 1
        commit = [a["commitInfo"] for a in actions if "commitInfo" in a]
        assert len(commit) == 1
        assert commit[0]["operation"] == "CREATE TABLE"
        assert not any("add" in a or "remove" in a or "cdc" in a for a in actions)
        result.update(
            logVersion=0,
            nativeOutcome="COMMITTED",
            nativeId=metadata[0]["id"],
            configuration=metadata[0]["configuration"],
            marker=json.loads(
                metadata[0]["configuration"]["gravitino.registration.operation"]
            ),
        )
        save(output / "log0-actions.json", actions)
        checksums = [
            v for v in new if v["key"].endswith("/_delta_log/00000000000000000000.crc")
        ]
        if checksums:
            assert len(checksums) == 1
            checksum = read(
                output
                / "after-objects"
                / hashlib.sha256(checksums[0]["key"].encode()).hexdigest()
            )
            save(
                output / "native-recipe-check.json",
                {"verified": verify_native(actions, checksum, fixture)},
            )
    reservations = []
    for event in wire_events:
        if (
            event["method"] != "POST"
            or not event.get("completed")
            or event["status"] != 200
        ):
            continue
        body = read(event["response"])
        if body.get("table", {}).get("name") == target:
            value = receipt(body["table"]["properties"])
            if value.get("state") == "RESERVED":
                reservations.append(value)
    assert len(reservations) <= 1, "Duplicate original public RESERVE"
    if reservations:
        result["reservation"] = reservations[0]
        r = (
            receipt(after["body"]["table"]["properties"])
            if after["status"] == 200
            else reservations[0]
        )
        rest = Public(
            plan["gravitinoUrl"], plan["serviceAdmin"], output / "status-trace"
        )
        status_command = {
            "version": 1,
            "operation": "STATUS",
            "expected": {
                k: int(r[k]) if k != "token" else r[k]
                for k in ["id", "generation", "token"]
            },
        }
        status = rest.call(
            "PUT",
            before["path"],
            {
                "updates": [
                    {
                        "@type": "setProperty",
                        "property": "delta.registration.command",
                        "value": json.dumps(status_command),
                    }
                ]
            },
        )
        result["status"] = receipt(status["table"]["properties"])
        result["receipt"] = result["status"]
        result["publicationOutcome"] = (
            "PUBLISHED" if result["status"]["state"] == "PUBLISHED" else "NOT_ATTEMPTED"
        )
        result["statusNeeded"] = result["status"]["state"] != "PUBLISHED"
        save(output / "status.json", status)
    if existing:
        result.update(
            nativeOutcome="NOT_ATTEMPTED",
            publicationOutcome="NOT_ATTEMPTED",
            receipt=receipt(before["body"]["table"]["properties"]),
            actorMutations=[e["method"] for e in wire_events if e["method"] != "GET"],
            journalEntries=[
                str(p.relative_to(output / "request/journal"))
                for p in (output / "request/journal").rglob("*")
                if p.is_file()
            ],
            metadataReads=[],
        )
        end = read(output / "exit.json")["finished"]
        for event in wire_events:
            if (
                event["method"] == "GET"
                and event["path"] == before["path"]
                and event["started"] <= end
                and event.get("completed")
                and event.get("status") == 200
            ):
                result["metadataReads"].append(
                    dict(
                        completed=True,
                        status=200,
                        receipt=receipt(read(event["response"])["table"]["properties"]),
                    )
                )
    result["frontend"] = frontend
    save(output / "outcome-before-check.json", result)
    driver = output / "driver"
    if not (driver / (name + ".json")).is_file():
        raise AssertionError(
            "Original driver failed before command; preserved effects; no replay"
        )
    result["result"] = read(driver / (name + ".json"))
    result["downloads"] = downloaded(state, driver, handles["rangerWire"])
    ready = read(driver / "ready.json")
    assert ready["settings"] == fixture["settings"], "Actual session settings differ"
    assert ready["spark"] == runtime["spark"] and ready["user"] == actor
    if group:
        assert ready["groups"] == [plan["group"]]
        result["groupEvidence"] = {
            "groups": ready["groups"],
            "expectedGroup": plan["group"],
            "createRole": plan["appRole"],
            "directRoles": read(state["publicStateFile"])["groupSubject"]["user"][
                "roles"
            ],
        }
    if group_rest_denied:
        result["statusNeeded"] = True
    for origin in ready["origins"].values():
        assert str(Path(unquote(urlsplit(origin).path)).resolve()) in {
            str(Path(p).resolve()) for p in runtime["jarHashes"]
        }
    expected_frontend = dict(
        frontend=frontend,
        spark=runtime["spark"],
        delta=runtime["delta"],
        fixtureSha256=inputs["fixtureSha256"],
    )
    if frontend == "pyspark":
        expected_frontend.update(
            pythonZipSha256=runtime["pythonZipSha256"],
            modules=runtime["pythonModules"],
            pythonExecutable=inputs["pythonRuntime"],
        )
        actual_frontend = read(output / "python/caller.json")["provenance"]
    else:
        actual_frontend = dict(expected_frontend, spark=ready["spark"])
    verify_frontend(expected_frontend, actual_frontend)
    result["frontendProof"] = actual_frontend
    if expected == "published" and "schema" in fixture:
        verify_registration(after["body"]["table"], fixture)
    save(output / "evidence.json", result)
    save(output / "checked.json", verify_case(result))
    return result


def select_deny_policy(policies, resources, actor):
    """Add SELECT denial to the unique owned literal policy without replacing its grants."""
    matches = [policy for policy in policies if policy["resources"] == resources]
    assert len(matches) == 1, "Expected one exact existing literal owner policy"
    result = json.loads(json.dumps(matches[0]))
    assert result.get("denyPolicyItems", []) == [], (
        "Preexisting deny is not this fixture's control"
    )
    result["denyPolicyItems"] = [
        dict(
            users=[actor],
            delegateAdmin=False,
            accesses=[dict(type="select", isAllowed=True)],
        )
    ]
    return result


def run_ordinary(plan, runtime, inputs, handles, output, membership):
    """Run only ordinary CREATE decisions, preserving each original target and result."""
    outcomes = {}

    def phase(name, expected, target=None):
        outcomes[name] = run_phase(
            plan,
            runtime,
            inputs,
            handles,
            name,
            expected in ("published", "noop"),
            output / name,
            expected=expected,
            target_name=target,
        )
        save(
            output / "completed-cases.json",
            {k: v["expected"] for k, v in outcomes.items()},
        )

    def deny(name, access, target):
        ranger = Ranger(plan, output / (name + "-trace"))
        resources = {
            k: dict(values=[v], isExcludes=False, isRecursive=False)
            for k, v in [
                ("catalog", plan["catalog"]),
                ("database", plan["database"]),
                ("table", inputs["targets"][target]),
                ("column", "*"),
            ]
        }
        if access == "select":
            downloaded = ranger.call(
                "GET",
                "/service/plugins/secure/policies/download/"
                + plan["resourceService"]
                + "?lastKnownVersion=-1&pluginId=select-deny-fixture",
                download=True,
            )
            assert downloaded["serviceName"] == plan["resourceService"]
            selected = select_deny_policy(
                downloaded["policies"], resources, plan["subject"]
            )
            path = "/service/public/v2/api/policy/" + str(selected["id"])
            before_policy = ranger.call("GET", path)
            assert before_policy["service"] == plan["resourceService"]
            save(output / (name + "-before.json"), before_policy)
            updated = select_deny_policy([before_policy], resources, plan["subject"])
            ranger.call("PUT", path, updated)
            policy = ranger.call("GET", path)
            for key, value in updated.items():
                if key not in ("version", "updateTime", "updatedBy"):
                    assert policy.get(key) == value, (
                        "Literal policy update changed unrelated content"
                    )
        else:
            policy = ranger.call(
                "POST",
                "/service/public/v2/api/policy",
                dict(
                    service=plan["resourceService"],
                    name=name + "_" + plan["nonce"],
                    policyType=0,
                    isEnabled=True,
                    isAuditEnabled=True,
                    resources=resources,
                    denyPolicyItems=[
                        dict(
                            users=[plan["subject"]],
                            groups=[],
                            roles=[],
                            accesses=[dict(type=access, isAllowed=True)],
                        )
                    ],
                ),
            )
        save(output / (name + ".json"), policy)

    phase("no_grant", "denied")
    membership("users", "subject", "appRole", True)
    phase("allowed", "published")
    phase("noop", "noop", "allowed")
    if inputs["profile"] == "full":
        phase("duplicate", "duplicate", "allowed")
        phase("invalid_nested", "invalid")
        deny("select_deny", "select", "allowed")
        phase("read_denied", "read_denied", "allowed")
        phase("noop_without_select", "noop", "allowed")
        deny("create_deny", "create", "specific_deny")
        phase("specific_deny", "denied")
    membership("users", "subject", "appRole", False)
    phase("revoked", "denied")
    save(
        output / "result.json",
        dict(
            qualified=True,
            spark=runtime["spark"],
            delta=runtime["delta"],
            cases=list(outcomes),
            profile="ordinary-schema-only",
            frontend=inputs["frontend"],
            groupOnlyCreate="UNSUPPORTED_SIMPLE_GROUPS",
            liveMidOperationFaults=False,
        ),
    )


def run(runtime_path, inputs_path, output):
    inputs = validate_inputs(read(inputs_path))
    runtime = validate_runtime(runtime_path)
    assert (
        inputs["fixtureSha256"]
        == read(runtime["assemblyManifest"])["sourceInputs"][inputs["fixtureResource"]]
    ), "Fixture changed since the source-bound build"
    output.mkdir(mode=0o700)
    save(
        output / "harness-source.json",
        {
            str(ROOT / "delta/acceptance" / name): sha(ROOT / "delta/acceptance" / name)
            for name in ["create_live.py", "create_live_checks.py"]
        },
    )
    save(
        output / "inputs-binding.json",
        {
            "path": str(inputs_path),
            "sha256": sha(inputs_path),
            "runtime": str(runtime_path),
            "runtimeSha256": sha(runtime_path),
            "fixture": inputs["fixtureResource"],
            "fixtureSha256": sha(ROOT / inputs["fixtureResource"]),
        },
    )
    verify_deployed(inputs, output / "deployed-association")
    handles = {}
    outcomes = {}
    try:
        plan = provision(runtime, inputs, output / "fixture", handles)
        rest = Public(
            plan["gravitinoUrl"], plan["serviceAdmin"], output / "transitions"
        )
        base = "/api/metalakes/" + plan["metalake"]

        def membership(who, name, role, grant):
            rest.call(
                "PUT",
                base
                + "/permissions/"
                + who
                + "/"
                + plan[name]
                + ("/grant/" if grant else "/revoke/"),
                {"roleNames": [plan[role]]},
            )

        def phase(name, allowed=False, group=False, group_rest_denied=False):
            outcomes[name] = run_phase(
                plan,
                runtime,
                inputs,
                handles,
                name,
                allowed,
                output / name,
                group,
                group_rest_denied,
            )
            save(
                output / "completed-cases.json",
                {k: v["publicationOutcome"] for k, v in outcomes.items()},
            )

        if inputs["fixtureResource"] == ORDINARY_FIXTURE:
            run_ordinary(plan, runtime, inputs, handles, output, membership)
            return
        phase("no_grant")
        if inputs["profile"] == "full":
            membership("users", "subject", "otherRole", True)
            phase("other_schema")
            membership("users", "subject", "otherRole", False)
        membership("users", "subject", "appRole", True)
        phase("allowed", True)
        rest.call(
            "PUT",
            base
            + "/permissions/roles/"
            + plan["appRole"]
            + "/schema/"
            + plan["catalog"]
            + "."
            + plan["database"]
            + "/revoke/",
            {"privileges": [{"name": "CREATE_TABLE", "condition": "ALLOW"}]},
        )
        phase("revoked")
        if inputs["profile"] == "full":
            rest.call(
                "PUT",
                base
                + "/permissions/roles/"
                + plan["appRole"]
                + "/schema/"
                + plan["catalog"]
                + "."
                + plan["database"]
                + "/grant/",
                {"privileges": [{"name": "CREATE_TABLE", "condition": "ALLOW"}]},
            )
            membership("users", "subject", "appRole", False)
            phase("direct_removed")
            membership("groups", "group", "appRole", True)
            phase("group_allowed", True, True, True)
            membership("groups", "group", "appRole", False)
            phase("group_removed", False, True)
            membership("users", "subject", "appRole", True)
            ranger = Ranger(plan, output / "deny-trace")
            resources = {
                k: {"values": [v], "isExcludes": False, "isRecursive": False}
                for k, v in [
                    ("catalog", plan["catalog"]),
                    ("database", plan["database"]),
                    ("table", inputs["targets"]["specific_deny"]),
                    ("column", "*"),
                ]
            }
            deny = ranger.call(
                "POST",
                "/service/public/v2/api/policy",
                {
                    "service": plan["resourceService"],
                    "name": "literal_create_deny_" + plan["nonce"],
                    "policyType": 0,
                    "isEnabled": True,
                    "isAuditEnabled": True,
                    "resources": resources,
                    "denyPolicyItems": [
                        {
                            "users": [plan["subject"]],
                            "groups": [],
                            "roles": [],
                            "accesses": [{"type": "create", "isAllowed": True}],
                        }
                    ],
                },
            )
            save(
                output / "literal-deny.json",
                ranger.call("GET", "/service/public/v2/api/policy/" + str(deny["id"])),
            )
            phase("specific_deny")
            phase("wildcard_allowed", True)
        save(
            output / "result.json",
            {
                "qualified": True,
                "spark": runtime["spark"],
                "delta": runtime["delta"],
                "cases": list(outcomes),
                "profile": "initial-schema-only-direct-user",
                "groupOnlyCreate": "UNSUPPORTED_SIMPLE_GROUPS",
                "liveMidOperationFaults": False,
            },
        )
    finally:
        if "publicWire" in handles:
            handles["publicWire"].close()
        if "server" in handles:
            handles["server"].stop()
        if "rangerWire" in handles:
            handles["rangerWire"].close()


def main():
    parser = argparse.ArgumentParser()
    for name in ["runtime", "inputs", "output"]:
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    run(
        Path(args.runtime).resolve(),
        Path(args.inputs).resolve(),
        Path(args.output).resolve(),
    )


if __name__ == "__main__":
    main()

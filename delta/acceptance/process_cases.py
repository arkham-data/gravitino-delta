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

"""Black-box recovery, observational adoption, and provider storage isolation cases."""

import json
from pathlib import Path
import uuid
import time

from harness import catalog_close_completed, manifests_equal, write_json
from s3_objects import OwnedObjects

RECEIPT = "delta.registration.receipt."


def command(operation, **fields):
    return dict(version=1, operation=operation, **fields)


def submit(rest, base, catalog, table, payload):
    path = base + "/catalogs/" + catalog + "/schemas/shared/tables"
    encoded = json.dumps(payload, separators=(",", ":"))
    if payload["operation"] in ("RESERVE", "ADOPT"):
        result = rest.call(
            "POST",
            path,
            {
                "name": table,
                "columns": [],
                "properties": {"delta.registration.command": encoded},
            },
        )
    else:
        result = rest.call(
            "PUT",
            path + "/" + table,
            {
                "updates": [
                    {
                        "@type": "setProperty",
                        "property": "delta.registration.command",
                        "value": encoded,
                    }
                ]
            },
        )
    return result["table"]["properties"]


def identity(receipt):
    return {
        "id": int(receipt[RECEIPT + "id"]),
        "generation": int(receipt[RECEIPT + "generation"]),
        "token": receipt[RECEIPT + "token"],
    }


def recovery_cases(inputs, output, server, proxy, rest, base, case, launch):
    records = []
    for name, op, mode, action, expected in (
        ("reserve_loss", "RESERVE", "drop-after", "ABORT", "ABANDONED"),
        ("commit_before_publish", "PUBLISH", "drop-before", "PUBLISH", "PUBLISHED"),
        ("publish_loss", "PUBLISH", "drop-after", "STATUS", "PUBLISHED"),
    ):
        fault = dict(
            case,
            mode="single-create" if name == "publish_loss" else "fault-create",
            table=name,
        )
        first_event = (
            len(proxy.events.read_text().splitlines()) if proxy.events.exists() else 0
        )
        proxy.arm(op, mode, persistent=(mode == "drop-before"))
        writer_dir = output / ("fault-" + name)
        code = launch(fault, writer_dir)
        if (name == "publish_loss" and code != 0) or (
            name != "publish_loss" and code == 0
        ):
            raise AssertionError("Unexpected transport-loss driver exit")
        proxy.wait_drained(timeout=30)
        write_json(
            output / ("drain-" + name + ".json"),
            {
                "driver_exit": code,
                "accept_loop_joined": not proxy.thread.is_alive(),
                "accepted_connections_remaining": proxy.active,
                "unresolved_upstream_completions": proxy.unresolved,
                "listener_closed": proxy.paused,
                "drain_proven": proxy.drain_proven,
            },
        )
        proxy.disarm()
        if name != "publish_loss":
            log = (writer_dir / "process.log").read_text()
            if (
                "DELTA_REGISTRATION_STATUS_NEEDED" not in log
                and "DELTA_PUBLICATION_STATUS_NEEDED" not in log
            ):
                raise AssertionError(
                    "Driver failed without product uncertainty diagnostic"
                )
        events = [json.loads(line) for line in proxy.events.read_text().splitlines()]
        attempts = [event for event in events[first_event:] if event["operation"] == op]
        if mode == "drop-before" and (
            not attempts
            or any(event["completion"] != "not-forwarded" for event in attempts)
        ):
            raise AssertionError(
                "Publication reached upstream before explicit recovery"
            )
        if op == "RESERVE" and len(attempts) != 1:
            raise AssertionError("Transport replayed reserve POST")
        lost = [
            e
            for e in attempts
            if e.get("downstream") == "dropped" and e["operation"] == op
        ]
        if not lost or (mode == "drop-after" and lost[-1]["completion"] != "confirmed"):
            raise AssertionError("No independent transport completion evidence")
        # Popen.wait above establishes driver exit; server termination waits for the owned process.
        server.stop()
        server.start()
        catalog = case["catalogs"][0]
        entries = []
        for path in (Path(case["journals"]) / catalog).glob("*.json"):
            entry = json.loads(path.read_text())
            if entry["table"] == name:
                entries.append((path, entry))
        if not entries:
            raise AssertionError("Failed driver did not leave durable journal")
        candidates = [pair for pair in entries if pair[1]["command"]["operation"] == op]
        if not candidates:
            raise AssertionError("No journal for uncertain operation")
        entry_path, original = candidates[-1]
        archive = output / ("journal-before-" + name + ".json")
        write_json(archive, original)
        recovery = dict(
            case,
            mode="recovery",
            table=name,
            entry=str(entry_path),
            action=action,
            expectedState=expected,
            handoff=(action in ("PUBLISH", "ABORT")),
            drainReference=f"owned-driver-{writer_dir.name}-exit-{code}-server-restart-{server.starts}",
        )
        count_before = sum(
            e["method"] == "POST" and e["path"].endswith("/tables") for e in events
        )
        recovery_code = launch(recovery, output / ("recover-" + name))
        if recovery_code:
            raise AssertionError("Fresh recovery driver failed: " + name)
        after = [json.loads(line) for line in proxy.events.read_text().splitlines()]
        count_after = sum(
            e["method"] == "POST" and e["path"].endswith("/tables") for e in after
        )
        if count_before != count_after:
            raise AssertionError("Recovery replayed POST")
        records.append(
            {
                "case": name,
                "original_exit": code,
                "recovery_exit": recovery_code,
                "transport": lost[-1],
                "attempts": attempts,
                "post_count_before": count_before,
                "post_count_after": count_after,
            }
        )
    write_json(output / "recovery-summary.json", records)


def adoption_and_isolation(inputs, output, rest, base, case, launch, create_catalog):
    prefix = inputs["prefix"] + "/" + output.name + "/isolation"
    catalogs = ["isolation_primary", "isolation_secondary"]
    stores = {}
    receipts = {}
    for profile, catalog, value in zip(("primary", "secondary"), catalogs, (201, 202)):
        # Identical S3A authority AND object path, distinct endpoints and credentials.
        create_catalog(catalog, profile, prefix)
        store = OwnedObjects(inputs[profile], prefix + "/adoption/table")
        stores[catalog] = store
        location = (
            "s3a://" + inputs[profile]["bucket"] + "/" + prefix + "/adoption/table"
        )
        native = dict(
            case,
            mode="native-adoption",
            profile=profile,
            catalogs=[catalog],
            location=location,
            value=value,
        )
        if launch(native, output / ("native-" + profile)):
            raise AssertionError("Native adoption writer failed")
        before = store.manifest(output / ("objects-before-" + profile))
        if not before:
            raise AssertionError("No owned native objects")
        receipt = submit(
            rest,
            base,
            catalog,
            "adopted",
            command(
                "ADOPT",
                token=str(uuid.uuid4()),
                location=location,
                runId=str(uuid.uuid4()),
                reference="owned-native-driver-exit0-" + profile,
            ),
        )
        if receipt[RECEIPT + "state"] != "ADOPTING":
            raise AssertionError("Adoption did not reserve existing snapshot")
        receipt = submit(
            rest,
            base,
            catalog,
            "adopted",
            command("PUBLISH", expected=identity(receipt)),
        )
        if receipt[RECEIPT + "state"] != "PUBLISHED":
            raise AssertionError("Adoption did not publish")
        after = store.manifest()
        if not manifests_equal(before, after):
            raise AssertionError("Observational adoption changed owned native objects")
        write_json(
            output / ("adoption-" + profile + ".json"),
            {"before": before, "after": after, "receipt": receipt},
        )
        receipts[catalog] = receipt
        read = dict(
            case,
            mode="read",
            profile=profile,
            catalogs=[catalog],
            table="adopted",
            value=value,
        )
        if launch(read, output / ("read-" + profile)):
            raise AssertionError("Fresh profile read failed")
    # Both provider contexts are loaded at this point, with same authority/path and different IDs.
    if (
        receipts[catalogs[0]][RECEIPT + "nativeId"]
        == receipts[catalogs[1]][RECEIPT + "nativeId"]
    ):
        raise AssertionError("Independent stores unexpectedly share native identity")
    loads = []
    for _ in range(3):
        for catalog in catalogs:
            info = rest.call(
                "GET", base + "/catalogs/" + catalog + "/schemas/shared/tables/adopted"
            )["table"]
            if (
                info["properties"][RECEIPT + "nativeId"]
                != receipts[catalog][RECEIPT + "nativeId"]
            ):
                raise AssertionError(
                    "Provider context reused other endpoint filesystem"
                )
            loads.append({"catalog": catalog, "receipt": info["properties"]})
    # Observe completed wrapper cleanup through existing production log messages.
    server_log = Path(case["serverLog"])
    offset = server_log.stat().st_size
    # Disable evicts/closes the primary provider via its ordinary catalog lifecycle.
    rest.call("PATCH", base + "/catalogs/" + catalogs[0], {"inUse": False})
    deadline = time.monotonic() + 30
    while True:
        with server_log.open() as stream:
            stream.seek(offset)
            cleanup_log = stream.read()
        if catalog_close_completed(cleanup_log, catalogs[0]):
            break
        if time.monotonic() >= deadline:
            raise TimeoutError("No observable primary catalog close completion")
        time.sleep(0.05)
    (output / "primary-close-completion.log").write_text(cleanup_log)
    info = rest.call(
        "GET", base + "/catalogs/" + catalogs[1] + "/schemas/shared/tables/adopted"
    )["table"]
    if (
        info["properties"][RECEIPT + "nativeId"]
        != receipts[catalogs[1]][RECEIPT + "nativeId"]
    ):
        raise AssertionError("Closing first catalog damaged second context")
    write_json(
        output / "provider-isolation.json",
        {"loads": loads, "after_primary_close": info},
    )


def verify_pair_objects(inputs, output, rest, base, case):
    records = []
    for catalog in case["catalogs"]:
        prefix = inputs["prefix"] + "/" + output.name + "/" + catalog
        store = OwnedObjects(inputs["primary"], prefix)
        before = store.manifest(output / "native-objects" / catalog)
        checks = []
        for path in sorted((Path(case["journals"]) / catalog).glob("*.json")):
            entry = json.loads(path.read_text())
            if entry["phase"] != "PUBLISHED" or entry["command"]["operation"] not in (
                "RESERVE",
                "BEGIN_REPLACE",
            ):
                continue
            receipt = entry["receipt"]
            location = receipt[RECEIPT + "location"]
            key = (
                location.split("/", 3)[3]
                + "/_delta_log/"
                + f"{int(receipt[RECEIPT + 'nativeVersion']):020d}.json"
            )
            actions = [
                json.loads(line) for line in store.request("GET", key).splitlines()
            ]
            metadata = [a["metaData"] for a in actions if "metaData" in a]
            commits = [a["commitInfo"] for a in actions if "commitInfo" in a]
            if len(metadata) != 1 or len(commits) != 1:
                raise AssertionError(
                    "Publication lacks exact metadata/commitInfo action"
                )
            marker = json.loads(
                metadata[0]["configuration"]["gravitino.registration.operation"]
            )
            opening = entry["command"]
            for field in ("id", "generation", "token", "location"):
                if str(marker[field]) != receipt[RECEIPT + field]:
                    raise AssertionError(
                        "Native operation marker differs from published identity"
                    )
            field = "schemaContract" if "schemaContract" in opening else "schema"
            if marker[field] != opening[field]:
                raise AssertionError(
                    "Native operation marker differs from original schema command"
                )
            expected_operation = (
                "CREATE" if opening["operation"] == "RESERVE" else "REPLACE"
            )
            if (
                marker["operation"] != expected_operation
                or metadata[0]["id"] != receipt[RECEIPT + "nativeId"]
            ):
                raise AssertionError(
                    "Native operation kind/UUID differs from publication"
                )
            if receipt[RECEIPT + "evidence"] != "EXACT_NATIVE_COMMIT":
                raise AssertionError("Provider did not verify exact native commit")
            checks.append(
                {
                    "table": entry["table"],
                    "marker": marker,
                    "commit": commits[0],
                    "object": key,
                }
            )
        tables = rest.call(
            "GET", base + "/catalogs/" + catalog + "/schemas/shared/tables"
        )["identifiers"]
        for table in tables:
            rest.call(
                "GET",
                base
                + "/catalogs/"
                + catalog
                + "/schemas/shared/tables/"
                + table["name"],
            )
        after = store.manifest()
        if not manifests_equal(before, after):
            raise AssertionError("Provider observation mutated native objects")
        records.append(
            {
                "catalog": catalog,
                "before": before,
                "after": after,
                "exact_publications": checks,
            }
        )
    write_json(
        output / ("objects-" + case["spark"] + "-" + case["delta"] + ".json"), records
    )


def authority_and_quarantine(inputs, output, rest, base, case, launch, create_catalog):
    catalog = "authority"
    prefix = inputs["prefix"] + "/" + output.name + "/authority"
    create_catalog(catalog, "primary", prefix)
    actor = "retired_" + uuid.uuid4().hex[:10]
    successor = "successor_" + uuid.uuid4().hex[:10]
    role = "writer_" + uuid.uuid4().hex[:10]
    for user in (actor, successor):
        rest.call("POST", base + "/users", {"name": user})

    def privileges(*names):
        return [{"name": name, "condition": "ALLOW"} for name in names]

    rest.call(
        "POST",
        base + "/roles",
        {
            "name": role,
            "properties": {},
            "securableObjects": [
                {
                    "type": "CATALOG",
                    "fullName": catalog,
                    "privileges": privileges("USE_CATALOG"),
                },
                {
                    "type": "SCHEMA",
                    "fullName": catalog + ".shared",
                    "privileges": privileges(
                        "USE_SCHEMA", "CREATE_TABLE", "MODIFY_TABLE"
                    ),
                },
            ],
        },
    )
    rest.call(
        "PUT", base + "/permissions/users/" + actor + "/grant", {"roleNames": [role]}
    )
    actor_rest = type(rest)(rest.uri, actor)
    actor_case = dict(
        case, mode="single-create", catalogs=[catalog], table="retired", user=actor
    )
    if launch(actor_case, output / "retired-actor-create"):
        raise AssertionError("Original actor native create failed")
    entries = [
        json.loads(file.read_text())
        for file in (Path(case["journals"]) / catalog).glob("*.json")
    ]
    payload = next(
        entry["command"]
        for entry in entries
        if entry["table"] == "retired" and entry["command"]["operation"] == "RESERVE"
    )
    receipt = actor_rest.call(
        "GET", base + "/catalogs/" + catalog + "/schemas/shared/tables/retired"
    )["table"]["properties"]
    ident = identity(receipt)
    rest.call(
        "PUT",
        base + "/owners/table/" + catalog + ".shared.retired",
        {"name": successor, "type": "USER"},
    )
    rest.call(
        "PUT", base + "/permissions/users/" + actor + "/revoke", {"roleNames": [role]}
    )
    denied = []
    for operation in (
        payload,
        command("STATUS", expected=ident),
        command("PUBLISH", expected=ident),
    ):
        try:
            submit(actor_rest, base, catalog, "retired", operation)
        except RuntimeError as error:
            if getattr(error, "status", None) != 403:
                raise
            denied.append({"operation": operation["operation"], "status": error.status})
        else:
            raise AssertionError("Retired actor authority resurrected")
    current_owner = rest.call(
        "GET", base + "/owners/table/" + catalog + ".shared.retired"
    )["owner"]
    if current_owner["name"] != successor:
        raise AssertionError("Denied replay changed owner")
    write_json(
        output / "revoked-authority.json",
        {"denied": denied, "owner": current_owner, "receipt": receipt},
    )
    # Use a successful published native table whose original creation command is durably available.
    quarantine = dict(
        case, mode="single-create", catalogs=[catalog], table="quarantine"
    )
    if launch(quarantine, output / "quarantine-create"):
        raise AssertionError("Quarantine fixture native create failed")
    table_path = base + "/catalogs/" + catalog + "/schemas/shared/tables/quarantine"
    published = rest.call("GET", table_path)["table"]["properties"]
    opening = None
    for file in (Path(case["journals"]) / catalog).glob("*.json"):
        entry = json.loads(file.read_text())
        if (
            entry["table"] == "quarantine"
            and entry["command"]["operation"] == "RESERVE"
        ):
            opening = entry["command"]
    if opening is None:
        raise AssertionError("No original reserve evidence")
    replay_codes = []
    for transferred in (False, True):
        if transferred:
            rest.call(
                "PUT",
                base + "/owners/table/" + catalog + ".shared.quarantine",
                {"name": successor, "type": "USER"},
            )
        try:
            submit(rest, base, catalog, "quarantine", opening)
        except RuntimeError as error:
            if getattr(
                error, "status", None
            ) != 500 or "REGISTRATION_CHANGED" not in error.result.get("message", ""):
                raise
            replay_codes.append(error.status)
        else:
            raise AssertionError("Published POST replay accepted")
    owner = rest.call("GET", base + "/owners/table/" + catalog + ".shared.quarantine")[
        "owner"
    ]
    if owner["name"] != successor:
        raise AssertionError("Rejected POST replay changed successor ownership")
    store = OwnedObjects(inputs["primary"], prefix)
    before = store.manifest(output / "quarantine-original-objects")
    location_key = published[RECEIPT + "location"].split("/", 3)[3]
    old_key = location_key + "/_delta_log/00000000000000000000.json"
    original = store.request("GET", old_key)
    actions = [json.loads(line) for line in original.splitlines()]
    metadata = next(action["metaData"] for action in actions if "metaData" in action)
    metadata["id"] = str(uuid.uuid4())
    # External-log identity fault: append one metadata-only version; preserve all original objects.
    new_key = location_key + "/_delta_log/00000000000000000001.json"
    injected = (
        json.dumps(
            {
                "commitInfo": {
                    "operation": "SET TBLPROPERTIES",
                    "userMetadata": "owned external identity fault",
                }
            }
        )
        + "\n"
        + json.dumps({"metaData": metadata})
        + "\n"
    ).encode()
    store.request("PUT", new_key, data=injected)
    after_fault = store.manifest()
    try:
        rest.call("GET", table_path)
    except RuntimeError as error:
        if getattr(
            error, "status", None
        ) != 404 or "is not published" not in error.result.get("message", ""):
            raise
        rejected = error.status
    else:
        raise AssertionError("Provider returned changed native identity")
    status = submit(rest, base, catalog, "quarantine", command("ADMINISTRATIVE_STATUS"))
    if (
        status[RECEIPT + "state"] != "QUARANTINED"
        or status[RECEIPT + "reason"] != "NATIVE_ID_CHANGED"
    ):
        raise AssertionError("Native-ID change not quarantined")
    for key in ("id", "generation", "token"):
        if status[RECEIPT + key] != published[RECEIPT + key]:
            raise AssertionError(
                "Quarantine changed retained registration identity: " + key
            )
    rejected_case = dict(
        case, mode="reject-identity", catalogs=[catalog], table="quarantine"
    )
    if launch(rejected_case, output / "quarantine-named-load"):
        raise AssertionError("Fresh named load did not reject quarantined identity")
    if not manifests_equal(after_fault, store.manifest()):
        raise AssertionError("Quarantine observation changed native objects")
    write_json(
        output / "native-id-quarantine.json",
        {
            "before": before,
            "after_fault": after_fault,
            "created_object": new_key,
            "status": status,
            "original_published_receipt": published,
            "original_native_id": published[RECEIPT + "nativeId"],
            "injected_native_id": metadata["id"],
            "load_rejected": rejected,
            "published_replay_statuses": replay_codes,
            "method": "External-log identity fault; appended metadata-only version after native driver exit; no objects deleted",
        },
    )

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

"""Bounded public SELECT/MODIFY transitions for owned append acceptance fixtures."""

import copy
import json
from pathlib import Path
import re
import time
from urllib.parse import quote

from cdf_live import Public, Ranger, read, save
from cdf_live_checks import project_modify
from cdf_merge_live_checks import file_ref
from live_common import validate_policy_boundary


def permissions(role):
    values = {}
    for resource in role["securableObjects"]:
        assert resource["fullName"] not in values
        values[resource["fullName"]] = {
            (p["name"].upper(), p["condition"].upper()) for p in resource["privileges"]
        }
    return values


def project_select(policies, role, target):
    values = {p["id"]: copy.deepcopy(p) for p in policies}
    assert len(values) == len(policies)
    policy_id, present = None, False
    for policy in values.values():
        if policy.get("policyType", 0) != 0 or policy["resources"]["table"][
            "values"
        ] != [target]:
            continue
        assert policy_id is None, "Ambiguous canonical policy"
        policy_id = policy["id"]
        retained = []
        for item in policy.get("policyItems", []):
            accesses = item.get("accesses", [])
            if role in item.get("roles", []) and any(
                access["type"] == "select" for access in accesses
            ):
                assert accesses == [{"type": "select", "isAllowed": True}]
                assert not any(
                    item.get(key)
                    for key in ("users", "groups", "conditions", "delegateAdmin")
                )
                assert not present
                present = True
                item["roles"].remove(role)
                if not item["roles"]:
                    continue
            if "roles" in item:
                item["roles"] = sorted(item["roles"])
            retained.append(item)
        policy["policyItems"] = retained
        for key in ("version", "updateTime", "updatedBy"):
            policy.pop(key, None)
    assert policy_id is not None
    return values, policy_id, present


def verify_public_transition(plan, table, privilege, grant, before, after, callbacks):
    """Require the exact requested privilege delta and no metadata, mask or role drift."""
    assert privilege in ("SELECT_TABLE", "MODIFY_TABLE") and type(grant) is bool
    assert before["policies"]["policyVersion"] < after["policies"]["policyVersion"]
    old, new = before["public"], after["public"]
    for key in ("appUser", "metadataUser", "metadataRole"):
        assert old[key] == new[key], (
            "Metadata/application identity or metadata authority changed"
        )
    left, right = permissions(old["appRole"]), permissions(new["appRole"])
    full = plan["catalog"] + "." + plan["database"] + "." + table
    expected = copy.deepcopy(left)
    item = (privilege, "ALLOW")
    expected.setdefault(full, set())
    if grant:
        assert item not in expected[full]
        expected[full].add(item)
    else:
        assert item in expected[full]
        expected[full].remove(item)
    # Public role DTOs may retain or omit an object with no remaining privileges.
    assert {k: v for k, v in expected.items() if v} == {
        k: v for k, v in right.items() if v
    }
    assert before["roles"].get("rangerRoles") == after["roles"].get("rangerRoles")
    assert before["roles"]["roleVersion"] == after["roles"]["roleVersion"]
    projection = project_select if privilege == "SELECT_TABLE" else project_modify
    left, lid, prior = projection(
        before["policies"]["policies"], plan["rangerAppRole"], table
    )
    right, rid, current = projection(
        after["policies"]["policies"], plan["rangerAppRole"], table
    )
    assert lid == rid and left == right, "Unrelated Ranger policy drift"
    assert (prior, current) == ((False, True) if grant else (True, False))
    assert callbacks and any(event["method"] == "PUT" for event in callbacks)
    for event in callbacks:
        assert event["completion"] == "confirmed" and event["downstream"] == "sent"
        assert (
            "transport_error" not in event
            and event["started_ns"] <= event["completed_ns"]
        )
        assert re.fullmatch("[0-9a-f]{64}", event["response_sha256"])
        if event["method"] != "GET":
            assert event["method"] == "PUT" and event["path"].split("?")[0].rstrip(
                "/"
            ) == "/service/public/v2/api/policy/" + str(lid)
            assert event["upstream_status"] in (200, 204)
        else:
            assert event["upstream_status"] == 200 or (
                event["upstream_status"] == 400 and "/roles/name/" in event["path"]
            )


def snapshot(plan, output):
    """Read public state and authenticated full Ranger downloads without repairing state."""
    output = Path(output)
    output.mkdir(mode=0o700)
    rest, ranger = (
        Public(plan["gravitinoUrl"], plan["serviceAdmin"], output / "public-trace"),
        Ranger(plan, output / "ranger-trace"),
    )
    base = "/api/metalakes/" + plan["metalake"]
    public = {
        key: rest.call("GET", base + path)["role" if "Role" in key else "user"]
        for key, path in [
            ("appRole", "/roles/" + plan["appRole"]),
            ("metadataRole", "/roles/" + plan["metaRole"]),
            ("appUser", "/users/" + plan["subject"]),
            ("metadataUser", "/users/" + plan["metadataPrincipal"]),
        ]
    }
    assert (
        len(
            {
                plan["subject"],
                plan["metadataPrincipal"],
                plan["serviceAdmin"],
                plan["plugin"]["username"],
            }
        )
        == 4
    )
    assert public["appUser"]["roles"] == [plan["appRole"]]
    assert public["metadataUser"]["roles"] == [plan["metaRole"]]
    assert ("SELECT_TABLE", "ALLOW") in permissions(public["metadataRole"])[
        plan["catalog"] + "." + plan["database"] + ".source"
    ]
    policies = ranger.call(
        "GET",
        "/service/plugins/secure/policies/download/"
        + quote(plan["resourceService"], safe="")
        + "?lastKnownVersion=-1&pluginId=append-aqe-supervisor",
        download=True,
    )
    roles = ranger.call(
        "GET",
        "/service/roles/secure/download/"
        + quote(plan["resourceService"], safe="")
        + "?lastKnownRoleVersion=-1&pluginId=append-aqe-supervisor",
        download=True,
    )
    validate_policy_boundary(plan, policies["policies"])
    public["appRole"]["rangerRole"] = plan["rangerAppRole"]
    save(output / "public-state.private.json", public)
    save(output / "ranger-state.private.json", dict(policies=policies, roles=roles))
    state = dict(
        publicStateFile=str(output / "public-state.private.json"),
        rangerStateFile=str(output / "ranger-state.private.json"),
        policy=policies["policyVersion"],
        role=roles["roleVersion"],
    )
    save(output / "state-evidence.private.json", state)
    return state


def transition(plan, table, privilege, grant, output):
    """Attempt one public mutation, retaining original request, callbacks and both snapshots."""
    output = Path(output)
    output.mkdir(mode=0o700)
    before = snapshot(plan, output / "before")
    callback_path = Path(plan["fixtureDirectory"]) / "callbacks.private.jsonl"
    cursor = len(callback_path.read_text().splitlines())
    save(
        output / "intent.json",
        dict(
            table=table,
            privilege=privilege,
            grant=grant,
            callbackCursor=cursor,
            startedNs=time.time_ns(),
            replayAllowed=False,
        ),
    )
    rest = Public(plan["gravitinoUrl"], plan["serviceAdmin"], output / "public")
    full = plan["catalog"] + "." + plan["database"] + "." + table
    path = (
        "/api/metalakes/"
        + plan["metalake"]
        + "/permissions/roles/"
        + quote(plan["appRole"], safe="")
        + "/table/"
        + quote(full, safe="")
        + ("/grant/" if grant else "/revoke/")
    )
    rest.call("PUT", path, {"privileges": [{"name": privilege, "condition": "ALLOW"}]})
    callbacks = [
        json.loads(line) for line in callback_path.read_text().splitlines()[cursor:]
    ]
    save(output / "callbacks.private.json", callbacks)
    after = snapshot(plan, output / "after")

    def contents(state):
        return dict(
            public=read(state["publicStateFile"]), **read(state["rangerStateFile"])
        )

    verify_public_transition(
        plan, table, privilege, grant, contents(before), contents(after), callbacks
    )
    descriptor = dict(
        privilege=privilege,
        grant=grant,
        beforePolicy=before["policy"],
        afterPolicy=after["policy"],
        publicRequest=file_ref(output / "public/0001.request.private.json"),
        publicResponse=file_ref(output / "public/0001.response.private.json"),
        callbacks=file_ref(output / "callbacks.private.json"),
        before=before,
        after=after,
    )
    save(output / "transition.json", descriptor)
    return after, output / "transition.json"

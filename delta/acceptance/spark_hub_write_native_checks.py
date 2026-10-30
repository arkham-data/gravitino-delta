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

"""Offline validation for the native-only Spark Hub write characterization."""

import hashlib
import json
from pathlib import Path
import re
import zipfile
from urllib.parse import urlsplit

CASES = (
    "legacy-create",
    "stage-create",
    "absent-create-or-replace",
    "append",
    "append-evolve",
    "append-explicit-false",
    "overwrite",
    "overwrite-empty",
    "overwrite-partitioned",
    "overwrite-explicit-false",
    "refresh-append",
    "auto-merge-precedence",
    "duplicate-same",
    "duplicate-changed",
    "ordinary-hooks",
)
PINS = {"spark": "3.5.6", "delta": "3.3.2", "java": "17", "scala": "2.12.21"}
OUTCOMES = {"committed", "not-committed", "unknown"}
PHASES = {"data", "metadata", "maintenance"}
CASE_STEPS = {name: (name + "-seed",) for name in CASES}
CASE_STEPS.update(
    {
        "legacy-create": ("legacy-create-empty",),
        "stage-create": (),
        "absent-create-or-replace": (),
        "refresh-append": (
            "refresh-append-seed",
            "refresh-append-7",
            "refresh-append-8",
        ),
        "duplicate-same": ("duplicate-same-seed", "duplicate-same-first-intent"),
        "duplicate-changed": (
            "duplicate-changed-seed",
            "duplicate-changed-first-intent",
        ),
        "ordinary-hooks": tuple(
            "ordinary-hooks-tiny-" + str(index) for index in range(6)
        ),
    }
)
STATE_FIELDS = {
    "exists",
    "uuid",
    "version",
    "rows",
    "schema",
    "properties",
    "protocol",
    "actions",
    "manifest",
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def validate_inputs(inputs: dict) -> None:
    """Limit the private request to one local, fresh fixture and Python 3.11."""
    require(
        set(inputs)
        == {"schemaVersion", "python", "primary", "prefix", "nativeIdentity"},
        "Unexpected input fields",
    )
    require(inputs["schemaVersion"] == 1, "Unsupported input schemaVersion")
    primary = inputs["primary"]
    require(
        set(primary) == {"endpoint", "region", "bucket", "credentialsFile"},
        "Unexpected primary fields",
    )
    endpoint = urlsplit(primary["endpoint"])
    require(
        endpoint.scheme == "http"
        and endpoint.hostname
        in {"localhost", "127.0.0.1", "::1", "host.docker.internal", "minio"}
        and not endpoint.username
        and not endpoint.password
        and endpoint.path in {"", "/"}
        and not endpoint.query
        and not endpoint.fragment,
        "A local HTTP loopback/Docker MinIO fixture endpoint is required",
    )
    require(
        bool(re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_./-]+", inputs["prefix"]))
        and ".." not in inputs["prefix"].split("/"),
        "Invalid owned prefix",
    )
    require(
        bool(re.fullmatch(r"[a-zA-Z0-9_-]+", inputs["nativeIdentity"])),
        "Invalid native fixture identity",
    )
    require(
        bool(re.fullmatch(r"[a-z0-9][a-z0-9.-]+", primary["bucket"])),
        "Invalid fixture bucket",
    )
    require(bool(primary["region"]), "Missing fixture region")
    for name, value in (
        ("python", inputs["python"]),
        ("credentialsFile", primary["credentialsFile"]),
    ):
        path = Path(value)
        require(path.is_absolute() and path.is_file(), "Missing absolute " + name)


def new_actions(case):
    return [
        action
        for action in case["after"].get("actions", [])
        if action.get("version", case["before"].get("version", -1) + 1)
        > case["before"].get("version", -1)
    ]


def native_outcomes(before, after, executed):
    """Derive independently evidenced phases; an unobserved phase stays unknown."""
    if not before.get("readback") or not after.get("readback"):
        return dict.fromkeys(("data", "metadata", "maintenance"), "unknown")
    actions = new_actions({"before": before, "after": after})
    data = any(
        ("add" in a and a["add"].get("dataChange", True))
        or ("remove" in a and a["remove"].get("dataChange", True))
        for a in actions
    )
    metadata = any("metaData" in a or "protocol" in a for a in actions)
    maintenance = any(
        a.get("commitInfo", {}).get("operation") == "OPTIMIZE" for a in actions
    )
    return {
        phase: (
            "committed"
            if changed
            else ("not-committed" if phase in executed else "unknown")
        )
        for phase, changed in (
            ("data", data),
            ("metadata", metadata),
            ("maintenance", maintenance),
        )
    }


def _validate_observation(observation, *, case_status, expected_phases):
    name = observation.get("id", "unnamed observation")
    require(
        {
            "before",
            "after",
            "exception",
            "outcomes",
            "executedPhases",
            "settingsBefore",
            "settingsAfter",
        }
        <= set(observation),
        "Incomplete observation state/executedPhases " + name,
    )
    executed = observation["executedPhases"]
    require(
        isinstance(executed, list)
        and all(isinstance(phase, str) for phase in executed)
        and len(executed) == len(set(executed))
        and set(executed) <= PHASES,
        "Invalid executedPhases " + name,
    )
    require(set(executed) == expected_phases, "Wrong fixed executedPhases " + name)
    require(
        isinstance(observation["outcomes"], dict)
        and set(observation["outcomes"]) == PHASES
        and set(observation["outcomes"].values()) <= OUTCOMES,
        "Invalid phase outcomes " + name,
    )
    before, after = observation["before"], observation["after"]
    for state in (before, after):
        require(
            isinstance(state, dict) and isinstance(state.get("readback"), bool),
            "Missing observation readback state " + name,
        )
        if state["readback"]:
            require(
                STATE_FIELDS <= set(state),
                "Incomplete native observation state " + name,
            )
    require(
        observation["outcomes"] == native_outcomes(before, after, executed),
        "Native action-derived outcomes differ; missing readback requires unknown "
        + name,
    )
    derived_status = {
        phase: (
            "attempted-or-native-effect"
            if phase in executed or outcome == "committed"
            else "not-executed"
        )
        for phase, outcome in observation["outcomes"].items()
    }
    require(
        not case_status or "phaseStatus" in observation,
        "Missing case phaseStatus " + name,
    )
    # Original immutable v1 substeps predate phaseStatus. Their required executed
    # phases and action-derived outcomes determine status without inventing a new
    # native observation. Validate a stored substep status whenever supplied.
    if "phaseStatus" in observation:
        require(
            observation["phaseStatus"] == derived_status,
            "Contradictory phaseStatus " + name,
        )
    require(
        observation["settingsBefore"] == observation["settingsAfter"],
        "Session settings not restored " + name,
    )


def _same_boundary(left, right, name):
    if left["readback"] and right["readback"]:
        require(
            all(left[field] == right[field] for field in STATE_FIELDS),
            "Disconnected native observation boundary " + name,
        )


def required_observations(case):
    """Require the fixed register and every original sequential phase boundary."""
    name = case["id"]
    require(
        "steps" in case and isinstance(case["steps"], list),
        "Missing fixed steps observation register " + name,
    )
    steps = case["steps"]
    require(
        all(isinstance(step, dict) and "id" in step for step in steps)
        and tuple(step["id"] for step in steps) == CASE_STEPS[name],
        "Missing, duplicate or reordered fixed steps " + name,
    )
    main_phases = (
        {"data", "maintenance"}
        if name == "ordinary-hooks"
        else (
            {"data", "metadata"}
            if name in {"stage-create", "absent-create-or-replace"}
            else {"data"}
        )
    )
    _validate_observation(case, case_status=True, expected_phases=main_phases)
    for step in steps:
        step_id = step["id"]
        phases = (
            {"metadata"}
            if step_id == "legacy-create-empty"
            else (
                {"data", "metadata"}
                if step_id.endswith("-seed")
                else (
                    {"data", "maintenance"}
                    if step_id.startswith("ordinary-hooks-tiny-")
                    else {"data"}
                )
            )
        )
        _validate_observation(step, case_status=False, expected_phases=phases)
    for previous, following in zip(steps, steps[1:]):
        _same_boundary(previous["after"], following["before"], following["id"])
    if steps:
        first = steps[0]["before"]
        if first["readback"]:
            require(
                not first["exists"]
                and first["uuid"] is None
                and first["version"] == -1
                and first["rows"] == [],
                "Fixed first observation boundary must be absent " + name,
            )
        if name == "ordinary-hooks":
            _same_boundary(first, case["before"], name + " first")
            _same_boundary(steps[-1]["after"], case["after"], name + " last")
        elif name == "refresh-append":
            _same_boundary(steps[0]["after"], case["before"], name + " seed")
            _same_boundary(steps[-1]["after"], case["after"], name + " last")
        else:
            _same_boundary(steps[-1]["after"], case["before"], name + " main")
        if name == "legacy-create" and steps[0]["after"]["readback"]:
            require(
                steps[0]["after"]["exists"]
                and steps[0]["after"]["version"] == 0
                and steps[0]["after"]["rows"] == [],
                "Legacy empty-create observation boundary missing",
            )
    elif case["before"]["readback"]:
        require(
            not case["before"]["exists"] and case["before"]["version"] == -1,
            "Create observation boundary must be absent " + name,
        )
    return [case] + steps


def verify_observations(result: dict) -> dict:
    """Check completeness separately from compatibility and never certify governance."""
    require(result.get("schemaVersion") == 1, "Unsupported result schemaVersion")
    require(result.get("pins") == PINS, "Wrong native runtime pins")
    require(
        not result.get("governedSupport"),
        "Native evidence cannot claim governed support",
    )
    require(
        not result.get("intentValidated"), "Native suppression cannot validate intent"
    )
    require(
        result.get("freshPrefix") == {"emptyBefore": True, "exclusive": True},
        "Fresh exclusive prefix evidence required",
    )
    process = result.get("process", {})
    require(
        all(
            isinstance(process.get(k), dict) and {"path", "sha256"} <= set(process[k])
            for k in ("command", "exit", "stdout", "stderr")
        )
        and "exitCode" in process,
        "Missing original process receipts",
    )
    cases = result.get("cases", [])
    require(
        len(cases) == len(CASES) and {c["id"] for c in cases} == set(CASES),
        "Missing or duplicate required case",
    )
    gaps = [
        "Native transaction suppression does not validate application intent",
        "Native oracle does not exercise Gravitino reservation or Ranger authority",
        "Full refresh must reject dynamic partition overwrite at the future adapter boundary",
    ]
    complete = process["exitCode"] == 0
    compatible = True
    for case in cases:
        name = case["id"]
        require(
            {
                "before",
                "after",
                "exception",
                "outcomes",
                "executedPhases",
                "settingsBefore",
                "settingsAfter",
                "evidence",
            }
            <= set(case),
            "Incomplete case observation or phase declaration " + name,
        )
        require(
            set(case["outcomes"]) == {"data", "metadata", "maintenance"}
            and set(case["outcomes"].values()) <= OUTCOMES,
            "Invalid phase outcomes " + name,
        )
        observations = required_observations(case)
        if any(
            not observation[side]["readback"]
            for observation in observations
            for side in ("before", "after")
        ):
            complete = compatible = False
            gaps.append(name + ": sequential native readback unavailable")
        before, after = case["before"], case["after"]
        if not before.get("readback") or not after.get("readback"):
            require(
                set(case["outcomes"].values()) == {"unknown"},
                "Missing readback requires unknown outcomes " + name,
            )
            complete = compatible = False
            gaps.append(name + ": native readback unavailable")
            continue
        for state in (before, after):
            require(
                {
                    "exists",
                    "uuid",
                    "version",
                    "rows",
                    "schema",
                    "properties",
                    "protocol",
                    "actions",
                    "manifest",
                }
                <= set(state),
                "Incomplete native state " + name,
            )
        require(
            case["settingsBefore"] == case["settingsAfter"],
            "Session settings not restored " + name,
        )
        if case["exception"] is None and after["exists"]:
            require(
                all(
                    isinstance(row, dict) and row.get("__arkham_loaded_at") is not None
                    for row in after["rows"]
                ),
                "Missing native loaded-at row projection " + name,
            )
            defaults = {
                "delta.enableChangeDataFeed": "true",
                "delta.logRetentionDuration": "interval 45 days",
                "delta.deletedFileRetentionDuration": "interval 364 days",
            }
            require(
                all(
                    after["properties"].get(key) == value
                    for key, value in defaults.items()
                ),
                "Native properties changed " + name,
            )
            expected = None
            if name in {"legacy-create", "stage-create", "absent-create-or-replace"}:
                expected = [{"id": 2, "region": "new"}]
            elif name == "append":
                expected = [{"id": 0, "region": "old-only"}, {"id": 2, "region": "new"}]
            elif name == "append-evolve":
                expected = [
                    {"id": 0, "region": "old-only", "extra": None},
                    {"id": 2, "region": "new", "extra": "evolved"},
                ]
            elif name in {"overwrite", "overwrite-partitioned"}:
                expected = [{"id": 8, "region": "new", "replacement": "replacement"}]
            elif name == "refresh-append":
                expected = [
                    {"id": 0, "region": "old-only"},
                    {"id": 2, "region": "new"},
                    {"id": 2, "region": "new"},
                ]
            if expected is not None:
                projection = [
                    {
                        key: value
                        for key, value in row.items()
                        if key != "__arkham_loaded_at"
                    }
                    for row in after["rows"]
                ]
                require(
                    sorted(map(lambda row: json.dumps(row, sort_keys=True), projection))
                    == sorted(
                        map(lambda row: json.dumps(row, sort_keys=True), expected)
                    ),
                    "Unexpected native row multiset " + name,
                )
                expected_fields = set(expected[0]) | {"__arkham_loaded_at"}
                require(
                    {field["name"] for field in after["schema"]["fields"]}
                    == expected_fields,
                    "Unexpected native schema " + name,
                )
            if name not in {"duplicate-same", "duplicate-changed"}:
                commits = [
                    a["commitInfo"]
                    for a in new_actions(case)
                    if "commitInfo" in a
                    and a["commitInfo"].get("operation") != "OPTIMIZE"
                ]
                actual_options = (
                    case["refreshOptions"][-1]["actual"]
                    if name == "refresh-append"
                    else case["actualOptions"]
                )
                expected_metadata = str(actual_options.get("userMetadata"))
                if not commits or not all(
                    commit.get("userMetadata") == expected_metadata
                    for commit in commits
                ):
                    compatible = False
                    gaps.append(
                        name
                        + ": writer userMetadata absent/different in native commitInfo"
                    )
        if name.startswith("overwrite") and case["outcomes"]["data"] == "committed":
            require(
                before["uuid"] == after["uuid"], "Overwrite changed native UUID " + name
            )
        if name == "overwrite-empty" and case["exception"] is None:
            require(after["rows"] == [], "empty refresh retained rows")
        if name == "overwrite-partitioned" and case["exception"] is None:
            require(
                not any(
                    row.get("region") == "old-only"
                    for row in after["rows"]
                    if isinstance(row, dict)
                ),
                "Full overwrite retained untouched old partition",
            )
        if name in {
            "append-explicit-false",
            "overwrite-explicit-false",
            "auto-merge-precedence",
        }:
            require(
                case["exception"] is not None
                and after["version"] == before["version"]
                and after["rows"] == before["rows"],
                "Explicit false must reject without native commit " + name,
            )
        elif case["exception"] is not None:
            gaps.append(
                name
                + ": actual native options rejected; see original exception and readback"
            )
            compatible = False
        if name == "refresh-append":
            refreshes = case.get("refreshOptions", [])
            require(
                {r["method"] for r in refreshes} == {7, 8},
                "Both refresh helper outputs required",
            )
            for refresh in refreshes:
                require(
                    refresh["mode"] == "append", "refresh must preserve APPEND strategy"
                )
                require(
                    refresh["actual"].get("overwriteSchema")
                    == refresh["actual"].get("mergeSchema")
                    == "true",
                    "refresh schema overrides missing",
                )
                require(
                    refresh["raw"]["userMetadata"] == refresh["actual"]["userMetadata"]
                    and refresh["actual"]["userMetadata"]["overwriteSchema"] is False
                    and refresh["actual"]["userMetadata"]["mergeSchema"] is False,
                    "Original refresh metadata flags changed",
                )
        if name.startswith("duplicate-") and case["exception"] is None:
            require(
                after["version"] == before["version"]
                and after["rows"] == before["rows"],
                "Native duplicate suppression missing",
            )
        if name == "ordinary-hooks":
            actions = new_actions(case)
            compacted = (
                any(
                    a.get("commitInfo", {}).get("operation") == "OPTIMIZE"
                    for a in actions
                )
                and any(
                    "remove" in a and a["remove"].get("dataChange") is False
                    for a in actions
                )
                and any(
                    "add" in a and a["add"].get("dataChange") is False for a in actions
                )
            )
            if not compacted:
                compatible = False
                gaps.append(
                    "ordinary-hooks: actual compaction commit/actions unavailable; settings alone insufficient"
                )
            if not after["manifest"]:
                compatible = False
                gaps.append("ordinary-hooks: native symlink manifest unavailable")
            gaps.append(
                "Postcommit hooks expose no independent public failure/recovery seam in this characterization"
            )
    return {
        "characterizationComplete": complete,
        "nativeCandidateViable": complete and compatible,
        "governedSupport": False,
        "gaps": gaps,
    }


def verify_result(path: Path) -> dict:
    """Rehash retained bytes, process receipts and native logs without running code."""
    result = json.loads(Path(path).read_text())
    references = []

    def collect(value):
        if isinstance(value, dict):
            if {"path", "sha256"} <= set(value):
                references.append(value)
            for member in value.values():
                collect(member)
        elif isinstance(value, list):
            for member in value:
                collect(member)

    collect(result)
    for reference in references:
        artifact = Path(reference["path"])
        if reference.get("member") and reference.get("container"):
            container = reference["container"]
            with zipfile.ZipFile(container["path"]) as archive:
                digest = hashlib.sha256(archive.read(reference["member"])).hexdigest()
            require(
                digest == reference["sha256"],
                "Changed Python provider artifact: " + str(artifact),
            )
        else:
            require(
                artifact.is_absolute()
                and artifact.is_file()
                and hashlib.sha256(artifact.read_bytes()).hexdigest()
                == reference["sha256"],
                "Changed or missing artifact: " + str(artifact),
            )
    # Hashes precede semantic validation so a changed artifact is never hidden by a report error.
    verdict = verify_observations(result)
    process = result["process"]
    require(
        json.loads(Path(process["exit"]["path"]).read_text())["exitCode"]
        == process["exitCode"],
        "Original process exit differs",
    )
    command = json.loads(Path(process["command"]["path"]).read_text())
    require(
        command["argv"] and command["request"]["sha256"], "Missing original command"
    )
    runtime = json.loads(Path(result["runtime"]["path"]).read_text())
    require(
        (runtime["spark"], runtime["delta"]) == (PINS["spark"], PINS["delta"]),
        "Runtime manifest pins differ",
    )
    request = json.loads(Path(result["request"]["path"]).read_text())
    require(
        command["request"] == result["request"]
        and command["argv"][-1] == result["request"]["path"],
        "Original command/request binding differs",
    )
    require(
        request["runtime"] == runtime and request["pins"] == result["pins"],
        "Original runtime request differs",
    )
    require(
        set(result["identity"].values()) == {request["nativeIdentity"]},
        "Original native actor differs",
    )
    objects = json.loads(Path(result["objectsAfter"]["path"]).read_text())
    object_index = {item["key"]: item for item in objects}
    for item in objects:
        require(
            item["key"].startswith(request["prefix"].rstrip("/") + "/"),
            "Object escaped owned prefix",
        )
        retained = (
            Path(result["objectsAfter"]["path"]).parent
            / "objects"
            / hashlib.sha256(item["key"].encode()).hexdigest()
        )
        require(
            retained.is_file()
            and retained.stat().st_size == item["size"]
            and hashlib.sha256(retained.read_bytes()).hexdigest() == item["sha256"],
            "Archived native object differs " + item["key"],
        )
    for name, artifact in result["origins"].items():
        require(
            runtime["jarHashes"].get(artifact["path"]) == artifact["sha256"],
            "Actual JVM origin differs " + name,
        )
    require(result["pythonVersion"].startswith("3.11."), "Actual Python pin differs")
    inventory = json.loads(Path(result["importInventory"]["path"]).read_text())
    require(
        {"src/shared/services/v2/delta.py", "src/shared/schemas/v2/base_jobs.py"}
        <= set(inventory["sourceInputs"]),
        "Actual sink helper import closure missing",
    )
    require(
        {"pyspark", "pyspark.sql.readwriter", "py4j"}
        <= set(inventory["pythonProviders"]),
        "Actual PySpark imports missing",
    )
    for case in result["cases"]:
        require(
            json.loads(Path(case["evidence"]["path"]).read_text())
            == {k: v for k, v in case.items() if k != "evidence"},
            "Case evidence differs " + case["id"],
        )
        require(
            case["location"]
            == "s3a://"
            + request["primary"]["bucket"]
            + "/"
            + request["prefix"].rstrip("/")
            + "/"
            + case["id"],
            "Native target escaped fixed scenario location",
        )
        for manifest in case["after"].get("manifest", []):
            require(
                object_index.get(manifest["key"]) == manifest,
                "Final native symlink observation differs",
            )
        observations = required_observations(case)
        for observation in observations:
            require(
                observation["settingsBefore"] == observation["settingsAfter"],
                "Sequential session settings differ " + observation["id"],
            )
            require(
                native_outcomes(
                    observation["before"],
                    observation["after"],
                    observation["executedPhases"],
                )
                == observation["outcomes"],
                "Sequential native outcomes differ " + observation["id"],
            )
        for state in [
            state
            for observation in observations
            for state in (observation["before"], observation["after"])
        ]:
            if not state.get("readback"):
                continue
            actions = []
            for log in state["logs"]:
                actions.extend(
                    dict(json.loads(line), version=log["version"])
                    for line in Path(log["artifact"]["path"]).read_text().splitlines()
                    if line
                )
            require(
                actions == state["actions"], "Native log actions differ " + case["id"]
            )
            metadata = [a["metaData"] for a in actions if "metaData" in a]
            protocols = [a["protocol"] for a in actions if "protocol" in a]
            require(
                state["version"]
                == max((log["version"] for log in state["logs"]), default=-1),
                "Native version differs",
            )
            require(state["exists"] == bool(metadata), "Native existence differs")
            if metadata:
                require(
                    state["uuid"] == metadata[-1]["id"]
                    and state["properties"] == metadata[-1]["configuration"]
                    and state["schema"] == json.loads(metadata[-1]["schemaString"])
                    and state["protocol"] == protocols[-1],
                    "Native metadata/schema/protocol differs",
                )
            require(
                json.loads(Path(state["rowEvidence"]["path"]).read_text())
                == state["rows"],
                "Native row evidence differs",
            )
        require(
            native_outcomes(case["before"], case["after"], case["executedPhases"])
            == case["outcomes"],
            "Native phase outcomes differ " + case["id"],
        )
    return verdict

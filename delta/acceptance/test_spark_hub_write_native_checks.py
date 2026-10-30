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

"""Offline contracts: synthetic reports never qualify a native runtime."""

import copy
import importlib
import json
from pathlib import Path
import sys
import tempfile
import subprocess
import unittest


class TestSparkHubWriteNativeChecks(unittest.TestCase):
    def setUp(self):
        self.checks = importlib.import_module("spark_hub_write_native_checks")
        self.launcher = importlib.import_module("spark_hub_write_native")
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def report(self):
        state = {
            "readback": True,
            "exists": True,
            "uuid": "native-id",
            "version": 1,
            "rows": [
                {"id": 0, "region": "old-only", "__arkham_loaded_at": "timestamp"}
            ],
            "schema": {
                "fields": [{"name": n} for n in ("id", "region", "__arkham_loaded_at")]
            },
            "properties": {
                "delta.enableChangeDataFeed": "true",
                "delta.logRetentionDuration": "interval 45 days",
                "delta.deletedFileRetentionDuration": "interval 364 days",
            },
            "protocol": {},
            "actions": [],
            "manifest": [],
        }
        result = {
            "schemaVersion": 1,
            "pins": {
                "spark": "3.5.6",
                "delta": "3.3.2",
                "java": "17",
                "scala": "2.12.21",
            },
            "revisions": {"gravitino": "g", "sparkHub": "s"},
            "freshPrefix": {"emptyBefore": True, "exclusive": True},
            "process": {
                "command": {"path": "cmd", "sha256": "x"},
                "exit": {"path": "exit", "sha256": "x"},
                "stdout": {"path": "out", "sha256": "x"},
                "stderr": {"path": "err", "sha256": "x"},
                "exitCode": 0,
            },
            "cases": [],
        }
        for name in self.checks.CASES:
            case = {
                "id": name,
                "before": copy.deepcopy(state),
                "after": copy.deepcopy(state),
                "exception": None,
                "outcomes": {
                    "data": "committed",
                    "metadata": "not-committed",
                    "maintenance": "not-committed",
                },
                "executedPhases": ["data"],
                "settingsBefore": {},
                "settingsAfter": {},
                "actualOptions": {"userMetadata": {"comment": "native"}},
                "rawOptions": {},
                "mode": "append",
                "evidence": {"path": name, "sha256": "x"},
            }
            case["after"]["version"] = 2
            case["after"]["actions"] = [
                {"add": {"dataChange": True}},
                {
                    "commitInfo": {
                        "operation": "WRITE",
                        "userMetadata": "{'comment': 'native'}",
                    }
                },
            ]
            case["after"]["rows"].append(
                {"id": 2, "region": "new", "__arkham_loaded_at": "timestamp"}
            )
            if name == "overwrite-empty":
                case["after"]["rows"] = []
            if name in (
                "append-explicit-false",
                "overwrite-explicit-false",
                "auto-merge-precedence",
            ):
                case["exception"] = {"class": "AnalysisException"}
                case["after"] = copy.deepcopy(case["before"])
                case["outcomes"]["data"] = "not-committed"
            if name.startswith("duplicate-"):
                case["after"] = copy.deepcopy(case["before"])
                case["outcomes"]["data"] = "not-committed"
                case["intentChanged"] = name == "duplicate-changed"
            if name == "refresh-append":
                case["refreshOptions"] = [
                    {
                        "method": m,
                        "raw": {
                            "overwriteSchema": "false",
                            "mergeSchema": "false",
                            "userMetadata": {
                                "overwriteSchema": False,
                                "mergeSchema": False,
                            },
                        },
                        "actual": {
                            "overwriteSchema": "true",
                            "mergeSchema": "true",
                            "userMetadata": {
                                "overwriteSchema": False,
                                "mergeSchema": False,
                            },
                        },
                        "mode": "append",
                    }
                    for m in (7, 8)
                ]
            if name == "ordinary-hooks":
                case["after"]["actions"] += [
                    {"commitInfo": {"operation": "OPTIMIZE"}},
                    {"remove": {"dataChange": False}},
                    {"add": {"dataChange": False}},
                ]
                case["after"]["manifest"] = ["_symlink_format_manifest/manifest"]
            if name in ("legacy-create", "stage-create", "absent-create-or-replace"):
                case["after"]["rows"] = [
                    {"id": 2, "region": "new", "__arkham_loaded_at": "timestamp"}
                ]
            if name in ("overwrite", "overwrite-partitioned"):
                case["after"]["rows"] = [
                    {
                        "id": 8,
                        "region": "new",
                        "replacement": "replacement",
                        "__arkham_loaded_at": "timestamp",
                    }
                ]
                case["after"]["schema"]["fields"].append({"name": "replacement"})
            if name == "append-evolve":
                case["after"]["rows"][0]["extra"] = None
                case["after"]["rows"][1]["extra"] = "evolved"
                case["after"]["schema"]["fields"].append({"name": "extra"})
            if name == "refresh-append":
                case["after"]["rows"].append(copy.deepcopy(case["after"]["rows"][-1]))
            result["cases"].append(case)
        # A complete synthetic register, independent of native qualification.
        absent = copy.deepcopy(state)
        absent.update(
            exists=False,
            uuid=None,
            version=-1,
            rows=[],
            schema={},
            properties={},
            protocol={},
            actions=[],
        )

        def observation(name, before, after, executed, exception=None):
            outcomes = self.checks.native_outcomes(before, after, executed)
            return {
                "id": name,
                "before": copy.deepcopy(before),
                "after": copy.deepcopy(after),
                "exception": exception,
                "executedPhases": executed,
                "outcomes": outcomes,
                "phaseStatus": {
                    phase: (
                        "attempted-or-native-effect"
                        if phase in executed or outcome == "committed"
                        else "not-executed"
                    )
                    for phase, outcome in outcomes.items()
                },
                "settingsBefore": {},
                "settingsAfter": {},
            }

        def initial(empty=False):
            value = copy.deepcopy(state)
            value["version"] = 0
            value["actions"] = [
                {"version": 0, "metaData": {}},
                {"version": 0, "protocol": {}},
            ]
            if empty:
                value["rows"] = []
            else:
                value["actions"].append({"version": 0, "add": {"dataChange": True}})
            return value

        def changed(before, rows, metadata, schema=None, compact=False):
            value = copy.deepcopy(before)
            value["version"] += 1
            value["rows"] = copy.deepcopy(rows)
            if schema is not None:
                value["schema"] = copy.deepcopy(schema)
            value["actions"] += [
                {"version": value["version"], "add": {"dataChange": True}},
                {
                    "version": value["version"],
                    "commitInfo": {"operation": "WRITE", "userMetadata": str(metadata)},
                },
            ]
            if compact:
                value["version"] += 1
                value["actions"] += [
                    {
                        "version": value["version"],
                        "commitInfo": {"operation": "OPTIMIZE"},
                    },
                    {"version": value["version"], "add": {"dataChange": False}},
                    {"version": value["version"], "remove": {"dataChange": False}},
                ]
            return value

        for case in result["cases"]:
            name = case["id"]
            old_after = case["after"]
            metadata = case["actualOptions"]["userMetadata"]
            before = initial(empty=name == "legacy-create")
            steps = []
            executed = ["data"]
            if name == "legacy-create":
                steps = [observation(name + "-empty", absent, before, ["metadata"])]
            elif name in ("stage-create", "absent-create-or-replace"):
                before = copy.deepcopy(absent)
                executed = ["data", "metadata"]
            elif name == "ordinary-hooks":
                before = copy.deepcopy(absent)
                current = initial(empty=True)
                current["version"] = -1
                current["actions"] = []
                for index in range(6):
                    previous = copy.deepcopy(absent if index == 0 else current)
                    rows = current["rows"] + [
                        {
                            "id": index,
                            "region": "tiny",
                            "__arkham_loaded_at": "timestamp",
                        }
                    ]
                    current = changed(current, rows, metadata, compact=index in (2, 4))
                    if index == 0:
                        current["actions"] = [
                            {"version": 0, "metaData": {}},
                            {"version": 0, "protocol": {}},
                        ] + current["actions"]
                    steps.append(
                        observation(
                            name + "-tiny-" + str(index),
                            previous,
                            current,
                            ["data", "maintenance"],
                        )
                    )
                current["manifest"] = ["_symlink_format_manifest/manifest"]
                steps[-1]["after"] = copy.deepcopy(current)
                after = current
                executed = ["data", "maintenance"]
            else:
                steps = [
                    observation(name + "-seed", absent, before, ["data", "metadata"])
                ]
            if name == "refresh-append":
                current = copy.deepcopy(before)
                for refresh in case["refreshOptions"]:
                    previous = copy.deepcopy(current)
                    current = changed(
                        current,
                        current["rows"]
                        + [
                            {
                                "id": 2,
                                "region": "new",
                                "__arkham_loaded_at": "timestamp",
                            }
                        ],
                        refresh["actual"]["userMetadata"],
                    )
                    steps.append(
                        observation(
                            name + "-" + str(refresh["method"]),
                            previous,
                            current,
                            ["data"],
                        )
                    )
                after = current
            elif name.startswith("duplicate-"):
                current = changed(
                    before,
                    before["rows"]
                    + [{"id": 2, "region": "new", "__arkham_loaded_at": "timestamp"}],
                    metadata,
                )
                steps.append(
                    observation(name + "-first-intent", before, current, ["data"])
                )
                before = current
                after = copy.deepcopy(current)
            elif name != "ordinary-hooks":
                if case["exception"] is not None:
                    after = copy.deepcopy(before)
                else:
                    after = changed(
                        before, old_after["rows"], metadata, schema=old_after["schema"]
                    )
                    if name in ("stage-create", "absent-create-or-replace"):
                        after["exists"], after["uuid"] = True, "native-id"
                        after["properties"] = copy.deepcopy(state["properties"])
                        after["actions"] = [
                            {"version": 0, "metaData": {}},
                            {"version": 0, "protocol": {}},
                        ] + after["actions"]
            case.update(
                observation(name, before, after, executed, case["exception"]),
                steps=steps,
            )
        return result

    def case(self, result, name):
        return next(c for c in result["cases"] if c["id"] == name)

    def test_rejects_existing_output_or_nonempty_prefix(self):
        with self.assertRaisesRegex(ValueError, "output"):
            self.launcher.require_fresh(self.root, [])
        with self.assertRaisesRegex(ValueError, "prefix"):
            self.launcher.require_fresh(self.root / "new", [{"key": "old"}])
        inputs = {
            "schemaVersion": 1,
            "python": sys.executable,
            "primary": {
                "endpoint": "https://s3.amazonaws.com",
                "bucket": "fixture",
                "region": "us-east-1",
                "credentialsFile": str(self.root / "private"),
            },
            "prefix": "fresh/native",
            "nativeIdentity": "fixture",
        }
        with self.assertRaisesRegex(ValueError, "local|loopback"):
            self.checks.validate_inputs(inputs)
        self.assertNotIn("pyspark", sys.modules)

    def test_rejects_wrong_pins_or_changed_artifact(self):
        report = self.report()
        report["pins"]["delta"] = "3.3.0"
        with self.assertRaisesRegex(ValueError, "pins"):
            self.checks.verify_observations(report)
        artifact = self.root / "raw.json"
        artifact.write_text("original")
        reference = self.launcher.ref(artifact)
        # Isolate the intended artifact: no missing process/case reference can
        # make this test pass before the changed-byte comparison is reached.
        report = {"artifacts": [reference]}
        path = self.root / "result.json"
        path.write_text(json.dumps(report))
        artifact.write_text("changed")
        with self.assertRaises(ValueError) as raised:
            self.checks.verify_result(path)
        self.assertEqual(
            "Changed or missing artifact: " + str(artifact), str(raised.exception)
        )

    def test_requires_every_case_and_original_process(self):
        report = self.report()
        self.assertTrue(
            self.checks.verify_observations(report)["characterizationComplete"]
        )
        report["cases"].pop()
        with self.assertRaisesRegex(ValueError, "case"):
            self.checks.verify_observations(report)
        report = self.report()
        del report["process"]["stderr"]
        with self.assertRaisesRegex(ValueError, "process"):
            self.checks.verify_observations(report)

    def test_empty_refresh_requires_empty_rows_same_uuid(self):
        for key, value in (("rows", [[9]]), ("uuid", "replacement-id")):
            report = self.report()
            self.case(report, "overwrite-empty")["after"][key] = value
            with self.assertRaisesRegex(ValueError, "empty|UUID"):
                self.checks.verify_observations(report)

    def test_compaction_setting_without_commit_is_insufficient(self):
        report = self.report()
        case = self.case(report, "ordinary-hooks")
        for observation in [case] + case["steps"]:
            for side in ("before", "after"):
                observation[side]["actions"] = [
                    action
                    for action in observation[side]["actions"]
                    if action.get("commitInfo", {}).get("operation") != "OPTIMIZE"
                    and not any(
                        action.get(kind, {}).get("dataChange") is False
                        for kind in ("add", "remove")
                    )
                ]
            observation["outcomes"] = self.checks.native_outcomes(
                observation["before"],
                observation["after"],
                observation["executedPhases"],
            )
            observation["phaseStatus"] = {
                phase: (
                    "attempted-or-native-effect"
                    if phase in observation["executedPhases"] or outcome == "committed"
                    else "not-executed"
                )
                for phase, outcome in observation["outcomes"].items()
            }
        verdict = self.checks.verify_observations(report)
        self.assertFalse(verdict["nativeCandidateViable"])
        self.assertTrue(any("compaction" in gap for gap in verdict["gaps"]))

    def test_exception_without_readback_is_unknown(self):
        report = self.report()
        case = self.case(report, "append-explicit-false")
        case["after"] = {"readback": False, "error": "observer failed"}
        case["outcomes"]["data"] = "not-committed"
        with self.assertRaisesRegex(ValueError, "unknown"):
            self.checks.verify_observations(report)
        case["outcomes"] = dict.fromkeys(case["outcomes"], "unknown")
        verdict = self.checks.verify_observations(report)
        self.assertFalse(verdict["characterizationComplete"])

    def test_duplicate_suppression_does_not_prove_intent(self):
        report = self.report()
        verdict = self.checks.verify_observations(report)
        self.assertTrue(any("intent" in gap for gap in verdict["gaps"]))
        report["intentValidated"] = True
        with self.assertRaisesRegex(ValueError, "intent"):
            self.checks.verify_observations(report)

    def test_refresh_append_and_original_metadata_flags_preserved(self):
        for field in ("mode", "metadata"):
            report = self.report()
            refresh = self.case(report, "refresh-append")["refreshOptions"][0]
            if field == "mode":
                refresh["mode"] = "overwrite"
            else:
                refresh["actual"]["userMetadata"]["overwriteSchema"] = True
            with self.assertRaisesRegex(ValueError, "refresh|metadata"):
                self.checks.verify_observations(report)

    def test_native_report_never_claims_governance(self):
        report = self.report()
        self.assertFalse(self.checks.verify_observations(report)["governedSupport"])
        report["governedSupport"] = True
        with self.assertRaisesRegex(ValueError, "govern"):
            self.checks.verify_observations(report)

    def test_native_success_requires_rows_schema_and_metadata(self):
        report = self.report()
        case = self.case(report, "append")
        case["after"]["rows"] = [{"id": 0, "region": "old-only"}]
        with self.assertRaisesRegex(ValueError, "row|loaded"):
            self.checks.verify_observations(report)

    def test_native_phase_outcomes_are_derived_from_actions(self):
        before = {"readback": True, "version": 0}
        after = {
            "readback": True,
            "actions": [
                {"version": 1, "add": {"dataChange": True}},
                {"version": 2, "remove": {"dataChange": False}},
                {"version": 2, "commitInfo": {"operation": "OPTIMIZE"}},
            ],
        }
        self.assertEqual(
            {"data": "committed", "metadata": "unknown", "maintenance": "committed"},
            self.checks.native_outcomes(before, after, ["data", "maintenance"]),
        )
        after["readback"] = False
        self.assertEqual(
            {"unknown"},
            set(self.checks.native_outcomes(before, after, ["data"]).values()),
        )

    def test_refresh_history_uses_actual_refresh_options(self):
        report = self.report()
        case = self.case(report, "refresh-append")
        for action in case["after"]["actions"]:
            if "commitInfo" in action:
                action["commitInfo"]["userMetadata"] = str(
                    case["refreshOptions"][0]["actual"]["userMetadata"]
                )
        verdict = self.checks.verify_observations(report)
        self.assertFalse(
            any(
                gap.startswith("refresh-append: writer userMetadata")
                for gap in verdict["gaps"]
            )
        )

    def test_wrong_properties_schema_or_metadata_cannot_qualify(self):
        report = self.report()
        self.case(report, "append")["after"]["properties"][
            "delta.enableChangeDataFeed"
        ] = "false"
        with self.assertRaisesRegex(ValueError, "properties"):
            self.checks.verify_observations(report)
        report = self.report()
        self.case(report, "append-evolve")["after"]["schema"]["fields"].pop()
        with self.assertRaisesRegex(ValueError, "schema"):
            self.checks.verify_observations(report)
        report = self.report()
        for action in self.case(report, "append")["after"]["actions"]:
            if "commitInfo" in action:
                action["commitInfo"]["userMetadata"] = "silently rewritten"
        verdict = self.checks.verify_observations(report)
        self.assertFalse(verdict["nativeCandidateViable"])
        self.assertTrue(
            any(
                gap.startswith("append: writer userMetadata") for gap in verdict["gaps"]
            )
        )

    def test_complete_fixture_contains_every_required_observation(self):
        report = self.report()
        self.assertEqual(
            ["legacy-create-empty"],
            [step["id"] for step in self.case(report, "legacy-create")["steps"]],
        )
        self.assertEqual(6, len(self.case(report, "ordinary-hooks")["steps"]))
        self.assertTrue(
            self.checks.verify_observations(report)["characterizationComplete"]
        )

    def test_requires_steps_field(self):
        report = self.report()
        for case in report["cases"]:
            del case["steps"]
        with self.assertRaisesRegex(ValueError, "step|observation"):
            self.checks.verify_observations(report)

    def test_requires_legacy_empty_create_step(self):
        report = self.report()
        self.case(report, "legacy-create")["steps"] = []
        with self.assertRaisesRegex(ValueError, "step|observation"):
            self.checks.verify_observations(report)

    def test_requires_both_refresh_steps(self):
        for method in (7, 8):
            with self.subTest(method=method):
                report = self.report()
                case = self.case(report, "refresh-append")
                case["steps"] = [
                    step
                    for step in case["steps"]
                    if step["id"] != "refresh-append-" + str(method)
                ]
                with self.assertRaisesRegex(ValueError, "step|observation"):
                    self.checks.verify_observations(report)

    def test_requires_first_transaction_intent_steps(self):
        for name in ("duplicate-same", "duplicate-changed"):
            with self.subTest(case=name):
                report = self.report()
                self.case(report, name)["steps"].pop()
                with self.assertRaisesRegex(ValueError, "step|observation"):
                    self.checks.verify_observations(report)

    def test_requires_every_tiny_hook_step_exactly_once(self):
        for index in range(6):
            with self.subTest(index=index):
                report = self.report()
                self.case(report, "ordinary-hooks")["steps"].pop(index)
                with self.assertRaisesRegex(ValueError, "step|observation"):
                    self.checks.verify_observations(report)
        report = self.report()
        steps = self.case(report, "ordinary-hooks")["steps"]
        steps[-1] = copy.deepcopy(steps[0])
        with self.assertRaisesRegex(ValueError, "step|observation"):
            self.checks.verify_observations(report)

    def test_requires_case_phase_status(self):
        report = self.report()
        for case in report["cases"]:
            del case["phaseStatus"]
        with self.assertRaisesRegex(ValueError, "phaseStatus"):
            self.checks.verify_observations(report)

    def test_rejects_contradictory_case_and_step_phase_status(self):
        for substep in (False, True):
            with self.subTest(substep=substep):
                report = self.report()
                case = self.case(report, "append")
                observation = case["steps"][0] if substep else case
                observation["phaseStatus"]["data"] = "not-executed"
                with self.assertRaisesRegex(ValueError, "phaseStatus"):
                    self.checks.verify_observations(report)

    def test_rejects_missing_or_invalid_executed_phases(self):
        for substep in (False, True):
            for change in ("missing", "invalid"):
                with self.subTest(substep=substep, change=change):
                    report = self.report()
                    case = self.case(report, "append")
                    observation = case["steps"][0] if substep else case
                    if change == "missing":
                        del observation["executedPhases"]
                    else:
                        observation["executedPhases"].append("invented-phase")
                    with self.assertRaisesRegex(ValueError, "executedPhases|phase"):
                        self.checks.verify_observations(report)

    def test_rejects_action_contradicting_case_or_step_outcome(self):
        for substep in (False, True):
            with self.subTest(substep=substep):
                report = self.report()
                case = self.case(report, "append")
                observation = case["steps"][0] if substep else case
                observation["outcomes"]["data"] = "not-committed"
                with self.assertRaisesRegex(ValueError, "outcome"):
                    self.checks.verify_observations(report)

    def test_rejects_missing_or_disconnected_step_boundaries(self):
        for change in ("missing", "disconnected"):
            with self.subTest(change=change):
                report = self.report()
                step = self.case(report, "legacy-create")["steps"][0]
                if change == "missing":
                    del step["after"]
                else:
                    step["after"]["uuid"] = "different-native-id"
                with self.assertRaisesRegex(ValueError, "boundary|state|observation"):
                    self.checks.verify_observations(report)

    def test_legacy_substep_status_is_derived_without_fabricating_native_fields(self):
        report = self.report()
        for case in report["cases"]:
            for step in case["steps"]:
                del step["phaseStatus"]
        self.assertTrue(
            self.checks.verify_observations(report)["characterizationComplete"]
        )
        self.assertTrue(
            all(
                "phaseStatus" not in step
                for case in report["cases"]
                for step in case["steps"]
            )
        )

    def git(self, root, *args):
        return subprocess.check_output(
            ["git", *args], cwd=root, text=True, stderr=subprocess.PIPE
        ).strip()

    def source_repositories(self):
        gravitino = self.root / "gravitino"
        spark_hub = self.root / "spark-hub"
        for root in (gravitino, spark_hub):
            root.mkdir()
            self.git(root, "init", "-q")
            (root / "native-source.scala").write_text("original native source")
            self.git(root, "add", ".")
            self.git(
                root,
                "-c",
                "user.name=Fixture",
                "-c",
                "user.email=fixture@example.invalid",
                "commit",
                "-qm",
                "fixture baseline",
            )
        return (
            gravitino,
            spark_hub,
            self.git(gravitino, "rev-parse", "HEAD"),
            self.git(spark_hub, "rev-parse", "HEAD"),
        )

    def test_descendant_head_records_current_sources_without_repinning_runtime(self):
        gravitino, spark_hub, baseline, spark_hub_pin = self.source_repositories()
        probe = gravitino / "delta/acceptance/probe.py"
        probe.parent.mkdir(parents=True)
        probe.write_text("reviewed successor probe")
        self.git(gravitino, "add", ".")
        self.git(
            gravitino,
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "-qm",
            "successor probe",
        )
        actual = self.git(gravitino, "rev-parse", "HEAD")
        self.assertNotEqual(baseline, actual)
        preflight = self.launcher.source_revisions(gravitino, spark_hub, spark_hub_pin)
        self.assertEqual(actual, preflight["gravitino"])
        self.assertEqual(probe.read_bytes(), b"reviewed successor probe")
        # Descendant source acceptance never skips the independent native bytes check.
        with self.assertRaisesRegex(ValueError, "runtime pins"):
            self.launcher.runtime_artifacts({"spark": "3.5.4", "delta": "3.3.2"})
        jar = self.root / "native.jar"
        jar.write_bytes(b"original")
        digest = self.launcher.ref(jar)["sha256"]
        jar.write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "Changed runtime artifact"):
            self.launcher.runtime_artifacts(
                {"spark": "3.5.6", "delta": "3.3.2", "jarHashes": {str(jar): digest}}
            )

    def test_consumed_probe_source_change_is_rejected(self):
        probe = self.root / "probe.py"
        probe.write_text("original consumed source")
        source_hashes = {str(probe): self.launcher.ref(probe)["sha256"]}
        self.launcher.verify_probe_sources(source_hashes)
        probe.write_text("changed while original process executes")
        with self.assertRaisesRegex(ValueError, "Probe sources changed"):
            self.launcher.verify_probe_sources(source_hashes)

    def test_run_still_rejects_wrong_or_changed_native_runtime_before_execution(self):
        inputs = self.root / "inputs.private.json"
        credentials = self.root / "credentials.private"
        credentials.write_text("fixture credentials remain unread")
        inputs.write_text(
            json.dumps(
                {
                    "schemaVersion": 1,
                    "python": sys.executable,
                    "primary": {
                        "endpoint": "http://127.0.0.1:9000",
                        "region": "us-east-1",
                        "bucket": "fixture",
                        "credentialsFile": str(credentials),
                    },
                    "prefix": "exclusive-offline-prefix",
                    "nativeIdentity": "fixture",
                }
            )
        )
        runtime = self.root / "runtime.json"
        output = self.root / "output"
        runtime.write_text(json.dumps({"spark": "3.5.4", "delta": "3.3.2"}))
        with self.assertRaisesRegex(ValueError, "runtime pins"):
            self.launcher.run(runtime, self.root, inputs, output)
        jar = self.root / "native.jar"
        jar.write_bytes(b"original")
        digest = self.launcher.ref(jar)["sha256"]
        jar.write_bytes(b"changed")
        runtime.write_text(
            json.dumps(
                {"spark": "3.5.6", "delta": "3.3.2", "jarHashes": {str(jar): digest}}
            )
        )
        with self.assertRaisesRegex(ValueError, "Changed runtime artifact"):
            self.launcher.run(runtime, self.root, inputs, output)
        self.assertFalse(output.exists())

    def test_source_preflight_preserves_exact_clean_spark_hub_pin(self):
        gravitino, spark_hub, baseline, spark_hub_pin = self.source_repositories()
        with self.assertRaisesRegex(ValueError, "sparkHub revision"):
            self.launcher.source_revisions(gravitino, spark_hub, "0" * 40)
        (spark_hub / "native-source.scala").write_text("dirty helper")
        with self.assertRaisesRegex(ValueError, "Spark Hub source"):
            self.launcher.source_revisions(gravitino, spark_hub, spark_hub_pin)


if __name__ == "__main__":
    unittest.main()

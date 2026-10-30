# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Pure register checks; synthetic evidence here never qualifies a runtime."""

import copy
import json
import unittest
from pathlib import Path

from spark_hub_coverage import coverage_summary, validate_coverage


def example_document():
    """Return a hand-written register with a source-only, unsupported case."""
    return {
        "schemaVersion": 1,
        "repositories": {"sparkHub": "a" * 40, "gravitino": "b" * 40},
        "evidence": [
            {
                "id": "source-write",
                "kind": "source",
                "repository": "sparkHub",
                "revision": "a" * 40,
                "reference": "src/shared/services/v2/delta.py",
            }
        ],
        "cases": [
            {
                "id": "append-options",
                "milestone": "M2",
                "scope": "required",
                "production": {
                    "repository": "sparkHub",
                    "path": "src/shared/services/v2/delta.py",
                    "symbol": "DeltaClient.write_dataset",
                },
                "api": "DataFrameWriter.options(...).mode(APPEND).save(path)",
                "planFamily": "external-or-transformed-batch",
                "options": {"userMetadata": "required", "mergeSchema": True},
                "features": ["schema-evolution"],
                "settings": {"spark.sql.adaptive.enabled": "true"},
                "policyProfile": "preserve-current-row-filters-masks-and-column-permissions",
                "expectedOutcome": "Preserve options and classify native commit outcome.",
                "status": "connector-gap",
                "evidenceRefs": ["source-write"],
                "predecessorIds": [],
            }
        ],
    }


class TestSparkHubCoverage(unittest.TestCase):
    def test_source_only_gap_is_valid_and_summary_never_calls_it_qualified(self):
        document = example_document()
        before = copy.deepcopy(document)
        self.assertIsNone(validate_coverage(document))
        summary = coverage_summary(document)
        self.assertEqual(1, summary.get("totalCases"))
        self.assertEqual(1, summary["byStatus"]["connector-gap"])
        self.assertEqual(0, summary["byStatus"]["qualified"])
        self.assertEqual({"M2": 1}, summary["byMilestone"])
        self.assertEqual([], summary["qualifiedCases"])
        self.assertEqual([], summary["optionalUnusedCases"])
        self.assertEqual(before, document)

    def test_rejects_malformed_schema_version(self):
        for version in (None, True, False, "1", 0, 2, 1.0):
            with self.subTest(version=version):
                document = example_document()
                document["schemaVersion"] = version
                with self.assertRaises(ValueError):
                    validate_coverage(document)

    def test_rejects_missing_register_and_case_fields(self):
        template = example_document()
        for field in template:
            with self.subTest(register_field=field):
                document = copy.deepcopy(template)
                del document[field]
                with self.assertRaises(ValueError):
                    validate_coverage(document)
        for field in template["cases"][0]:
            with self.subTest(case_field=field):
                document = copy.deepcopy(template)
                del document["cases"][0][field]
                with self.assertRaises(ValueError):
                    validate_coverage(document)
        for field in ("repository", "path", "symbol"):
            with self.subTest(production_field=field):
                document = example_document()
                del document["cases"][0]["production"][field]
                with self.assertRaises(ValueError):
                    validate_coverage(document)

    def test_rejects_wrong_field_shapes_empty_values_and_unknown_repository(self):
        for field, value in (
            ("id", " "),
            ("milestone", None),
            ("api", []),
            ("scope", "active-maybe"),
            ("planFamily", ""),
            ("options", []),
            ("features", {}),
            ("features", [""]),
            ("settings", None),
            ("policyProfile", ""),
            ("expectedOutcome", ""),
            ("evidenceRefs", "source-write"),
            ("predecessorIds", "append-options"),
        ):
            with self.subTest(field=field, value=value):
                document = example_document()
                document["cases"][0][field] = value
                with self.assertRaises(ValueError):
                    validate_coverage(document)
        for field, value in (("repository", "unknown"), ("path", ""), ("symbol", 1)):
            with self.subTest(production_field=field):
                document = example_document()
                document["cases"][0]["production"][field] = value
                with self.assertRaises(ValueError):
                    validate_coverage(document)

    def test_rejects_missing_repository_pins_and_malformed_revisions(self):
        for repositories in (
            {},
            {"sparkHub": "a" * 40},
            {"sparkHub": "HEAD", "gravitino": "b" * 40},
        ):
            with self.subTest(repositories=repositories):
                document = example_document()
                document["repositories"] = repositories
                with self.assertRaises(ValueError):
                    validate_coverage(document)

    def test_rejects_empty_or_malformed_collections(self):
        for field, value in (("cases", []), ("cases", {}), ("evidence", {})):
            with self.subTest(field=field, value=value):
                document = example_document()
                document[field] = value
                with self.assertRaises(ValueError):
                    validate_coverage(document)
        for value in (None, [], "register"):
            with self.subTest(document=value), self.assertRaises(ValueError):
                validate_coverage(value)

    def test_rejects_duplicate_case_and_evidence_ids(self):
        for collection in ("cases", "evidence"):
            with self.subTest(collection=collection):
                document = example_document()
                document[collection].append(copy.deepcopy(document[collection][0]))
                with self.assertRaises(ValueError):
                    validate_coverage(document)

    def test_rejects_unsupported_case_relabeled_passed_or_qualified_without_runtime(self):
        for status in ("passed", "supported", "qualified"):
            with self.subTest(status=status):
                document = example_document()
                document["cases"][0]["status"] = status
                with self.assertRaises(ValueError):
                    validate_coverage(document)

    def test_qualification_requires_original_runtime_reference_for_this_case(self):
        document = example_document()
        document["evidence"].append(
            {
                "id": "synthetic-runtime-shape",
                "kind": "runtime",
                "repository": "gravitino",
                "revision": "b" * 40,
                "reference": "unit-fixture-only/not-live.json",
                "original": True,
                "caseId": "append-options",
            }
        )
        document["cases"][0].update(status="qualified", evidenceRefs=["synthetic-runtime-shape"])
        # Structural acceptance only; Task 4 independently verifies actual original artifacts.
        validate_coverage(document)
        self.assertEqual(["append-options"], coverage_summary(document).get("qualifiedCases"))
        document["evidence"][1]["original"] = False
        with self.assertRaises(ValueError):
            validate_coverage(document)
        document["evidence"][1]["original"] = "true"
        with self.assertRaises(ValueError):
            validate_coverage(document)
        document["evidence"][1]["original"] = True
        other = copy.deepcopy(document["cases"][0])
        other.update(id="other-case", status="unqualified", evidenceRefs=["source-write"])
        document["cases"].append(other)
        document["evidence"][1]["caseId"] = "other-case"
        with self.assertRaises(ValueError):
            validate_coverage(document)

    def test_rejects_unresolved_evidence_and_invalid_records(self):
        document = example_document()
        document["cases"][0]["evidenceRefs"] = ["absent"]
        with self.assertRaises(ValueError):
            validate_coverage(document)
        for field, value in (
            ("id", ""),
            ("kind", "passed"),
            ("repository", "absent"),
            ("revision", "HEAD"),
            ("reference", ""),
        ):
            with self.subTest(field=field):
                document = example_document()
                document["evidence"][0][field] = value
                with self.assertRaises(ValueError):
                    validate_coverage(document)

    def test_resolves_predecessor_ids_independently_of_order(self):
        document = example_document()
        prerequisite = copy.deepcopy(document["cases"][0])
        prerequisite.update(id="named-routing", milestone="M1", status="adaptation-required")
        document["cases"][0]["predecessorIds"] = ["named-routing"]
        document["cases"].append(prerequisite)
        validate_coverage(document)
        document["cases"][0]["predecessorIds"] = ["missing-case"]
        with self.assertRaises(ValueError):
            validate_coverage(document)
        document["cases"][0]["predecessorIds"] = ["append-options"]
        with self.assertRaises(ValueError):
            validate_coverage(document)

    def test_all_allowed_statuses_remain_distinct_and_optional_is_separate(self):
        document = example_document()
        cases = document["cases"]
        for case_id, status in (
            ("building-block", "supported-building-block"),
            ("route", "adaptation-required"),
            ("unused-optimize", "unqualified"),
        ):
            case = copy.deepcopy(cases[0])
            case.update(id=case_id, status=status)
            cases.append(case)
        cases[-1]["scope"] = "optional-unused"
        summary = coverage_summary(document)
        self.assertEqual(4, summary.get("totalCases"))
        self.assertEqual(1, summary["byStatus"]["supported-building-block"])
        self.assertEqual(1, summary["byStatus"]["adaptation-required"])
        self.assertEqual(1, summary["byStatus"]["unqualified"])
        self.assertEqual(["unused-optimize"], summary["optionalUnusedCases"])

    def test_summary_rejects_invalid_input(self):
        with self.assertRaises(ValueError):
            coverage_summary({"schemaVersion": 1})

    def test_checked_in_register_preserves_unqualified_workloads(self):
        document = json.loads(Path(__file__).with_name("spark_hub_coverage.json").read_text())
        validate_coverage(document)
        summary = coverage_summary(document)
        self.assertEqual(
            {"named-filter-mask", "projected-count", "column-denied", "unknown-binding", "unpublished-registration", "fresh-action-revoke"},
            set(summary["qualifiedCases"]),
        )
        cases = {case["id"]: case for case in document["cases"]}
        for case_id in (
            "named-filter-mask",
            "projected-count",
            "column-denied",
            "unknown-binding",
            "unpublished-registration",
            "fresh-action-revoke",
            "raw-path-glue-migration",
            "external-ingestion-create",
            "append-options",
            "overwrite-schema-evolution",
            "snapshot-delete-by-absence",
            "transformed-checkpointed-merge",
            "bounded-cdf",
            "snapshot-bootstrap",
            "incremental-state",
            "generated-partitions",
            "auto-compaction",
            "retention-vacuum",
            "manifest-handling",
            "structured-streaming",
            "deployed-identity-session",
        ):
            with self.subTest(case_id=case_id):
                self.assertIn(case_id, cases)
                self.assertEqual("required", cases[case_id]["scope"])
        self.assertEqual(
            ["optional-optimize", "optional-standalone-delete"], summary["optionalUnusedCases"]
        )

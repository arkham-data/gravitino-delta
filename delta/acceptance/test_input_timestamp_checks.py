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

"""Timestamp qualification rejects corrupted semantic and provenance evidence."""
import copy
import unittest
import json
import tempfile
from pathlib import Path
from cdf_merge_live_checks import file_ref
from input_timestamp_checks import (
    timestamp_cases,
    timestamp_tables,
    verify_timestamp_rows,
    verify_timestamp_outcome,
)


class TestInputTimestampChecks(unittest.TestCase):
    def images(self):
        seed = [0, "sentinel", "seed", None, None]
        ts = "2026-11-05 18:00:00.123456"
        return dict(version=0, rows=[seed], publications=[0]), dict(
            version=1,
            rows=[
                seed,
                [1, "MASKED", "allowed", ts, ts],
                [3, "MASKED", "allowed", ts, ts],
            ],
            publications=[0, 1],
        )

    def test_finite_inventory_and_initial_schema(self):
        lead = timestamp_cases("lead")
        self.assertEqual(
            [c["profile"] for c in lead],
            ["plain", "partitioned", "target-denied", "revoke"],
        )
        self.assertEqual(len(timestamp_cases("remaining")), 2)
        tables = timestamp_tables(
            dict(prefix="owned", primary=dict(bucket="bucket")), lead
        )
        self.assertEqual(len(tables), 5)
        self.assertEqual(tables[0]["name"], "source")
        for table in tables[1:]:
            self.assertIn("__arkham_loaded_at TIMESTAMP", table["schema"])
            self.assertEqual(len(table["rows"][0]), 5)
        with self.assertRaises(AssertionError):
            timestamp_cases("partial")

    def test_exact_rows_and_equal_non_null_query_timestamp(self):
        before, after = self.images()
        verify_timestamp_rows(before, after, True)
        for index, value in [
            (3, None),
            (4, "2026-11-05 18:00:01"),
            (1, "alpha"),
            (2, "blocked"),
            (0, 2),
        ]:
            changed = copy.deepcopy(after)
            changed["rows"][1][index] = value
            with self.subTest(index=index), self.assertRaises(AssertionError):
                verify_timestamp_rows(before, changed, True)
        changed = copy.deepcopy(after)
        changed["rows"][2][3:] = ["2026-11-05 18:00:02"] * 2
        with self.assertRaises(AssertionError):
            verify_timestamp_rows(before, changed, True)

    def test_native_version_and_denial_have_no_effect(self):
        before, after = self.images()
        verify_timestamp_rows(before, before, False)
        for changed in [dict(after, version=2), dict(after, publications=[0, 1, 2])]:
            with self.assertRaises(AssertionError):
                verify_timestamp_rows(before, changed, True)
        with self.assertRaises(AssertionError):
            verify_timestamp_rows(before, after, False)

    def test_denial_cannot_be_swallowed_or_relabel_committed(self):
        failure = [
            dict(
                className="java.lang.SecurityException",
                message="Delta Ranger access denied",
                stack=[
                    "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerPolicyEngine.authorize(Policy.scala:1)"
                ],
            )
        ]
        verify_timestamp_outcome("target-denied", failure)
        verify_timestamp_outcome("committed", [])
        for outcome, errors in [
            ("target-denied", []),
            (
                "target-denied",
                [
                    dict(
                        failure[0],
                        message="committed; observation unconfirmed; do not replay",
                    )
                ],
            ),
            ("committed", failure),
            ("postcommit-failure", failure),
        ]:
            with self.subTest(outcome=outcome), self.assertRaises(AssertionError):
                verify_timestamp_outcome(outcome, errors)

    def test_one_qualification_inventory_for_both_checker_gates(self):
        from input_timestamp_checks import qualification_cases

        self.assertEqual(len(qualification_cases({})), 26)
        self.assertEqual(
            qualification_cases(dict(inputTimestampProfile="lead")),
            timestamp_cases("lead"),
        )
        self.assertEqual(
            qualification_cases(dict(inputTimestampProfile="remaining")),
            timestamp_cases("remaining"),
        )

    def test_read_only_source_delta_cannot_change_caller_or_production(self):
        from input_timestamp_checks import verify_source_delta
        from cdf_read_live_package import canonical_digest

        path = "delta/acceptance/append_aqe_checks.py"
        before = {path: "a" * 64, "production/src/main/Guard.scala": "c" * 64}
        after = dict(before, **{path: "b" * 64})
        delta = dict(
            scope="input-timestamp-read-only-v1",
            beforeInventory=before,
            afterInventory=after,
            beforeDigest=canonical_digest(before),
            afterDigest=canonical_digest(after),
            files={path: dict(before="a" * 64, after="b" * 64)},
        )
        verify_source_delta(before, after, delta)
        for changed in [
            dict(delta, afterDigest="0" * 64),
            dict(delta, files={}),
            dict(delta, scope="live"),
            dict(delta, files={path: dict(before="0" * 64, after="b" * 64)}),
        ]:
            with self.assertRaises(AssertionError):
                verify_source_delta(before, after, changed)
        for forbidden in [
            "production/src/main/Guard.scala",
            "delta/acceptance/append_aqe_pyspark.py",
            "delta/acceptance/append_aqe_live.py",
        ]:
            changed = dict(before, **{forbidden: "d" * 64})
            forged = dict(
                delta,
                afterDigest=canonical_digest(changed),
                afterInventory=changed,
                files={forbidden: dict(before=before.get(forbidden), after="d" * 64)},
            )
            with self.assertRaises(AssertionError):
                verify_source_delta(before, changed, forged)

    def test_native_registration_join_preserves_exact_finite_schema_and_receipt(self):
        from append_aqe_profiles import verify_registration_native

        fields = [
            dict(name=name, type=kind, nullable=True, metadata={})
            for name, kind in [
                ("id", "long"),
                ("secret", "string"),
                ("region", "string"),
                ("__arkham_loaded_at", "timestamp"),
                ("second_time", "timestamp"),
            ]
        ]
        native = dict(
            location="s3a://bucket/owned",
            version=0,
            native=dict(id="native"),
            schemaJson=json.dumps(dict(type="struct", fields=fields)),
        )
        columns = [
            dict(
                name=f["name"],
                type="timestamp_tz(6)" if f["type"] == "timestamp" else f["type"],
                nullable=True,
            )
            for f in fields
        ]
        receipt = {
            "delta.registration.receipt." + key: value
            for key, value in dict(
                nativeId="native", location=native["location"], nativeVersion="0"
            ).items()
        }
        registrations = dict(
            target=dict(
                code=0, table=dict(name="target", columns=columns, properties=receipt)
            )
        )
        verify_registration_native(registrations, dict(target=native), True)
        source_native = dict(
            native, schemaJson=json.dumps(dict(type="struct", fields=fields[:3]))
        )
        source_registration = dict(
            source=dict(
                code=0,
                table=dict(name="source", columns=columns[:3], properties=receipt),
            )
        )
        verify_registration_native(source_registration, dict(source=source_native))
        verify_registration_native(
            source_registration, dict(source=source_native), True
        )
        for key, value in [
            ("nativeId", "foreign"),
            ("location", "s3a://foreign"),
            ("nativeVersion", "1"),
        ]:
            changed = copy.deepcopy(registrations)
            changed["target"]["table"]["properties"][
                "delta.registration.receipt." + key
            ] = value
            with self.subTest(key=key), self.assertRaises(AssertionError):
                verify_registration_native(changed, dict(target=native), True)
        for key, value in [
            ("type", "string"),
            ("name", "changed"),
            ("nullable", False),
            ("autoIncrement", True),
        ]:
            changed = copy.deepcopy(registrations)
            changed["target"]["table"]["columns"][-1][key] = value
            with self.subTest(key=key), self.assertRaises(AssertionError):
                verify_registration_native(changed, dict(target=native), True)
        with self.assertRaises(AssertionError):
            verify_registration_native(registrations, dict(target=native))

    def test_timestamp_extension_retains_existing_append_authority_checks(self):
        from test_append_aqe_checks import TestDeltaAppendAqeChecks
        from append_aqe_checks import verify_delta_append
        from cdf_merge_live_checks import read_ref

        fixture = TestDeltaAppendAqeChecks()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        result = fixture.fixture()
        case = result["cases"][0]
        before, after = self.images()
        for side, image in [("before", before), ("after", after)]:
            value = read_ref(case[side])
            value.update(image)
            case[side] = fixture.artifact(value)
        case.update(inputTimestamp=True, revokeSource=False)
        verify_delta_append(result)
        case["rangerRequests"] = []
        with self.assertRaisesRegex(AssertionError, "Missing source SELECT"):
            verify_delta_append(result)

    def test_rejects_stale_runtime_reference(self):
        from input_timestamp_checks import verify_input_timestamp

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.json"
            path.write_text("{}")
            result = dict(inputTimestampProfile="lead", runtime=file_ref(path))
            path.write_text('{"changed":true}')
            with self.assertRaisesRegex(AssertionError, "bytes changed"):
                verify_input_timestamp(result)

    def test_rejects_stale_source_inventory(self):
        from input_timestamp_checks import verify_input_timestamp

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.json"
            path.write_text(json.dumps(dict(sourceInputs={"stale": "0" * 64})))
            with self.assertRaisesRegex(AssertionError, "Source moved"):
                verify_input_timestamp(
                    dict(inputTimestampProfile="remaining", runtime=file_ref(path))
                )


if __name__ == "__main__":
    unittest.main()

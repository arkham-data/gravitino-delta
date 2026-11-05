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

"""Reject incomplete CREATE evidence; synthetic documents exercise only the verifier."""

import copy
import json
import unittest
import tempfile
from pathlib import Path
from create_live_checks import verify_case, verify_hashes, verify_native


class TestCreateLiveChecks(unittest.TestCase):
    def case(self):
        marker = {
            "version": 2,
            "operation": "CREATE",
            "id": 10,
            "generation": 0,
            "token": "token",
            "location": "s3a://fixture/owned/table",
        }
        receipt = dict(
            id="10",
            generation="0",
            token="token",
            runId="run",
            operation="RESERVE",
            state="PUBLISHED",
            nativeId="native",
            nativeVersion="0",
            location=marker["location"],
        )
        return {
            "expected": "published",
            "command": {
                "sql": "CREATE TABLE cat.shared.t (id BIGINT) USING delta",
                "expectedRows": [],
            },
            "result": {"authorizationDenied": False, "failureClass": None, "rows": []},
            "exitCode": 0,
            "before": {"registration": {"status": 404}, "objects": []},
            "after": {
                "registration": {"status": 200},
                "objects": [
                    {"key": "owned/table/_delta_log/00000000000000000000.json"},
                    {"key": "owned/table/_delta_log/00000000000000000000.crc"},
                ],
            },
            "reservation": dict(receipt, state="RESERVED", marker=marker),
            "receipt": receipt,
            "status": copy.deepcopy(receipt),
            "logVersion": 0,
            "nativeId": "native",
            "marker": marker,
            "configuration": {"delta.enableChangeDataFeed": "true"},
            "settings": {"spark.databricks.delta.optimizeWrite.enabled": "true"},
            "fixture": {
                "tableProperties": {"delta.enableChangeDataFeed": "true"},
                "settings": {"spark.databricks.delta.optimizeWrite.enabled": "true"},
            },
            "downloads": {
                "policy": {
                    "completed": True,
                    "version": 9,
                    "expectedVersion": 9,
                    "bodySha256": "a" * 64,
                    "expectedBodySha256": "a" * 64,
                },
                "role": {
                    "completed": True,
                    "version": 5,
                    "expectedVersion": 5,
                    "bodySha256": "b" * 64,
                    "expectedBodySha256": "b" * 64,
                },
            },
            "nativeOutcome": "COMMITTED",
            "publicationOutcome": "PUBLISHED",
            "statusNeeded": False,
            "publicOperations": ["RESERVE", "PUBLISH"],
        }

    def reject(self, change):
        case = self.case()
        change(case)
        with self.assertRaises((AssertionError, ValueError, KeyError)):
            verify_case(case)

    def test_requires_actual_publication_at_create_zero(self):
        for change in [
            lambda c: c.pop("receipt"),
            lambda c: c.update(logVersion=1),
            lambda c: c["receipt"].update(nativeId="wrong"),
            lambda c: c["receipt"].update(runId="wrong"),
            lambda c: c["marker"].update(token="wrong"),
            lambda c: c["marker"].update(operation="REPLACE"),
            lambda c: c["receipt"].update(state="RESERVED"),
            lambda c: c["result"].update(failureClass="failure"),
        ]:
            with self.subTest(change=change):
                self.reject(change)

    def test_denied_case_requires_positive_absence_checks(self):
        for key in ["registration", "objects"]:
            case = self.case()
            case.update(
                expected="denied",
                nativeOutcome="NOT_COMMITTED",
                publicationOutcome="NOT_ATTEMPTED",
            )
            case["result"].update(
                authorizationDenied=True, failureClass="java.lang.SecurityException"
            )
            case["after"] = {"registration": {"status": 404}, "objects": []}
            del case["after"][key]
            with (
                self.subTest(key=key),
                self.assertRaises((AssertionError, ValueError, KeyError)),
            ):
                verify_case(case)

    def test_specific_deny_and_revocation_require_downloaded_versions(self):
        for key in ["policy", "role"]:
            for change in [
                lambda d: d.update(completed=False),
                lambda d: d.update(version=1),
                lambda d: d.update(bodySha256="c" * 64),
            ]:
                self.reject(lambda c: change(c["downloads"][key]))

    def test_rejects_stale_package_or_fixture(self):
        for category in ["source", "class", "jar", "fixture"]:
            with (
                self.subTest(category=category),
                self.assertRaises((AssertionError, ValueError)),
            ):
                verify_hashes({category: "a" * 64}, {category: "b" * 64})

    def test_preserves_committed_unpublished_outcome(self):
        case = self.case()
        case.update(
            expected="retained", publicationOutcome="REFUSED", statusNeeded=True
        )
        case["publicOperations"] = ["RESERVE"]
        case["receipt"].update(state="RESERVED")
        case["status"] = copy.deepcopy(case["receipt"])
        result = verify_case(case)
        self.assertEqual("COMMITTED", result["nativeOutcome"])
        self.assertEqual("REFUSED", result["publicationOutcome"])
        self.assertTrue(result["statusNeeded"])
        case["nativeOutcome"] = "NOT_COMMITTED"
        with self.assertRaises((AssertionError, ValueError)):
            verify_case(case)

    def test_denied_case_rejects_hidden_reservation(self):
        case = self.case()
        case.update(
            expected="denied",
            nativeOutcome="NOT_COMMITTED",
            publicationOutcome="NOT_ATTEMPTED",
        )
        case["result"].update(
            authorizationDenied=True, failureClass="java.lang.SecurityException"
        )
        case["after"] = {"registration": {"status": 404}, "objects": []}
        with self.assertRaises(AssertionError):
            verify_case(case)

    def test_publication_requires_native_checksum_and_no_write_one(self):
        self.reject(
            lambda c: c["after"].update(
                objects=[{"key": "owned/table/_delta_log/00000000000000000000.json"}]
            )
        )
        self.reject(
            lambda c: c["after"]["objects"].append(
                {"key": "owned/table/_delta_log/00000000000000000001.json"}
            )
        )

    def test_rejects_consistently_forged_nonzero_generation(self):
        case = self.case()
        case["marker"]["generation"] = 1
        for key in ["reservation", "receipt", "status"]:
            case[key]["generation"] = "1"
        with self.assertRaises(AssertionError):
            verify_case(case)

    def test_denied_case_requires_no_public_reserve_or_publish(self):
        case = self.case()
        case.update(
            expected="denied",
            nativeOutcome="NOT_COMMITTED",
            publicationOutcome="NOT_ATTEMPTED",
        )
        case.pop("reservation")
        case["result"].update(
            authorizationDenied=True, failureClass="java.lang.SecurityException"
        )
        case["after"] = {"registration": {"status": 404}, "objects": []}
        for operations in [["RESERVE"], ["PUBLISH"]]:
            case["publicOperations"] = operations
            with self.subTest(operations=operations), self.assertRaises(AssertionError):
                verify_case(case)

    def native(self):
        schema = {
            "type": "struct",
            "fields": [
                {"name": "id", "type": "long", "nullable": True, "metadata": {}}
            ],
        }
        marker = dict(self.case()["marker"], version=1, schema=schema)
        metadata = {
            "id": "native",
            "schemaString": json.dumps(schema),
            "partitionColumns": [],
            "format": {"provider": "parquet", "options": {}},
            "configuration": dict(
                self.case()["fixture"]["tableProperties"],
                **{
                    "delta.columnMapping.mode": "none",
                    "gravitino.registration.operation": json.dumps(marker),
                },
            ),
        }
        protocol = {
            "minReaderVersion": 1,
            "minWriterVersion": 7,
            "writerFeatures": ["changeDataFeed", "appendOnly", "invariants"],
        }
        actions = [
            {"metaData": metadata},
            {"protocol": protocol},
            {"commitInfo": {"operation": "CREATE TABLE", "txnId": "txn"}},
        ]
        checksum = {
            "metadata": copy.deepcopy(metadata),
            "protocol": copy.deepcopy(protocol),
            "allFiles": [],
            "numFiles": 0,
            "tableSizeBytes": 0,
            "numMetadata": 1,
            "numProtocol": 1,
            "txnId": "txn",
        }
        return actions, checksum

    def test_requires_exact_metadata_recipe_and_real_empty_checksum(self):
        actions, checksum = self.native()
        self.assertTrue(verify_native(actions, checksum, self.case()["fixture"]))
        for mutate in [
            lambda a, c: a[0]["metaData"]["configuration"].update(extra="forged"),
            lambda a, c: c.update(numFiles=1),
            lambda a, c: c["metadata"].update(id="wrong"),
            lambda a, c: a.append({"add": {"path": "payload.parquet"}}),
            lambda a, c: a[1]["protocol"].update(minWriterVersion=4),
            lambda a, c: c.update(txnId="wrong"),
        ]:
            actions, checksum = self.native()
            mutate(actions, checksum)
            with self.assertRaises((AssertionError, KeyError)):
                verify_native(actions, checksum, self.case()["fixture"])

    def test_group_rest_denied_is_unsupported_not_published(self):
        case = self.case()
        case.update(
            expected="group_rest_denied",
            nativeOutcome="NOT_COMMITTED",
            publicationOutcome="NOT_ATTEMPTED",
            statusNeeded=True,
            exitCode=1,
        )
        case.pop("reservation")
        case["result"].update(failureClass="java.lang.IllegalStateException")
        case["after"] = {"registration": {"status": 404}, "objects": []}
        case["publicOperations"] = ["RESERVE"]
        case["publicRequests"] = [
            {"operation": "RESERVE", "status": 403, "completed": True}
        ]
        case["groupEvidence"] = {
            "groups": ["owned_group"],
            "expectedGroup": "owned_group",
            "directRoles": ["visibility"],
            "createRole": "create",
        }
        self.assertEqual("UNSUPPORTED", verify_case(case)["qualification"])
        for mutate in [
            lambda c: c["groupEvidence"].update(groups=[]),
            lambda c: c["groupEvidence"]["directRoles"].append("create"),
            lambda c: c["publicRequests"][0].update(status=200),
            lambda c: c["publicOperations"].append("PUBLISH"),
            lambda c: c["after"]["objects"].append({"key": "log0"}),
        ]:
            changed = copy.deepcopy(case)
            mutate(changed)
            with self.assertRaises((AssertionError, KeyError)):
                verify_case(changed)

    def test_rejects_unknown_or_missing_nested_fixture_fields(self):
        from create_live import validate_inputs, FIXTURE, ROOT, sha, CASES

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            file = root / "private"
            file.write_text("fixture")
            endpoints = root / "endpoints.json"
            endpoints.write_text(
                json.dumps(
                    {
                        "ranger-admin": "http://127.0.0.1:1",
                        "minio": "http://127.0.0.1:2",
                    }
                )
            )
            (root / "log4j2.xml").write_text("fixture")
            identities = {
                k: "owned_" + k.lower()
                for k in [
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
                ]
            }
            inputs = {
                "endpoints": str(endpoints),
                "rangerAdminFile": str(file),
                "minioCredentialsFile": str(file),
                "rangerRootProof": str(file),
                "javaHome": "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
                "sourceSecurityDirectory": str(root),
                "sparkHomes": {"3.5.4": str(root), "3.5.6": str(root)},
                "python": str(file),
                "fixtureResource": FIXTURE,
                "fixtureSha256": sha(ROOT / FIXTURE),
                "identities": identities,
                "targets": {name: "table_" + name for name in CASES},
                "requiredDownloads": {"policy": 0, "role": 0},
                "profile": "repeat",
            }
            validate_inputs(inputs)
            for mutate in [
                lambda c: c.update(unknown=True),
                lambda c: c["sparkHomes"].update(unknown=str(root)),
                lambda c: c["sparkHomes"].pop("3.5.6"),
                lambda c: c["requiredDownloads"].update(policy=-1),
                lambda c: c.update(fixtureSha256="0" * 64),
            ]:
                changed = copy.deepcopy(inputs)
                mutate(changed)
                with self.assertRaises(AssertionError):
                    validate_inputs(changed)

    def test_accepts_complete_publication(self):
        self.assertEqual("PUBLISHED", verify_case(self.case())["publicationOutcome"])


if __name__ == "__main__":
    unittest.main()

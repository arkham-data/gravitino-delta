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

"""Fail-closed joins of downloaded authorization, CREATE0 and publication evidence."""

import json


def verify_hashes(expected, actual):
    """Require complete current-source/class/JAR/fixture byte inventories."""
    assert expected and expected == actual, "Stale or incomplete byte inventory"
    assert all(
        isinstance(value, str) and len(value) == 64 for value in expected.values()
    )
    return True


def verify_invalid_create(result):
    """Require the nested-field validator cause, not an unrelated native/Python failure."""
    assert result["failureClass"] is not None and result["authorizationDenied"] is False
    origin = (
        "org.apache.gravitino.spark.connector.delta.ranger."
        "DeltaRangerCreateDeclaration$.denied(DeltaRangerCreateDeclaration.scala:"
    )
    assert any(
        cause.get("class") == "java.lang.SecurityException"
        and cause.get("message") == "Schema CREATE rejected: unsupported column type"
        and cause.get("stack")
        and cause["stack"][0].startswith(origin)
        for cause in result.get("failureDetails", [])
    ), "Expected the original unsupported-column validation cause"
    return True


def verify_case(case):
    """Keep native commit and publication independent, including retained failures."""
    for kind in ("policy", "role"):
        download = case["downloads"][kind]
        assert download["completed"] is True, "Incomplete secured download"
        assert download["version"] == download["expectedVersion"], (
            "Unrefreshed generation"
        )
        assert len(download["bodySha256"]) == 64
        assert download["bodySha256"] == download["expectedBodySha256"], (
            "Wrong downloaded body"
        )
    assert case["settings"] == case["fixture"]["settings"], "Fixture settings changed"
    if case["expected"] in ("noop", "duplicate", "read_denied"):
        assert case["before"]["registration"]["status"] == 200
        assert case["before"] == case["after"], "Existing target changed"
        assert case["nativeOutcome"] == case["publicationOutcome"] == "NOT_ATTEMPTED"
        assert case["statusNeeded"] is False and not case.get("reservation")
        if case["expected"] != "duplicate":
            assert case["journalEntries"] == [], (
                "Existing-target operation journaled a mutation"
            )
        if case["expected"] == "noop":
            assert case["publicOperations"] == case["actorMutations"] == []
            assert case["exitCode"] == 0 and case["result"]["failureClass"] is None
            assert case["result"]["authorizationDenied"] is False
            reads = case["metadataReads"]
            assert reads, "No-op lacks its own positive metadata read"
            assert all(r["completed"] and r["status"] == 200 for r in reads)
            assert all(r["receipt"] == case["receipt"] for r in reads)
            assert case["receipt"]["state"] == "PUBLISHED"
            assert case["receipt"]["nativeVersion"] == "0"
            assert case["result"]["rows"] == []
        else:
            assert case["result"]["failureClass"] is not None
            if case["expected"] == "read_denied":
                assert case["publicOperations"] == case["actorMutations"] == []
                assert case["result"]["authorizationDenied"] is True
            else:
                assert case["result"]["authorizationDenied"] is False
                assert "already exists" in json.dumps(case["result"]).lower()
        return {
            key: case[key]
            for key in ("nativeOutcome", "publicationOutcome", "statusNeeded")
        }
    assert case["before"]["registration"]["status"] == 404
    assert case["before"]["objects"] == [], "Target was already native"
    assert case["settings"] == case["fixture"]["settings"], "Fixture settings changed"
    if case["expected"] == "invalid":
        verify_invalid_create(case["result"])
        assert (
            case["after"]["registration"]["status"] == 404
            and case["after"]["objects"] == []
        )
        assert not case.get("reservation") and case["publicOperations"] == []
        assert (
            case["nativeOutcome"] == "NOT_COMMITTED"
            and case["publicationOutcome"] == "NOT_ATTEMPTED"
        )
        return {
            key: case[key]
            for key in ("nativeOutcome", "publicationOutcome", "statusNeeded")
        }
    if case["expected"] == "group_rest_denied":
        group = case["groupEvidence"]
        assert group["groups"] == [group["expectedGroup"]]
        assert group["createRole"] not in group["directRoles"]
        assert case["publicOperations"] == ["RESERVE"]
        assert case["publicRequests"] == [
            {"operation": "RESERVE", "status": 403, "completed": True}
        ]
        assert (
            not case.get("reservation")
            and case["after"]["registration"]["status"] == 404
        )
        assert (
            case["after"]["objects"] == [] and case["nativeOutcome"] == "NOT_COMMITTED"
        )
        assert case["publicationOutcome"] == "NOT_ATTEMPTED"
        assert case["exitCode"] != 0 and case["result"]["failureClass"] is not None
        return dict(
            {
                key: case[key]
                for key in ("nativeOutcome", "publicationOutcome", "statusNeeded")
            },
            qualification="UNSUPPORTED",
        )
    if case["expected"] == "denied":
        assert case["result"]["authorizationDenied"] is True
        assert case["exitCode"] == 0
        assert case["after"]["registration"]["status"] == 404
        assert case["after"]["objects"] == [], "Denied target has native effects"
        assert not case.get("reservation"), "Hidden reservation is not absence"
        assert case["publicOperations"] == [], "Denied CREATE reached a public mutation"
        assert case["nativeOutcome"] == "NOT_COMMITTED"
        assert case["publicationOutcome"] == "NOT_ATTEMPTED"
    else:
        assert case["logVersion"] == 0, "CREATE must commit at version zero"
        assert any(
            x["key"].endswith("/_delta_log/00000000000000000000.json")
            for x in case["after"]["objects"]
        )
        assert case["nativeOutcome"] == "COMMITTED", (
            "Do not erase committed native work"
        )
        marker, reserved, receipt = case["marker"], case["reservation"], case["receipt"]
        assert marker["operation"] == "CREATE"
        assert reserved["operation"] == receipt["operation"] == "RESERVE"
        for key in ("id", "generation", "token"):
            assert str(marker[key]) == reserved[key] == receipt[key], (
                "Wrong marker identity: " + key
            )
        assert marker["generation"] == 0, "Initial CREATE requires generation zero"
        assert reserved["marker"] == marker
        assert receipt["runId"] == reserved["runId"]
        assert case["status"] == receipt, "STATUS differs from observed receipt"
        assert all(
            case["configuration"].get(k) == v
            for k, v in case["fixture"]["tableProperties"].items()
        )
        if case["expected"] == "published":
            assert case["publicOperations"] == ["RESERVE", "PUBLISH"], (
                "Missing or duplicate public lifecycle"
            )
            keys = [item["key"] for item in case["after"]["objects"]]
            assert any(
                key.endswith("/_delta_log/00000000000000000000.crc") for key in keys
            ), "Missing native checksum"
            assert not any(
                key.endswith(".json")
                and "/_delta_log/" in key
                and not key.endswith("/00000000000000000000.json")
                for key in keys
            ), "Native CREATE advanced beyond zero"
            assert not any(key.endswith(".parquet") for key in keys), (
                "Schema-only CREATE wrote payload"
            )
            assert case["exitCode"] == 0 and case["result"]["failureClass"] is None
            assert (
                case["result"]["authorizationDenied"] is False
                and case["result"]["rows"] == []
            )
            assert case["command"]["expectedRows"] == []
            assert receipt["state"] == "PUBLISHED" and receipt["nativeVersion"] == "0"
            assert receipt["nativeId"] == case["nativeId"]
            assert receipt["location"] == marker["location"]
            assert (
                case["publicationOutcome"] == "PUBLISHED"
                and case["statusNeeded"] is False
            )
        else:
            assert case["expected"] == "retained" and receipt["state"] != "PUBLISHED"
            assert case["publicationOutcome"] in (
                "REFUSED",
                "UNKNOWN",
                "FAILED",
                "NOT_ATTEMPTED",
            )
    return {
        key: case[key]
        for key in ("nativeOutcome", "publicationOutcome", "statusNeeded")
    }


def verify_native(actions, checksum, fixture):
    """Check the initial empty schema recipe and the checksum's exact native inputs."""
    assert len(actions) == 3 and {key for a in actions for key in a} == {
        "commitInfo",
        "metaData",
        "protocol",
    }
    metadata = next(a["metaData"] for a in actions if "metaData" in a)
    protocol = next(a["protocol"] for a in actions if "protocol" in a)
    commit = next(a["commitInfo"] for a in actions if "commitInfo" in a)
    schema = fixture.get(
        "schema",
        {
            "type": "struct",
            "fields": [
                {"name": "id", "type": "long", "nullable": True, "metadata": {}}
            ],
        },
    )
    assert json.loads(metadata["schemaString"]) == schema
    assert metadata["partitionColumns"] == fixture.get(
        "partitionColumns", []
    ) and metadata["format"] == {
        "provider": "parquet",
        "options": {},
    }
    assert metadata.get("description") == fixture.get("comment")
    if "schema" in fixture:
        assert (
            json.loads(commit["operationParameters"]["partitionBy"])
            == fixture["partitionColumns"]
        )
    marker = json.loads(metadata["configuration"]["gravitino.registration.operation"])
    assert (
        marker["version"] == 1
        and marker["operation"] == "CREATE"
        and marker["generation"] == 0
        and marker["schema"] == schema
    )
    configuration = dict(
        fixture["tableProperties"],
        **{
            "delta.columnMapping.mode": "none",
            "gravitino.registration.operation": metadata["configuration"][
                "gravitino.registration.operation"
            ],
        },
    )
    prefix = "spark.databricks.delta.properties.defaults."
    for key, value in fixture["settings"].items():
        if key.startswith(prefix):
            configuration.setdefault("delta." + key[len(prefix) :], value)
    assert metadata["configuration"] == configuration, "Native metadata recipe changed"
    assert set(protocol) == {"minReaderVersion", "minWriterVersion", "writerFeatures"}
    assert protocol["minReaderVersion"] == 1 and protocol["minWriterVersion"] == 7
    assert sorted(protocol["writerFeatures"]) == [
        "appendOnly",
        "changeDataFeed",
        "invariants",
    ]
    assert commit["operation"] == "CREATE TABLE" and "userMetadata" not in commit
    assert checksum["metadata"] == metadata and checksum["protocol"] == protocol
    assert checksum["txnId"] == commit["txnId"]
    assert (
        checksum["allFiles"] == []
        and checksum["numFiles"] == checksum["tableSizeBytes"] == 0
    )
    assert checksum["numMetadata"] == checksum["numProtocol"] == 1
    return True


def verify_frontend(expected, actual):
    """Bind actual caller, installed wheels, archive and pinned connection to its inputs."""
    assert all(actual[key] == value for key, value in expected.items()), (
        "Wrong frontend provenance"
    )
    assert expected["frontend"] in ("sql", "pyspark")
    if expected["frontend"] == "pyspark":
        assert actual["pinned"] is True and actual["sameThread"] is True
        assert actual["wheelVersions"] == {
            "pyspark": expected["spark"],
            "delta-spark": expected["delta"],
            "py4j": "0.10.9.7",
        }
    return True


def verify_registration(table, fixture):
    """Bind the provider's public columns, comments and ordered identities to the fixture."""
    assert {
        k: v
        for k, v in table["properties"].items()
        if not k.startswith("delta.registration.receipt.")
    } == fixture["registrationProperties"]
    assert table["columns"] == fixture["registrationColumns"]
    assert table.get("comment") == fixture.get("comment")
    assert table["partitioning"] == [
        {"strategy": "identity", "fieldName": [name]}
        for name in fixture["partitionColumns"]
    ]
    return True

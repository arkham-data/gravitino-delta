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

"""Explicit bounded final append inventory; fixture settings never toggle caller AQE."""

PROFILES = (
    "plain",
    "partitioned",
    "source-denied",
    "target-denied",
    "duplicate",
    "denied-duplicate",
    "revoke",
    "warm-target-first",
    "warm-project-first",
    "cdf-enabled",
    "cdf-disabled",
    "checkpoint",
    "no-default",
)


def case_specs():
    return [
        dict(
            name=api + "_" + profile.replace("-", "_"),
            api=api,
            profile=profile,
            target=("insert_" if api == "insertInto" else "save_")
            + profile.replace("-", "_"),
            duplicate=profile
            in (
                "duplicate",
                "denied-duplicate",
                "warm-target-first",
                "warm-project-first",
            ),
            appendCount=(
                3
                if profile == "checkpoint"
                else (2 if profile.startswith("cdf-") else 1)
            ),
            warmOrder=(
                profile.removeprefix("warm-") if profile.startswith("warm-") else "none"
            ),
        )
        for api in ("insertInto", "saveAsTable")
        for profile in PROFILES
    ]


def settings(case, transaction_app_id):
    return {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.shuffle.partitions": "8",
        "spark.sql.autoBroadcastJoinThreshold": "-1",
        "spark.sql.adaptive.autoBroadcastJoinThreshold": "-1",
        "spark.databricks.delta.write.txnVersion.autoReset.enabled": "false",
        "spark.databricks.delta.write.txnAppId": transaction_app_id,
        "spark.databricks.delta.write.txnVersion": "1",
        "spark.sql.defaultColumn.useNullsForMissingDefaultValues": str(
            case["profile"] != "no-default"
        ).lower(),
        "spark.databricks.delta.checkpoint.partSize": "1",
        "spark.databricks.delta.asyncUpdate.stalenessTimeLimit": "0",
    }


def fixture_tables(plan):
    tables = [
        dict(
            name="source",
            rows=[
                [1, "alpha", "allowed"],
                [2, "beta", "blocked"],
                [3, "gamma", "allowed"],
            ],
        )
    ]
    for case in case_specs():
        profile = case["profile"]
        table = dict(
            name=case["target"],
            rows=[[0, "sentinel", "seed"]],
            partitioned=profile == "partitioned",
        )
        if profile in ("cdf-enabled", "cdf-disabled"):
            table.update(
                cdf=profile == "cdf-disabled", cdfHistory=[profile == "cdf-enabled"]
            )
        if profile == "checkpoint":
            table["properties"] = {"delta.checkpointInterval": "2"}
        tables.append(table)
    for table in tables:
        table["prefix"] = plan["prefix"] + "/adoption/" + table["name"] + "/"
        table["location"] = (
            "s3a://" + plan["primary"]["bucket"] + "/" + table["prefix"].rstrip("/")
        )
    return tables


def metadata_body(value):
    """Return the metadata body observed by the native fixture collector."""
    assert isinstance(value, dict)
    if "metaData" in value:
        assert set(value) == {"metaData"}
        value = value["metaData"]
    assert isinstance(value, dict) and "partitionColumns" in value
    assert isinstance(value["partitionColumns"], list) and all(
        isinstance(column, str) for column in value["partitionColumns"]
    )
    return value


def verify_registration_transition(
    before, after, target, before_version, after_version
):
    """Permit only the exact native-version receipt advance, retaining all other bytes."""
    import copy

    assert type(before_version) is int and type(after_version) is int
    assert 0 <= before_version <= after_version <= before_version + 1
    assert set(before) == set(after) == {"source", target} and target != "source"
    key = "delta.registration.receipt.nativeVersion"
    for image, version in ((before, before_version), (after, after_version)):
        for name, response in image.items():
            assert response["code"] == 0 and response["table"]["name"] == name
        assert image[target]["table"]["properties"][key] == str(version)
    expected = copy.deepcopy(before)
    expected[target]["table"]["properties"][key] = str(after_version)
    assert (
        expected == after
    ), "Registration changed outside exact target receipt version"


def module_provenance(module):
    """Record loaded source bytes and their actual file or ZIP container without a JVM."""
    import hashlib
    from pathlib import Path

    origin = module.__file__
    loader = module.__spec__.loader
    assert callable(
        getattr(loader, "get_data", None)
    ), "Python provider has no byte loader"
    data = loader.get_data(origin)
    archive = getattr(loader, "archive", None)
    container = Path(archive or origin).resolve()
    if archive is not None:
        assert origin.startswith(archive + "/")
    return dict(
        path=origin,
        sha256=hashlib.sha256(data).hexdigest(),
        loader=type(loader).__module__ + "." + type(loader).__name__,
        member=origin[len(archive) + 1 :] if archive else None,
        container=dict(
            path=str(container),
            sha256=hashlib.sha256(container.read_bytes()).hexdigest(),
        ),
    )


def verify_registration_native(registrations, native, input_timestamp=False):
    """Join native receipt/schema evidence, extending only the finite timestamp target.

    This is the original append fixture's primitive registration join, with the
    explicit timestamp pair mapped to Gravitino's public microsecond TZ type.
    """
    import json

    assert set(registrations) == set(native)
    for name, observed in native.items():
        response = registrations[name]
        assert response["code"] == 0
        table = response["table"]
        assert table["name"] == name
        properties = table["properties"]
        for key, value in {
            "nativeId": observed["native"]["id"],
            "location": observed["location"],
            "nativeVersion": str(observed["version"]),
        }.items():
            assert (
                properties["delta.registration.receipt." + key] == value
            ), "Registration native receipt differs"
        schema = json.loads(observed["schemaJson"])
        assert set(schema) == {"type", "fields"} and schema["type"] == "struct"
        expected = [("id", "long"), ("secret", "string"), ("region", "string")]
        if input_timestamp and name != "source":
            expected += [
                ("__arkham_loaded_at", "timestamp"),
                ("second_time", "timestamp"),
            ]
        fields = schema["fields"]
        assert len(fields) == len(expected)
        for field, (field_name, field_type) in zip(fields, expected):
            assert set(field) == {"name", "type", "nullable", "metadata"}
            assert field["name"] == field_name and field["type"] == field_type
            assert type(field["nullable"]) is bool and type(field["metadata"]) is dict
        columns = table["columns"]
        assert len(columns) == len(fields)
        assert all(column.get("autoIncrement", False) is False for column in columns)
        assert [
            {key: column[key] for key in ("name", "type", "nullable")}
            for column in columns
        ] == [
            dict(
                name=field["name"],
                type=(
                    "timestamp_tz(6)" if field["type"] == "timestamp" else field["type"]
                ),
                nullable=field["nullable"],
            )
            for field in fields
        ], "Registration primitive schema mismatch"

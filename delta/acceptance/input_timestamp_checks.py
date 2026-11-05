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

"""Finite input timestamp qualification layered on the native append evidence."""
from collections import Counter
from datetime import datetime


def timestamp_cases(profile):
    """Return only the required timestamp targets, leaving old inventories untouched."""
    assert profile in ("lead", "remaining")
    pairs = (
        [
            ("insertInto", "plain"),
            ("saveAsTable", "partitioned"),
            ("insertInto", "target-denied"),
            ("saveAsTable", "revoke"),
        ]
        if profile == "lead"
        else [("insertInto", "plain"), ("saveAsTable", "target-denied")]
    )
    return [
        dict(
            name=api + "_timestamp_" + kind.replace("-", "_"),
            api=api,
            profile=kind,
            target="timestamp_" + api.lower() + "_" + kind.replace("-", "_"),
            duplicate=False,
            appendCount=1,
            warmOrder="none",
            inputTimestamp=True,
            revokeSource=kind == "revoke",
        )
        for api, kind in pairs
    ]


def timestamp_tables(plan, specs):
    """Create timestamp targets with their final schema at native version zero."""
    from append_aqe_profiles import fixture_tables

    source = fixture_tables(plan)[0]
    tables = [source]
    for spec in specs:
        prefix = plan["prefix"] + "/adoption/" + spec["target"] + "/"
        tables.append(
            dict(
                name=spec["target"],
                rows=[[0, "sentinel", "seed", None, None]],
                schema="id LONG, secret STRING, region STRING, __arkham_loaded_at TIMESTAMP, second_time TIMESTAMP",
                partitioned=spec["profile"] == "partitioned",
                prefix=prefix,
                location="s3a://"
                + plan["primary"]["bucket"]
                + "/"
                + prefix.rstrip("/"),
            )
        )
    return tables


def verify_timestamp_rows(before, after, committed):
    """Require exact masked/filtered rows and one non-null instant for the query."""
    assert type(committed) is bool
    assert after["version"] == before["version"] + int(
        committed
    ), "Unexpected native publication"
    assert after["publications"] == before["publications"] + (
        [after["version"]] if committed else []
    ), "Unexpected native log"
    old = Counter(tuple(row) for row in before["rows"])
    new = Counter(tuple(row) for row in after["rows"])
    assert not old - new, "Original rows changed"
    added = list((new - old).elements())
    if not committed:
        assert new == old, "Denied write changed rows"
        return
    assert len(added) == 2 and all(len(row) == 5 for row in added)
    assert Counter(row[:3] for row in added) == Counter(
        [(1, "MASKED", "allowed"), (3, "MASKED", "allowed")]
    ), "Unmasked or unfiltered rows"
    values = [value for row in added for value in row[3:]]
    assert all(isinstance(value, str) and value for value in values), "Null timestamp"
    assert len(set(values)) == 1, "Query timestamps differ"
    try:
        datetime.fromisoformat(values[0])
    except ValueError as error:
        raise AssertionError("Invalid timestamp") from error


def verify_timestamp_outcome(outcome, failures):
    """A committed/unconfirmed write can never qualify as a no-effect denial."""
    assert outcome in ("committed", "target-denied", "after-helper-revoked")
    if outcome == "committed":
        assert failures == [], "Committed observation unconfirmed; do not replay"
        return
    method = "requireSnapshot" if outcome == "after-helper-revoked" else "authorize"
    message = (
        "Delta Ranger publication changed; reanalyze the query"
        if method == "requireSnapshot"
        else "Delta Ranger access denied"
    )
    assert any(
        f["className"] == "java.lang.SecurityException"
        and f["message"] == message
        and f["stack"]
        and f["stack"][0].startswith(
            "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerPolicyEngine."
            + method
            + "("
        )
        for f in failures
    ), "Denial swallowed or substituted"


def verify_input_timestamp(result):
    """Reuse the complete append checker after binding the exact timestamp inventory."""
    from append_aqe_checks import verify_delta_append
    from cdf_merge_live_checks import read_ref
    from cdf_read_live_package import capture_sources, ROOT

    assert result["inputTimestampProfile"] in ("lead", "remaining")
    manifest = read_ref(result["runtime"])
    current = capture_sources(ROOT)
    if result.get("revalidationSourceDelta"):
        verify_source_delta(
            manifest["sourceInputs"],
            current,
            read_ref(result["revalidationSourceDelta"]),
        )
    else:
        assert current == manifest["sourceInputs"], "Source moved after package capture"
    assert result["qualification"] is True
    assert result["caseInventory"] == [
        case["name"] for case in timestamp_cases(result["inputTimestampProfile"])
    ]
    assert all(case.get("inputTimestamp") is True for case in result["cases"])
    verify_delta_append(result)


def qualification_cases(result):
    """Use the same finite inventory at both qualification gates."""
    if result.get("inputTimestampProfile"):
        return timestamp_cases(result["inputTimestampProfile"])
    from append_aqe_profiles import case_specs

    return case_specs()


def verify_source_delta(before, after, delta):
    """Bind an explicit read-only checker/docs delta; caller/helper/product bytes stay exact."""
    from cdf_read_live_package import canonical_digest

    allowed = {
        "delta/acceptance/append_aqe_checks.py",
        "delta/acceptance/cdf_read_live_package.py",
        "delta/acceptance/input_timestamp_checks.py",
        "delta/acceptance/test_input_timestamp_checks.py",
        "docs/superpowers/specs/2026-11-05-delta-input-timestamp-findings.md",
    }
    assert delta["scope"] == "input-timestamp-read-only-v1"
    assert delta["beforeInventory"] == before and delta["afterInventory"] == after
    assert delta["beforeDigest"] == canonical_digest(before)
    assert delta["afterDigest"] == canonical_digest(after)
    changed = {
        path for path in set(before) | set(after) if before.get(path) != after.get(path)
    }
    assert changed and changed <= allowed and set(delta["files"]) == changed
    assert all(
        delta["files"][path] == dict(before=before[path], after=after[path])
        for path in changed
    )

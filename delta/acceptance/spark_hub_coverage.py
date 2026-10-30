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

"""Pure validation and summaries for the version-1 Spark Hub coverage register.

The document contains ``schemaVersion=1``, ``repositories`` (sparkHub and
gravitino full commit hashes), ``evidence`` records and a nonempty ``cases``
list. Cases carry a production repository/path/symbol, milestone, API, plan
family, options/features/settings, policy profile, expected outcome, status,
scope (required or optional-unused), evidenceRefs and predecessorIds.

Evidence records carry id, kind (source, contract or runtime), repository,
revision and reference. Runtime records also carry caseId and boolean original.
Only an original runtime reference for that same case permits ``qualified``.
Predecessors resolve within this finite register; they do not imply execution
or qualification of a dependency. Additional fields may hold narrative context.

These checks validate references and schema, never open artifacts, contact
services or certify a runtime pass. The independent acceptance checker owns
verification of original processes, policies, artifacts and actual outcomes.
"""

import re
from collections import Counter
from pathlib import PurePosixPath

STATUSES = (
    "supported-building-block",
    "adaptation-required",
    "connector-gap",
    "unqualified",
    "qualified",
)


def validate_coverage(document: dict) -> None:
    """Raise ValueError on malformed fields, unresolved IDs or unevidenced claims."""
    _require_fields(document, ("schemaVersion", "repositories", "evidence", "cases"), "register")
    if type(document["schemaVersion"]) is not int or document["schemaVersion"] != 1:
        raise ValueError("schemaVersion must be integer 1")
    repositories = document["repositories"]
    _require_fields(repositories, ("sparkHub", "gravitino"), "repositories")
    for name, revision in repositories.items():
        _text(name, "repository name")
        _revision(revision)

    evidence = _records(document["evidence"], "evidence")
    cases = _records(document["cases"], "cases")
    if not cases:
        raise ValueError("cases must not be empty")

    for record in evidence.values():
        _require_fields(record, ("kind", "repository", "revision", "reference"), "evidence")
        if record["kind"] not in ("source", "contract", "runtime"):
            raise ValueError("unsupported evidence kind")
        _repository(record["repository"], repositories)
        _revision(record["revision"])
        _text(record["reference"], "evidence reference")
        if record["kind"] == "runtime":
            _require_fields(record, ("caseId", "original"), "runtime evidence")
            _text(record["caseId"], "runtime caseId")
            if record["caseId"] not in cases or type(record["original"]) is not bool:
                raise ValueError("runtime evidence needs a resolved caseId and boolean original")

    for case_id, case in cases.items():
        _require_fields(
            case,
            (
                "production",
                "milestone",
                "scope",
                "api",
                "planFamily",
                "options",
                "features",
                "settings",
                "policyProfile",
                "expectedOutcome",
                "status",
                "evidenceRefs",
                "predecessorIds",
            ),
            case_id,
        )
        production = case["production"]
        _require_fields(production, ("repository", "path", "symbol"), "production")
        _repository(production["repository"], repositories)
        _text(production["path"], "production path")
        path = PurePosixPath(production["path"])
        if path.is_absolute() or ".." in path.parts or not path.parts:
            raise ValueError("production path must be repository-relative")
        _text(production["symbol"], "production symbol")
        for field in ("milestone", "api", "planFamily", "policyProfile", "expectedOutcome"):
            _text(case[field], field)
        if case["scope"] not in ("required", "optional-unused"):
            raise ValueError("unsupported case scope")
        if case["status"] not in STATUSES:
            raise ValueError("unsupported coverage status")
        for field in ("options", "settings"):
            if not isinstance(case[field], dict):
                raise ValueError(f"{field} must be an object")
        _strings(case["features"], "features")
        _strings(case["evidenceRefs"], "evidenceRefs")
        if not case["evidenceRefs"] or any(ref not in evidence for ref in case["evidenceRefs"]):
            raise ValueError(f"{case_id} needs resolved evidenceRefs")
        _strings(case["predecessorIds"], "predecessorIds")
        if any(ref not in cases or ref == case_id for ref in case["predecessorIds"]):
            raise ValueError(f"{case_id} has an unresolved or self predecessor")
        if case["status"] == "qualified" and not any(
            evidence[ref]["kind"] == "runtime"
            and evidence[ref]["original"] is True
            and evidence[ref]["caseId"] == case_id
            for ref in case["evidenceRefs"]
        ):
            raise ValueError(f"{case_id} requires its own original runtime evidence")


def coverage_summary(document: dict) -> dict:
    """Summarize a validated register, keeping optional cases and each status explicit."""
    validate_coverage(document)
    cases = document["cases"]
    statuses = Counter(case["status"] for case in cases)
    return {
        "schemaVersion": document["schemaVersion"],
        "repositories": dict(document["repositories"]),
        "totalCases": len(cases),
        "byStatus": {status: statuses[status] for status in STATUSES},
        "byMilestone": dict(sorted(Counter(case["milestone"] for case in cases).items())),
        "qualifiedCases": [case["id"] for case in cases if case["status"] == "qualified"],
        "optionalUnusedCases": [case["id"] for case in cases if case["scope"] == "optional-unused"],
    }


def _require_fields(value, fields, label):
    if not isinstance(value, dict) or any(field not in value for field in fields):
        raise ValueError(f"{label} must be an object with fields {', '.join(fields)}")


def _text(value, label):
    if not isinstance(value, str) or not value.strip() or "\x00" in value:
        raise ValueError(f"{label} must be nonempty text without NUL")


def _revision(value):
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{40}", value) is None:
        raise ValueError("repository revisions must be full lowercase Git commit hashes")


def _repository(value, repositories):
    _text(value, "repository")
    if value not in repositories:
        raise ValueError("unresolved repository")


def _strings(value, label):
    if not isinstance(value, list):
        raise ValueError(f"{label} must be a list")
    for item in value:
        _text(item, label)


def _records(value, label):
    if not isinstance(value, list):
        raise ValueError(f"{label} must be a list")
    records = {}
    for record in value:
        _require_fields(record, ("id",), label)
        _text(record["id"], f"{label} id")
        if record["id"] in records:
            raise ValueError(f"duplicate {label} id: {record['id']}")
        records[record["id"]] = record
    return records

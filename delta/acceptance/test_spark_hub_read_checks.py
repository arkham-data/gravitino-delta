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

"""Unit artifacts are deliberately nonqualifying; mutations exercise the real gate."""

import copy
import json
from pathlib import Path
import tempfile
import unittest

from cdf_merge_live_checks import file_ref
from spark_hub_read_checks import verify_observations, verify_result


class TestSparkHubReadChecks(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.serial = 0
        resources = {
            key: {"values": values, "isExcludes": False, "isRecursive": False}
            for key, values in {
                "catalog": ["c"],
                "database": ["db"],
                "table": ["source", "primitive"],
            }.items()
        }

        def item(users):
            return {
                "users": users,
                "groups": [],
                "roles": [],
                "accesses": [{"type": "select", "isAllowed": True}],
            }

        def rule(identifier, kind, users, columns=None, payload="policyItems"):
            resource = copy.deepcopy(resources)
            if columns is not None:
                resource["column"] = {
                    "values": columns,
                    "isExcludes": False,
                    "isRecursive": False,
                }
            return {
                "id": identifier,
                "service": "service",
                "policyType": kind,
                "isEnabled": True,
                "resources": resource,
                payload: [item(users)],
            }

        rules = [
            rule(1, 0, ["bob"], ["id"]),
            rule(2, 0, ["alice", "carol"], ["*"]),
            rule(3, 0, ["carol"], ["email"], "denyPolicyItems"),
            rule(4, 2, ["alice", "bob", "carol"], payload="rowFilterPolicyItems"),
            rule(5, 1, ["alice", "bob", "carol"], ["email"], "dataMaskPolicyItems"),
        ]
        rules[3]["rowFilterPolicyItems"][0]["rowFilterInfo"] = {
            "filterExpr": "region = 'east'"
        }
        rules[4]["dataMaskPolicyItems"][0]["dataMaskInfo"] = {
            "dataMaskType": "CUSTOM",
            "valueExpr": "'MASKED'",
        }
        policy = {"serviceName": "service", "policyVersion": 10, "policies": rules}
        self.policy = self.artifact(policy)
        self.applied = self.applied_policy(policy)
        self.source = {
            "datasetId": "00000000-0000-0000-0000-000000000001",
            "catalog": "c",
            "schema": "db",
            "table": "source",
        }
        self.unpublished = dict(
            self.source,
            datasetId="00000000-0000-0000-0000-000000000002",
            table="unpublished",
        )
        self.unknown = dict(
            self.source, datasetId="00000000-0000-0000-0000-000000000003", table=None
        )
        self.value = {
            "schemaVersion": 1,
            "evidenceKind": "unit-only-nonqualifying",
            "spark": "3.5.4",
            "delta": "3.3.0",
            "rangerServerVersion": "2.8.0",
            "rangerClientVersion": "2.8.0",
            "engine": "delta",
            "catalogRoute": "gravitino",
            "storage": "minio",
            "catalog": "c",
            "database": "db",
            "resourceService": "service",
            "subjects": {
                "table-wide": "alice",
                "id-only": "bob",
                "denied-email": "carol",
            },
            "metadataPrincipal": "metadata",
            "setupIdentity": "setup",
            "runtimeManifest": self.artifact({"spark": "3.5.4", "delta": "3.3.0"}),
            "deployedRanger": self.artifact({"deployedRangerVersion": "2.8.0"}),
            "sparkHubPackage": self.artifact(
                {"sourceInputs": {"src/shared/services/v2/spark.py": "a" * 64}}
            ),
            "sourceInputs": {"src/shared/services/v2/spark.py": "a" * 64},
            "policyIds": {
                "id-only": 1,
                "table-wide": 2,
                "deny-email": 3,
                "filter": 4,
                "mask": 5,
            },
            "processExits": {
                key: self.artifact({"exitCode": 0, "terminated": False})
                for key in [
                    "supervisor",
                    "nativeSeed",
                    "nativeReadback",
                    "table-wide",
                    "id-only",
                    "denied-email",
                ]
            },
            "profiles": {},
            "cases": [],
            "binding": self.source,
            "unpublishedBinding": self.unpublished,
            "registration": self.artifact(
                {
                    "source": {
                        "state": "PUBLISHED",
                        "nativeId": "native",
                        "nativeVersion": 0,
                    },
                    "unpublished": {"state": "ADOPTING"},
                }
            ),
        }
        native = {
            "nativeIdentity": "setup",
            "spark": "3.5.4",
            "tables": {
                "source": {
                    "id": "native",
                    "version": 0,
                    "rows": [[1, "east", "a"], [2, "west", "b"], [3, "east", "c"]],
                }
            },
        }
        self.value["nativeBefore"] = self.path_artifact(
            "cell/fixture/native-seed/result.json", native
        )
        self.value["nativeAfter"] = self.artifact(native)
        for profile, subject in self.value["subjects"].items():
            self.value["profiles"][profile] = {
                "observed": self.artifact(
                    {
                        "spark": "3.5.4",
                        "scala": "2.12.21",
                        "java": "17",
                        "subject": subject,
                    }
                ),
                "metadata": self.artifact(
                    {"principal": "metadata", "successfulTargetGet": True}
                ),
                "caller": self.artifact(
                    {
                        "subject": subject,
                        "sessionReused": True,
                        "nativeDataFrame": True,
                        "pinnedConnection": True,
                        "pinEnvironment": "true",
                        "thread": {
                            "python": 1,
                            "jvm": 2,
                            "subject": subject,
                            "ugi": subject,
                        },
                        "sourceInputs": self.value["sourceInputs"],
                        "python": "/pinned/python",
                        "pythonVersion": "3.11.14",
                        "settings": {
                            "spark.sql.adaptive.enabled": "true",
                            "spark.sql.shuffle.partitions": "4",
                            "spark.sql.adaptive.coalescePartitions.initialPartitionNum": "4",
                            "spark.sql.autoBroadcastJoinThreshold": "-1",
                            "spark.sql.adaptive.autoBroadcastJoinThreshold": "-1",
                        },
                    }
                ),
            }
        self.value["livePython"] = "/pinned/python"
        for name, profile, api, rows, columns, outcome in [
            (
                "named-filter-mask",
                "table-wide",
                "collect",
                [[1, "east", "MASKED"], [3, "east", "MASKED"]],
                ["id", "region", "email"],
                "allowed",
            ),
            ("projected-count", "id-only", "count", [[2]], ["id"], "allowed"),
            ("column-denied", "denied-email", "collect", None, ["email"], "denied"),
            ("unknown-binding", "table-wide", "collect", None, [], "rejected"),
            ("unpublished-registration", "table-wide", "collect", None, [], "rejected"),
            ("fresh-action-revoke", "id-only", "collect", None, ["id"], "denied"),
        ]:
            source = (
                self.unknown
                if name == "unknown-binding"
                else self.unpublished
                if name == "unpublished-registration"
                else self.source
            )
            requests = [
                {
                    "case": name,
                    "user": self.value["subjects"][profile],
                    "policyType": 0,
                    "accessType": "select",
                    "allowed": outcome == "allowed",
                    "resource": {
                        "catalog": "c",
                        "database": "db",
                        "table": "source",
                        "column": column,
                    },
                }
                for column in columns
            ]
            case = {
                "name": name,
                "identityProfile": profile,
                "method": "GravitinoDatasetCatalog.get_table_df_by_id",
                "api": api,
                "actualApi": api if outcome != "rejected" else None,
                "actionRoute": "native-pyspark",
                "sourceBinding": source,
                "outcome": outcome,
                "observedRows": rows,
                "policyPhase": "initial",
                "policySnapshot": self.policy,
                "appliedPolicy": self.applied,
                "originalExit": self.value["processExits"][profile],
                "rangerRequests": requests,
                "listenerQueuesDrained": True,
                "actualActionQueries": 1 if outcome == "allowed" else 0,
                "caseSequence": len(self.value["cases"]) + 1,
                "callbacks": [],
                "originalDemands": [],
                "analyzedPlan": self.text_artifact("original analyzed plan"),
                "finalPlan": self.text_artifact("original final plan"),
                "error": None,
                "tableRequestsBefore": [],
                "tableRequestsAfter": [],
                "adapterCalls": [
                    {
                        "method": "GravitinoDatasetCatalog.get_table_df_by_id",
                        "datasetId": source["datasetId"],
                    }
                ],
                "forbiddenCalls": [],
                "pythonConnections": [],
                "threadBefore": {
                    "python": 1,
                    "jvm": 2,
                    "subject": self.value["subjects"][profile],
                    "ugi": self.value["subjects"][profile],
                },
                "threadAfter": {
                    "python": 1,
                    "jvm": 2,
                    "subject": self.value["subjects"][profile],
                    "ugi": self.value["subjects"][profile],
                },
                "nativeCalls": (
                    []
                    if name == "unknown-binding"
                    else [{"module": "pyspark.sql.session", "method": "table"}]
                    * (2 if name == "named-filter-mask" else 1)
                )
                + (
                    [{"module": "pyspark.sql.dataframe", "method": api}]
                    if outcome == "allowed"
                    else []
                ),
            }
            if outcome == "allowed":
                case["callbacks"] = [
                    {
                        "case": name,
                        "caseSequence": case["caseSequence"],
                        "function": api,
                        "failed": False,
                    }
                ]
            if columns:
                case["originalDemands"] = [
                    {
                        "catalog": "c",
                        "database": "db",
                        "table": "source",
                        "columns": sorted(columns),
                    }
                    for _ in range(2 if name == "named-filter-mask" else 1)
                ]
            if name == "named-filter-mask":
                case["originalDemands"][1]["columns"] = ["id"]
                case["adapterCalls"] *= 2
                plan = self.text_artifact(
                    "AdaptiveSparkPlan isFinalPlan=true\nShuffleQueryStage 0"
                )
                case["finalPlan"] = plan
                case["execution"] = {
                    "isPayload": True,
                    "phase": "ordinary",
                    "rootClass": "org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec",
                    "isFinalPlan": True,
                    "materializedShuffle": 1,
                    "materializedBroadcast": 0,
                    "coalesced": False,
                    "local": False,
                    "skew": False,
                    "reuse": False,
                    "operators": ["AdaptiveSparkPlanExec", "ShuffleQueryStageExec"],
                    "plan": plan,
                }
            if outcome == "denied":
                case["error"] = {
                    "class": "Py4JJavaError",
                    "message": "java.lang.SecurityException: Delta Ranger access denied",
                }
            if name == "unknown-binding":
                case["error"] = {"class": "KeyError", "message": source["datasetId"]}
            if name == "unpublished-registration":
                case["error"] = {
                    "class": "Py4JJavaError",
                    "message": "java.lang.SecurityException: Native Delta registration binding is unavailable",
                }
                case["tableRequestsAfter"] = [
                    {
                        "sequence": 1,
                        "method": "GET",
                        "path": "/api/metalakes/lake/catalogs/c/schemas/db/tables/unpublished",
                        "status": 404,
                    }
                ]
            if name == "fresh-action-revoke":
                case["policyPhase"] = "revoke-id"
                revoked = copy.deepcopy(policy)
                revoked["policyVersion"] = 11
                revoked["policies"] = revoked["policies"][1:]
                case["policySnapshot"] = self.artifact(revoked)
                case["appliedPolicy"] = self.applied_policy(revoked)
                disabled = dict(
                    copy.deepcopy(policy["policies"][0]),
                    service="service",
                    isEnabled=False,
                )
                case["revokeRequest"] = self.path_artifact(
                    "cell/id-only/revoke-id/ranger-trace/0002.request.private.json",
                    {
                        "method": "PUT",
                        "path": "/service/public/v2/api/policy/1",
                        "body": disabled,
                    },
                )
                case["revokeResponse"] = self.path_artifact(
                    "cell/id-only/revoke-id/ranger-trace/0002.response.private.bin",
                    disabled,
                )
                case["precondition"] = self.artifact(
                    {
                        "outcome": "allowed",
                        "observedRows": [[1], [3]],
                        "policySnapshot": self.policy,
                        "appliedPolicy": self.applied,
                        "freshConstruction": True,
                        "rangerRequests": [dict(requests[0], allowed=True)],
                        "actualActionQueries": 1,
                    }
                )
                case["freshConstruction"] = True
            case["artifact"] = self.artifact(case)
            self.value["cases"].append(case)

    def artifact(self, value):
        return self.text_artifact(json.dumps(value))

    def path_artifact(self, name, value):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value))
        return file_ref(path)

    def text_artifact(self, value):
        self.serial += 1
        path = self.root / str(self.serial)
        path.write_text(value)
        return file_ref(path)

    def applied_policy(self, policy):
        download = self.artifact(policy)
        return self.artifact(
            {
                "policy": policy["policyVersion"],
                "sameJvm": True,
                "download": download,
                "downloadSha256": download["sha256"],
                "downloadEvent": {
                    "downstreamResponseCompleted": True,
                    "downloadKind": "policy",
                    "responseBodyFile": download["path"],
                    "responseSha256": download["sha256"],
                },
            }
        )

    def case(self, name):
        return next(c for c in self.value["cases"] if c["name"] == name)

    def refresh(self, case):
        case["artifact"] = self.artifact(
            {key: value for key, value in case.items() if key != "artifact"}
        )

    def rejects(self, message):
        with self.assertRaisesRegex((AssertionError, KeyError), message):
            verify_result(self.value)

    def test_semantic_positive_is_explicitly_nonqualifying(self):
        verify_observations(self.value)
        self.rejects("not live qualification")

    def test_remove_case(self):
        self.value["cases"].pop()
        self.rejects("case inventory")

    def test_successful_case_cannot_replace_another(self):
        self.value["cases"][1] = copy.deepcopy(self.value["cases"][0])
        self.rejects("case inventory")

    def test_remove_runtime(self):
        del self.value["runtimeManifest"]
        self.rejects("runtimeManifest")

    def test_remove_original_exit(self):
        del self.value["processExits"]["id-only"]
        self.rejects("original exit inventory")

    def test_hidden_failed_caller_exit(self):
        self.value["processExits"]["id-only"] = self.artifact({"exitCode": 17})
        self.rejects("original process failed")

    def test_changed_source_hash(self):
        self.value["sourceInputs"]["src/shared/services/v2/spark.py"] = "b" * 64
        self.rejects("packaged source differs")

    def test_wrong_filter_or_mask_policy(self):
        for kind, field, replacement in [
            ("rowFilterPolicyItems", "rowFilterInfo", {"filterExpr": "true"}),
            (
                "dataMaskPolicyItems",
                "dataMaskInfo",
                {"dataMaskType": "CUSTOM", "valueExpr": "email"},
            ),
        ]:
            with self.subTest(kind=kind):
                value = copy.deepcopy(self.value)
                case = next(
                    c for c in value["cases"] if c["name"] == "named-filter-mask"
                )
                policy = json.loads(Path(case["policySnapshot"]["path"]).read_text())
                next(rule for rule in policy["policies"] if kind in rule)[kind][0][
                    field
                ] = replacement
                case["policySnapshot"] = self.artifact(policy)
                case["appliedPolicy"] = self.applied_policy(policy)
                self.refresh(case)
                with self.assertRaisesRegex(
                    AssertionError, "fixture (filter|mask) differs"
                ):
                    verify_result(value)

    def test_wrong_subject(self):
        case = self.case("column-denied")
        case["rangerRequests"][0]["user"] = "setup"
        self.refresh(case)
        self.rejects("request subject")

    def test_wrong_policy_download(self):
        case = self.case("named-filter-mask")
        case["appliedPolicy"] = self.applied_policy(
            {"serviceName": "wrong", "policyVersion": 10}
        )
        self.refresh(case)
        self.rejects("policy download content")

    def test_wrong_masked_rows(self):
        case = self.case("named-filter-mask")
        case["observedRows"][0][2] = "a"
        self.refresh(case)
        self.rejects("exact row multiset")

    def test_exchange_control_rejects_inherited_two_thousand_partitions(self):
        profile = self.value["profiles"]["table-wide"]
        caller = json.loads(Path(profile["caller"]["path"]).read_text())
        caller["settings"][
            "spark.sql.adaptive.coalescePartitions.initialPartitionNum"
        ] = "2000"
        profile["caller"] = self.artifact(caller)
        self.rejects("fixture startup settings differ")

    def test_wrong_count(self):
        case = self.case("projected-count")
        case["observedRows"] = [[3]]
        self.refresh(case)
        self.rejects("exact row multiset")

    def test_metadata_aqe_cannot_qualify_payload(self):
        case = self.case("named-filter-mask")
        case["execution"]["isPayload"] = False
        self.refresh(case)
        self.rejects("metadata is not payload")

    def test_aqe_plan_cannot_be_substituted(self):
        case = self.case("named-filter-mask")
        case["execution"]["plan"] = self.text_artifact(
            "AdaptiveSparkPlan isFinalPlan=true\nShuffleQueryStage 0"
        )
        self.refresh(case)
        self.rejects("AQE plan is not case payload")

    def test_unknown_binding_with_table_request(self):
        case = self.case("unknown-binding")
        case["tableRequestsAfter"] = [
            {"method": "GET", "path": "/tables/source", "status": 403}
        ]
        self.refresh(case)
        self.rejects("unknown binding requested a table")

    def test_unpublished_cannot_be_unknown_binding(self):
        case = self.case("unpublished-registration")
        case["error"] = {"class": "KeyError", "message": "missing"}
        self.refresh(case)
        self.rejects("unpublished named rejection")

    def test_unpublished_cannot_use_authorization_failure_as_named_rejection(self):
        case = self.case("unpublished-registration")
        case["tableRequestsAfter"][0]["status"] = 403
        self.refresh(case)
        self.rejects("unpublished named request")

    def test_denial_requires_actual_denied_email_select(self):
        case = self.case("column-denied")
        case["rangerRequests"][0]["allowed"] = True
        self.refresh(case)
        self.rejects("denied SELECT absent")

    def test_revoke_requires_new_construction(self):
        case = self.case("fresh-action-revoke")
        case["freshConstruction"] = False
        self.refresh(case)
        self.rejects("fresh revoke action")

    def test_revoke_requires_applied_generation_change(self):
        case = self.case("fresh-action-revoke")
        case["policySnapshot"] = self.policy
        case["appliedPolicy"] = self.applied
        self.refresh(case)
        self.rejects("revoke generation|revoked policy")

    def test_revoke_rejects_wrong_policy_or_unsuccessful_disable(self):
        for key, field, value in (
            ("revokeRequest", "id", 99),
            ("revokeRequest", "isEnabled", True),
            ("revokeResponse", "id", 99),
            ("revokeResponse", "isEnabled", True),
        ):
            with self.subTest(key=key, field=field):
                case = self.case("fresh-action-revoke")
                original = case[key]
                path = Path(original["path"])
                original_text = path.read_text()
                record = json.loads(original_text)
                (record["body"] if key == "revokeRequest" else record)[field] = value
                path.write_text(json.dumps(record))
                case[key] = file_ref(path)
                self.refresh(case)
                self.rejects("revoke")
                path.write_text(original_text)
                case[key] = original
                self.refresh(case)

    def test_revoke_rejects_substituted_response(self):
        case = self.case("fresh-action-revoke")
        case["revokeResponse"] = self.artifact({"id": 1, "isEnabled": False})
        self.refresh(case)
        self.rejects("revoke original trace")

    def test_native_source_cannot_change(self):
        native = json.loads(Path(self.value["nativeAfter"]["path"]).read_text())
        native["tables"]["source"]["version"] = 1
        self.value["nativeAfter"] = self.artifact(native)
        self.rejects("native source changed")

    def test_original_case_artifact_cannot_be_relabelled(self):
        self.case("named-filter-mask")["api"] = "count"
        self.rejects("original case artifact differs")

    def test_changed_thread_or_subject_is_not_the_actual_caller(self):
        case = self.case("named-filter-mask")
        case["threadAfter"]["jvm"] = 99
        self.refresh(case)
        self.rejects("actual caller thread")

    def test_isolated_python_caller_cannot_contact_external_service(self):
        case = self.case("named-filter-mask")
        case["pythonConnections"] = [["198.51.100.2", 443]]
        self.refresh(case)
        self.rejects("external Python service")

    def test_success_requires_actual_native_method(self):
        case = self.case("projected-count")
        case["nativeCalls"] = [{"module": "pyspark.sql.session", "method": "table"}]
        self.refresh(case)
        self.rejects("actual native action method")

    def test_rejected_lookup_cannot_claim_native_collect(self):
        case = self.case("unknown-binding")
        case["actualApi"] = "collect"
        self.refresh(case)
        self.rejects("actual action invocation differs")

    def test_unknown_cannot_call_spark_table_even_if_cached(self):
        case = self.case("unknown-binding")
        case["nativeCalls"] = [{"module": "pyspark.sql.session", "method": "table"}]
        self.refresh(case)
        self.rejects("unknown binding reached Spark")

    def test_forbidden_path_or_aws_calls(self):
        case = self.case("projected-count")
        case["forbiddenCalls"] = [{"method": "SparkClient.load_delta_table"}]
        self.refresh(case)
        self.rejects("forbidden fallback")


class TestPythonRuntimeProviders(unittest.TestCase):
    def setUp(self):
        import hashlib
        import zipfile

        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        root = Path(self.directory.name)
        libraries = root / "python/lib"
        libraries.mkdir(parents=True)
        names = [
            "py4j",
            "py4j.clientserver",
            "pyspark",
            "pyspark.java_gateway",
            "pyspark.sql.dataframe",
            "pyspark.sql.readwriter",
            "pyspark.sql.session",
            "pyspark.sql.functions",
        ]
        providers = {}
        expected = {}
        for family in ("py4j", "pyspark"):
            path = libraries / (
                "py4j-0.10.9.7-src.zip" if family == "py4j" else "pyspark.zip"
            )
            with zipfile.ZipFile(path, "w") as archive:
                for name in names:
                    if name.startswith(family):
                        member = name.replace(".", "/") + ".py"
                        data = ("# " + name).encode()
                        archive.writestr(member, data)
                        expected[name] = {"sha256": hashlib.sha256(data).hexdigest()}
                        providers[name] = {
                            "path": str(path) + "/" + member,
                            "loader": "zipimport.zipimporter",
                            "member": member,
                            "sha256": expected[name]["sha256"],
                        }
            for name in names:
                if name.startswith(family):
                    providers[name]["container"] = file_ref(path)
        self.caller = {
            "nativeSparkVersion": "3.5.4",
            "nativePy4jVersion": "0.10.9.7",
            "pinEnvironment": "true",
            "pythonProviders": providers,
        }
        self.package = {"pythonProviders": expected}
        self.runtime = {"spark": "3.5.4", "sparkHome": str(root)}

    def test_actual_stock_python_sources_match_installed_probe_bytes(self):
        from spark_hub_read_checks import verify_python_runtime

        verify_python_runtime(self.caller, self.package, self.runtime)

    def test_substituted_python_module_is_rejected(self):
        from spark_hub_read_checks import verify_python_runtime

        self.caller["pythonProviders"]["pyspark.sql.dataframe"]["sha256"] = "f" * 64
        with self.assertRaisesRegex(AssertionError, "actual Python provider bytes"):
            verify_python_runtime(self.caller, self.package, self.runtime)

    def test_wrong_py4j_version_is_rejected(self):
        from spark_hub_read_checks import verify_python_runtime

        self.caller["nativePy4jVersion"] = "0.10.9.9"
        with self.assertRaisesRegex(AssertionError, "actual native Python versions"):
            verify_python_runtime(self.caller, self.package, self.runtime)


class TestSparkHubPackageBoundaries(unittest.TestCase):
    def test_source_archive_rejects_settings_values_and_unrelated_files(self):
        import zipfile
        from spark_hub_read_live import package_members

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.zip"
            with zipfile.ZipFile(path, "w") as archive:
                archive.writestr("src/settings.py", "# settings code only")
            self.assertEqual(set(package_members(path)), {"src/settings.py"})
            with zipfile.ZipFile(path, "a") as archive:
                archive.writestr(".env", "must not be packaged")
            with self.assertRaisesRegex(AssertionError, "Unrelated Spark Hub"):
                package_members(path)

    def test_duplicate_archive_sources_are_rejected(self):
        import warnings
        import zipfile
        from spark_hub_read_live import package_members

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.zip"
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                with zipfile.ZipFile(path, "w") as archive:
                    archive.writestr("src/settings.py", "first")
                    archive.writestr("src/settings.py", "replacement")
            with self.assertRaisesRegex(AssertionError, "Duplicate Spark Hub"):
                package_members(path)

    def test_call_observation_restores_existing_profiler(self):
        import sys
        from spark_hub_read_pyspark import CallObserver

        previous = sys.getprofile()

        def marker(*args):
            pass

        sys.setprofile(marker)
        try:
            with CallObserver() as observer:
                observer.audit("socket.connect", (None, ("127.0.0.1", 12345)))
                raise ValueError("action failed")
        except ValueError:
            self.assertIs(sys.getprofile(), marker)
            self.assertEqual(observer.connections, [["127.0.0.1", 12345]])
            observer.audit("socket.connect", (None, ("198.51.100.2", 443)))
            self.assertEqual(observer.connections, [["127.0.0.1", 12345]])
        finally:
            sys.setprofile(previous)


class TestObserverClassification(unittest.TestCase):
    def test_adapter_native_and_forbidden_boundaries(self):
        from types import SimpleNamespace
        from spark_hub_read_pyspark import CallObserver

        observer = CallObserver()

        def call(module, method, locals=None):
            frame = SimpleNamespace(
                f_globals={"__name__": module},
                f_code=SimpleNamespace(co_name=method),
                f_locals=locals or {},
            )
            observer.profile(frame, "call", None)

        call(
            "src.shared.services.v3.gravitino_catalog",
            "get_table_df_by_id",
            {"id": "123"},
        )
        call("pyspark.sql.session", "table")
        call("pyspark.sql.dataframe", "collect")
        call("pyspark.sql.dataframe", "count")
        for module, method in (
            ("src.shared.services.v2.spark", "load_delta_table"),
            ("pyspark.sql.readwriter", "load"),
            ("delta.tables", "forPath"),
            ("delta.tables", "isDeltaTable"),
            ("src.shared.glue.client", "query"),
            ("src.shared.athena.client", "query"),
        ):
            call(module, method)
        for service in ("glue", "athena", "s3"):
            client = SimpleNamespace(
                meta=SimpleNamespace(
                    service_model=SimpleNamespace(service_name=service)
                )
            )
            call("botocore.client", "_make_api_call", {"self": client})
        self.assertEqual(
            observer.adapter,
            [
                {
                    "method": "GravitinoDatasetCatalog.get_table_df_by_id",
                    "datasetId": "123",
                }
            ],
        )
        self.assertEqual(
            [entry["method"] for entry in observer.native],
            ["table", "collect", "count"],
        )
        self.assertEqual(
            [entry["method"] for entry in observer.forbidden],
            [
                "load_delta_table",
                "load",
                "forPath",
                "isDeltaTable",
                "query",
                "query",
                "_make_api_call",
                "_make_api_call",
            ],
        )
        self.assertEqual(
            [entry["service"] for entry in observer.forbidden[-2:]], ["glue", "athena"]
        )


class TestReadFixtureExtension(unittest.TestCase):
    def test_metadata_grants_only_existing_published_tables(self):
        from read_parity_live import metadata_grant_tables

        self.assertEqual(
            metadata_grant_tables(("source", "primitive"), ()), ("source", "primitive")
        )
        self.assertEqual(
            metadata_grant_tables(("source", "unpublished"), ("unpublished",)),
            ("source",),
        )

    def test_native_version_uses_explicit_delta_snapshot_update_arguments(self):
        from read_parity_pyspark import native_version
        from unittest.mock import MagicMock

        spark = MagicMock()
        jvm = spark._jvm
        jvm.scala.Option.empty.return_value = "empty-option"
        delta_log = jvm.org.apache.spark.sql.delta.DeltaLog.forTable.return_value
        delta_log.update.return_value.version.return_value = 42
        self.assertEqual(native_version(spark, "s3a://bucket/table"), 42)
        jvm.org.apache.spark.sql.delta.DeltaLog.forTable.assert_called_once_with(
            spark._jsparkSession, "s3a://bucket/table"
        )
        delta_log.update.assert_called_once_with(False, "empty-option", "empty-option")

    def test_process_capture_uses_explicit_clean_working_directory(self):
        import sys
        from read_parity_live import start, finish

        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            clean = directory / "clean"
            clean.mkdir()
            capture = directory / "capture"
            process, out, err = start(
                [sys.executable, "-c", "import os; print(os.getcwd())"],
                {},
                capture,
                cwd=clean,
            )
            process.wait(timeout=20)
            self.assertEqual(finish(process, out, err, capture), 0)
            self.assertEqual(
                Path((capture / "stdout").read_text().strip()).resolve(),
                clean.resolve(),
            )
            self.assertEqual(
                json.loads((capture / "exit.json").read_text())["pid"], process.pid
            )

    def test_verify_only_reads_without_accessing_writer(self):
        from read_parity_pyspark import seed_table

        class Reader:
            def format(self, value):
                self.value = value
                return self

            def load(self, location):
                return (self.value, location)

        class Spark:
            read = Reader()

        class Data:
            @property
            def write(self):
                raise AssertionError("verify-only attempted mutation")

        self.assertEqual(
            seed_table(
                Spark(), {"mode": "verify"}, {"location": "s3a://bucket/table"}, Data()
            ),
            ("delta", "s3a://bucket/table"),
        )

    def test_seed_rejects_existing_prefix_before_writer(self):
        from read_parity_pyspark import seed_table
        from types import SimpleNamespace
        from unittest.mock import patch

        class Objects:
            def __init__(self, *args):
                pass

            def manifest(self):
                return [{"key": "existing"}]

        with patch.dict(
            "sys.modules", {"s3_objects": SimpleNamespace(OwnedObjects=Objects)}
        ):
            with self.assertRaisesRegex(AssertionError, "Fresh isolated seed prefix"):
                seed_table(
                    None,
                    {"mode": "seed", "primary": {}},
                    {"prefix": "owned", "location": "s3a://bucket/table"},
                    None,
                )

    def test_invalid_mode_cannot_mutate(self):
        from read_parity_pyspark import seed_table

        with self.assertRaisesRegex(AssertionError, "native mode"):
            seed_table(None, {"mode": "append"}, {}, None)

    def test_explicit_three_row_fixture_preserves_default_seed(self):
        from read_parity_pyspark import fixture_data

        class Spark:
            def createDataFrame(self, rows, schema):
                return (rows, schema)

            def sql(self, query):
                return query

        from spark_hub_read_checks import ROWS

        self.assertEqual(
            fixture_data(Spark(), {"fixtureRows": ROWS}),
            (ROWS, "id LONG, region STRING, email STRING"),
        )
        self.assertIn("'s1' secret", fixture_data(Spark(), {}))
        self.assertIn("SELECT 5L", fixture_data(Spark(), {}))


class TestMetadataAuthority(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve() / "cell/fixture"
        self.trace = self.root / "public-trace"
        self.trace.mkdir(parents=True)
        native = self.root / "native-seed/result.json"
        native.parent.mkdir()
        native.write_text("{}")
        objects = [
            {
                "type": "CATALOG",
                "fullName": "c",
                "privileges": [{"name": "USE_CATALOG", "condition": "ALLOW"}],
            },
            {
                "type": "SCHEMA",
                "fullName": "c.db",
                "privileges": [
                    {"name": "USE_SCHEMA", "condition": "ALLOW"},
                ],
            },
            {
                "type": "TABLE",
                "fullName": "c.db.source",
                "privileges": [{"name": "SELECT_TABLE", "condition": "ALLOW"}],
            },
        ]
        self.values = {
            "createRequest": {
                "method": "POST",
                "path": "/api/metalakes/lake/roles",
                "body": {"name": "metadata-role", "securableObjects": objects},
            },
            "createResponse": {
                "code": 0,
                "role": {
                    "name": "metadata-role",
                    "securableObjects": copy.deepcopy(objects),
                },
            },
            "grantRequest": {
                "method": "PUT",
                "path": "/api/metalakes/lake/permissions/users/metadata/grant/",
                "body": {"roleNames": ["metadata-role"]},
            },
            "grantResponse": {
                "code": 0,
                "user": {"name": "metadata", "roles": ["metadata-role"]},
            },
            "ownerSetRequest": {
                "method": "PUT",
                "path": "/api/metalakes/lake/owners/schema/c.db",
                "body": {"name": "metadata", "type": "USER"},
            },
            "ownerSetResponse": {"code": 0, "set": True},
            "ownerReadRequest": {
                "method": "GET",
                "path": "/api/metalakes/lake/owners/schema/c.db",
                "body": None,
            },
            "ownerReadResponse": {
                "code": 0,
                "owner": {"name": "metadata", "type": "user"},
            },
        }
        self.paths = {
            "createRequest": "0020.request.private.json",
            "createResponse": "0020.response.private.json",
            "grantRequest": "0021.request.private.json",
            "grantResponse": "0021.response.private.json",
            "ownerSetRequest": "0022.request.private.json",
            "ownerSetResponse": "0022.response.private.json",
            "ownerReadRequest": "0023.request.private.json",
            "ownerReadResponse": "0023.response.private.json",
        }
        self.result = {
            "catalog": "c",
            "database": "db",
            "metadataRole": "metadata-role",
            "metadataPrincipal": "metadata",
            "metadataAuthorityMode": "isolated-schema-owner",
            "nativeBefore": file_ref(native),
            "metadataAuthority": {},
        }
        for key in self.values:
            self.write(key)

    def write(self, key):
        path = self.trace / self.paths[key]
        path.write_text(json.dumps(self.values[key]))
        self.result["metadataAuthority"][key] = file_ref(path)

    def test_original_metadata_authority(self):
        from spark_hub_read_checks import verify_metadata_authority

        verify_metadata_authority(self.result)

    def test_role_and_owner_receipts_cannot_be_substituted(self):
        from spark_hub_read_checks import verify_metadata_authority

        for key in self.values:
            with self.subTest(key=key):
                before = copy.deepcopy(self.values[key])
                if key == "createRequest":
                    self.values[key]["body"]["securableObjects"][1][
                        "privileges"
                    ].append({"name": "MODIFY_TABLE", "condition": "ALLOW"})
                elif key == "createResponse":
                    self.values[key]["role"]["securableObjects"][1]["privileges"].pop()
                elif key == "grantRequest":
                    self.values[key]["body"]["roleNames"] = ["other"]
                elif key == "grantResponse":
                    self.values[key]["code"] = 1
                elif key == "ownerSetRequest":
                    self.values[key]["body"]["name"] = "other"
                elif key == "ownerSetResponse":
                    self.values[key]["set"] = False
                elif key == "ownerReadRequest":
                    self.values[key]["path"] += "other"
                else:
                    self.values[key]["owner"]["name"] = "other"
                self.write(key)
                with self.assertRaises(AssertionError):
                    verify_metadata_authority(self.result)
                self.values[key] = before
                self.write(key)

    def test_schema_owner_policy_binds_distinct_metadata_principal(self):
        from spark_hub_read_checks import verify_metadata_owner_policy

        policy = {
            "policies": [
                {
                    "isEnabled": True,
                    "resources": {
                        key: {
                            "values": value,
                            "isExcludes": False,
                            "isRecursive": False,
                        }
                        for key, value in {
                            "catalog": ["c"],
                            "database": ["db"],
                            "table": ["*"],
                            "column": ["*"],
                        }.items()
                    },
                    "policyItems": [
                        {
                            "roles": ["GRAVITINO_OWNER_ROLE"],
                            "users": ["metadata"],
                            "groups": [],
                            "accesses": [{"type": "all", "isAllowed": True}],
                        }
                    ],
                }
            ]
        }
        path = self.root / "policy.json"
        path.write_text(json.dumps(policy))
        case = {"policySnapshot": file_ref(path)}
        verify_metadata_owner_policy(case, self.result)
        policy["policies"][0]["isEnabled"] = False
        path.write_text(json.dumps(policy))
        case["policySnapshot"] = file_ref(path)
        with self.assertRaisesRegex(
            AssertionError, "metadata schema owner policy scope"
        ):
            verify_metadata_owner_policy(case, self.result)
        policy["policies"][0]["isEnabled"] = True
        policy["policies"][0]["policyItems"][0]["users"] = ["application"]
        path.write_text(json.dumps(policy))
        case["policySnapshot"] = file_ref(path)
        with self.assertRaisesRegex(
            AssertionError, "metadata schema owner policy differs"
        ):
            verify_metadata_owner_policy(case, self.result)


class TestProcessProvenance(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve() / "cell"
        self.root.mkdir()
        self.result = {
            "processExits": {},
            "processCaptures": {},
            "profiles": {},
            "nativeRequests": {},
        }
        self.roles = (
            "supervisor",
            "nativeSeed",
            "nativeReadback",
            "table-wide",
            "id-only",
            "denied-email",
        )
        self.runtime = self.write(self.root / "runtime.json", {"sparkHome": "/spark"})
        self.inputs = self.write(self.root / "inputs.json", {})
        self.package = self.write(self.root / "package.json", {"root": "/source"})
        self.result.update(
            runtimeManifest=self.runtime,
            supervisorInputs=self.inputs,
            sparkHubPackage=self.package,
        )
        for role in self.roles:
            if role == "supervisor":
                directory = self.root.parent / "cell-supervisor"
                argv = [
                    "/python",
                    str(Path(__file__).parent / "spark_hub_read_live.py"),
                    "--worker",
                    "--runtime",
                    self.runtime["path"],
                    "--spark-hub-root",
                    "/source",
                    "--inputs",
                    self.inputs["path"],
                    "--out",
                    str(self.root),
                ]
            elif role.startswith("native"):
                output = self.root / (
                    "fixture/native-seed" if role == "nativeSeed" else "native-readback"
                )
                request = self.write(
                    output / "request.private.json",
                    {
                        "mode": "seed" if role == "nativeSeed" else "verify",
                        "output": str(output),
                        "nativeIdentity": "setup",
                        "spark": "3.5.4",
                        "delta": "3.3.0",
                    },
                )
                self.result["nativeRequests"][role] = request
                artifact = self.write(output / "result.json", {})
                self.result[
                    "nativeBefore" if role == "nativeSeed" else "nativeAfter"
                ] = artifact
                directory = output / "process"
                argv = [
                    "/spark/bin/spark-submit",
                    "--driver-memory",
                    "2g",
                    str(Path(__file__).parent / "read_parity_pyspark.py"),
                    request["path"],
                ]
            else:
                output = self.root / role
                request = self.write(
                    output / "request/spark-hub-request.private.json",
                    {
                        "identityProfile": role,
                        "pythonOutput": str(output / "python-evidence"),
                        "output": str(output / "app-evidence"),
                    },
                )
                caller = self.write(
                    output / "python-evidence/caller.json", {"request": request}
                )
                self.result["profiles"][role] = {"caller": caller}
                directory = output / "process"
                argv = [
                    "/spark/bin/spark-submit",
                    str(Path(__file__).parent / "spark_hub_read_pyspark.py"),
                    request["path"],
                ]
            self.result["processCaptures"][role] = {
                name: self.write(
                    directory / name,
                    argv if name == "argv.json" else "log",
                    raw=name != "argv.json",
                )
                for name in ("argv.json", "stdout", "stderr")
            }
            self.result["processExits"][role] = self.write(
                directory / "exit.json",
                {"exitCode": 0, "pid": self.roles.index(role) + 1, "terminated": False},
            )
        self.result.update(setupIdentity="setup", spark="3.5.4", delta="3.3.0")
        self.sync_worker()

    def write(self, path, value, raw=False):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value if raw else json.dumps(value))
        return file_ref(path)

    def sync_worker(self):
        worker = copy.deepcopy(self.result)
        worker.pop("workerResult", None)
        worker.pop("supervisorInputs", None)
        worker["processCaptures"].pop("supervisor")
        worker["processExits"].pop("supervisor")
        self.result["workerResult"] = self.write(
            self.root / "result.worker.json", worker
        )

    def verify(self):
        from spark_hub_read_checks import verify_process_provenance

        verify_process_provenance(self.result)

    def test_original_six_processes(self):
        self.verify()

    def test_each_noncaller_exit_substitution_is_rejected(self):
        for role in ("supervisor", "nativeSeed", "nativeReadback"):
            with self.subTest(role=role):
                original = self.result["processExits"][role]
                self.result["processExits"][role] = self.result["processExits"][
                    "table-wide"
                ]
                self.sync_worker()
                with self.assertRaisesRegex(
                    AssertionError, "original process exit differs"
                ):
                    self.verify()
                self.result["processExits"][role] = original
                self.sync_worker()

    def test_hidden_failing_original_exit_is_rejected(self):
        original = Path(self.result["processExits"]["nativeSeed"]["path"])
        original.write_text(json.dumps({"exitCode": 1, "pid": 2, "terminated": False}))
        self.result["processExits"]["nativeSeed"] = self.result["processExits"][
            "table-wide"
        ]
        self.sync_worker()
        with self.assertRaisesRegex(AssertionError, "original process exit differs"):
            self.verify()

    def test_missing_each_caller_log_is_rejected(self):
        from spark_hub_read_checks import verify_process_provenance

        for role in ("table-wide", "id-only", "denied-email"):
            for log in ("stdout", "stderr"):
                original = self.result["processCaptures"][role].pop(log)
                self.sync_worker()
                with self.subTest(role=role, log=log), self.assertRaisesRegex(
                    AssertionError, "process capture inventory"
                ):
                    verify_process_provenance(self.result)
                self.result["processCaptures"][role][log] = original
                self.sync_worker()

    def test_wrong_native_command_is_rejected(self):
        capture = self.result["processCaptures"]["nativeSeed"]
        argv = json.loads(Path(capture["argv.json"]["path"]).read_text())
        argv[-1] = str(self.root / "native-readback/request.private.json")
        capture["argv.json"] = self.write(Path(capture["argv.json"]["path"]), argv)
        self.sync_worker()
        with self.assertRaisesRegex(AssertionError, "native command differs"):
            self.verify()

    def test_changed_native_request_is_rejected(self):
        path = Path(self.result["nativeRequests"]["nativeSeed"]["path"])
        request = json.loads(path.read_text())
        request["mode"] = "verify"
        self.result["nativeRequests"]["nativeSeed"] = self.write(path, request)
        self.sync_worker()
        with self.assertRaisesRegex(AssertionError, "native request differs"):
            self.verify()

    def test_supervisor_cannot_point_to_another_output(self):
        capture = self.result["processCaptures"]["supervisor"]
        path = Path(capture["argv.json"]["path"])
        argv = json.loads(path.read_text())
        argv[-1] = str(self.root / "other")
        capture["argv.json"] = self.write(path, argv)
        with self.assertRaisesRegex(AssertionError, "supervisor command differs"):
            self.verify()

    def test_changed_worker_result_is_rejected(self):
        self.result["extra"] = "changed after worker"
        with self.assertRaisesRegex(AssertionError, "original worker result differs"):
            self.verify()


if __name__ == "__main__":
    unittest.main()

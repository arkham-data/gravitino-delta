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

"""Negative controls for the frozen actual shared-writer evidence contract."""
import copy
import unittest

import shared_writer_checks as checks


class TestSharedWriterChecks(unittest.TestCase):
    def test_exact_inventory(self):
        cells = [{"spark": s, "delta": d, "cases": list(checks.case_names((s, d)))} for s, d in checks.PAIRS]
        checks.verify_inventory(cells)
        self.assertEqual(sum(len(c["cases"]) for c in cells), 27)
        for changed in (cells[:-1], cells + cells[:1]):
            with self.assertRaises(AssertionError):
                checks.verify_inventory(changed)
        for mutation in (lambda v: v.pop(), lambda v: v.append("unknown"), lambda v: v.append(v[0])):
            changed = copy.deepcopy(cells)
            mutation(changed[0]["cases"])
            with self.assertRaises(AssertionError):
                checks.verify_inventory(changed)

    def test_frozen_jobs_and_actions(self):
        for name in checks.COMMON + checks.EXTRA:
            contract = checks.contract(name)
            self.assertEqual(len(contract), 2 if name == "shared_additive_append" else 1)
            for index, job in enumerate(contract):
                checks.verify_actions(name, index, copy.deepcopy(job["calls"]))
                for key in job["calls"]:
                    changed = copy.deepcopy(job["calls"])
                    changed[key] += 1
                    with self.assertRaises(AssertionError):
                        checks.verify_actions(name, index, changed)
                with self.assertRaises(AssertionError):
                    checks.verify_actions(name, index, {})
        self.assertEqual(checks.contract("shared_additive_append")[1]["job"], "matching_schema_next_append")

    def test_phase_outcomes_are_independent_and_nonretryable(self):
        for name in checks.COMMON + checks.EXTRA:
            for index, expected in enumerate(checks.contract(name)):
                value = dict(phases=expected["phases"], errorPhase=expected["errorPhase"], attempted=True, retryEligible=False)
                checks.verify_outcome(name, index, value)
                for phase in ("create", "cdf", "data"):
                    changed = copy.deepcopy(value)
                    changed["phases"][phase] = next(v for v in ("unknown", "confirmed", "not_attempted") if v != value["phases"][phase])
                    with self.assertRaises(AssertionError):
                        checks.verify_outcome(name, index, changed)
                value["retryEligible"] = True
                with self.assertRaises(AssertionError):
                    checks.verify_outcome(name, index, value)
        late = checks.contract("shared_late_target_revoke")[0]
        self.assertEqual(late["phases"], dict(create="confirmed", cdf="not_attempted", data="unknown"))

    def test_runtime_and_caller_identity(self):
        value = dict(spark="3.5.6", delta="3.3.2", server="2.8.0", client="2.8.0", java="17", scala="2.12.21")
        checks.verify_identities(value)
        for key in value:
            changed = dict(value, **{key: "wrong"})
            with self.assertRaises(AssertionError):
                checks.verify_identities(changed)
        main = dict(python=1, jvm=2, session="s", ugi="app", groups=["group"])
        tracker = dict(main, python=3, jvm=4)
        caller = dict(main=main, tracker=tracker, joinedBeforeClose=True, readerLiveDuringCount=True,
                      readerLiveUntilClose=True, readerClosed=True, sparkStopped=False, remoteCount=3, trackerCount=3)
        checks.verify_caller(caller)
        for key in ("python", "jvm", "session", "ugi", "groups"):
            changed = copy.deepcopy(caller)
            changed["tracker"][key] = main[key] if key in ("python", "jvm") else "foreign"
            with self.assertRaises(AssertionError):
                checks.verify_caller(changed)
        for key in ("joinedBeforeClose", "readerLiveDuringCount", "readerLiveUntilClose", "readerClosed"):
            changed = dict(caller, **{key: False})
            with self.assertRaises(AssertionError):
                checks.verify_caller(changed)

    def test_real_wrapper_calls_not_import_claim(self):
        calls = [dict(module=module, function=function) for module, function in checks.REQUIRED_CALLS]
        checks.verify_calls(calls)
        for index in range(len(calls)):
            with self.assertRaises(AssertionError):
                checks.verify_calls(calls[:index] + calls[index + 1:])
        with self.assertRaises(AssertionError):
            checks.verify_calls([dict(module="fake", function="job_wrapper")])

    def test_denial_cause_and_late_barrier(self):
        denial = [dict(className="java.lang.SecurityException", message="Delta Ranger access denied",
                        stack=["DeltaRangerPolicyEngine.authorize"])]
        checks.verify_denial("shared_update_denied", denial)
        with self.assertRaises(AssertionError):
            checks.verify_denial("shared_update_denied", [dict(className="java.lang.RuntimeException", message="oops", stack=[])])
        with self.assertRaises(AssertionError):
            checks.verify_denial("shared_create_append", denial)
        with self.assertRaises(AssertionError):
            checks.verify_barrier("shared_late_target_revoke", {})

    def test_credentials_and_credential_derived_records_rejected(self):
        checks.verify_public_record(dict(case="shared_create_append", sourceSha256="a" * 64))
        for key in ("password", "jdbcOptions", "credentialDigest", "environment", "fixturePlanSha256", "originalSecuritySha256"):
            with self.assertRaises(AssertionError):
                checks.verify_public_record({key: "hidden"})
        with self.assertRaises(AssertionError):
            checks.verify_public_record(dict(value="jdbc:postgresql://private"))

    def test_current_producer_requires_source_bytes_and_process(self):
        commit, archive, digest = "a" * 40, "/immutable/hub.zip", "b" * 64
        sources, modules = {}, {}
        for module, _ in checks.REQUIRED_CALLS:
            member = module.replace(".", "/") + ".py"
            sources[member] = "c" * 64
            modules[module] = dict(container=dict(path=archive, sha256=digest), member=member,
                                   path=archive + "/" + member, sha256=sources[member])
        value = dict(producer=commit, processId=101, modules=modules)
        checks.verify_producer(value, commit, sources, archive, digest, 101)
        for key, wrong in (("producer", "d" * 40), ("processId", 102), ("modules", {})):
            changed = copy.deepcopy(value)
            changed[key] = wrong
            with self.assertRaises(AssertionError):
                checks.verify_producer(changed, commit, sources, archive, digest, 101)
        changed = copy.deepcopy(value)
        next(iter(changed["modules"].values()))["sha256"] = "e" * 64
        with self.assertRaises(AssertionError):
            checks.verify_producer(changed, commit, sources, archive, digest, 101)

    def test_native_outcomes_cannot_hide_partial_commits(self):
        before = dict(version=0, publications=[0], nativeId="native", rows=[[0, "sentinel", "seed", None]],
                      checksums=[0], registeredVersion=0, properties={"delta.enableChangeDataFeed": "true"})
        after = copy.deepcopy(before)
        after.update(version=1, publications=[0, 1], checksums=[0, 1], registeredVersion=1,
                     rows=before["rows"] + [[1, "alpha", "allowed", "time"], [2, "beta", "blocked", "time"], [3, "gamma", "allowed", "time"]])
        checks.verify_native("shared_existing_append", 0, before, after)
        checks.verify_native("shared_update_denied", 0, before, before)
        for key, value in (("checksums", [0]), ("nativeId", "replaced"), ("rows", before["rows"]), ("registeredVersion", 0), ("publications", [0])):
            changed = copy.deepcopy(after)
            changed[key] = value
            with self.assertRaises(AssertionError):
                checks.verify_native("shared_existing_append", 0, before, changed)
        with self.assertRaises(AssertionError):
            checks.verify_native("shared_update_denied", 0, before, after)
        with self.assertRaises(AssertionError):
            checks.verify_native("shared_postcommit_hook_failure", 0, before, before)

    def test_denials_reject_unexpected_or_missing_crc(self):
        before = dict(version=0, publications=[0], nativeId="native", rows=[], checksums=[0], registeredVersion=0, properties={})
        checks.verify_native("shared_update_denied", 0, before, before)
        for checksums in ([0, 1], []):
            with self.assertRaises(AssertionError):
                checks.verify_native("shared_update_denied", 0, before, dict(before, checksums=checksums))
        absent = dict(before, version=-1, publications=[], nativeId=None, checksums=[], registeredVersion=-1)
        checks.verify_native("shared_late_target_revoke", 0, absent, before)
        with self.assertRaises(AssertionError):
            checks.verify_native("shared_late_target_revoke", 0, absent, dict(before, checksums=[0, 1]))

    def test_held_late_denial_rejects_next_json_and_crc(self):
        from unittest.mock import patch
        value = dict(held=dict(runId="run"), forwarded=dict(runId="run", status=200, transparent=True),
                     release=dict(exactRangerDenyComplete=True, unrelatedPoliciesUnchanged=True, roleMatchesInitialDownload=True,
                                  access="update", initialPolicy=2, policy=3, policyPriority=1),
                     rawBefore=[], rawHeld=[dict(key="owned/part.parquet"), dict(key="owned/_delta_log/00000000000000000000.json"), dict(key="owned/_delta_log/00000000000000000000.crc")])
        with patch.object(checks, "read_ref", side_effect=lambda v: v):
            checks.verify_barrier("shared_late_target_revoke", value)
            for suffix in ("json", "crc"):
                changed = copy.deepcopy(value)
                changed["rawHeld"].append(dict(key="owned/_delta_log/00000000000000000001." + suffix))
                with self.assertRaises(AssertionError):
                    checks.verify_barrier("shared_late_target_revoke", changed)

    def test_create_role_combines_privileges_without_duplicate_objects(self):
        from shared_writer_live import role_objects
        original = [dict(type="CATALOG", fullName="c", privileges=[dict(name="USE_CATALOG", condition="ALLOW")]),
                    dict(type="SCHEMA", fullName="c.db", privileges=[dict(name="USE_SCHEMA", condition="ALLOW")])]
        before = copy.deepcopy(original)
        result = role_objects(original, "c.db")
        self.assertEqual(len(result), 2)
        self.assertEqual(result[1]["privileges"], [dict(name="USE_SCHEMA", condition="ALLOW"), dict(name="CREATE_TABLE", condition="ALLOW")])
        self.assertEqual(original, before)
        self.assertEqual(role_objects(original, None), original)
        with self.assertRaises(AssertionError):
            role_objects(original + [original[1]], "c.db")

    def test_segment_join_keeps_original_runtime_and_rejects_product_drift(self):
        source = {"spark-connector/v3.5/spark-delta-ranger/src/main/scala/Owned.scala": "owned", "core/src/main/java/Shared.java": "shared"}
        original = dict(spark="3.5.6", delta="3.3.2", sourceInputs=source, changedRuntime="runtime.jar", jarHashes={"runtime.jar": "runtime", "old-app.jar": "old"},
                        sharedArtifacts={"server.jar": "server"}, stockJarHashes={"spark.jar": "spark"}, changedRuntimeClasses={"Owned": "owned"},
                        pythonSourceInputs={"facade.py": "facade"}, pythonZipMembers={"facade.py": "facade"},
                        providers={"Observer": "old-app.jar"}, providerClasses={"Observer": "old-class"})
        current = copy.deepcopy(original);current["jarHashes"]["new-app.jar"] = "new";current["providers"]["Observer"] = "new-app.jar";current["providerClasses"]["Observer"] = "new-class"
        checks.verify_segment_runtime(original, current)
        for key, wrong in (("spark", "3.5.4"), ("sourceInputs", {}), ("changedRuntimeClasses", {"Owned": "changed"}), ("sharedArtifacts", {})):
            changed = copy.deepcopy(original);changed[key] = wrong
            with self.assertRaises(AssertionError):checks.verify_segment_runtime(changed, current)
        origin = dict(Observer=dict(jar="old-app.jar", jarSha256="old", classSha256="old-class"))
        checks.verify_origins(origin, original)
        with self.assertRaises(AssertionError):checks.verify_origins(origin, current)

    def test_exact_deny_uses_override_priority_without_managed_labels(self):
        from shared_writer_live import deny_policy
        plan = dict(resourceService="service", catalog="catalog", database="schema", rangerAppRole="actor-role")
        for access in ("create", "alter", "update"):
            policy = deny_policy(plan, "shared_update_denied", access)
            self.assertEqual(policy["policyPriority"], 1)
            self.assertEqual(policy["resources"]["table"]["values"], ["shared_update_denied"])
            self.assertEqual(policy["resources"]["column"]["values"], ["*"])
            self.assertNotIn("policyLabels", policy)
            self.assertEqual(policy["denyPolicyItems"], [dict(roles=["actor-role"], delegateAdmin=False, accesses=[dict(type=access, isAllowed=True)])])
        with self.assertRaises(AssertionError):
            deny_policy(plan, "shared_update_denied", "all")

    def test_canonical_ranger_deny_readback_rejects_extra_authority(self):
        from shared_writer_live import deny_policy, verify_deny_policy
        plan = dict(resourceService="service", catalog="catalog", database="schema", rangerAppRole="actor-role")
        expected = deny_policy(plan, "shared_update_denied", "update")
        actual = copy.deepcopy(expected)
        actual["denyPolicyItems"][0].pop("users", None);actual["denyPolicyItems"][0].pop("groups", None)
        actual["denyPolicyItems"][0]["delegateAdmin"] = False
        actual.update(id=2192, version=1)
        verify_deny_policy(actual, expected)
        for key, value in (("policyPriority", 0), ("policyItems", [dict(users=["other"])]), ("policyLabels", ["gravitino-managed"]), ("conditions", [dict(type="expression", values=["true"])])):
            changed = copy.deepcopy(actual);changed[key] = value
            with self.assertRaises(AssertionError):verify_deny_policy(changed, expected)
        for key in ("users", "groups"):
            changed = copy.deepcopy(actual);changed["denyPolicyItems"][0][key] = ["other"]
            with self.assertRaises(AssertionError):verify_deny_policy(changed, expected)

    def test_retained_exact_public_grant_is_recognized_without_regrant(self):
        from shared_writer_live import has_exact_table_privilege
        role = dict(securableObjects=[dict(type="table", fullName="c.s.t", privileges=[dict(name="modify_table", condition="allow")])])
        self.assertTrue(has_exact_table_privilege(role, "c.s.t", "MODIFY_TABLE"))
        self.assertFalse(has_exact_table_privilege(role, "c.s.other", "MODIFY_TABLE"))
        self.assertFalse(has_exact_table_privilege(role, "c.s.t", "SELECT_TABLE"))
        changed = copy.deepcopy(role);changed["securableObjects"][0]["privileges"][0]["condition"] = "deny"
        self.assertFalse(has_exact_table_privilege(changed, "c.s.t", "MODIFY_TABLE"))
        with self.assertRaises(AssertionError):
            has_exact_table_privilege(dict(securableObjects=role["securableObjects"] * 2), "c.s.t", "MODIFY_TABLE")

    def test_owner_dto_requires_exact_canonical_user(self):
        from shared_writer_live import verify_owner
        verify_owner({"name": "actor", "type": "user"}, "actor")
        for owner in ({"name": "other", "type": "user"}, {"name": "actor", "type": "group"}, {"name": "actor", "type": "USER"}, {"name": "actor", "type": "user", "extra": True}):
            with self.assertRaises(AssertionError):
                verify_owner(owner, "actor")

    def test_only_early_denials_lack_separate_table_metadata_witness(self):
        absent = {name for name in checks.COMMON + checks.EXTRA if not checks.contract(name)[0]["separateMetadataExpected"]}
        self.assertEqual(absent, {"shared_create_denied", "shared_metadata_denied"})

    def test_actor_visibility_requires_real_forwarded_status(self):
        observed = [dict(method="GET", path="/api/metalakes/l/catalogs/c/schemas/s/tables/shared_create_append", status=404, identityKind="shared-application")]
        checks.verify_visibility("shared_create_append", observed)
        for key, value in (("status", 0), ("identityKind", "metadata"), ("path", "/tables/another")):
            changed = copy.deepcopy(observed)
            changed[0][key] = value
            with self.assertRaises(AssertionError):
                checks.verify_visibility("shared_create_append", changed)

    def test_declared_query_normalizes_varchar_before_issued_plan(self):
        for name in checks.COMMON + checks.EXTRA:
            for job in checks.contract(name):
                self.assertIn("CAST(secret AS TEXT) AS secret", job["query"])
                self.assertEqual(job["sql"]["schemaQueries"], 3)
                self.assertEqual(job["calls"]["sourceRead"], 2)
                self.assertEqual("CAST(42 AS INTEGER) extra" in job["query"], name in ("shared_additive_append", "shared_merge_schema_false", "shared_alter_denied"))

    def test_startup_defaults_cannot_replace_trusted_extensions(self):
        from shared_writer_live import startup_settings, shared_settings
        pair = "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerSparkExtension,io.delta.sql.DeltaSparkSessionExtension"
        defaults = {"spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension", "spark.default.parallelism": "256"}
        inherited = {"spark.sql.extensions": pair, "spark.default.parallelism": "2"}
        effective = startup_settings(defaults, inherited)
        self.assertEqual(effective["spark.sql.extensions"], pair)
        self.assertEqual(effective["spark.default.parallelism"], "2")
        self.assertEqual(effective["spark.sql.adaptive.enabled"], "true")
        self.assertTrue(all(effective[k] == v for k, v in shared_settings().items()))
        self.assertNotIn("spark.databricks.delta.write.txnAppId", effective)
        self.assertNotIn("spark.databricks.delta.write.txnVersion", effective)
        self.assertNotIn("spark.databricks.delta.snapshotPartitions", effective)

    def test_fixture_keeps_create_absent_and_existing_exact(self):
        from shared_writer_live import fixture_tables
        plan = dict(prefix="owned", primary=dict(bucket="bucket"))
        tables = fixture_tables(plan, checks.LEAD)
        targets = {t["name"]: t for t in tables}
        self.assertEqual(set(targets), set(checks.case_names(checks.LEAD)) | {"source"})
        for name in ("shared_create_append", "shared_create_denied", "shared_late_target_revoke"):
            self.assertFalse(targets[name]["seed"])
        self.assertTrue(targets["shared_existing_append"]["cdf"])
        self.assertFalse(targets["shared_cdf_enable_append"]["cdf"])
        self.assertEqual(targets["shared_existing_append"]["rows"], [[0, "sentinel", "seed", None]])
        self.assertNotIn("second_time", targets["shared_existing_append"]["schema"])


if __name__ == "__main__":
    unittest.main()

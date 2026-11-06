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

"""External qualification rejects incomplete or relabeled evidence."""
import copy
import importlib.util
import unittest
from pathlib import Path


class TestExternalJdbcChecks(unittest.TestCase):
    def checker(self):
        path = Path(__file__).with_name("external_jdbc_checks.py")
        self.assertTrue(path.exists(), "External evidence checker is not implemented")
        spec = importlib.util.spec_from_file_location("external_jdbc_checks", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_exact_nineteen_case_inventory(self):
        checks = self.checker()
        cells = [dict(spark=s, delta=d, cases=list(checks.case_names((s, d)))) for s, d in checks.PAIRS]
        self.assertEqual(sum(len(c["cases"]) for c in cells), 19)
        checks.verify_inventory(cells)
        for changed in (cells[:-1], cells + [cells[0]]):
            with self.assertRaises(AssertionError):
                checks.verify_inventory(changed)
        changed = copy.deepcopy(cells)
        changed[0]["cases"].append("external_hub_tracker")
        with self.assertRaises(AssertionError):
            checks.verify_inventory(changed)

    def test_live_dispatch_is_finite(self):
        path = Path(__file__).with_name("external_jdbc_live.py")
        self.assertTrue(path.exists(), "Live dispatch is not implemented")
        import external_jdbc_live as live
        checks = self.checker()
        for pair in checks.PAIRS:
            specs = live.case_specs(pair)
            self.assertEqual([s["name"] for s in specs], list(checks.case_names(pair)))
            self.assertTrue(all(s["name"] == s["target"] and s["externalJdbc"] for s in specs))
            self.assertEqual([s["name"] for s in specs if s["profile"] == "partitioned"], ["external_partitioned_save_as_table"])

    def test_external_postgres_owns_retained_volume(self):
        from external_jdbc_live import ExternalPostgres
        import tempfile
        with tempfile.TemporaryDirectory() as directory:
            pg = ExternalPostgres(Path(directory)/"source", owner="external-jdbc-test")
            self.assertEqual(getattr(pg, "storage", "bind"), "volume")
            self.assertTrue(pg.name.startswith("external-jdbc-test-"))
            self.assertFalse(pg.started)

    def test_explicit_subset_cannot_expand_or_duplicate_cases(self):
        import inspect
        from external_jdbc_live import case_specs
        self.assertIn("selected", inspect.signature(case_specs).parameters)
        selected = ["external_insert_into"]
        self.assertEqual([s["name"] for s in case_specs(("3.5.6", "3.3.2"), selected)], selected)
        for invalid in ([], selected * 2, ["arbitrary_case"], ["external_hub_tracker"]):
            with self.assertRaises(AssertionError):
                case_specs(("3.5.4", "3.3.0"), invalid)

    def test_barrier_uses_existing_deadline_and_same_pinned_thread(self):
        import external_jdbc_pyspark as caller
        self.assertTrue(hasattr(caller, "arm_publication_barrier"))
        class NativeBarrier:
            def __init__(self, thread):
                self.thread = thread
                self.calls = []
            def armCurrentThread(self, target, seconds):
                assert target == "owned_target" and 5 <= seconds <= 120
                self.calls.append((target, seconds))
                return self.thread
        bridge = NativeBarrier(7)
        caller.arm_publication_barrier(bridge, "owned_target", 7)
        self.assertEqual(len(bridge.calls), 1)
        with self.assertRaises(AssertionError):
            caller.arm_publication_barrier(NativeBarrier(8), "owned_target", 7)

    def test_only_completed_unissued_denial_allows_metadata_absence(self):
        checks = self.checker()
        self.assertTrue(hasattr(checks, "verify_early_unissued_close"))
        before = dict(version=0, publications=[0], nativeId="id", rows=[[0,"seed","seed",None,None]])
        failure = dict(className="java.lang.SecurityException", stack=["DeltaRangerExternalJdbcSources$.resolve(Source.scala:91)"])
        args = dict(name="external_unissued_rejected", message="requirement failed: Separately authenticated metadata lookup was not observed",
                    checkpoint=dict(checkpoint="external_unissued_rejected-append", verified=True),
                    observation=dict(failure=[failure], rangerRequests=[]), before=before, after=copy.deepcopy(before),
                    counts=dict(schemaQueries=2, remoteCountQueries=0, payloadQueries=0, mutationQueries=0),
                    identity=dict(spark="3.5.6", java="17", scala="2.12.21", subject="owned", origin="executing-jvm-before-close", jvmPid=123))
        checks.verify_early_unissued_close(**args)
        mutations = [("name","external_insert_into"),("message","another close failure"),("checkpoint",{}),
                     ("observation",dict(failure=[],rangerRequests=[])),("identity",dict(args["identity"],origin="assumed")),
                     ("after",dict(before,version=1))]
        for key in ("payloadQueries","mutationQueries","remoteCountQueries"):
            mutations.append(("counts",dict(args["counts"],**{key:1})))
        mutations.append(("observation",dict(args["observation"],rangerRequests=[{}])))
        for key,value in mutations:
            with self.assertRaises((AssertionError,KeyError)):
                checks.verify_early_unissued_close(**dict(args,**{key:value}))

    def test_capacity_reserve_is_scoped_and_boundary_is_strict(self):
        import external_jdbc_live as live
        self.assertTrue(hasattr(live, "verify_capacity"))
        gib = 1024**3
        self.assertEqual(live.verify_capacity(8*gib+1), 8*gib)
        for pair in (("3.5.4","3.3.2"),("3.5.6","3.3.0")):
            self.assertEqual(live.verify_capacity(5*gib+1, pair, True), 5*gib)
            with self.assertRaises(AssertionError):live.verify_capacity(5*gib, pair, True)
        with self.assertRaises(AssertionError):live.verify_capacity(8*gib)
        with self.assertRaises(AssertionError):live.verify_capacity(9*gib, ("3.5.6","3.3.2"), True)
        with self.assertRaises(AssertionError):live.verify_capacity(6*gib, ("3.5.4","3.3.2"))

    def test_fixture_identity_excludes_credentials_and_query_material(self):
        checks = self.checker()
        self.assertTrue(hasattr(checks, "fixture_identity"))
        plan = {k:"owned" for k in ("catalog","database","resourceService","subject","python","rangerAppRole")}
        plan.update(serverPid=123,sourcePolicyIds=[1],fixtureDirectory="/owned/fixture",primary=dict(endpoint="http://localhost:9000",credentialsFile="private"),
                    plugin=dict(password="fixture-secret"),rangerAdmin=dict(password="fixture-secret"),jdbcOptions=dict(dbtable="raw private query"))
        value = checks.fixture_identity(plan)
        import json
        text = json.dumps(value)
        self.assertNotIn("fixture-secret",text)
        self.assertNotIn("password",text)
        self.assertNotIn("raw private query",text)
        self.assertNotIn("sha256",text)
        self.assertEqual(value["primary"],dict(endpoint="http://localhost:9000"))
        self.assertEqual(value["privateStatePath"],"/owned/fixture/state.private.json")
        for endpoint in ("http://u:p@localhost:9000", "http://localhost:9000?secret=value", "http://localhost:9000#secret"):
            with self.assertRaises(AssertionError):checks.fixture_identity(dict(plan,primary=dict(endpoint=endpoint)))

    def test_safe_fixture_identity_requires_original_fields_and_owner_only_state(self):
        import tempfile,json
        from cdf_merge_live_checks import file_ref
        checks = self.checker()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = {k:"owned" for k in ("catalog","database","resourceService","subject","python","rangerAppRole")}
            plan.update(serverPid=123,sourcePolicyIds=[1],fixtureDirectory=str(root),primary=dict(endpoint="http://localhost:9000"),plugin=dict(password="fixture-secret"))
            private = root/"state.private.json"
            private.write_text(json.dumps(plan));private.chmod(0o600)
            public = root/"identity.json"
            public.write_text(json.dumps(checks.fixture_identity(plan)))
            result = dict(fixtureIdentity=file_ref(public))
            self.assertEqual(checks.load_fixture_identity(result)["subject"],"owned")
            with self.assertRaises(AssertionError):checks.load_fixture_identity(dict(fixturePlan=dict(path=str(private))))
            value = json.loads(public.read_text());value["subject"]="foreign"
            public.write_text(json.dumps(value))
            with self.assertRaises(AssertionError):checks.load_fixture_identity(dict(fixtureIdentity=file_ref(public)))
            public.write_text(json.dumps(checks.fixture_identity(plan)));private.chmod(0o644)
            with self.assertRaises(AssertionError):checks.load_fixture_identity(dict(fixtureIdentity=file_ref(public)))

    def test_deployed_inspection_is_selected_before_collection(self):
        import cdf_live
        self.assertTrue(hasattr(cdf_live,"deployment_inspect_command"))
        self.assertEqual(cdf_live.deployment_inspect_command("owned",False),["docker","inspect","owned"])
        command = cdf_live.deployment_inspect_command("owned",True)
        self.assertIn("--format",command)
        expression = command[-1]
        for field in (".Id", ".Image", ".State.Running", ".NetworkSettings.Ports"):
            self.assertIn(field,expression)
        self.assertNotIn(".Config",expression)
        self.assertNotIn(".Env",expression)
        self.assertNotIn("json .State}",expression)

    def test_action_inventory_is_explicit(self):
        checks = self.checker()
        for name in checks.COMMON + checks.EXTRA:
            expected = checks.action_names(name)
            self.assertTrue(expected)
            checks.verify_actions(name, list(expected))
            for invalid in ([], list(expected) + [expected[0]], list(expected[:-1])):
                with self.assertRaises(AssertionError):
                    checks.verify_actions(name, invalid)

    def test_sql_categories_are_disjoint(self):
        checks = self.checker()
        lines = ["SELECT 1", "SELECT * FROM source_schema.source WHERE 1=0",
                 "SELECT COUNT(*) FROM (SELECT * FROM source_schema.source) count_source",
                 'SELECT "id" FROM source_schema.source',
                 "SELECT 1 FROM (SELECT * FROM source_schema.source) empty_projection",
                 "INSERT INTO source_schema.source VALUES (4)"]
        text = "\n".join("2026-11-06 01:02:03.123 UTC [42] extractor@db LOG: statement: " + sql for sql in lines)
        self.assertEqual(checks.postgres_counts(text), dict(schemaQueries=2, remoteCountQueries=1, payloadQueries=2, mutationQueries=1))

    def test_terminal_outcomes_never_allow_replay(self):
        checks = self.checker()
        for outcome in ("committed", "unknown", "denied"):
            checks.verify_terminal(dict(outcome=outcome, attempted=True, retryEligible=False))
            with self.assertRaises(AssertionError):
                checks.verify_terminal(dict(outcome=outcome, attempted=True, retryEligible=True))

    def test_summary_rejects_connection_secrets(self):
        checks = self.checker()
        checks.verify_summary({"name": "external_insert_into", "outcome": "committed"})
        for value in ({"password": "s"}, {"message": "jdbc:postgresql://local/db"}, {"message": "password=s"}, {"jdbcOptions": {}}):
            with self.assertRaises(AssertionError):
                checks.verify_summary(value)

    def test_hub_caller_requires_actual_modules_and_threads(self):
        checks = self.checker()
        value = dict(modules=["src.data_platform.sources.jdbc.v2.main", "src.shared.commands.progress_tracker", "src.shared.services.v2.spark", "src.shared.services.v3.gravitino_jdbc"],
                     remoteCount=3, trackerCount=3, identities={"main": dict(python=1, jvm=3, session="x", ugi="app", groups=[]), "tracker": dict(python=2, jvm=4, session="x", ugi="app", groups=[])})
        checks.verify_hub(value)
        for key in ("modules", "identities"):
            changed = copy.deepcopy(value)
            changed[key] = [] if key == "modules" else {}
            with self.assertRaises(AssertionError):
                checks.verify_hub(changed)
        changed = copy.deepcopy(value)
        changed["identities"]["tracker"]["jvm"] = 3
        with self.assertRaises(AssertionError):
            checks.verify_hub(changed)

    def test_versions_require_both_ranger_sides_and_pinned_pair(self):
        checks = self.checker()
        self.assertTrue(hasattr(checks, "verify_identities"), "Missing service and runtime identity join")
        good = dict(spark="3.5.6", delta="3.3.2", server="2.8.0", client="2.8.0", java="17", scala="2.12.21")
        checks.verify_identities(good)
        for key in good:
            changed = dict(good, **{key: "wrong"})
            with self.assertRaises(AssertionError):
                checks.verify_identities(changed)

    def test_hub_requires_actual_call_trace(self):
        checks = self.checker()
        self.assertTrue(hasattr(checks, "verify_hub_calls"), "Missing actual production call trace check")
        required = [
            ("src.data_platform.sources.jdbc.v2.main", "run_source_jdbc_v2"),
            ("src.shared.services.v2.spark", "query_jdbc"),
            ("src.shared.services.v3.gravitino_jdbc", "read"),
            ("src.shared.commands.progress_tracker", "_run"),
            ("src.shared.services.v2.spark", "init_df_tracking"),
        ]
        calls = [dict(module=m, function=f, pythonThread=1) for m,f in required]
        checks.verify_hub_calls(calls)
        for index in range(len(calls)):
            with self.assertRaises(AssertionError):
                checks.verify_hub_calls(calls[:index]+calls[index+1:])

    def test_native_observer_partition_field_is_exact(self):
        checks = self.checker()
        self.assertTrue(hasattr(checks, "verify_native_table"), "Missing exact native observer field join")
        image = dict(nativeId="id", version=1, rows=[[1]])
        table = dict(native=dict(id="id"), version=1, reopenVersion=1, rows=[[1]], reopenRows=[[1]], partitionColumns=["region"])
        checks.verify_native_table("external_partitioned_save_as_table", image, table)
        for bad in ([], ["id"]):
            with self.assertRaises(AssertionError):
                checks.verify_native_table("external_partitioned_save_as_table", image, dict(table, partitionColumns=bad))

    def test_segments_preserve_exact_matrix_and_producer_identity(self):
        checks = self.checker()
        self.assertTrue(hasattr(checks, "verify_segment_inventory"), "Missing strict segmented matrix check")
        segments = []
        for pair in checks.PAIRS:
            names = list(checks.case_names(pair))
            for selected in (names[:1], names[1:]):
                segments.append(dict(spark=pair[0], delta=pair[1], cases=selected,
                                     actions={name: list(checks.action_names(name)) for name in selected},
                                     producer="producer-"+"-".join(pair), hub="same-hub", verified=True))
        checks.verify_segment_inventory(segments)
        mutations = [segments[:-1], segments + [segments[0]]]
        for path, value in (((1, "producer"), "wrong"), ((1, "hub"), "wrong"), ((1, "spark"), "9.0"), ((1, "verified"), False), ((1, "cases"), ["extra"])):
            changed = copy.deepcopy(segments)
            changed[path[0]][path[1]] = value
            mutations.append(changed)
        changed = copy.deepcopy(segments)
        changed[0]["actions"][changed[0]["cases"][0]] = []
        mutations.append(changed)
        changed = copy.deepcopy(segments)
        changed[0]["actions"][changed[0]["cases"][0]].append("extra")
        mutations.append(changed)
        for changed in mutations:
            with self.assertRaises(AssertionError):
                checks.verify_segment_inventory(changed)

    def test_external_policy_transition_stays_in_own_catalog(self):
        checks = self.checker()
        self.assertTrue(hasattr(checks, "local_table"), "Missing local resource check")
        plan = dict(catalog="owned", database="db")
        resource = dict(catalog="owned", database="db", table="target")
        self.assertEqual(checks.local_table(plan, resource, ("target",)), "target")
        for key in ("catalog", "database", "table"):
            with self.assertRaises(AssertionError):
                checks.local_table(plan, dict(resource, **{key:"other"}), ("target",))

    def test_late_denials_require_every_barrier_witness(self):
        checks = self.checker()
        required = {"held", "release", "forwarded", "rawHeld", "rawBefore"}
        for name in ("external_scope_closed_before_publish", "external_target_revoked_before_publish"):
            with self.subTest(case=name):
                complete = dict(name=name, barrier={key:dict(path=key,sha256="original") for key in required})
                checks.verify_barrier_inventory(complete)
                changed = dict(name=name)
                with self.assertRaises(AssertionError):checks.verify_barrier_inventory(changed)
                for missing in required:
                    changed = copy.deepcopy(complete)
                    del changed["barrier"][missing]
                    with self.subTest(missing=missing), self.assertRaises(AssertionError):
                        checks.verify_barrier_inventory(changed)
        checks.verify_barrier_inventory(dict(name="external_insert_into"))

    def test_late_revocation_requires_snapshot_cause(self):
        checks = self.checker()
        failure = dict(className="java.lang.SecurityException", message="Delta Ranger publication changed; reanalyze the query", stack=["org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerPolicyEngine.requireSnapshot(DeltaRangerPolicyEngine.java:1)"])
        checks.verify_denial("external_target_revoked_before_publish", [failure])
        with self.assertRaises(AssertionError):
            checks.verify_denial("external_target_revoked_before_publish", [dict(failure, stack=[])])

    def test_partition_cursor_continuation_is_not_an_extra_statement(self):
        checks = self.checker()
        self.assertTrue(hasattr(checks, "verify_partition_protocol"), "Missing cursor continuation proof")
        lower = 'SELECT "id","secret","region" FROM (SELECT id, secret, region FROM source_schema.source) external_input WHERE "id" < 3 or "id" is null'
        upper = lower.split(' WHERE ')[0] + ' WHERE "id" >= 3'
        events = [dict(pid=148, portal="<unnamed>/C_1", fetch=False, sql=upper),
                  dict(pid=149, portal="<unnamed>/C_1", fetch=False, sql=lower),
                  dict(pid=149, portal="<unnamed>/C_1", fetch=True, sql=lower)]
        checks.verify_partition_protocol(events, 3)
        for key, value in (("pid",150),("portal","foreign"),("sql",upper)):
            changed = copy.deepcopy(events)
            changed[2][key] = value
            with self.assertRaises(AssertionError):
                checks.verify_partition_protocol(changed, 3)
        with self.assertRaises(AssertionError):
            checks.verify_partition_protocol(events+[events[0]], 4)
        with self.assertRaises(AssertionError):
            checks.verify_partition_protocol(events, 2)

    def test_hub_dependencies_are_explicit_and_rehashed(self):
        import external_jdbc_live as live
        checks = self.checker()
        self.assertTrue(hasattr(live, "external_python_path"), "Missing retained dependency path")
        self.assertTrue(hasattr(checks, "verify_dependencies"), "Missing actual dependency byte check")
        import tempfile
        import hashlib
        import os
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/"module.py"
            path.write_bytes(b"real source")
            deps = {"package":dict(version="1", files={str(path):hashlib.sha256(path.read_bytes()).hexdigest()})}
            checks.verify_dependencies(deps)
            self.assertEqual(live.external_python_path("hub.zip", "facade.zip", directory).split(os.pathsep)[:3], ["hub.zip", "facade.zip", directory])
            path.write_bytes(b"changed")
            with self.assertRaises(AssertionError):
                checks.verify_dependencies(deps)
            with self.assertRaises(AssertionError):
                checks.verify_dependencies({})

    def test_native_commit_and_publication_are_exact(self):
        checks = self.checker()
        before = dict(version=0, publications=[0], nativeId="id", rows=[[0, "sentinel", "seed", None, None]])
        after = dict(version=1, publications=[0, 1], nativeId="id", rows=before["rows"] + [[i, s, r, "2026-11-06T01:02:03", "2026-11-06T01:02:03"] for i, s, r in [(1, "alpha", "allowed"), (2, "beta", "blocked"), (3, "gamma", "allowed")]])
        checks.verify_native("external_insert_into", before, after)
        for key, bad in (("version", 2), ("publications", [0]), ("nativeId", "other"), ("rows", before["rows"])):
            changed = copy.deepcopy(after)
            changed[key] = bad
            with self.assertRaises(AssertionError):
                checks.verify_native("external_insert_into", before, changed)

    def test_denial_requires_product_cause(self):
        checks = self.checker()
        good = [dict(className="java.lang.SecurityException", message="Delta Ranger access denied", stack=["org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerPolicyEngine.authorize(DeltaRangerPolicyEngine.java:1)"])]
        checks.verify_denial("external_target_update_denied", good)
        for bad in ([], [dict(className="java.net.ConnectException", message="refused", stack=[])], [dict(good[0], stack=[])]):
            with self.assertRaises(AssertionError):
                checks.verify_denial("external_target_update_denied", bad)

    def test_artifacts_rehash_original_bytes(self):
        checks = self.checker()
        import hashlib
        import tempfile
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/"original"
            path.write_bytes(b"original")
            ref = dict(path=str(path), sha256=hashlib.sha256(path.read_bytes()).hexdigest())
            checks.verify_artifact(ref)
            path.write_bytes(b"stale")
            with self.assertRaises(AssertionError):
                checks.verify_artifact(ref)


if __name__ == "__main__":
    unittest.main()

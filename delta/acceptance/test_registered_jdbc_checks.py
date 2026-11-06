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

"""Adversarial tests for the registered JDBC live evidence contract."""
import copy
import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from registered_jdbc_checks import (
    PAIRS, LEAD, case_names, verify_inventory, verify_source, verify_outcome,
    verify_artifact,
)


class TestRegisteredJdbcChecks(unittest.TestCase):
    def test_exact_seventeen_cases(self):
        cells = [{"spark": p[0], "delta": p[1], "cases": list(case_names(p))} for p in PAIRS]
        self.assertEqual(17, sum(len(c["cases"]) for c in cells))
        self.assertEqual(8, len(case_names(LEAD)))
        verify_inventory(cells)
        mutations = [cells[:-1], cells + [cells[0]]]
        for key, value in [("spark", "3.5.5"), ("delta", "3.3.1"), ("cases", []),
                           ("cases", list(case_names(PAIRS[0])) + ["unplanned"]),
                           ("cases", list(case_names(PAIRS[0])) + ["jdbc_insert_into"])]:
            bad = copy.deepcopy(cells)
            bad[0][key] = value
            mutations.append(bad)
        for bad in mutations:
            with self.subTest(bad=bad), self.assertRaises(AssertionError):
                verify_inventory(bad)

    def test_full_identity_driver_and_physical_table(self):
        expected = dict(catalog="source_a", schema="shared", table="source",
                        database="owned_db", provider="jdbc-postgresql", driverVersion="42.7.11",
                        driver="org.postgresql.Driver", serverVersion="16.10",
                        imageId="sha256:owned", kind="TABLE")
        verify_source(expected, expected)
        for key, wrong in [("catalog", "source_b"), ("schema", "owned_db"),
                           ("table", "target"), ("provider", "lakehouse-delta"),
                           ("database", "ranger"), ("driverVersion", "42.7.8"),
                           ("driver", "other.Driver"), ("serverVersion", "15.1"),
                           ("imageId", "sha256:unowned"), ("kind", "VIEW")]:
            bad = dict(expected, **{key: wrong})
            with self.subTest(key=key), self.assertRaises(AssertionError):
                verify_source(bad, expected)
        with self.assertRaises(AssertionError):
            verify_source(dict(expected, nativeVersion=0), expected)

    def outcome(self, denied=False):
        resource = dict(catalog="source_a", database="shared", table="source", column="id")
        return dict(name="jdbc_source_select_denied" if denied else "jdbc_insert_into",
                    outcome="source-denied" if denied else "committed",
                    before=dict(version=0, publications=[0], rows=[[0, "sentinel", "seed", None, None]]),
                    after=dict(version=0 if denied else 1, publications=[0] if denied else [0, 1],
                               rows=[[0, "sentinel", "seed", None, None]] + ([] if denied else
                                     [[1, "MASKED", "allowed", "2026-11-05T12:00:00", "2026-11-05T12:00:00"],
                                      [3, "MASKED", "allowed", "2026-11-05T12:00:00", "2026-11-05T12:00:00"]])),
                    expectedResource=resource, payloadQueries=0 if denied else 1,
                    metadataQueries=2, requests=[dict(resource=resource, accessType="select", allowed=not denied)],
                    failures=[] if not denied else [dict(className="java.lang.SecurityException",
                        message="Delta Ranger access denied",
                        stack=["org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerPolicyEngine.authorize(DeltaRangerPolicyEngine.java:1)"])])

    def test_denial_requires_actual_ranger_cause_and_no_payload(self):
        case = self.outcome(True)
        verify_outcome(case)
        mutations = [dict(case, failures=[]), dict(case, payloadQueries=1), dict(case, requests=[])]
        for cls, message in [("java.sql.SQLException", "Connection refused"),
                             ("java.lang.SecurityException", "Authentication failed")]:
            mutations.append(dict(case, failures=[dict(className=cls, message=message, stack=[])]))
        wrong = copy.deepcopy(case)
        wrong["requests"][0]["resource"]["catalog"] = "source_b"
        # Detach the expected independently: actual and expected must never share mutable data.
        wrong["expectedResource"] = dict(case["expectedResource"])
        mutations.append(wrong)
        for bad in mutations:
            with self.subTest(bad=bad), self.assertRaises(AssertionError):
                verify_outcome(bad)

    def test_exact_native_publications_and_masked_filtered_rows(self):
        case = self.outcome()
        verify_outcome(case)
        for version, publications in [(0, [0]), (2, [0, 1, 2]), (1, [0, 1, 2]), (1, [1])]:
            bad = copy.deepcopy(case)
            bad["after"].update(version=version, publications=publications)
            with self.subTest(version=version, publications=publications), self.assertRaises(AssertionError):
                verify_outcome(bad)
        for key, value in [("secret", "alpha"), ("region", "blocked"), ("time", None)]:
            bad = copy.deepcopy(case)
            bad["after"]["rows"][1][{"secret": 1, "region": 2, "time": 3}[key]] = value
            with self.subTest(key=key), self.assertRaises(AssertionError):
                verify_outcome(bad)
        with self.assertRaises(AssertionError):
            verify_outcome(dict(case, failures=[dict(className="Exception", message="after commit", stack=[])]))
        denied = self.outcome(True)
        denied["after"] = case["after"]
        with self.assertRaises(AssertionError):
            verify_outcome(denied)

    def test_catalog_qualified_policy_projection_preserves_isolation(self):
        from append_aqe_public import qualified_projection

        def policy(catalog, identifier):
            return dict(id=identifier, policyType=0,
                        resources={key: dict(values=[value], isExcludes=False, isRecursive=False)
                                   for key, value in dict(catalog=catalog, database="same", table="source", column="*").items()},
                        policyItems=[dict(roles=["app", "metadata"], accesses=[dict(type="select", isAllowed=True)])])
        original = [policy("first", 1), policy("second", 2)]
        projected, identifier, present = qualified_projection(original, "app", dict(catalog="first", database="same", table="source"), "SELECT_TABLE")
        self.assertEqual((1, True), (identifier, present))
        self.assertEqual(original[1], projected[2])
        self.assertEqual(["metadata"], projected[1]["policyItems"][0]["roles"])
        self.assertIn("app", original[0]["policyItems"][0]["roles"])
        for wrong in ("third", "FIRST"):
            with self.assertRaises(AssertionError):
                qualified_projection(original, "app", dict(catalog=wrong, database="same", table="source"), "SELECT_TABLE")

    def lead_case(self, name):
        source = dict(catalog="jdbc_source", database="source_schema", table="source_view" if name == "jdbc_view_rejected" else "source")
        before = dict(self.outcome(True)["before"], nativeId="native-owned")
        state = dict(aqe="true", jvmThread=7)
        actions = []
        labels = (["id_only", "hidden_predicate", "pruned_original_demand"] if name == "jdbc_column_composition" else (["physical_control", "first"] if name == "jdbc_view_rejected" else ["first"]))
        for label in labels:
            failure = self.outcome(True)["failures"]
            payload = 0
            rows = None
            if label in ("id_only", "physical_control"):
                failure, payload, rows = [], 1, [[1], [3]]
            elif name == "jdbc_source_revoked_before_publish":
                failure = [dict(className="java.lang.SecurityException", message="Delta Ranger publication changed; reanalyze the query",
                                stack=["org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerPolicyEngine.requireSnapshot(File.java:1)"])]
                payload = 1
            elif name == "jdbc_config_changed_before_publish":
                failure = [dict(className="org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerJdbcSourceBindings$Rejected",
                                message="Registered JDBC source rejected: unqualified extraction profile",
                                stack=["org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerJdbcSourceBindings$Observation.requireSame(File.java:1)"])]
                payload = 1
            elif name == "jdbc_view_rejected":
                failure = [dict(className="java.lang.SecurityException", message="Registered JDBC source load rejected",
                                stack=["org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerPostgreSqlCatalogSpark35.loadTable(File.java:1)"])]
            requests = [dict(resource=dict(source, table="source" if label == "physical_control" else source["table"], column="id" if label in ("id_only", "physical_control") else "secret"), accessType="select", allowed=label in ("id_only", "physical_control"))]
            caller = dict(before=state, after=state, pinnedEnvironment="true", rows=rows)
            observation = dict(failure=failure, rangerRequests=requests, aqe=True, listenerQueuesDrained=True, writerThreadId=7)
            actions.append(dict(action=label, failures=failure, requests=requests, rows=rows,
                                payloadQueries=payload, metadataQueries=2, caller=caller, observation=observation))
        value = dict(name=name, source=source, target=dict(catalog="delta_target", database="shared", table=name),
                     before=before, after=before, writerExit=dict(exitCode=0, timedOut=False), actions=actions,
                     request=dict(registeredJdbc=True, sourceCatalog=source["catalog"], sourceSchema=source["database"], sourceTable=source["table"], expectedSparkVersion="3.5.6"),
                     pythonRuntime=dict(spark="3.5.6", pinEnvironment="true"), wrapper=dict(className="org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerPostgreSqlCatalogSpark35"), transitions=[])
        if name in ("jdbc_source_revoked_before_publish", "jdbc_config_changed_before_publish"):
            value["barrier"] = dict(held=dict(writerThreadId=7), release={}, forwarded={}, rawBefore=[], rawHeld=[dict(key="fresh/payload.parquet")])
        return value

    def test_lead_actions_late_rejections_and_view_are_distinct(self):
        from registered_jdbc_checks import verify_case

        with patch("cdf_merge_live_checks.read_ref", side_effect=lambda value: value):
            for name in ("jdbc_column_composition", "jdbc_source_revoked_before_publish", "jdbc_config_changed_before_publish", "jdbc_view_rejected"):
                case = self.lead_case(name)
                verify_case(case)
                mutations = [dict(case, actions=[])]
                bad = copy.deepcopy(case)
                bad["actions"][-1]["payloadQueries"] += 1
                mutations.append(bad)
                bad = copy.deepcopy(case)
                bad["actions"][-1]["failures"] = []
                bad["actions"][-1]["observation"]["failure"] = []
                mutations.append(bad)
                for bad in mutations:
                    with self.subTest(name=name), self.assertRaises(AssertionError):
                        verify_case(bad)
            case = self.lead_case("jdbc_column_composition")
            case["actions"][0]["rows"] = [[1], [2], [3]]
            with self.assertRaises(AssertionError):
                verify_case(case)
            case = self.lead_case("jdbc_config_changed_before_publish")
            case["actions"][0]["failures"][0]["stack"] = ["java.sql.Driver.connect(File.java:1)"]
            with self.assertRaises(AssertionError):
                verify_case(case)

    def test_source_delta_cannot_relabel_helper_or_product(self):
        from registered_jdbc_checks import verify_source_delta
        from cdf_read_live_package import canonical_digest

        def delta(before, after):
            return dict(scope="registered-jdbc-read-only-v1", beforeInventory=before, afterInventory=after,
                        beforeDigest=canonical_digest(before), afterDigest=canonical_digest(after),
                        files={key: dict(before=before.get(key), after=after.get(key)) for key in set(before) | set(after) if before.get(key) != after.get(key)})
        path = "delta/acceptance/registered_jdbc_checks.py"
        before, after = {path: "old"}, {path: "new"}
        verify_source_delta(before, after, delta(before, after))
        for forbidden in ("delta/acceptance/registered_jdbc_live.py", "delta/acceptance/append_aqe_pyspark.py", "spark-connector/src/main/Source.java"):
            changed = dict(after, **{forbidden: "new"})
            with self.assertRaises(AssertionError):
                verify_source_delta(before, changed, delta(before, changed))
        bad = delta(before, after)
        bad["beforeDigest"] = "wrong"
        with self.assertRaises(AssertionError):
            verify_source_delta(before, after, bad)

    def test_timestamp_bounds_reject_reused_or_future_values(self):
        from registered_jdbc_checks import timestamp_millis, verify_timestamp_bounds

        rows = [[1, "MASKED", "allowed", "1970-01-01 00:00:01.001999", "1970-01-01 00:00:01.001999"]]
        self.assertEqual(1001, timestamp_millis(rows[0][3], "UTC"))
        verify_timestamp_bounds(rows, "UTC", 0, 2)
        for started, finished in ((2, 3), (0, 1)):
            with self.assertRaises(AssertionError):
                verify_timestamp_bounds(rows, "UTC", started, finished)
        with self.assertRaises(AssertionError):
            verify_timestamp_bounds([[0, "seed", "seed", None, None]], "UTC", 0, 2)

    def test_postgres_counts_include_lowercase_stock_probes_only_once(self):
        from registered_jdbc_checks import postgres_counts

        def record(sql, role="extractor", kind="execute <unnamed>"):
            return "2026-11-06 00:34:24.567 UTC [206] " + role + "@owned LOG:  " + kind + ": " + sql + "\n"
        text = record("select current_catalog") + record("select current_schema()", "extractor_rotated")
        self.assertEqual(dict(metadataQueries=2, payloadQueries=0), postgres_counts(text))
        text += record('SeLeCt "id" FrOm "source_schema"."source"', kind="statement")
        text += record('select * from source_schema.source where 1 = 0', kind="statement")
        text += record("SELECT 1", "postgres")
        text += record("SET application_name = 'SELECT FROM source_schema.source'")
        text += "2026-11-06 00:34:24.568 UTC [206] extractor@owned DETAIL:  SELECT FROM source_schema.source\n"
        text += "extractor@owned LOG: statement: SELECT FROM source_schema.source\n"
        self.assertEqual(dict(metadataQueries=3, payloadQueries=1), postgres_counts(text))
        self.assertEqual(dict(metadataQueries=0, payloadQueries=1), postgres_counts(record(
            'select id\nfrom source_schema.source_view', kind="statement")))

    def sql_correction_fixture(self, directory):
        import json
        from cdf_merge_live_checks import file_ref

        def save(name, value):
            path = Path(directory) / name
            path.write_text(json.dumps(value) if not isinstance(value, str) else value)
            return file_ref(path)
        prefix = "2026-11-06 00:34:24.567 UTC [206] extractor@owned LOG:  execute <unnamed>: "
        old = prefix + "SELECT 1\n"
        new = old + prefix + "select current_catalog\n" + prefix + "select current_schema()\n"
        new += prefix + 'SELECT * FROM "source_schema"."source" WHERE 1=0\n'
        new += prefix + 'SELECT "id" FROM "source_schema"."source"\n'
        empty = save("stdout", "")
        before = save("before.json", dict(metadataQueries=1, payloadQueries=0,
                      logStdout=empty, logStderr=save("before.log", old)))
        after = save("after.json", dict(metadataQueries=2, payloadQueries=1,
                     logStdout=empty, logStderr=save("after.log", new)))
        case = dict(name="jdbc_insert_into", postgresBefore=before, postgresAfter=after,
                    actions=[dict(action="first", postgresBefore=before, postgresAfter=after,
                                  metadataQueries=1, payloadQueries=1)])
        return case, save

    def test_sql_correction_requires_original_counts_and_log_suffix_hashes(self):
        from registered_jdbc_checks import postgres_case_evidence

        with tempfile.TemporaryDirectory() as directory:
            case, save = self.sql_correction_fixture(directory)
            with self.assertRaises(AssertionError):
                postgres_case_evidence(case)
            evidence = postgres_case_evidence(case, original_classifier=True)
            self.assertEqual(dict(metadataQueries=1, payloadQueries=1), evidence["actions"][0]["recorded"])
            self.assertEqual(dict(metadataQueries=3, payloadQueries=1), evidence["actions"][0]["derived"])
            for key in ("metadataQueries", "payloadQueries"):
                bad = copy.deepcopy(case)
                bad["actions"][0][key] += 1
                with self.subTest(key=key), self.assertRaises(AssertionError):
                    postgres_case_evidence(bad, original_classifier=True)
            import json
            bad = copy.deepcopy(case)
            altered = json.loads(Path(directory, "before.json").read_text())
            altered["logStderr"] = save("different-prefix.log", Path(directory, "before.log").read_text().replace("SELECT 1", "SELECT 2"))
            bad["actions"][0]["postgresBefore"] = save("different-prefix.json", altered)
            with self.assertRaisesRegex(AssertionError, "replaced or truncated"):
                postgres_case_evidence(bad, original_classifier=True)
            Path(directory, "after.log").write_text("replaced")
            with self.assertRaises(AssertionError):
                postgres_case_evidence(case, original_classifier=True)

    def test_sql_correction_cannot_relabel_original_case_or_counts(self):
        from registered_jdbc_checks import postgres_case_evidence, verify_sql_correction

        with tempfile.TemporaryDirectory() as directory:
            case, save = self.sql_correction_fixture(directory)
            original = dict(cases=[case], spark="3.5.6", delta="3.3.2")
            correction = dict(scope="registered-jdbc-sql-count-correction-v1",
                              originalResult=save("raw.json", original),
                              cases=[postgres_case_evidence(case, original_classifier=True)])
            result = dict(original, sqlCountCorrection=save("correction.json", correction))
            verify_sql_correction(result)
            mutations = []
            for path, value in [(('scope',), 'wrong'), (('cases',), []),
                                (('cases', 0, 'actions'), []),
                                (('cases', 0, 'actions', 0, 'recorded', 'metadataQueries'), 3),
                                (('cases', 0, 'actions', 0, 'derived', 'metadataQueries'), 1),
                                (('cases', 0, 'actions', 0, 'derived', 'payloadQueries'), 0),
                                (('cases', 0, 'actions', 0, 'suffixes', 'logStderr', 'sha256'), 'wrong')]:
                changed = copy.deepcopy(correction)
                target = changed
                for key in path[:-1]:
                    target = target[key]
                target[path[-1]] = value
                mutations.append(changed)
            for index, changed in enumerate(mutations):
                bad = dict(result, sqlCountCorrection=save('bad-' + str(index) + '.json', changed))
                with self.subTest(index=index), self.assertRaises(AssertionError):
                    verify_sql_correction(bad)
            bad = copy.deepcopy(result)
            bad['cases'][0]['actions'][0]['metadataQueries'] = 3
            with self.assertRaises(AssertionError):
                verify_sql_correction(bad)

    def test_original_artifact_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "original"
            path.write_bytes(b"original")
            ref = dict(path=str(path), sha256=hashlib.sha256(b"original").hexdigest())
            verify_artifact(ref)
            path.write_bytes(b"changed")
            with self.assertRaises(AssertionError):
                verify_artifact(ref)
            path.unlink()
            with self.assertRaises(AssertionError):
                verify_artifact(ref)


if __name__ == "__main__":
    unittest.main()

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

"""Mutations must not turn unsupported observations into append qualification."""

import copy
import json
from pathlib import Path
import tempfile
import unittest
from append_aqe_checks import verify_iceberg_append
from cdf_merge_live_checks import file_ref
import zipfile


class TestAppendAqeChecks(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.counter = 0

    def artifact(self, value):
        self.counter += 1
        p = self.root / str(self.counter)
        p.write_text(json.dumps(value))
        return file_ref(p)

    def fixture(self, supplemental=False):
        original = [[0, "seed", "original", True]]
        after = original + [
            [1, "east", "M", True],
            [1, "east", "M", True],
            [2, "west", "M", True],
        ]
        cases = []
        for api in ["insertInto", "saveAsTable", "writeTo", "sql"]:
            for form in ["plain", "partitioned"]:
                for identity in ["allowed", "source-denied", "target-denied"]:
                    name = api + "_" + form + "_" + identity.replace("-", "_")
                    events = [
                        dict(
                            reqUser=identity,
                            resource="db/source/id",
                            access="QUERY",
                            action="select",
                            result=int(identity != "source-denied"),
                        ),
                        dict(
                            reqUser=identity,
                            resource="db/" + name,
                            access="QUERY",
                            action="update",
                            result=int(identity != "target-denied"),
                        ),
                    ]
                    cases.append(
                        dict(
                            name=name,
                            api=api,
                            form=form,
                            identity=identity,
                            subject=identity,
                            target=name,
                            outcome="allowed" if identity == "allowed" else "denied",
                            error=None
                            if identity == "allowed"
                            else "Permission denied: org.apache.kyuubi.plugin.spark.authz.AccessControlException",
                            before=dict(snapshot=1, rows=original),
                            after=dict(
                                snapshot=2 if identity == "allowed" else 1,
                                rows=after if identity == "allowed" else original,
                            ),
                            rangerRequests=events,
                            callbacks=[
                                dict(
                                    kind="write-payload",
                                    queryId=7,
                                    failed=False,
                                    planNodes=[
                                        dict(
                                            className="AdaptiveSparkPlanExec",
                                            isFinalPlan=True,
                                        ),
                                        dict(
                                            className="ShuffleQueryStageExec",
                                            materialized=True,
                                        ),
                                        dict(
                                            className="AQEShuffleReadExec",
                                            partitionSpecs="CoalescedPartitionSpec",
                                        ),
                                    ],
                                )
                            ],
                            jobs=[1],
                        )
                    )
        profiles = ["allowed", "source-denied", "target-denied"]
        if supplemental:
            profiles.append("id-only")
            cases.append(
                dict(
                    name="id_only_probe",
                    target="id_only_probe",
                    identity="id-only",
                    outcome="denied",
                    error="Permission denied: org.apache.kyuubi.plugin.spark.authz.AccessControlException",
                    before=dict(snapshot=1, rows=[[0]]),
                    after=dict(snapshot=1, rows=[[0]]),
                    callbacks=[],
                    rangerRequests=[
                        dict(
                            reqUser="id-only",
                            resource="db/source/id",
                            access="QUERY",
                            action="select",
                            result=1,
                        ),
                        dict(
                            reqUser="id-only",
                            resource="db/source/region",
                            access="QUERY",
                            action="select",
                            result=0,
                        ),
                    ],
                )
            )
        for case in cases:
            for event in case["rangerRequests"]:
                event.update(
                    repo="fixture-service", observationKind="audited-permission-event"
                )
            raw = b"".join(
                (
                    json.dumps(
                        {k: v for k, v in event.items() if k != "observationKind"}
                    )
                    + "\n"
                ).encode()
                for event in case["rangerRequests"]
            )
            self.counter += 1
            sink = self.root / ("audit-" + str(self.counter))
            sink.write_bytes(raw)
            case.update(
                auditEvidence=file_ref(sink),
                auditSinks=[file_ref(sink)],
                auditRange=dict(
                    firstByte=0,
                    endByte=len(raw),
                    firstEvent=0,
                    endEvent=2,
                    submittedTotal=2,
                    failedTotal=0,
                    deferredTotal=0,
                ),
                pluginPolicyVersion=1,
            )
            policy = self.artifact(
                dict(policyVersion=1, serviceName="fixture-service", policies=[])
            )
            event = dict(
                method="GET",
                path="/service/plugins/secure/policies/download/fixture-service?version=-1",
                completion="confirmed",
                downstream="sent",
                upstream_status=200,
                response_sha256=policy["sha256"],
            )
            self.counter += 1
            http = self.root / ("http-" + str(self.counter))
            http.write_text(json.dumps(event) + "\n")
            case.update(
                policySnapshot=policy,
                policyDownload=dict(
                    event=event, rawPolicy=policy, processHttp=file_ref(http)
                ),
            )
            for cb in case["callbacks"]:
                analyzed = self.root / ("analyzed-" + case["name"])
                analyzed.write_text("AppendData fixture")
                final = self.root / ("final-" + case["name"])
                final.write_text(
                    "AdaptiveSparkPlan isFinalPlan=true\nShuffleQueryStage 0\nAQEShuffleRead coalesced"
                )
                cb.update(analyzedPlan=file_ref(analyzed), finalPlan=file_ref(final))
        self.counter += 1
        jar = self.root / ("provider-" + str(self.counter) + ".jar")
        with zipfile.ZipFile(jar, "w") as archive:
            archive.writestr(
                "META-INF/maven/org.apache.ranger/ranger-plugins-common/pom.properties",
                "version=2.8.0",
            )
            archive.writestr(
                "META-INF/maven/org.apache.kyuubi/kyuubi-spark-authz_2.12/pom.properties",
                "version=1.10.2",
            )
            archive.writestr("iceberg-build.properties", "git.build.version=1.11.0")
        provider = file_ref(jar)
        origins = {
            name: dict(artifact=provider)
            for name in [
                "org.apache.ranger.plugin.service.RangerBasePlugin",
                "org.apache.iceberg.spark.SparkCatalog",
                "org.apache.kyuubi.plugin.spark.authz.ranger.RangerSparkExtension",
            ]
        }
        runtime_manifest = self.artifact(
            dict(
                spark="3.5.4",
                iceberg="1.11.0",
                kyuubi="1.10.2",
                ranger="2.8.0",
                jars={provider["path"]: provider["sha256"]},
            )
        )
        runtimes = []
        for identity in profiles:
            runtimes += [
                self.artifact(
                    dict(
                        spark="3.5.4",
                        scala="2.12.21",
                        java="17.0.1",
                        sparkUser=identity,
                        origins=origins,
                    )
                ),
                self.artifact(
                    dict(actualClass="org.apache.iceberg.spark.SparkCatalog")
                ),
            ]
        result = dict(
            schemaVersion=1,
            spark="3.5.4",
            catalogRoute="native",
            database="db",
            cases=[c for c in cases if c["identity"] != "id-only"],
            supplementalCases=[c for c in cases if c["identity"] == "id-only"],
            processExits={
                n: self.artifact(dict(exitCode=0, terminated=False))
                for n in ["seed", "verify", "supervisor", *profiles]
            },
            catalogClass="org.apache.iceberg.spark.SparkCatalog",
            verificationSubject="setup",
            subjects={n: n for n in profiles},
            artifacts=[self.artifact(dict(source="original"))],
            service="fixture-service",
            runtimeManifest=runtime_manifest,
            runtimeObservations=runtimes,
        )
        before = self.artifact({c["target"]: c["before"] for c in cases})
        after = self.artifact({c["target"]: c["after"] for c in cases})
        result["artifacts"] += [before, after]
        return result

    def test_failed_callback_does_not_materialize_rejected_plan(self):
        from append_aqe_checks import callback_plan

        class RejectedQuery:
            def analyzed(self):
                raise AssertionError("failed query analyzed again")

            def executedPlan(self):
                raise AssertionError("failed query executed again")

        self.assertEqual(
            callback_plan(RejectedQuery(), True),
            ("Rejected callback; no plan materialization", None),
        )

    def test_injected_column_probe_requires_actual_region_denial(self):
        from append_aqe_checks import verify_injected_column_probe

        probe = dict(
            outcome="denied",
            error="org.apache.kyuubi.plugin.spark.authz.AccessControlException Permission denied:",
            before=dict(rows=[[0]], snapshot=12),
            after=dict(rows=[[0]], snapshot=12),
            rangerRequests=[
                dict(
                    reqUser="id-only",
                    resource="db/source/id",
                    action="select",
                    access="QUERY",
                    result=1,
                ),
                dict(
                    reqUser="id-only",
                    resource="db/source/region",
                    action="select",
                    access="QUERY",
                    result=0,
                ),
            ],
        )
        verify_injected_column_probe(probe, "db", "id-only")
        for change in ["allowed", "wrong-column", "changed-target", "wrong-subject"]:
            value = copy.deepcopy(probe)
            if change == "allowed":
                value["rangerRequests"][1]["result"] = 1
            if change == "wrong-column":
                value["rangerRequests"][1]["resource"] = "db/source/email"
            if change == "changed-target":
                value["after"]["rows"] = [[0], [1]]
            if change == "wrong-subject":
                value["rangerRequests"][1]["reqUser"] = "someone-else"
            with self.subTest(change=change), self.assertRaises(AssertionError):
                verify_injected_column_probe(value, "db", "id-only")

    def test_public_runner_preserves_real_child_exits_and_immutable_results(self):
        import contextlib
        import io
        from unittest.mock import patch
        import iceberg_append_aqe

        for child_exit in [0, 7]:
            with self.subTest(child_exit=child_exit):
                output = self.root / ("cell-" + str(child_exit))
                observations = self.artifact(self.fixture())
                driver = self.root / ("worker-" + str(child_exit) + ".py")
                driver.write_text(
                    "import argparse, pathlib, shutil, sys\n"
                    "parser=argparse.ArgumentParser()\n"
                    "parser.add_argument('--out')\n"
                    "args,_=parser.parse_known_args()\n"
                    "out=pathlib.Path(args.out);out.mkdir()\n"
                    + "shutil.copyfile("
                    + repr(observations["path"])
                    + ",out/'observations.json')\n"
                    + "sys.exit("
                    + str(child_exit)
                    + ")\n"
                )
                with (
                    patch.object(iceberg_append_aqe, "__file__", str(driver)),
                    contextlib.redirect_stdout(io.StringIO()),
                ):
                    if child_exit:
                        with self.assertRaisesRegex(
                            AssertionError, "Original comparator supervisor failed"
                        ):
                            iceberg_append_aqe.run(
                                "3.5.4", "native", self.root / "private-input", output
                            )
                    else:
                        iceberg_append_aqe.run(
                            "3.5.4", "native", self.root / "private-input", output
                        )
                exit_path = output.parent / (output.name + "-supervisor") / "exit.json"
                self.assertEqual(
                    json.loads(exit_path.read_text())["exitCode"], child_exit
                )
                if child_exit:
                    self.assertFalse((output / "result.json").exists())
                else:
                    result_path = output / "result.json"
                    original = result_path.read_bytes()
                    self.assertEqual(json.loads(original)["qualification"], "qualified")
                    with self.assertRaises(AssertionError):
                        iceberg_append_aqe.run(
                            "3.5.4", "native", self.root / "private-input", output
                        )
                    self.assertEqual(result_path.read_bytes(), original)

    def test_run_refuses_existing_output_before_any_side_effect(self):
        from iceberg_append_aqe import run

        with self.assertRaises(AssertionError):
            run("3.5.4", "native", self.root / "absent-private-inputs", self.root)
        self.assertEqual(list(self.root.iterdir()), [])

    def test_supplemental_identity_requires_its_original_process_exit(self):
        result = self.fixture(supplemental=True)
        verify_iceberg_append(result)
        del result["processExits"]["id-only"]
        with self.assertRaisesRegex(AssertionError, "missing original exit"):
            verify_iceberg_append(result)

    def test_supplemental_identity_failed_exit_cannot_qualify(self):
        result = self.fixture(supplemental=True)
        result["processExits"]["id-only"] = self.artifact(
            dict(exitCode=7, terminated=False)
        )
        with self.assertRaisesRegex(AssertionError, "original process failed"):
            verify_iceberg_append(result)

    def test_valid_exact_multiset(self):
        verify_iceberg_append(self.fixture())

    def test_rejects_mutations(self):
        mutations = {
            "flag-only": lambda r: [
                c.update(callbacks=[], aqeEnabled=True) for c in r["cases"]
            ],
            "readback-only": lambda r: [
                cb.update(kind="readback") for c in r["cases"] for cb in c["callbacks"]
            ],
            "missing-exit": lambda r: r["processExits"].pop("allowed"),
            "failed-exit": lambda r: r["processExits"].update(
                allowed=self.artifact(dict(exitCode=1))
            ),
            "wrong-subject": lambda r: r["cases"][0]["rangerRequests"][0].update(
                reqUser="other"
            ),
            "wrong-resource": lambda r: r["cases"][0]["rangerRequests"][0].update(
                resource="else/source/*"
            ),
            "wrong-access": lambda r: r["cases"][0]["rangerRequests"][1].update(
                access="QUERY", action="select"
            ),
            "allowed-only-denial": lambda r: [
                e.update(result=1) for e in r["cases"][1]["rangerRequests"]
            ],
            "wrong-mask": lambda r: r["cases"][0]["after"].update(
                rows=[
                    [0, "seed", "original", True],
                    [1, "east", "z", True],
                    [1, "east", "z", True],
                    [2, "west", "M", True],
                ]
            ),
            "duplicate-loss": lambda r: r["cases"][0]["after"].update(
                rows=[
                    [0, "seed", "original", True],
                    [1, "east", "M", True],
                    [2, "west", "M", True],
                ]
            ),
            "type-coercion": lambda r: r["cases"][0]["after"].update(
                rows=[
                    [False, "seed", "original", True],
                    [1, "east", "M", True],
                    [1, "east", "M", True],
                    [2, "west", "M", True],
                ]
            ),
            "same-snapshot": lambda r: r["cases"][0]["after"].update(snapshot=1),
            "changed-bytes": lambda r: Path(r["artifacts"][0]["path"]).write_text("{}"),
            "wrong-service": lambda r: r.update(service="different"),
            "altered-audit": lambda r: Path(
                r["cases"][0]["auditSinks"][0]["path"]
            ).write_text("{}"),
            "missing-audit": lambda r: r["cases"][0].update(auditSinks=[]),
            "download-mismatch": lambda r: r["cases"][0]["policyDownload"][
                "event"
            ].update(response_sha256="0" * 64),
            "wrong-runtime": lambda r: r.update(
                runtimeManifest=self.artifact(dict(spark="3.5.6"))
            ),
            "fake-adaptive-label": lambda r: [
                cb.update(finalPlan=self.artifact("ordinary scan"))
                for c in r["cases"]
                for cb in c["callbacks"]
            ],
            "wrong-query-kind": lambda r: [
                cb.update(analyzedPlan=self.artifact("readback"))
                for c in r["cases"]
                for cb in c["callbacks"]
            ],
            "unmaterialized": lambda r: [
                n.update(materialized=False)
                for c in r["cases"]
                for cb in c["callbacks"]
                for n in cb["planNodes"]
                if n["className"] == "ShuffleQueryStageExec"
            ],
            "wrapper-identity": lambda r: r.update(catalogRoute="gravitino"),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                value = self.fixture()
                mutate(value)
                with self.assertRaises(AssertionError):
                    verify_iceberg_append(value)


class TestDeltaAppendAqeChecks(unittest.TestCase):
    """Behavioral Delta mutants are independent of the Iceberg contract."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.counter = 0

    def artifact(self, value):
        self.counter += 1
        path = self.root / str(self.counter)
        path.write_text(json.dumps(value))
        return file_ref(path)

    def fixture(self, outcome="committed"):
        before = dict(
            version=0,
            rows=[[0, "sentinel", "seed"]],
            nativeId="native-id",
            publications=[],
        )
        after = copy.deepcopy(before)
        if outcome in ("committed", "postcommit-failure"):
            after.update(
                version=1,
                rows=before["rows"]
                + [[1, "MASKED", "allowed"], [3, "MASKED", "allowed"]],
                publications=[1],
            )
        if outcome == "duplicate":
            before = copy.deepcopy(after)
            before.update(
                version=1,
                rows=[
                    [0, "sentinel", "seed"],
                    [1, "MASKED", "allowed"],
                    [3, "MASKED", "allowed"],
                ],
                publications=[1],
            )
            after = copy.deepcopy(before)
        resource = dict(catalog="catalog", database="db", table="source", column="id")
        events = [
            dict(
                resource=resource,
                user="app",
                accessType="select",
                policyType=0,
                allowed=outcome != "source-denied",
            ),
            dict(
                resource=dict(resource, table="target", column="*"),
                user="app",
                accessType="update",
                policyType=0,
                allowed=outcome not in ("target-denied", "denied-duplicate"),
            ),
        ]
        connection = dict(
            gatewayType="py4j.clientserver.ClientServer",
            clientType="py4j.clientserver.JavaClient",
            pythonThread=1,
            connectionId=2,
            socket=["127.0.0.1", 1],
            peer=["127.0.0.1", 2],
            jvmThread=3,
            aqe="true",
        )

        def policy_entry(table, accesses):
            return dict(
                id=1 if table == "source" else 2,
                policyType=0,
                resources={
                    k: dict(values=[v], isExcludes=False, isRecursive=False)
                    for k, v in dict(
                        catalog="catalog", database="db", table=table, column="*"
                    ).items()
                },
                policyItems=[
                    dict(
                        roles=["role"],
                        accesses=[dict(type=a, isAllowed=True) for a in accesses],
                    )
                ],
            )

        source_allowed = outcome != "source-denied"
        target_allowed = outcome not in ("target-denied", "denied-duplicate")
        policy = dict(
            policyVersion=2,
            serviceName="service",
            policies=[
                policy_entry("source", ["select"] if source_allowed else []),
                policy_entry(
                    "target",
                    ["select", "update", "alter"] if target_allowed else ["select"],
                ),
            ],
        )
        public = dict(
            appUser=dict(name="app", roles=["role"]),
            appRole=dict(
                name="role",
                rangerRole="role",
                securableObjects=[
                    dict(
                        fullName="catalog.db.source",
                        privileges=[dict(name="select_table", condition="allow")]
                        if source_allowed
                        else [],
                    ),
                    dict(
                        fullName="catalog.db.target",
                        privileges=[
                            dict(name=n.lower(), condition="allow")
                            for n in (
                                ["SELECT_TABLE", "MODIFY_TABLE"]
                                if target_allowed
                                else ["SELECT_TABLE"]
                            )
                        ],
                    ),
                ],
            ),
            metadataUser=dict(name="metadata"),
        )
        roles = dict(
            roleVersion=1,
            rangerRoles=[
                dict(
                    name="role",
                    users=[dict(name="app", isAdmin=False)],
                    groups=[],
                    roles=[],
                )
            ],
        )
        applied = dict(
            policy=2,
            role=1,
            download=self.artifact(policy),
            roleDownload=self.artifact(roles),
            publicState=self.artifact(public),
        )
        privilege = "SELECT_TABLE" if not source_allowed else "MODIFY_TABLE"
        grant = source_allowed and target_allowed
        table = "source" if not source_allowed else "target"
        transition = dict(
            privilege=privilege,
            grant=grant,
            beforePolicy=1,
            afterPolicy=2,
            publicRequest=self.artifact(
                dict(
                    method="PUT",
                    path="/api/metalakes/lake/permissions/roles/role/table/catalog.db."
                    + table
                    + ("/grant/" if grant else "/revoke/"),
                    body=dict(privileges=[dict(name=privilege, condition="ALLOW")]),
                )
            ),
            publicResponse=self.artifact(dict(code=0)),
            callbacks=self.artifact(
                [
                    dict(
                        method="PUT",
                        path="/service/public/v2/api/policy/1",
                        completion="confirmed",
                        downstream="sent",
                        upstream_status=200,
                        started_ns=1,
                        completed_ns=2,
                        response_sha256="a" * 64,
                    )
                ]
            ),
        )
        case = dict(
            name="case",
            api="insertInto",
            outcome=outcome,
            source="source",
            target="target",
            callerBefore=connection,
            callerAfter=copy.deepcopy(connection),
            before=self.artifact(before),
            after=self.artifact(after),
            rangerRequests=events,
            applied=self.artifact(applied),
            publicTransition=self.artifact(transition),
            rangerState=self.artifact(dict(policies=policy, roles=roles)),
            failure=[],
            callbacks=[
                dict(
                    kind="write-payload",
                    failed=False,
                    queryId=7,
                    appendOwner=True,
                    planNodes=[
                        dict(className="AdaptiveSparkPlanExec", isFinalPlan=True),
                        dict(className="ShuffleQueryStageExec", materialized=True),
                    ],
                )
            ],
        )
        if outcome not in ("committed", "duplicate"):
            case["failure"] = [
                dict(
                    className="java.lang.SecurityException",
                    message="Delta Ranger access denied",
                    stack=[
                        "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerPolicyEngine.authorize(DeltaRangerPolicyEngine.java:100)"
                    ],
                )
            ]
        if outcome == "postcommit-failure":
            case["failure"] = [
                dict(
                    className="java.lang.IllegalStateException",
                    message="postcommit readback failed",
                    stack=[],
                )
            ]
        return dict(
            schemaVersion=1,
            qualification=False,
            spark="3.5.4",
            delta="3.3.0",
            subject="app",
            metadataSubject="metadata",
            verificationSubject="setup",
            metalake="lake",
            catalog="catalog",
            database="db",
            processExits={
                name: self.artifact(dict(exitCode=0, timedOut=False))
                for name in ["writer", "native-before", "native-after", "supervisor"]
            },
            cases=[case],
        )

    def check(self, value):
        import append_aqe_checks

        checker = getattr(append_aqe_checks, "verify_delta_append", None)
        self.assertTrue(callable(checker), "Delta observations need a separate checker")
        checker(value)

    def test_delta_commit_and_duplicate_and_postcommit_classification(self):
        for outcome in [
            "committed",
            "duplicate",
            "source-denied",
            "target-denied",
            "denied-duplicate",
            "postcommit-failure",
        ]:
            with self.subTest(outcome=outcome):
                self.check(self.fixture(outcome))

    def test_delta_rejects_missing_authority_and_metadata_only_aqe(self):
        mutations = {
            "missing-source-select": lambda c: c.update(
                rangerRequests=c["rangerRequests"][1:]
            ),
            "missing-target-update": lambda c: c.update(
                rangerRequests=c["rangerRequests"][:1]
            ),
            "missing-public-transition": lambda c: c.pop("publicTransition"),
            "missing-applied-download": lambda c: c.update(
                applied=self.artifact(dict(policy=2, role=1))
            ),
            "metadata-only-aqe": lambda c: c["callbacks"][0].update(kind="metadata"),
            "unowned-adaptive-plan": lambda c: c["callbacks"][0].update(
                appendOwner=False
            ),
            "caller-aqe-toggle": lambda c: c["callerAfter"].update(aqe="false"),
            "new-py4j-connection": lambda c: c["callerAfter"].update(connectionId=8),
            "unexpected-rows": lambda c: c.update(
                after=self.artifact(
                    dict(
                        version=1,
                        rows=[[0, "sentinel", "seed"]],
                        nativeId="native-id",
                        publications=[1],
                    )
                )
            ),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                value = self.fixture()
                mutate(value["cases"][0])
                with self.assertRaises(AssertionError):
                    self.check(value)

    def test_posthelper_refusal_requires_real_held_thread_witness(self):
        value = self.fixture("target-denied")
        case = value["cases"][0]
        case["outcome"] = "after-helper-revoked"
        case["rangerRequests"][1]["allowed"] = True
        case["failure"] = [
            dict(
                className="java.lang.SecurityException",
                message="Delta Ranger publication changed; reanalyze the query",
                stack=[
                    "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerPolicyEngine.requireSnapshot(DeltaRangerPolicyEngine.java:100)"
                ],
            )
        ]
        with self.assertRaises(AssertionError):
            self.check(value)

    def test_delta_requires_actual_downloaded_role_membership(self):
        value = self.fixture()
        case = value["cases"][0]
        applied = json.loads(Path(case["applied"]["path"]).read_text())
        applied.pop("roleDownload")
        case["applied"] = self.artifact(applied)
        with self.assertRaises(AssertionError):
            self.check(value)

    def test_delta_payload_ownership_cannot_be_borrowed_from_another_callback(self):
        value = self.fixture()
        case = value["cases"][0]
        case["callbacks"][0]["appendOwner"] = False
        case["callbacks"].append(
            dict(
                kind="write-payload",
                failed=False,
                appendOwner=True,
                queryId=8,
                planNodes=[],
            )
        )
        with self.assertRaises(AssertionError):
            self.check(value)

    def test_delta_rejects_false_denial_duplicate_checkpoint_and_cache(self):
        cases = [
            (
                "generic-error-denial",
                "source-denied",
                lambda c: c.update(
                    failure=[
                        dict(
                            className="java.net.SocketTimeoutException",
                            message="timeout",
                            stack=[],
                        )
                    ]
                ),
            ),
            (
                "duplicate-publication",
                "duplicate",
                lambda c: c.update(
                    after=self.artifact(
                        dict(
                            version=2,
                            rows=[
                                [0, "sentinel", "seed"],
                                [1, "MASKED", "allowed"],
                                [3, "MASKED", "allowed"],
                            ],
                            nativeId="native-id",
                            publications=[1, 2],
                        )
                    )
                ),
            ),
            (
                "postcommit-called-rollback",
                "target-denied",
                lambda c: c.update(
                    after=self.artifact(
                        dict(
                            version=1,
                            rows=[
                                [0, "sentinel", "seed"],
                                [1, "MASKED", "allowed"],
                                [3, "MASKED", "allowed"],
                            ],
                            nativeId="native-id",
                            publications=[1],
                        )
                    )
                ),
            ),
            (
                "wrong-checkpoint-rows",
                "committed",
                lambda c: c.update(
                    checkpoint=self.artifact(
                        dict(
                            version=1,
                            rows=[],
                            nativeId="native-id",
                            files=[self.artifact("checkpoint")],
                        )
                    )
                ),
            ),
            (
                "wrong-warm-cache-result",
                "committed",
                lambda c: c.update(
                    warm=self.artifact(
                        dict(
                            order="target-first",
                            targetRows=[],
                            repeatedTargetRows=[],
                            dependentRows=[],
                        )
                    )
                ),
            ),
        ]
        for name, outcome, mutate in cases:
            with self.subTest(name=name):
                value = self.fixture(outcome)
                mutate(value["cases"][0])
                with self.assertRaises(AssertionError):
                    self.check(value)


class TestAppendAqePublicTransitions(unittest.TestCase):
    def fixture(self):
        def public(selected):
            return dict(
                appUser=dict(name="app", roles=["role"]),
                metadataUser=dict(name="meta", roles=["meta-role"]),
                metadataRole=dict(
                    name="meta-role",
                    securableObjects=[
                        dict(
                            fullName="cat.db.source",
                            privileges=[dict(name="SELECT_TABLE", condition="ALLOW")],
                        )
                    ],
                ),
                appRole=dict(
                    name="role",
                    securableObjects=[
                        dict(
                            fullName="cat.db.source",
                            privileges=[dict(name="SELECT_TABLE", condition="ALLOW")]
                            if selected
                            else [],
                        )
                    ],
                ),
            )

        def snapshot(selected, version):
            return dict(
                public=public(selected),
                policies=dict(
                    policyVersion=version,
                    policies=[
                        dict(
                            id=1,
                            policyType=0,
                            resources=dict(table=dict(values=["source"])),
                            policyItems=[
                                dict(
                                    roles=["meta-role"]
                                    + (["role"] if selected else []),
                                    accesses=[dict(type="select", isAllowed=True)],
                                )
                            ],
                        ),
                        dict(
                            id=2,
                            policyType=2,
                            resources=dict(table=dict(values=["source"])),
                            rowFilterPolicyItems=[dict(filter="region = 'allowed'")],
                        ),
                    ],
                ),
                roles=dict(roleVersion=1, rangerRoles=[]),
            )

        plan = dict(catalog="cat", database="db", appRole="role", rangerAppRole="role")
        callbacks = [
            dict(
                method="PUT",
                path="/service/public/v2/api/policy/1",
                completion="confirmed",
                downstream="sent",
                upstream_status=200,
                started_ns=1,
                completed_ns=2,
                response_sha256="a" * 64,
            )
        ]
        return plan, snapshot(True, 1), snapshot(False, 2), callbacks

    def check(self, plan, before, after, callbacks):
        try:
            from append_aqe_public import verify_public_transition
        except ImportError:
            self.fail("Public SELECT needs a bounded transition oracle")
        verify_public_transition(
            plan, "source", "SELECT_TABLE", False, before, after, callbacks
        )

    def test_source_select_revoke_preserves_metadata_and_source_filter(self):
        plan, before, after, callbacks = self.fixture()
        self.check(plan, before, after, callbacks)

    def test_source_select_rejects_unrelated_policy_and_metadata_drift(self):
        for kind in ["filter", "metadata", "callback", "role"]:
            with self.subTest(kind=kind):
                plan, before, after, callbacks = self.fixture()
                if kind == "filter":
                    after["policies"]["policies"][1]["rowFilterPolicyItems"] = []
                elif kind == "metadata":
                    after["public"]["metadataRole"]["securableObjects"] = []
                elif kind == "callback":
                    callbacks[0]["path"] = "/service/public/v2/api/policy/8"
                else:
                    after["roles"]["rangerRoles"] = [dict(name="other")]
                with self.assertRaises(AssertionError):
                    self.check(plan, before, after, callbacks)


class TestAppendAqePythonProviders(unittest.TestCase):
    def test_real_file_and_zip_import_provenance(self):
        import hashlib
        import importlib.util
        import zipimport
        from append_aqe_profiles import module_provenance

        source = b"answer = 42\n"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plain = root / "plain_provider.py"
            plain.write_bytes(source)
            archive = root / "providers.zip"
            with zipfile.ZipFile(archive, "w") as output:
                output.writestr("zip_provider.py", source)
            specs = [
                importlib.util.spec_from_file_location("plain_provider", plain),
                zipimport.zipimporter(str(archive)).find_spec("zip_provider"),
            ]
            for spec in specs:
                with self.subTest(loader=type(spec.loader).__name__):
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    try:
                        evidence = module_provenance(module)
                    except OSError:
                        self.fail(
                            "Zip-imported providers must be read through their actual loader"
                        )
                    self.assertEqual(
                        hashlib.sha256(source).hexdigest(), evidence["sha256"]
                    )
                    self.assertEqual(module.__file__, evidence["path"])
                    self.assertIn("container", evidence)
                    self.assertTrue(Path(evidence["container"]["path"]).is_file())
                    if isinstance(spec.loader, zipimport.zipimporter):
                        self.assertEqual("zip_provider.py", evidence["member"])


class TestAppendAqeNativeMetadata(unittest.TestCase):
    def test_delta_metadata_accepts_action_wrapper_and_direct_body(self):
        from append_aqe_profiles import metadata_body

        body = dict(id="native", partitionColumns=["region"], configuration={})
        self.assertEqual(body, metadata_body(body))
        self.assertEqual(body, metadata_body(dict(metaData=body)))
        for malformed in [
            dict(metaData=body, protocol={}),
            dict(metaData={}),
            dict(partitionColumns="region"),
        ]:
            with self.subTest(malformed=malformed):
                with self.assertRaises(AssertionError):
                    metadata_body(malformed)


class TestAppendAqeRegistrations(unittest.TestCase):
    def test_only_exact_target_receipt_version_may_advance(self):
        from append_aqe_profiles import verify_registration_transition

        def registration(name):
            return dict(
                code=0,
                table=dict(
                    name=name,
                    properties={
                        "delta.registration.receipt.nativeVersion": "0",
                        "delta.registration.receipt.nativeId": name + "-native",
                        "location": "s3a://fixture/" + name,
                    },
                ),
            )

        before = {name: registration(name) for name in ("source", "target")}
        after = copy.deepcopy(before)
        key = "delta.registration.receipt.nativeVersion"
        after["target"]["table"]["properties"][key] = "1"
        verify_registration_transition(before, after, "target", 0, 1)
        verify_registration_transition(after, after, "target", 1, 1)
        for table, field, value in [
            ("source", key, "1"),
            ("target", key, "2"),
            ("target", key, "01"),
            ("target", "delta.registration.receipt.nativeId", "other"),
            ("target", "location", "s3a://other/"),
        ]:
            with self.subTest(table=table, field=field):
                mutated = copy.deepcopy(after)
                mutated[table]["table"]["properties"][field] = value
                with self.assertRaises(AssertionError):
                    verify_registration_transition(before, mutated, "target", 0, 1)


class TestAppendAqeMaintenanceEvidence(unittest.TestCase):
    def native(self, profile="cdf-enabled", commits=2):
        enabled = profile == "cdf-enabled"
        before = dict(
            version=commits,
            metadata={"metaData": {"id": "native"}},
            protocol={"protocol": {"minWriterVersion": 4}},
            schemaJson="fixed-schema",
            partitionColumns=["region"] if profile == "partitioned" else [],
            native=dict(
                properties={"delta.enableChangeDataFeed": str(enabled).lower()}
            ),
            intervals=[],
        )
        after = copy.deepcopy(before)
        after["version"] = commits + 1
        if enabled:
            after["intervals"] = [
                dict(
                    startingVersion=2,
                    endingVersion=commits + 1,
                    error=None,
                    rows=[
                        [row[0], row[1], row[2], "insert", version]
                        for version in range(2, commits + 2)
                        for row in [[1, "MASKED", "allowed"], [3, "MASKED", "allowed"]]
                    ],
                )
            ]
        return before, after

    def warm(self, order="target-first"):
        before = dict(
            aqe=True,
            asyncStaleness=0,
            roles=["target", "project"],
            matchingOrder=["project", "target"]
            if order == "target-first"
            else ["target", "project"],
            unrelated=dict(key="other", loaded=True),
            entries=[
                dict(key=key, builder="old-" + key, loaded=True, owner=None)
                for key in ["target", "project"]
            ],
        )
        after = copy.deepcopy(before)
        after.update(
            observedHead="head-1", observedVersion=1, oldBuffersUnloaded=[True, True]
        )
        for entry in after["entries"]:
            entry.update(
                builder="new-" + entry["key"],
                loaded=False,
                owner="owner-" + entry["key"],
                ownerKey=entry["key"],
                selectedVersion=1,
                selectedHead="head-1",
                producerClass="org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerRawProducer",
            )
        return dict(order=order, freshDataFrames=True), before, after

    def test_native_profiles_and_both_warm_orders(self):
        from append_aqe_checks import (
            _verify_delta_native_profile,
            _verify_delta_warm_state,
        )

        for profile in ["cdf-enabled", "cdf-disabled", "partitioned", "plain"]:
            for commits in [1, 2]:
                _verify_delta_native_profile(profile, *self.native(profile, commits))
        for order in ["target-first", "project-first"]:
            _verify_delta_warm_state(*self.warm(order), 1)

    def test_rejects_native_profile_and_cdf_interval_drift(self):
        from append_aqe_checks import _verify_delta_native_profile

        for kind in [
            "metadata",
            "protocol",
            "schema",
            "partition",
            "cdf-property",
            "missing-interval",
            "feed-error",
            "wrong-row",
            "wrong-change-type",
            "missing-first-commit",
            "wrong-ending-version",
            "disabled-feed",
        ]:
            with self.subTest(kind=kind):
                profile = "cdf-disabled" if kind == "disabled-feed" else "cdf-enabled"
                before, after = self.native(profile)
                if kind in ["metadata", "protocol"]:
                    after[kind] = {"changed": True}
                elif kind == "schema":
                    after["schemaJson"] = "changed"
                elif kind == "partition":
                    after["partitionColumns"] = ["region"]
                elif kind == "cdf-property":
                    for image in [before, after]:
                        image["native"]["properties"]["delta.enableChangeDataFeed"] = (
                            "false"
                        )
                elif kind == "missing-interval":
                    after["intervals"] = []
                elif kind == "feed-error":
                    after["intervals"][0]["error"] = {"class": "Failure"}
                elif kind == "wrong-row":
                    after["intervals"][0]["rows"][0][1] = "unmasked"
                elif kind == "wrong-change-type":
                    after["intervals"][0]["rows"][0][3] = "delete"
                elif kind == "missing-first-commit":
                    after["intervals"][0]["startingVersion"] = 3
                    after["intervals"][0]["rows"] = after["intervals"][0]["rows"][2:]
                elif kind == "wrong-ending-version":
                    after["intervals"][0]["endingVersion"] = 4
                elif kind == "disabled-feed":
                    after["intervals"] = [{"error": None}]
                with self.assertRaises(AssertionError):
                    _verify_delta_native_profile(profile, before, after)

    def test_rejects_warm_buffer_key_and_owner_drift(self):
        from append_aqe_checks import _verify_delta_warm_state

        for kind in [
            "retained-qe",
            "wrong-order",
            "not-unloaded",
            "already-loaded",
            "old-builder",
            "key",
            "owner",
            "head",
            "version",
            "unrelated",
        ]:
            with self.subTest(kind=kind):
                warm, before, after = self.warm()
                if kind == "retained-qe":
                    warm["freshDataFrames"] = False
                elif kind == "wrong-order":
                    before["matchingOrder"].reverse()
                elif kind == "not-unloaded":
                    after["oldBuffersUnloaded"][0] = False
                elif kind == "already-loaded":
                    after["entries"][0]["loaded"] = True
                elif kind == "old-builder":
                    after["entries"][0]["builder"] = before["entries"][0]["builder"]
                elif kind == "key":
                    after["entries"][0]["key"] = "copied-key"
                elif kind == "owner":
                    after["entries"][0]["ownerKey"] = "copied-key"
                elif kind == "head":
                    after["entries"][0]["selectedHead"] = "stale-head"
                elif kind == "version":
                    after["entries"][0]["selectedVersion"] = 0
                elif kind == "unrelated":
                    after["unrelated"]["key"] = "changed"
                with self.assertRaises(AssertionError):
                    _verify_delta_warm_state(warm, before, after, 1)


class TestAppendAqeProfiles(unittest.TestCase):
    def profiles(self):
        try:
            from append_aqe_profiles import case_specs
        except ImportError:
            self.fail("Final matrix requires an explicit bounded case inventory")
        return case_specs()

    def test_both_native_frontends_have_every_required_profile(self):
        cases = self.profiles()
        self.assertEqual(26, len(cases))
        self.assertEqual(26, len({case["name"] for case in cases}))
        for api in ["insertInto", "saveAsTable"]:
            profiles = {case["profile"] for case in cases if case["api"] == api}
            self.assertEqual(
                {
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
                },
                profiles,
            )

    def test_profiles_preserve_explicit_transactions_and_caller_aqe(self):
        from append_aqe_profiles import settings

        for case in self.profiles():
            values = settings(case, "owned-app-id")
            self.assertEqual("true", values["spark.sql.adaptive.enabled"])
            self.assertEqual(
                "false",
                values["spark.databricks.delta.write.txnVersion.autoReset.enabled"],
            )
            self.assertEqual(
                "owned-app-id", values["spark.databricks.delta.write.txnAppId"]
            )
            self.assertEqual("1", values["spark.databricks.delta.write.txnVersion"])
            self.assertEqual(
                case["profile"] != "no-default",
                values["spark.sql.defaultColumn.useNullsForMissingDefaultValues"]
                == "true",
            )


class TestAppendAqePackageProfile(unittest.TestCase):
    def test_separate_append_profile_preserves_existing_read_profile(self):
        from cdf_read_live_package import app_profile

        profile = app_profile("append-aqe")
        self.assertEqual("appendAqeLiveThinJar", profile["task"])
        self.assertEqual("appendAqeLiveAppOut", profile["property"])
        self.assertEqual("append-aqe-live.jar", profile["jar"])
        self.assertTrue(profile["provider"].endswith("DeltaRangerLiveAppendAqe"))
        self.assertEqual("readParityLiveThinJar", app_profile("read-parity")["task"])
        with self.assertRaises(AssertionError):
            app_profile("unknown-append")


class TestDeltaQualificationInventory(unittest.TestCase):
    """The finite qualification inventory comes from actual bound evidence."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()

    def write(self, relative, value):
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value))
        return file_ref(path)

    def exit(self, relative):
        descriptor = self.write(relative, dict(exitCode=0, timedOut=False, pid=123))
        self.write(str(Path(relative).with_name("process.json")), dict(pid=123))
        return descriptor

    def fixture(self, qualifying=True, names=None):
        from append_aqe_profiles import case_specs
        from append_aqe_checks import _delta_attempts, _delta_expected_outcome

        specs = [s for s in case_specs() if names is None or s["name"] in names]
        tables = [
            dict(
                name=s["target"],
                prefix=s["target"] + "/",
                location="s3a://bucket/" + s["target"],
            )
            for s in specs
        ]
        result = dict(
            qualification=qualifying,
            runtime=self.write("package/manifest.json", dict(sparkHome="/spark")),
            fixturePlan=self.write(
                "fixture/state.private.json", dict(nonce="nonce", tables=tables)
            ),
            persistenceBefore=self.write(
                "registrations-before-restart.private.json", {}
            ),
            persistenceAfter=self.write("registrations-after-restart.private.json", {}),
            caseInventory=[s["name"] for s in specs],
            subject="writer",
            cases=[],
            processExits={
                key: self.exit(key)
                for key in (
                    "fixture-command/exit.json",
                    "fixture/native-seed/process/exit.json",
                )
            },
        )
        exits = result["processExits"]
        for spec in specs:
            base = "cases/" + spec["name"]
            directory = self.root / base
            attempts = _delta_attempts(spec)
            request = self.write(
                base + "/request/request.private.json",
                dict(
                    caseName=spec["name"],
                    frontend=spec["api"],
                    source="source",
                    target=spec["target"],
                    duplicate=spec["duplicate"],
                    denyDuplicate=spec["profile"] == "denied-duplicate",
                    warmOrder=spec["warmOrder"],
                    barrier=spec["profile"] == "revoke",
                    appendCount=spec["appendCount"],
                    transactionVersions=[version for _, version in attempts],
                    targetLocation="s3a://bucket/" + spec["target"],
                    output=str(directory / "jvm"),
                    pythonOutput=str(directory / "python"),
                    controlDirectory=str(directory / "request/control"),
                ),
            )
            writer_key = base + "/writer/exit.json"
            exits[writer_key] = self.exit(writer_key)
            self.write(
                base + "/writer/command.json",
                dict(
                    argv=[
                        "/spark/bin/spark-submit",
                        "/repo/append_aqe_pyspark.py",
                        request["path"],
                    ]
                ),
            )
            previous = "before"
            version = 2 if spec["profile"].startswith("cdf-") else 0

            def image(side, current):
                native = self.write(base + "/" + side + "/native/result.json", {})
                key = base + "/" + side + "/native/process/exit.json"
                exits[key] = self.exit(key)
                objects = {}
                if current > (2 if spec["profile"].startswith("cdf-") else 0):
                    txn_version = (
                        1
                        if spec["duplicate"]
                        else current - (2 if spec["profile"].startswith("cdf-") else 0)
                    )
                    txn = self.write(
                        base + "/" + side + "/txn.json",
                        dict(
                            txn=dict(
                                appId="append-aqe-nonce-" + spec["name"],
                                version=txn_version,
                            )
                        ),
                    )
                    objects[
                        spec["target"]
                        + "/_delta_log/"
                        + str(current).zfill(20)
                        + ".json"
                    ] = txn
                return self.write(
                    base + "/" + side + "/image.json",
                    dict(
                        nativeTable=spec["target"],
                        nativeEvidence=native,
                        version=current,
                        objects=objects,
                    ),
                )

            before = image(previous, version)
            for index, (attempt, txn_version) in enumerate(attempts):
                name = spec["name"] + "-" + attempt
                outcome = _delta_expected_outcome(spec, attempt)
                version += int(outcome == "committed")
                after = image("after-" + attempt, version)
                caller = dict(
                    transactionAppId="append-aqe-nonce-" + spec["name"],
                    transactionVersion=str(txn_version),
                    jvmThread=10,
                )
                failure, callbacks, requests = [], [], []
                case = dict(
                    name=name,
                    api=spec["api"],
                    profile=spec["profile"],
                    target=spec["target"],
                    source="source",
                    outcome=outcome,
                    request=request,
                    before=before,
                    after=after,
                    callerBefore=caller,
                    callerAfter=copy.deepcopy(caller),
                    failure=failure,
                    callbacks=callbacks,
                    rangerRequests=requests,
                )
                case["callerEvidence"] = self.write(
                    base + "/python/" + name + "-caller.json",
                    dict(before=caller, after=caller),
                )
                case["observation"] = self.write(
                    base + "/jvm/" + name + "-append.json",
                    dict(
                        writerThreadId=10,
                        subject="writer",
                        aqe=True,
                        failure=failure,
                        callbacks=callbacks,
                        rangerRequests=requests,
                    ),
                )
                if spec["warmOrder"] != "none":
                    warm_before = self.write(base + "/jvm/warm-before.json", {})
                    warm_after = self.write(base + "/jvm/" + name + "-warm.json", {})
                    case["warm"] = self.write(
                        base + "/" + name + "-warm-result.json",
                        dict(
                            order=spec["warmOrder"],
                            before=warm_before,
                            after=warm_after,
                        ),
                    )
                if spec["profile"] == "checkpoint" and index == len(attempts) - 1:
                    case["checkpoint"] = self.write(
                        base + "/checkpoint-result.json", {}
                    )
                if spec["profile"] == "revoke":
                    case["barrier"] = self.write(base + "/barrier-result.json", {})
                result["cases"].append(case)
                before = after
        first = specs[0]["name"]
        for alias, path in {
            "writer": "writer",
            "native-before": "before/native/process",
            "native-after": "after-first/native/process",
        }.items():
            exits[alias] = exits["cases/" + first + "/" + path + "/exit.json"]
        exits["supervisor"] = self.exit("supervisor/exit.json")
        argv = [
            "python",
            "/repo/append_aqe_live.py",
            "--runtime",
            result["runtime"]["path"],
            "--out",
            str(self.root),
        ]
        if not qualifying:
            for spec in specs:
                argv.extend(["--case", spec["name"]])
        self.write("supervisor/command.json", dict(argv=argv))
        return result

    def check(self, result):
        from append_aqe_checks import _verify_delta_contract

        _verify_delta_contract(result)

    def rewrite(self, case, key, change):
        value = json.loads(Path(case[key]["path"]).read_text())
        change(value)
        Path(case[key]["path"]).write_text(json.dumps(value))
        case[key] = file_ref(case[key]["path"])

    def test_full_inventory_and_explicit_bounded_development(self):
        self.check(self.fixture())
        self.check(self.fixture(False, ["saveAsTable_checkpoint"]))

    def test_qualifying_mode_requires_runtime_even_with_reported_full_inventory(self):
        from append_aqe_checks import verify_delta_append

        fixture = TestDeltaAppendAqeChecks()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        value = fixture.fixture()
        value["qualification"] = True
        value["caseInventory"] = [
            s["name"] for s in __import__("append_aqe_profiles").case_specs()
        ]
        value["cases"] = [
            dict(copy.deepcopy(value["cases"][0]), name="cloned-" + str(i))
            for i in range(42)
        ]
        with self.assertRaisesRegex(AssertionError, "requires runtime"):
            verify_delta_append(value)

    def test_actual_inventory_rejects_clones_missing_and_substituted_profiles(self):
        original = self.fixture()
        mutations = [
            lambda v: v["cases"].pop(),
            lambda v: v["cases"].__setitem__(
                1, dict(copy.deepcopy(v["cases"][0]), name=v["cases"][1]["name"])
            ),
            lambda v: v["cases"][0].__setitem__(
                "name", "saveAsTable_plain-first-substituted"
            ),
            lambda v: v["caseInventory"].pop(),
        ]
        for mutate in mutations:
            value = copy.deepcopy(original)
            mutate(value)
            with self.subTest(mutation=mutate), self.assertRaises(AssertionError):
                self.check(value)

    def test_rejects_api_target_profile_attempt_outcome_and_request_drift(self):
        original = self.fixture()
        for key, wrong in dict(
            api="saveAsTable",
            target="save_plain",
            source="other",
            profile="partitioned",
            outcome="duplicate",
        ).items():
            value = copy.deepcopy(original)
            value["cases"][0][key] = wrong
            with self.subTest(field=key), self.assertRaises(AssertionError):
                self.check(value)
        for key, wrong in dict(
            frontend="saveAsTable",
            target="save_plain",
            transactionVersions=[2],
            appendCount=2,
            duplicate=True,
            warmOrder="target-first",
        ).items():
            value = self.fixture()
            self.rewrite(
                value["cases"][0], "request", lambda v: v.__setitem__(key, wrong)
            )
            with self.subTest(request=key), self.assertRaises(AssertionError):
                self.check(value)

    def test_exact_original_process_inventory_and_receipt_binding(self):
        original = self.fixture()
        for key in list(original["processExits"]):
            value = copy.deepcopy(original)
            value["processExits"].pop(key)
            with self.subTest(missing=key), self.assertRaises(AssertionError):
                self.check(value)
        for key in [
            "fixture-command/exit.json",
            "fixture/native-seed/process/exit.json",
            "cases/saveAsTable_plain/writer/exit.json",
            "cases/saveAsTable_checkpoint/after-append3/native/process/exit.json",
            "writer",
            "native-before",
            "native-after",
        ]:
            value = copy.deepcopy(original)
            value["processExits"][key] = original["processExits"]["supervisor"]
            with self.subTest(substituted=key), self.assertRaises(AssertionError):
                self.check(value)
        value = copy.deepcopy(original)
        value["processExits"]["unrelated"] = original["processExits"]["supervisor"]
        with self.assertRaises(AssertionError):
            self.check(value)

    def test_required_maintenance_attachments_cannot_be_removed_or_borrowed(self):
        original = self.fixture()
        for index, case in enumerate(original["cases"]):
            for key in ("warm", "checkpoint", "barrier"):
                if key not in case:
                    continue
                value = copy.deepcopy(original)
                value["cases"][index].pop(key)
                with (
                    self.subTest(case=case["name"], missing=key),
                    self.assertRaises(AssertionError),
                ):
                    self.check(value)
                value = copy.deepcopy(original)
                value["cases"][index][key] = original["cases"][0]["request"]
                with (
                    self.subTest(case=case["name"], substituted=key),
                    self.assertRaises(AssertionError),
                ):
                    self.check(value)

    def test_caller_observation_native_evidence_and_transaction_sequence_are_bound(
        self,
    ):
        for key in ("before", "after", "request", "callerEvidence", "observation"):
            value = self.fixture()
            value["cases"][0][key] = value["cases"][1][key]
            with self.subTest(substituted=key), self.assertRaises(AssertionError):
                self.check(value)
        for field, wrong in (
            ("transactionVersion", "2"),
            ("transactionAppId", "another-app"),
        ):
            value = self.fixture()
            case = value["cases"][0]
            case["callerBefore"][field] = case["callerAfter"][field] = wrong
            self.rewrite(
                case,
                "callerEvidence",
                lambda v: (
                    v["before"].__setitem__(field, wrong),
                    v["after"].__setitem__(field, wrong),
                ),
            )
            with self.subTest(caller=field), self.assertRaises(AssertionError):
                self.check(value)
        value = self.fixture()
        self.rewrite(
            value["cases"][0],
            "observation",
            lambda v: v.__setitem__("writerThreadId", 11),
        )
        with self.assertRaises(AssertionError):
            self.check(value)
        value = self.fixture()
        case = value["cases"][0]
        after = json.loads(Path(case["after"]["path"]).read_text())
        log = next(iter(after["objects"].values()))
        Path(log["path"]).write_text(
            json.dumps(dict(txn=dict(appId="other", version=1)))
        )
        after["objects"][next(iter(after["objects"]))] = file_ref(log["path"])
        Path(case["after"]["path"]).write_text(json.dumps(after))
        case["after"] = file_ref(case["after"]["path"])
        with self.assertRaises(AssertionError):
            self.check(value)


if __name__ == "__main__":
    unittest.main()

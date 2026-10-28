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


if __name__ == "__main__":
    unittest.main()

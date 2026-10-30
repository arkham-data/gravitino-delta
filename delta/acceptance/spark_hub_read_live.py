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

"""Serial six-case Spark Hub caller qualification using the existing live fixture.

Packaging and execution are explicit runtime operations. Pure checker imports do
not start services, import product dependencies, build JARs or launch Spark.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid
import zipfile

from cdf_merge_live_checks import file_ref, read_ref
from spark_hub_read_checks import ROWS, SETTINGS, verify_result

ROOT = Path(__file__).resolve().parents[2]
SPARK_HUB_REVISION = "d01024b6f459ad2743b7b55099226b8c3dc742fd"
REQUIRED_SOURCES = {
    "src/settings.py",
    "src/shared/services/v2/spark.py",
    "src/shared/services/v3/gravitino_catalog.py",
    "src/shared/services/v3/gravitino_runtime.py",
    "src/shared/schemas/v3/gravitino.py",
    "src/shared/schemas/v3/__init__.py",
}


def digest(path):
    """Hash retained source and dependency bytes without importing them."""
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def save(path, value):
    """Never overwrite an original artifact or process receipt."""
    path = Path(path)
    assert not path.exists(), "Fresh artifact path required"
    path.write_text(json.dumps(value, indent=2) + "\n")


def package_members(path):
    """Reject duplicate or unrelated archive members and hash exact source bytes."""
    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        assert len(names) == len(
            set(names)
        ), "Duplicate Spark Hub source archive member"
        assert all(
            name.startswith("src/")
            and ".." not in Path(name).parts
            and (name.endswith(".py") or name.endswith("/"))
            for name in names
        ), "Unrelated Spark Hub package file"
        return {
            name: hashlib.sha256(archive.read(name)).hexdigest()
            for name in names
            if not name.endswith("/")
        }


def verify_spark_hub_package(package):
    """Bind the normal import closure to pinned tracked S code and installed deps."""
    assert package["gitHead"] == SPARK_HUB_REVISION, "Spark Hub revision differs"
    assert REQUIRED_SOURCES <= set(
        package["sourceInputs"]
    ), "Missing production source import"
    archive = package["archive"]
    assert file_ref(archive["path"]) == archive, "Spark Hub source archive changed"
    assert (
        package_members(archive["path"]) == package["sourceInputs"]
    ), "Spark Hub source members differ"
    root = Path(package["root"])
    for name, expected in package["sourceInputs"].items():
        data = subprocess.check_output(
            ["git", "show", SPARK_HUB_REVISION + ":" + name], cwd=root
        )
        assert (
            hashlib.sha256(data).hexdigest() == expected == digest(root / name)
        ), "Pinned Spark Hub source differs"
    assert (
        file_ref(package["python"]) == package["interpreter"]
    ), "Live Python interpreter changed"
    assert package["pythonVersion"].startswith("3.11."), "Live Python 3.11 required"
    for dependency in package["dependencies"].values():
        assert dependency["files"] and all(
            digest(path) == value for path, value in dependency["files"].items()
        ), "Installed Python dependency changed"
    installed = {
        name.lower().replace("_", "-"): value["version"]
        for name, value in package["dependencies"].items()
    }
    assert (
        installed.get("pyspark") == package["spark"]
        and installed.get("delta-spark") == package["delta"]
    ), "Installed Spark/Delta Python packages differ"
    for key in ("sourceProbeExit", "archiveProbeExit"):
        assert (
            read_ref(package[key])["exitCode"] == 0
        ), "Original import inventory process failed"
    source_probe, archive_probe = (
        read_ref(package["sourceProbe"]),
        read_ref(package["archiveProbe"]),
    )
    for probe in (source_probe, archive_probe):
        assert (
            probe["sourceInputs"] == package["sourceInputs"]
            and probe["dependencies"] == package["dependencies"]
        ), "Actual normal imports differ"
        assert (
            probe["python"] == package["python"]
            and probe["pythonVersion"] == package["pythonVersion"]
        ), "Import probe interpreter differs"
    assert (
        source_probe["pythonProviders"]
        == archive_probe["pythonProviders"]
        == package["pythonProviders"]
    ), "Python probe provider differs"
    assert all(
        path.startswith(archive["path"] + "/src/")
        for path in archive_probe["importPaths"].values()
    ), "Probe did not import packaged production code"


def package_spark_hub(root, python, runtime, output):
    """Package only the actual import closure; retain installed dependency hashes.

    Each inventory is a normal fresh-process import using the pinned live
    interpreter. No Settings values, .env, unrelated tracked or untracked files
    are copied. Runtime Java artifacts remain owned by the G runtime manifest.
    """
    from read_parity_live import start, finish

    output.mkdir(mode=0o700)
    assert (
        subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=root, text=True
        ).strip()
        == SPARK_HUB_REVISION
    ), "Unexpected Spark Hub HEAD"
    assert not subprocess.check_output(
        ["git", "status", "--porcelain", "--untracked-files=no"], cwd=root, text=True
    ).strip(), "Spark Hub tracked source is dirty"
    python = str(Path(python).absolute())
    assert Path(python).is_file(), "Pinned live interpreter missing"
    environment = {
        "PATH": os.environ.get("PATH", ""),
        "TZ": "UTC",
        "ENV": "qualification",
        "KAFKA_BROKERS": "unused.invalid:9092",
        "KAFKA_TOPIC": "unused",
        "IS_LOCAL": "false",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1",
    }

    def probe(source, name):
        request = output / (name + "-request.json")
        result = output / (name + ".json")
        save(
            request,
            {"mode": "inventory", "sparkHubRoot": str(source), "output": str(result)},
        )
        process, out, err = start(
            [python, ROOT / "delta/acceptance/spark_hub_read_pyspark.py", request],
            environment,
            output / (name + "-process"),
            cwd=output,
        )
        try:
            process.wait(timeout=180)
        finally:
            code = finish(
                process, out, err, output / (name + "-process"), process.poll() is None
            )
        assert code == 0, "Original normal import probe failed; retained without replay"
        return json.loads(result.read_text())

    source_probe = probe(root, "source-probe")
    assert REQUIRED_SOURCES <= set(
        source_probe["sourceInputs"]
    ), "Missing normal production import"
    tracked = set(
        subprocess.check_output(["git", "ls-files"], cwd=root, text=True).splitlines()
    )
    assert (
        set(source_probe["sourceInputs"]) <= tracked
    ), "Untracked production module imported"
    archive = output / "spark-hub-sources.zip"
    with zipfile.ZipFile(archive, "x", compression=zipfile.ZIP_DEFLATED) as target:
        directories = set()
        for name in source_probe["sourceInputs"]:
            directories.update(
                str(parent) + "/" for parent in Path(name).parents if str(parent) != "."
            )
        for name in sorted(directories):
            target.writestr(name, b"")
        for name in source_probe["sourceInputs"]:
            target.write(root / name, name)
    archive_probe = probe(archive, "archive-probe")
    package = {
        "schemaVersion": 1,
        "gitHead": SPARK_HUB_REVISION,
        "root": str(root),
        "spark": runtime["spark"],
        "delta": runtime["delta"],
        "archive": file_ref(archive),
        "sourceInputs": source_probe["sourceInputs"],
        "dependencies": source_probe["dependencies"],
        "pythonProviders": source_probe["pythonProviders"],
        "python": python,
        "pythonVersion": source_probe["pythonVersion"],
        "interpreter": file_ref(python),
        "computeDefaults": source_probe["computeDefaults"],
        "sourceProbe": file_ref(output / "source-probe.json"),
        "archiveProbe": file_ref(output / "archive-probe.json"),
        "sourceProbeExit": file_ref(output / "source-probe-process/exit.json"),
        "archiveProbeExit": file_ref(output / "archive-probe-process/exit.json"),
        "dependencyScope": "Exact installed distributions imported by normal production modules; retained interpreter files, excluding pyc and independently manifested JVM jars",
    }
    assert (
        archive_probe["sourceInputs"] == source_probe["sourceInputs"]
    ), "Archive normal import closure differs"
    verify_spark_hub_package(package)
    save(output / "manifest.json", package)
    return output / "manifest.json"


def application(plan, runtime, package_path, bindings, unknown, profile, output):
    """Launch one original caller process with isolated identities and policy control."""
    from cdf_live import clean_environment, prepare_request
    from cdf_merge_live import await_file
    from read_parity_live import start, finish, snapshot, change_policy

    output.mkdir(mode=0o700)
    package = json.loads(package_path.read_text())
    actual_plan = dict(plan, subject=plan["subjects"][profile])
    initial = snapshot(plan, output / "initial")
    request = prepare_request(actual_plan, runtime, initial, output / "request")
    properties = Path(request["sparkConfigurationFile"])
    original = dict(
        line.split("=", 1)
        for line in properties.read_text().splitlines()
        if "=" in line
    )
    # Product defaults are present before session construction. The fixture's
    # four explicit AQE settings override defaults, including both thresholds.
    settings = {
        **package["computeDefaults"],
        **original,
        **SETTINGS,
        "spark.sql.session.timeZone": "UTC",
    }
    properties.write_text(
        "".join(
            key + "=" + str(value) + "\n" for key, value in sorted(settings.items())
        )
    )
    request.update(
        identityProfile=profile,
        output=str(output / "app-evidence"),
        pythonOutput=str(output / "python-evidence"),
        bindings=bindings,
        unknownDatasetId=unknown,
        sparkHubZip=package["archive"]["path"],
        sparkHubPackage=file_ref(package_path),
        profileKeys=sorted(settings),
        fixtureUpstreamUri=plan["gravitinoUrl"],
    )
    # prepare_request retains its original request; application-specific inputs
    # get a new immutable artifact rather than overwriting that receipt.
    request_path = output / "request/spark-hub-request.private.json"
    request["originalRequest"] = str(request_path)
    save(request_path, request)
    environment = clean_environment(actual_plan)
    environment.update(
        TZ="UTC",
        SPARK_HOME=runtime["sparkHome"],
        PYSPARK_PYTHON=package["python"],
        PYSPARK_DRIVER_PYTHON=package["python"],
        PYSPARK_PIN_THREAD="true",
        SPARK_USER=actual_plan["subject"],
        HADOOP_USER_NAME=actual_plan["subject"],
        ENV="qualification",
        KAFKA_BROKERS="unused.invalid:9092",
        KAFKA_TOPIC="unused",
        IS_LOCAL="false",
        PYTHONNOUSERSITE="1",
        PYTHONDONTWRITEBYTECODE="1",
    )
    argv = [
        runtime["sparkHome"] + "/bin/spark-submit",
        "--driver-memory",
        "3g",
        "--master",
        "local[2]",
        "--properties-file",
        properties,
        "--driver-class-path",
        request["driverSecurityDirectory"],
        ROOT / "delta/acceptance/spark_hub_read_pyspark.py",
        request_path,
    ]
    process, out, err = start(argv, environment, output / "process", cwd=output)
    try:
        if profile == "id-only":
            await_file(
                Path(request["controlDirectory"]) / "revoke-id-ready.json", process, 300
            )
            state = change_policy(plan, "revoke-id", output / "revoke-id")
            save(
                Path(request["controlDirectory"]) / "revoke-id-verified.json",
                {
                    "stateEvidenceFile": str(
                        Path(state["rangerStateFile"]).parent
                        / "state-evidence.private.json"
                    )
                },
            )
        process.wait(timeout=360)
    finally:
        code = finish(process, out, err, output / "process", process.poll() is None)
    assert code == 0, "Original Spark Hub caller failed; retained without replay"
    cases = json.loads((output / "python-evidence/cases.json").read_text())
    for case in cases:
        raw_name = case["name"] + (
            "-case" if case["name"] == "fresh-action-revoke" else ""
        )
        case["rawArtifact"] = file_ref(
            output / "python-evidence" / (raw_name + ".json")
        )
        case["originalExit"] = file_ref(output / "process/exit.json")
        path = output / (case["name"] + "-joined.json")
        save(path, case)
        case["artifact"] = file_ref(path)
    return cases


def worker(runtime_path, spark_hub_root, inputs_path, output):
    """Run fixture setup and three isolated original application profiles serially."""
    from cdf_live import verify_deployed, RECEIPT
    from cdf_read_live_package import verify_runtime
    from read_parity_live import provision, change_policy, seed
    from s3_objects import OwnedObjects

    output.mkdir(mode=0o700)
    runtime = json.loads(runtime_path.read_text())
    runtime["manifestPath"] = str(runtime_path)
    verify_runtime(runtime)
    inputs = json.loads(inputs_path.read_text())
    assert inputs.get("storage", "minio") == "minio", "This lane qualifies MinIO only"
    package_path = package_spark_hub(
        spark_hub_root, inputs["livePython"], runtime, output / "spark-hub-package"
    )
    package = json.loads(package_path.read_text())
    handles = {}
    try:
        association = verify_deployed(inputs, output / "deployed-ranger")
        plan = provision(
            runtime,
            inputs,
            output / "fixture",
            association,
            handles,
            fixture_rows=ROWS,
            unpublished_tables=("unpublished",),
            table_names=("source", "unpublished"),
            python=package["python"],
        )
        change_policy(
            plan, "constant-mask", output / "mask-policy", mask_expression="'MASKED'"
        )
        native = json.loads(Path(plan["nativeSeed"]).read_text())
        bindings = [
            {
                "datasetId": native["tables"][table]["id"],
                "catalog": plan["catalog"],
                "schema": plan["database"],
                "table": table,
            }
            for table in ("source", "unpublished")
        ]
        unknown = str(uuid.uuid4())
        save(output / "bindings.json", {"bindings": bindings, "unknown": unknown})
        objects = OwnedObjects(plan["primary"], plan["prefix"] + "/")
        before = objects.manifest()
        save(output / "objects-before.json", before)
        cases = []
        for profile in ("table-wide", "denied-email", "id-only"):
            cases.extend(
                application(
                    plan,
                    runtime,
                    package_path,
                    bindings,
                    unknown,
                    profile,
                    output / profile,
                )
            )
        seed(plan, runtime, output / "native-readback", mode="verify")
        after = objects.manifest()
        save(output / "objects-after.json", after)
        assert before == after, "Application reads changed native object bytes"
        receipts = json.loads((output / "fixture/receipts.private.json").read_text())
        published = receipts["source"]
        registration = {
            "source": {
                "state": published[RECEIPT + "state"],
                "nativeId": published[RECEIPT + "nativeId"],
                "nativeVersion": int(published[RECEIPT + "nativeVersion"]),
            },
            "unpublished": {"state": receipts["unpublished"][RECEIPT + "state"]},
        }
        save(output / "registration.json", registration)
        profiles = {
            profile: {
                key: file_ref(output / profile / directory / filename)
                for key, directory, filename in (
                    ("observed", "app-evidence", "observed.json"),
                    ("origins", "app-evidence", "origins.json"),
                    ("metadata", "app-evidence", "metadata-identity-final.json"),
                    ("security", "app-evidence", "security-manifest.json"),
                    ("caller", "python-evidence", "caller.json"),
                )
            }
            for profile in ("table-wide", "denied-email", "id-only")
        }
        result = {
            "schemaVersion": 1,
            "evidenceKind": "actual-live",
            "spark": runtime["spark"],
            "delta": runtime["delta"],
            "engine": "delta",
            "catalogRoute": "gravitino",
            "storage": "minio",
            "catalog": plan["catalog"],
            "database": plan["database"],
            "resourceService": plan["resourceService"],
            "policyIds": plan["policyIds"],
            "rangerServerVersion": "2.8.0",
            "rangerClientVersion": "2.8.0",
            "runtimeManifest": file_ref(runtime_path),
            "sparkHubPackage": file_ref(package_path),
            "sourceInputs": package["sourceInputs"],
            "deployedRanger": file_ref(association),
            "livePython": package["python"],
            "subjects": {profile: plan["subjects"][profile] for profile in profiles},
            "setupIdentity": plan["serviceAdmin"],
            "metadataPrincipal": plan["metadataPrincipal"],
            "profiles": profiles,
            "cases": cases,
            "binding": bindings[0],
            "unpublishedBinding": bindings[1],
            "bindings": file_ref(output / "bindings.json"),
            "registration": file_ref(output / "registration.json"),
            "publicationReceipts": file_ref(output / "fixture/receipts.private.json"),
            "nativeBefore": file_ref(plan["nativeSeed"]),
            "nativeAfter": file_ref(output / "native-readback/result.json"),
            "nativeRequests": {
                "nativeSeed": file_ref(
                    output / "fixture/native-seed/request.private.json"
                ),
                "nativeReadback": file_ref(
                    output / "native-readback/request.private.json"
                ),
            },
            "processCaptures": {
                role: {
                    name: file_ref(directory / name)
                    for name in ("argv.json", "stdout", "stderr")
                }
                for role, directory in {
                    "nativeSeed": output / "fixture/native-seed/process",
                    "nativeReadback": output / "native-readback/process",
                    **{profile: output / profile / "process" for profile in profiles},
                }.items()
            },
            "nativeObjectsBefore": file_ref(output / "objects-before.json"),
            "nativeObjectsAfter": file_ref(output / "objects-after.json"),
            "policyAuthoring": "isolated fixture-authored policies; public metadata grants are distinct; not production migration",
            "processExits": {
                "nativeSeed": file_ref(
                    output / "fixture/native-seed/process/exit.json"
                ),
                "nativeReadback": file_ref(
                    output / "native-readback/process/exit.json"
                ),
                **{
                    profile: file_ref(output / profile / "process/exit.json")
                    for profile in profiles
                },
            },
        }
        save(output / "result.worker.json", result)
    finally:
        if "server" in handles:
            handles["server"].stop()
        if "proxy" in handles:
            handles["proxy"].__exit__(None, None, None)


def run_cell(
    runtime_path: Path, spark_hub_root: Path, inputs_path: Path, output: Path
) -> dict:
    """Capture an original supervisor exit before applying the independent full gate."""
    from read_parity_live import start, finish

    runtime_path, spark_hub_root, inputs_path, output = map(
        lambda path: Path(path).absolute(),
        (runtime_path, spark_hub_root, inputs_path, output),
    )
    assert not output.exists(), "Fresh evidence directory required"
    supervisor = output.parent / (output.name + "-supervisor")
    argv = [
        sys.executable,
        Path(__file__).absolute(),
        "--worker",
        "--runtime",
        runtime_path,
        "--spark-hub-root",
        spark_hub_root,
        "--inputs",
        inputs_path,
        "--out",
        output,
    ]
    process, out, err = start(argv, dict(os.environ), supervisor)
    try:
        process.wait(timeout=2400)
    finally:
        code = finish(process, out, err, supervisor, process.poll() is None)
    assert code == 0, "Original Spark Hub supervisor failed; retained without replay"
    result = json.loads((output / "result.worker.json").read_text())
    result["workerResult"] = file_ref(output / "result.worker.json")
    result["supervisorInputs"] = file_ref(inputs_path)
    result["processCaptures"]["supervisor"] = {
        name: file_ref(supervisor / name) for name in ("argv.json", "stdout", "stderr")
    }
    result["processExits"]["supervisor"] = file_ref(supervisor / "exit.json")
    save(output / "result.json", result)
    verify_result(result)
    return result


def main():
    parser = argparse.ArgumentParser()
    for name in ("runtime", "spark-hub-root", "inputs", "out"):
        parser.add_argument("--" + name, required=True, type=Path)
    parser.add_argument("--worker", action="store_true")
    args = parser.parse_args()
    os.umask(0o077)
    operation = worker if args.worker else run_cell
    operation(
        args.runtime.absolute(),
        args.spark_hub_root.absolute(),
        args.inputs.absolute(),
        args.out.absolute(),
    )


if __name__ == "__main__":
    main()

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

"""Launch exactly one stock-native cell on a fresh owned MinIO prefix."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

from spark_hub_write_native_checks import PINS, require, validate_inputs, verify_result

ROOT = Path(__file__).resolve().parents[2]
SPARK_HUB_REVISION = "d01024b6f459ad2743b7b55099226b8c3dc742fd"


def ref(path):
    path = Path(path).absolute()
    return {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def save(path, value):
    """Create evidence once; never overwrite an existing observation."""
    with Path(path).open("x") as stream:
        json.dump(value, stream, indent=2, default=str)
        stream.write("\n")


def require_fresh(output, objects):
    require(not Path(output).exists(), "Existing output directory; use a fresh output")
    require(not objects, "Nonempty owned prefix; never replay a native attempt")


def runtime_artifacts(runtime):
    """Verify installed native bytes and immutable build receipts without repackaging.

    Historical sourceInputs describe the old build, not this probe's source/import closure.
    Current G product sources are not loaded by this stock-native oracle; the
    consumed probe sources are independently archived for each new execution.
    """
    require(
        (runtime["spark"], runtime["delta"]) == (PINS["spark"], PINS["delta"]),
        "Wrong runtime pins",
    )
    references = []
    for path, digest in runtime["jarHashes"].items():
        artifact = ref(path)
        require(artifact["sha256"] == digest, "Changed runtime artifact: " + path)
        references.append(artifact)
    for path, target in runtime["runtimeJarLinks"].items():
        require(str(Path(path).resolve()) == target, "Changed runtime JAR link")
    require(
        {p.name for p in Path(runtime["sparkHome"], "jars").glob("*.jar")}
        == {Path(p).name for p in runtime["jarHashes"]},
        "Unexpected runtime JARs",
    )
    for key in ("freshBuildReceipt", "sharedProductionManifest"):
        artifact = ref(runtime[key])
        require(
            artifact["sha256"] == runtime[key + "Sha256"], "Changed runtime receipt"
        )
        references.append(artifact)
    receipt = json.loads(Path(runtime["freshBuildReceipt"]).read_text())
    require(
        json.loads(Path(receipt["exit"]).read_text())["exitCode"] == 0,
        "Native runtime build did not succeed",
    )
    for path, digest in receipt["artifacts"].items():
        artifact = ref(path)
        require(artifact["sha256"] == digest, "Changed original build artifact")
        references.append(artifact)
    references.extend(
        ref(receipt[key]) for key in ("command", "exit", "before", "after")
    )
    return references


def source_revisions(gravitino_root, spark_hub_root, spark_hub_pin=SPARK_HUB_REVISION):
    """Record the current probe checkout separately from immutable runtime history.

    Stock native artifacts, rather than current G product sources, execute this
    oracle. Their strict runtime verification is independent of the current G HEAD;
    the actual consumed Python sources are archived and checked around execution.
    """
    revisions = {
        name: subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=root, text=True
        ).strip()
        for name, root in (("gravitino", gravitino_root), ("sparkHub", spark_hub_root))
    }
    require(revisions["sparkHub"] == spark_hub_pin, "Unexpected sparkHub revision")
    require(
        not subprocess.check_output(
            ["git", "status", "--porcelain", "--untracked-files=no"],
            cwd=spark_hub_root,
            text=True,
        ).strip(),
        "Spark Hub source must remain read-only/clean",
    )
    return revisions


def verify_probe_sources(source_hashes):
    """Reject changes to consumed probe bytes during the original process."""
    require(
        all(ref(path)["sha256"] == digest for path, digest in source_hashes.items()),
        "Probe sources changed during original process",
    )


def run(
    runtime_path: Path, spark_hub_root: Path, inputs_path: Path, output: Path
) -> Path:
    """Execute one fixed register; preserve failures and never retry a write prefix."""
    from s3_objects import OwnedObjects

    runtime_path, spark_hub_root, inputs_path, output = (
        Path(p).absolute() for p in (runtime_path, spark_hub_root, inputs_path, output)
    )
    inputs = json.loads(inputs_path.read_text())
    validate_inputs(inputs)
    require_fresh(output, [])
    runtime = json.loads(runtime_path.read_text())
    artifacts = runtime_artifacts(runtime)
    require(
        subprocess.check_output(
            [
                inputs["python"],
                "-c",
                "import sys; print('%d.%d' % sys.version_info[:2])",
            ],
            text=True,
        ).strip()
        == "3.11",
        "Python 3.11 required",
    )
    revisions = source_revisions(ROOT, spark_hub_root)
    objects = OwnedObjects(inputs["primary"], inputs["prefix"])
    before_objects = objects.manifest()
    require_fresh(output, before_objects)
    output.mkdir(mode=0o700, parents=True)
    source_names = (
        "spark_hub_write_native.py",
        "spark_hub_write_native_app.py",
        "spark_hub_write_native_checks.py",
        "spark_hub_read_pyspark.py",
        "append_aqe_profiles.py",
        "read_parity_pyspark.py",
        "s3_objects.py",
        "harness.py",
    )
    source_hashes = {}
    sources = []
    (output / "sources").mkdir(mode=0o700)
    for name in source_names:
        original = ROOT / "delta/acceptance" / name
        source_hashes[str(original)] = ref(original)["sha256"]
        retained = output / "sources" / name
        with retained.open("xb") as stream:
            stream.write(original.read_bytes())
        sources.append({"original": str(original), "artifact": ref(retained)})
    request = dict(
        inputs,
        output=str(output),
        sparkHubRoot=str(spark_hub_root),
        runtime=runtime,
        pins=PINS,
    )
    request_path = output / "request.private.json"
    save(request_path, request)
    save(output / "prefix-before.json", before_objects)
    environment = {
        "PATH": os.environ.get("PATH", ""),
        "TZ": "UTC",
        "ENV": "qualification",
        "KAFKA_BROKERS": "127.0.0.1:62912",
        "KAFKA_TOPIC": "unused",
        "IS_LOCAL": "false",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1",
        "SPARK_LOCAL_IP": "127.0.0.1",
        "JAVA_HOME": runtime["javaHome"],
        "SPARK_HOME": runtime["sparkHome"],
        "PYSPARK_PYTHON": inputs["python"],
        "PYSPARK_DRIVER_PYTHON": inputs["python"],
        "PYSPARK_PIN_THREAD": "true",
        "SPARK_USER": inputs["nativeIdentity"],
        "HADOOP_USER_NAME": inputs["nativeIdentity"],
    }
    argv = [
        runtime["sparkHome"] + "/bin/spark-submit",
        "--driver-memory",
        "2g",
        "--master",
        "local[2]",
        str(ROOT / "delta/acceptance/spark_hub_write_native_app.py"),
        str(request_path),
    ]
    save(
        output / "command.json",
        {
            "argv": argv,
            "request": ref(request_path),
            "started": time.time(),
            "environment": environment,
        },
    )
    timed_out = False
    with (output / "stdout.log").open("xb") as stdout, (output / "stderr.log").open(
        "xb"
    ) as stderr:
        process = subprocess.Popen(
            argv, cwd=output, env=environment, stdout=stdout, stderr=stderr
        )
        try:
            code = process.wait(timeout=1200)
        except subprocess.TimeoutExpired:
            timed_out = True
            process.terminate()
            try:
                code = process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                process.kill()
                code = process.wait()
    save(
        output / "exit.json",
        {
            "pid": process.pid,
            "exitCode": code,
            "timedOut": timed_out,
            "finished": time.time(),
        },
    )
    # Even incomplete executions retain an original result and exit; never manufacture missing cases.
    observed = (
        json.loads((output / "observations.json").read_text())
        if (output / "observations.json").is_file()
        else {"cases": []}
    )
    objects_after = objects.manifest(output / "objects")
    save(output / "objects-after.json", objects_after)
    verify_probe_sources(source_hashes)
    result = dict(
        observed,
        schemaVersion=1,
        pins=PINS,
        revisions=revisions,
        runtime=ref(runtime_path),
        artifacts=artifacts,
        sources=sources,
        inputs=ref(inputs_path),
        request=ref(request_path),
        freshPrefix={"emptyBefore": not before_objects, "exclusive": True},
        prefixBefore=ref(output / "prefix-before.json"),
        objectsAfter=ref(output / "objects-after.json"),
        process={
            **{
                key: ref(output / filename)
                for key, filename in (
                    ("command", "command.json"),
                    ("exit", "exit.json"),
                    ("stdout", "stdout.log"),
                    ("stderr", "stderr.log"),
                )
            },
            "exitCode": code,
        },
        governedSupport=False,
    )
    result_path = output / "result.json"
    save(result_path, result)
    verdict = verify_result(result_path)
    save(output / "verification.json", verdict)
    require(
        code == 0 and verdict["characterizationComplete"],
        "Original native cell incomplete; retained, no replay",
    )
    return result_path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ("runtime", "spark-hub-root", "inputs", "out"):
        parser.add_argument("--" + flag, required=True, type=Path)
    args = parser.parse_args()
    print(run(args.runtime, args.spark_hub_root, args.inputs, args.out))


if __name__ == "__main__":
    main()

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

"""Run only replacement base admission against immutable production packages."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import shutil
import subprocess
import time
import uuid

from harness import owned_directory, sha256, write_json
from run import Rest, Server, catalog_properties, run_spark
from s3_objects import OwnedObjects


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--inputs", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--packages", required=True)
    parser.add_argument("--red", action="store_true")
    args = parser.parse_args()
    output = Path(args.output)
    owned_directory(output)
    inputs = json.loads(Path(args.inputs).read_text())
    inputs["prefix"] = "production-admission-" + uuid.uuid4().hex
    packages = {
        version: Path(args.packages) / ("package-" + version)
        for version in ("3.3.0", "3.3.2")
    }
    inputs["_runtime"] = str(
        next((packages["3.3.0"] / "spark-jars").glob("gravitino-*.jar"))
    )
    harness = output / "harness"
    shutil.copytree(
        Path(__file__).parent, harness, ignore=shutil.ignore_patterns("__pycache__")
    )
    shutil.copyfile(harness / "admission_spark.py", harness / "spark_cases.py")
    inputs["_harness"] = str(harness)
    write_json(output / "inputs.json", inputs)
    write_json(
        output / "source.json",
        {
            "head": subprocess.check_output(
                ["git", "rev-parse", "HEAD"], text=True
            ).strip(),
            "harness": {p.name: sha256(p) for p in harness.glob("*.py")},
        },
    )
    write_json(
        output / "artifacts.json",
        {
            v: {str(p.relative_to(root)): sha256(p) for p in root.rglob("*.jar")}
            for v, root in packages.items()
        },
    )
    environments = json.loads(Path(inputs["sparkEnvironmentsFile"]).read_text())[
        "environments"
    ]
    server = Server(packages["3.3.0"], output, inputs)
    summary = []
    try:
        server.start()
        rest = Rest(server.uri, inputs["serviceAdmin"])
        metalake = "admission_" + uuid.uuid4().hex[:12]
        base = "/api/metalakes/" + metalake
        rest.call(
            "POST",
            "/api/metalakes",
            {
                "name": metalake,
                "comment": "Owned admission regression",
                "properties": {},
            },
        )
        pairs = [(env, delta) for env in environments for delta in ("3.3.0", "3.3.2")]
        for environment, delta in pairs[:1] if args.red else pairs:
            pair = environment["spark"] + "-" + delta
            catalog = "a_" + pair.replace(".", "_").replace("-", "_")
            prefix = inputs["prefix"] + "/" + catalog
            rest.call(
                "POST",
                base + "/catalogs",
                {
                    "name": catalog,
                    "type": "RELATIONAL",
                    "provider": "lakehouse-delta",
                    "comment": "Owned admission regression",
                    "properties": catalog_properties(inputs, "primary", prefix),
                },
            )
            rest.call(
                "POST",
                base + "/catalogs/" + catalog + "/schemas",
                {"name": "shared", "properties": {}},
            )
            case = {
                "mode": "red" if args.red else "green",
                "uri": server.uri,
                "metalake": metalake,
                "catalogs": [catalog],
                "journals": str(output / "journals"),
                "spark": environment["spark"],
                "delta": delta,
                "user": inputs["serviceAdmin"],
                "adoptionLocation": "s3a://"
                + inputs["primary"]["bucket"]
                + "/"
                + prefix
                + "/adoption/semantic",
            }
            directory = output / pair
            archives = {}
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(
                    run_spark, inputs, packages[delta], environment, case, directory
                )
                while not future.done():
                    for gate in directory.glob("*-ready"):
                        name = gate.name.removesuffix("-ready")
                        if name in archives:
                            continue
                        location = gate.read_text()
                        store = OwnedObjects(
                            inputs["primary"], location.split("/", 3)[3]
                        )
                        manifest = store.manifest(
                            output / "original-objects" / pair / name
                        )
                        archives[name] = {"location": location, "before": manifest}
                        write_json(output / ("objects-" + pair + ".json"), archives)
                        (directory / (name + "-continue")).touch()
                    time.sleep(0.2)
                code = future.result()
            for name, record in archives.items():
                store = OwnedObjects(
                    inputs["primary"], record["location"].split("/", 3)[3]
                )
                record["after"] = store.manifest()
                record["unchanged"] = record["before"] == record["after"]
            write_json(output / ("objects-" + pair + ".json"), archives)
            summary.append({"pair": pair, "exit": code, "objects": archives})
            write_json(output / "summary.json", summary)
            if args.red:
                assert code != 0, "Baseline did not produce original failing exit"
                assert any(
                    not record["unchanged"] for record in archives.values()
                ), "Baseline failed before native mutation"
                regression = json.loads((directory / "result.json").read_text())[
                    "records"
                ][-1]
                assert "DELTA_REGISTRATION_BINDING_MISMATCH" in regression["rejection"]
                assert (
                    regression["nativeAfter"]["version"]
                    == regression["before"]["version"] + 1
                )
                assert regression["nativeAfter"]["uuid"] != regression["before"]["uuid"]
                assert regression["before"]["rows"] == [{"id": 17}]
                assert regression["nativeAfter"]["rows"] == []
            else:
                assert code == 0, "Focused Spark regression failed: " + pair
                assert all(
                    record["unchanged"] for record in archives.values()
                ), "Rejected native objects changed"
        write_json(
            output / "completed.json",
            {"red": args.red, "pairs": [item["pair"] for item in summary]},
        )
    finally:
        server.stop()


if __name__ == "__main__":
    main()

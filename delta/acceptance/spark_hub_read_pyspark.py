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

"""Actual Spark Hub imports and native DataFrame actions; no Scala payload bridge."""

import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import sys
import threading
from uuid import UUID
import zipfile

from read_parity_pyspark import normalize, ref, save

PRODUCT_MODULES = (
    "src.shared.services.v2.spark",
    "src.shared.services.v3.gravitino_catalog",
    "src.shared.services.v3.gravitino_runtime",
    "src.shared.schemas.v3.gravitino",
)


def import_product(source):
    """Import the ordinary production packages, including eager schema dependencies."""
    sys.path.insert(0, str(source))
    from src.shared.services.v2.spark import SparkClient
    from src.shared.services.v3.gravitino_catalog import GravitinoDatasetCatalog
    from src.shared.schemas.v3.gravitino import DatasetBinding, GravitinoRuntimeProfile

    importlib.import_module("src.shared.services.v3.gravitino_runtime")
    return SparkClient, GravitinoDatasetCatalog, DatasetBinding, GravitinoRuntimeProfile


def import_inventory(source):
    """Hash actual imported source bytes and installed external distribution files.

    Production settings *code* is included; Settings values and environment are
    never serialized. The retained interpreter owns external dependencies.
    """
    from append_aqe_profiles import module_provenance

    provider_names = (
        "py4j",
        "py4j.clientserver",
        "pyspark",
        "pyspark.java_gateway",
        "pyspark.sql.dataframe",
        "pyspark.sql.readwriter",
        "pyspark.sql.session",
        "pyspark.sql.functions",
    )
    providers = {
        name: module_provenance(importlib.import_module(name))
        for name in provider_names
    }
    source = Path(source).absolute()
    files = {}
    top_levels = set()
    paths = {}
    for name, module in list(sys.modules.items()):
        path = getattr(module, "__file__", None)
        if not path:
            continue
        if name == "src" or name.startswith("src."):
            marker = str(source) + "/"
            assert str(path).startswith(
                marker
            ), "Production import escaped packaged source"
            relative = str(path)[len(marker) :]
            assert relative.endswith(".py"), "Production imports must use source bytes"
            if source.suffix == ".zip":
                with zipfile.ZipFile(source) as archive:
                    data = archive.read(relative)
            else:
                data = Path(path).read_bytes()
            files[relative] = hashlib.sha256(data).hexdigest()
            paths[name] = str(path)
        else:
            top_levels.add(name.split(".")[0])
    mapping = importlib.metadata.packages_distributions()
    distributions = set().union(*(set(mapping.get(name, [])) for name in top_levels))
    dependencies = {}
    for name in sorted(distributions):
        distribution = importlib.metadata.distribution(name)
        inventory = {}
        for member in distribution.files or []:
            # Native runtime JARs are independently verified by the G manifest.
            if str(member).endswith((".pyc", ".jar")) or "/__pycache__/" in str(member):
                continue
            path = Path(distribution.locate_file(member)).absolute()
            if path.is_file():
                inventory[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
        assert inventory, "Dependency has no retained installed file inventory"
        dependencies[distribution.metadata["Name"]] = {
            "version": distribution.version,
            "files": inventory,
        }
    return {
        "sourceInputs": dict(sorted(files.items())),
        "importPaths": paths,
        "dependencies": dependencies,
        "pythonProviders": providers,
        "python": sys.executable,
        "pythonVersion": platform.python_version(),
    }


class CallObserver:
    """Observe Python adapter and fallback boundaries without replacing any method."""

    def __init__(self):
        self.adapter = []
        self.forbidden = []
        self.native = []
        self.previous = None
        self.connections = []
        self.active = False

    def profile(self, frame, event, argument):
        if event != "call":
            return
        module = frame.f_globals.get("__name__", "")
        method = frame.f_code.co_name
        if (
            module == "src.shared.services.v3.gravitino_catalog"
            and method == "get_table_df_by_id"
        ):
            self.adapter.append(
                {
                    "method": "GravitinoDatasetCatalog.get_table_df_by_id",
                    "datasetId": str(frame.f_locals["id"]),
                }
            )
        if module.startswith("pyspark.sql") and method in ("collect", "count", "table"):
            self.native.append({"module": module, "method": method})
        if (
            module == "src.shared.services.v2.spark"
            and method == "load_delta_table"
            or module.startswith("pyspark.sql.readwriter")
            and method == "load"
            or module.startswith("delta.tables")
            and method in ("forPath", "isDeltaTable")
            or module.startswith("src.")
            and ("glue" in module.lower() or "athena" in module.lower())
        ):
            self.forbidden.append({"module": module, "method": method})
        if module == "botocore.client" and method == "_make_api_call":
            client = frame.f_locals.get("self")
            service = client.meta.service_model.service_name
            if service in ("glue", "athena"):
                self.forbidden.append(
                    {"module": module, "method": method, "service": service}
                )

    def audit(self, event, arguments):
        if self.active and event == "socket.connect":
            address = arguments[1]
            self.connections.append(
                list(address) if isinstance(address, tuple) else str(address)
            )

    def __enter__(self):
        self.active = True
        sys.addaudithook(self.audit)
        self.previous = sys.getprofile()
        sys.setprofile(self.profile)
        return self

    def __exit__(self, *error):
        self.active = False
        sys.setprofile(self.previous)


def run_cases(spark, bridge, request, product):
    """Issue six finite caller cases; the bridge supplies observations only."""
    from pyspark.sql import DataFrame, functions as F
    from spark_hub_read_checks import CASE_SPECS, SETTINGS

    SparkClient, Catalog, Binding, Profile = product
    startup = spark.sparkContext.getConf()
    keys = request["profileKeys"]
    profile_conf = {
        key: str(
            spark.conf.get(key) if key.startswith("spark.sql.") else startup.get(key)
        )
        for key in keys
    }
    # The bridge's transparent gateway is the actual session URI. The upstream
    # server URI remains a separate fixture reference, never substituted here.
    profile = Profile(
        request["expectedSparkVersion"], request["expectedDeltaVersion"], profile_conf
    )
    client = SparkClient("spark-hub-shared-read", gravitino_profile=profile)
    assert client.session._jsparkSession.equals(
        spark._jsparkSession
    ), "Actual compatible session was not reused"
    bindings = [
        Binding(UUID(item["datasetId"]), item["catalog"], item["schema"], item["table"])
        for item in request["bindings"]
    ]
    catalog = Catalog(client, bindings)
    known, unpublished = bindings
    unknown = UUID(request["unknownDatasetId"])
    output = Path(request["pythonOutput"])
    output.mkdir()
    cases = []
    native_frames = []

    def load(identifier):
        frame = catalog.get_table_df_by_id(identifier)
        assert isinstance(frame, DataFrame), "Actual native DataFrame absent"
        native_frames.append(type(frame).__module__ + "." + type(frame).__qualname__)
        return frame

    def policy_fields(phase):
        applied_path = Path(request["output"]) / (phase + "-applied.json")
        applied = json.loads(applied_path.read_text())
        state = json.loads(Path(applied["state"]["path"]).read_text())
        return {
            "policySnapshot": ref(state["policiesFile"]),
            "appliedPolicy": ref(applied_path),
        }

    def native_thread():
        return {
            "python": threading.get_ident(),
            "jvm": int(spark._jvm.java.lang.Thread.currentThread().getId()),
            "subject": str(
                spark._jvm.org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerQuerySubject.current(
                    spark._jsparkSession
                ).user()
            ),
            "ugi": str(
                spark._jvm.org.apache.hadoop.security.UserGroupInformation.getCurrentUser().getShortUserName()
            ),
        }

    def run(name, factory, identifier, phase="initial", precondition=False):
        observation_name = name + "-precondition" if precondition else name
        bridge.begin(observation_name)
        thread_before = native_thread()
        frame = None
        rows = None
        error = None
        actual_api = None
        api = "collect" if precondition else CASE_SPECS[name][1]
        with CallObserver() as calls:
            try:
                frame = factory()
                actual_api = api
                rows = (
                    [[int(frame.count())]]
                    if api == "count"
                    else [
                        [normalize(value) for value in row] for row in frame.collect()
                    ]
                )
            except Exception as failure:
                error = {"class": type(failure).__name__, "message": str(failure)}
        observation = json.loads(
            Path(
                str(
                    bridge.finish(
                        observation_name,
                        None if frame is None else frame._jdf,
                        error is None,
                    )
                )
            ).read_text()
        )
        binding = next(
            (
                value
                for value in request["bindings"]
                if value["datasetId"] == str(identifier)
            ),
            {
                "datasetId": str(identifier),
                "catalog": request["catalog"],
                "schema": request["database"],
                "table": None,
            },
        )
        outcome = (
            "allowed"
            if error is None
            else "rejected"
            if name in ("unknown-binding", "unpublished-registration")
            else "denied"
        )
        value = {
            "name": observation_name,
            "identityProfile": request["identityProfile"],
            "method": "GravitinoDatasetCatalog.get_table_df_by_id",
            "api": api,
            "actualApi": actual_api,
            "actionRoute": "native-pyspark",
            "sourceBinding": binding,
            "outcome": outcome,
            "observedRows": rows,
            "error": error,
            "policyPhase": phase,
            "adapterCalls": calls.adapter,
            "forbiddenCalls": calls.forbidden,
            "nativeCalls": calls.native,
            "pythonConnections": calls.connections,
            "threadBefore": thread_before,
            "threadAfter": native_thread(),
            **observation,
            **policy_fields(phase),
        }
        if name == "fresh-action-revoke":
            value["freshConstruction"] = True
            if not precondition:
                trace = Path(request["output"]).parent / "revoke-id/ranger-trace"
                value["revokeRequest"] = ref(trace / "0002.request.private.json")
                value["revokeResponse"] = ref(trace / "0002.response.private.bin")
        if not precondition:
            expected = CASE_SPECS[name][2]
            assert outcome == expected, (name, outcome, error)
        save(output / (observation_name + ".json"), value)
        return value

    try:
        profile_name = request["identityProfile"]
        if profile_name == "table-wide":

            def joined():
                left = load(known.dataset_id).alias("l")
                right = load(known.dataset_id).alias("r")
                return left.join(right, F.col("l.id") == F.col("r.id")).select(
                    "l.id", "l.region", "l.email"
                )

            cases.append(run("named-filter-mask", joined, known.dataset_id))
            cases.append(run("unknown-binding", lambda: load(unknown), unknown))
            cases.append(
                run(
                    "unpublished-registration",
                    lambda: load(unpublished.dataset_id),
                    unpublished.dataset_id,
                )
            )
        elif profile_name == "denied-email":
            cases.append(
                run(
                    "column-denied",
                    lambda: load(known.dataset_id).select("email"),
                    known.dataset_id,
                )
            )
        elif profile_name == "id-only":
            cases.append(
                run(
                    "projected-count",
                    lambda: load(known.dataset_id).select("id"),
                    known.dataset_id,
                )
            )
            precondition = run(
                "fresh-action-revoke",
                lambda: load(known.dataset_id).select("id"),
                known.dataset_id,
                precondition=True,
            )
            assert (
                precondition["outcome"] == "allowed"
            ), "Revoke precondition was not authorized"
            bridge.phase("revoke-id")
            revoked = run(
                "fresh-action-revoke",
                lambda: load(known.dataset_id).select("id"),
                known.dataset_id,
                "revoke-id",
            )
            revoked["precondition"] = ref(
                output / "fresh-action-revoke-precondition.json"
            )
            # Preserve original raw attempts; the final case links its two actions.
            save(output / "fresh-action-revoke-case.json", revoked)
            cases.append(revoked)
        else:
            raise AssertionError("Unknown caller profile")
        inventory = import_inventory(request["sparkHubZip"])
        import py4j
        import pyspark
        from py4j.clientserver import ClientServer

        assert (
            isinstance(spark.sparkContext._gateway, ClientServer)
            and os.environ.get("PYSPARK_PIN_THREAD") == "true"
        ), "Pinned actual Py4J connection required"
        caller = {
            **inventory,
            "package": request["sparkHubPackage"],
            "subject": request["subject"],
            "sessionReused": True,
            "nativeDataFrame": bool(native_frames),
            "pinnedConnection": True,
            "thread": native_thread(),
            "request": ref(request["originalRequest"]),
            "pinEnvironment": os.environ.get("PYSPARK_PIN_THREAD"),
            "nativeSparkVersion": pyspark.__version__,
            "nativePy4jVersion": py4j.__version__,
            "nativeFrameClasses": native_frames,
            "settings": {key: spark.conf.get(key) for key in SETTINGS},
            "actualGatewayUri": spark.conf.get("spark.sql.gravitino.uri"),
            "fixtureUpstreamUri": request["fixtureUpstreamUri"],
        }
        save(output / "caller.json", caller)
        save(output / "cases.json", cases)
    finally:
        bridge.close()


def main():
    request_path = Path(sys.argv[1])
    request = json.loads(request_path.read_text())
    if request.get("mode") == "inventory":
        import_product(request["sparkHubRoot"])
        from src.shared.schemas.v2.base_jobs import ComputeProperties

        save(
            request["output"],
            {
                **import_inventory(request["sparkHubRoot"]),
                "computeDefaults": ComputeProperties().model_dump(by_alias=True),
            },
        )
        return
    product = import_product(request["sparkHubZip"])
    from pyspark import SparkContext
    from pyspark.sql import SparkSession

    SparkContext._ensure_initialized()
    bridge = SparkContext._jvm.org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerLiveReadParity(
        str(request_path)
    )
    jsc = SparkContext._jvm.org.apache.spark.api.java.JavaSparkContext.fromSparkContext(
        bridge.session().sparkContext()
    )
    context = SparkContext(gateway=SparkContext._gateway, jsc=jsc)
    spark = SparkSession(context, bridge.session())
    run_cases(spark, bridge, request, product)


if __name__ == "__main__":
    main()

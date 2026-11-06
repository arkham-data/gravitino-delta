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

"""Exact registered PostgreSQL qualification contracts."""

LEAD = ("3.5.6", "3.3.2")
PAIRS = tuple((spark, delta) for spark in ("3.5.4", "3.5.6") for delta in ("3.3.0", "3.3.2"))
COMMON = ("jdbc_insert_into", "jdbc_source_select_denied", "jdbc_target_update_denied")
EXTRA = ("jdbc_mixed_save_as_table", "jdbc_column_composition", "jdbc_source_revoked_before_publish", "jdbc_config_changed_before_publish", "jdbc_view_rejected")


def case_names(pair):
    return COMMON + (EXTRA if tuple(pair) == LEAD else ())


def verify_inventory(cells):
    """Require each pinned pair once and each assigned case exactly once."""
    from collections import Counter

    assert Counter((c["spark"], c["delta"]) for c in cells) == Counter(PAIRS)
    for cell in cells:
        assert Counter(cell["cases"]) == Counter(case_names((cell["spark"], cell["delta"])))



def verify_source(source, expected):
    """Keep upstream identity and native Delta state separate."""
    keys = {"catalog", "schema", "table", "database", "provider", "driverVersion",
            "driver", "serverVersion", "imageId", "kind"}
    assert keys <= source.keys() and keys <= expected.keys()
    assert {key: source[key] for key in keys} == {key: expected[key] for key in keys}
    assert source["provider"] == "jdbc-postgresql"
    assert source["driver"] == "org.postgresql.Driver" and source["driverVersion"] == "42.7.11"
    assert source["kind"] == "TABLE" and "nativeVersion" not in source



def verify_outcome(case):
    """Check native publication and genuine, correctly scoped pre-payload denials."""
    from input_timestamp_checks import verify_timestamp_rows, verify_timestamp_outcome

    outcome = case["outcome"]
    assert outcome in ("committed", "source-denied", "target-denied")
    committed = outcome == "committed"
    verify_timestamp_rows(case["before"], case["after"], committed)
    verify_timestamp_outcome("committed" if committed else "target-denied", case["failures"])
    assert isinstance(case["metadataQueries"], int) and case["metadataQueries"] >= 0
    assert case["payloadQueries"] == int(committed)
    if not committed:
        access = "select" if outcome == "source-denied" else "update"
        assert any(request["resource"] == case["expectedResource"]
                   and request["accessType"] == access and request["allowed"] is False
                   for request in case["requests"]), "Wrong resource or fabricated denial"



def verify_artifact(reference):
    """Rehash original retained bytes, never trust a path or a copied result label."""
    import hashlib
    from pathlib import Path

    path = Path(reference["path"])
    assert path.is_file(), "Missing original artifact"
    assert hashlib.sha256(path.read_bytes()).hexdigest() == reference["sha256"], "Changed artifact"



def verify_case(case):
    """Join original process/artifact/native bytes for each finite JDBC scenario."""
    from cdf_merge_live_checks import read_ref
    from input_timestamp_checks import verify_timestamp_rows, verify_timestamp_outcome

    assert case["name"] in COMMON + EXTRA
    committed = case["name"] in ("jdbc_insert_into", "jdbc_mixed_save_as_table")
    before, after = read_ref(case["before"]), read_ref(case["after"])
    verify_timestamp_rows(before, after, committed)
    assert before["nativeId"] == after["nativeId"]
    source, target = case["source"], case["target"]
    assert source["catalog"] == "jdbc_source" and source["database"] == "source_schema"
    assert source["table"] == ("source_view" if case["name"] == "jdbc_view_rejected" else "source")
    assert source != target and target["table"] == case["name"]
    assert read_ref(case["writerExit"])["exitCode"] == 0 and not read_ref(case["writerExit"])["timedOut"]
    request = read_ref(case["request"])
    assert request["registeredJdbc"] is True and request["sourceCatalog"] == source["catalog"]
    assert request["sourceSchema"] == source["database"] and request["sourceTable"] == source["table"]
    python = read_ref(case["pythonRuntime"])
    assert python["spark"] == request["expectedSparkVersion"] and python["pinEnvironment"] == "true"
    assert read_ref(case["wrapper"])["className"].endswith(".DeltaRangerPostgreSqlCatalogSpark35")
    actions = case["actions"]
    expected = (["id_only", "hidden_predicate", "pruned_original_demand"] if case["name"] == "jdbc_column_composition" else
                (["physical_control", "first"] if case["name"] == "jdbc_view_rejected" else ["first"]))
    assert [action["action"] for action in actions] == expected
    for action in actions:
        original = read_ref(action["observation"])
        caller = read_ref(action["caller"])
        assert original["failure"] == action["failures"] and original["rangerRequests"] == action["requests"]
        assert caller["before"] == caller["after"]
        assert caller["pinnedEnvironment"] == "true" and caller["before"]["aqe"] == "true"
        assert original["aqe"] is True and original["listenerQueuesDrained"] is True
        assert original["writerThreadId"] == caller["before"]["jvmThread"]
        assert action["metadataQueries"] >= 0
        if committed or action["action"] in ("id_only", "physical_control"):
            assert action["failures"] == [] and action["payloadQueries"] == 1
            selected_source = dict(source, table="source") if action["action"] == "physical_control" else source
            assert any(all(item["resource"].get(key) == value for key, value in selected_source.items())
                       and item["accessType"] == "select" and item["allowed"] is True
                       for item in action["requests"]), "Missing exact source SELECT decision"
            if action["action"] in ("id_only", "physical_control"):
                assert action["rows"] == caller["rows"], "Copied allowed rows differ from original caller"
                assert sorted(action["rows"]) == [[1], [3]]
            else:
                assert any(callback["appendOwner"] and any(node["className"] == "org.apache.spark.sql.execution.RowDataSourceScanExec" for node in callback["planNodes"])
                           for callback in original["callbacks"]), "Missing actual native JDBC physical scan"
        elif case["name"] in ("jdbc_source_select_denied", "jdbc_target_update_denied", "jdbc_column_composition"):
            verify_timestamp_outcome("target-denied", action["failures"])
            assert action["payloadQueries"] == 0
            resource = target if case["name"] == "jdbc_target_update_denied" else source
            access = "update" if resource == target else "select"
            assert any(all(item["resource"].get(k) == v for k, v in resource.items()) and item["accessType"] == access and item["allowed"] is False
                       for item in action["requests"]), "Denial came from another source/target"
        elif case["name"] == "jdbc_source_revoked_before_publish":
            verify_timestamp_outcome("after-helper-revoked", action["failures"])
            assert action["payloadQueries"] == 1
        else:
            assert case["name"] in ("jdbc_config_changed_before_publish", "jdbc_view_rejected")
            assert action["payloadQueries"] == int(case["name"] == "jdbc_config_changed_before_publish")
            if case["name"] == "jdbc_view_rejected":
                assert any(failure["className"] == "java.lang.SecurityException"
                           and failure["message"] == "Registered JDBC source load rejected"
                           and failure["stack"] and failure["stack"][0].startswith("org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerPostgreSqlCatalogSpark35.loadTable(")
                           for failure in action["failures"]), "Missing physical-kind load rejection"
            else:
                assert any(failure["className"].startswith("org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerJdbcSourceBindings$")
                           and failure["message"].startswith("Registered JDBC source rejected: ") and failure["stack"]
                           and any("DeltaRangerJdbcSourceBindings$Observation.requireSame(" in frame for frame in failure["stack"])
                           for failure in action["failures"]), "Missing genuine metadata equality rejection"
    if case["name"] == "jdbc_mixed_save_as_table":
        assert case["api"] == "saveAsTable"
        warm = read_ref(case["warm"])
        assert warm["freshDataFrames"] is True and len(warm["targetRows"]) == len(after["rows"])
        assert sorted(row[:3] for row in warm["targetRows"]) == sorted(row[:3] for row in after["rows"])
        assert warm["repeatedTargetRows"] == warm["targetRows"]
        assert sorted(warm["dependentRows"]) == sorted([row[0], row[2]] for row in after["rows"])
        assert any(item["resource"].get("catalog") == target["catalog"] and item["resource"].get("table") == "source" and item["accessType"] == "select" and item["allowed"]
                   for item in actions[0]["requests"]), "Missing named Delta source decision"
    if case["name"] in ("jdbc_source_revoked_before_publish", "jdbc_config_changed_before_publish"):
        barrier = {key: read_ref(value) for key, value in case["barrier"].items()}
        assert barrier["held"]["writerThreadId"] == read_ref(actions[0]["caller"])["before"]["jvmThread"]
        before_keys = {item["key"] for item in barrier["rawBefore"]}
        residue = {item["key"] for item in barrier["rawHeld"]} - before_keys
        assert any(key.endswith(".parquet") for key in residue)
        assert not any(key.endswith("/00000000000000000001.json") for key in residue)
        assert after["publications"] == before["publications"]
    for transition in case["transitions"]:
        read_ref(transition)
    return True


def postgres_counts(text):
    """Count timestamped source-role SQL records, including lowercase stock probes."""
    import re

    timestamp = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)? \S+ "
    records = re.finditer(timestamp + r"\[\d+\] (?P<role>\S+)@\S+ (?P<message>.*?)"
                          + r"(?=" + timestamp + r"|\Z)", text, re.MULTILINE | re.DOTALL)
    statements = []
    for record in records:
        if record["role"] not in ("extractor", "extractor_rotated"):
            continue
        message = re.match(r"LOG:\s+(?:statement:|execute [^:\n]+:)\s*(.*)",
                           record["message"], re.DOTALL)
        if message and re.match(r"SELECT\b", message[1], re.IGNORECASE):
            statements.append(message[1])
    payload = [sql for sql in statements
               if re.search(r'\bFROM\s+(?:"source_schema"|source_schema)\s*\.\s*(?:"source(?:_view)?"|source(?:_view)?)(?=\s|;|$)', sql, re.IGNORECASE)
               and not re.search(r"\bWHERE\s+1\s*=\s*0\b", sql, re.IGNORECASE)]
    return dict(payloadQueries=len(payload), metadataQueries=len(statements) - len(payload))


def _original_postgres_counts(text):
    """Reproduce the superseded classifier only to authenticate historical counters."""
    import re

    lines = [line for line in text.splitlines() if (" extractor@" in line or " extractor_rotated@" in line) and "SELECT" in line]
    payload = [line for line in lines if re.search(r'FROM (?:"source_schema"|source_schema)\.(?:"source(?:_view)?"|source(?:_view)?)(?:\s|$)', line) and "WHERE 1=0" not in line]
    return dict(payloadQueries=len(payload), metadataQueries=len(lines) - len(payload))


def postgres_case_evidence(case, original_classifier=False):
    """Derive counts from hashed originals without replacing their recorded counters."""
    import hashlib
    from pathlib import Path
    from cdf_merge_live_checks import read_ref

    keys = ("metadataQueries", "payloadQueries")
    classifier = _original_postgres_counts if original_classifier else postgres_counts

    def snapshot(reference):
        value = read_ref(reference)
        streams = {}
        for stream in ("logStdout", "logStderr"):
            verify_artifact(value[stream])
            streams[stream] = Path(value[stream]["path"]).read_bytes()
        text = "\n".join(stream.decode() for stream in streams.values())
        recorded = {key: value[key] for key in keys}
        derived = postgres_counts(text)
        assert classifier(text) == recorded, "Original snapshot counters do not match their classifier"
        assert recorded["payloadQueries"] == derived["payloadQueries"], "Payload counts cannot be corrected"
        return dict(original=reference, recorded=recorded, derived=derived), value, streams

    snapshots = [snapshot(case[label])[0] for label in ("postgresBefore", "postgresAfter")]
    actions = []
    for action in case["actions"]:
        before, old, old_streams = snapshot(action["postgresBefore"])
        after, new, new_streams = snapshot(action["postgresAfter"])
        suffixes = {}
        texts = []
        for stream in old_streams:
            assert new_streams[stream].startswith(old_streams[stream]), "PostgreSQL log was replaced or truncated"
            suffix = new_streams[stream][len(old_streams[stream]):]
            suffixes[stream] = dict(before=old[stream], after=new[stream], bytes=len(suffix),
                                    sha256=hashlib.sha256(suffix).hexdigest())
            texts.append(suffix.decode())
        text = "\n".join(texts)
        recorded = {key: action[key] for key in keys}
        derived = postgres_counts(text)
        assert classifier(text) == recorded, "Original action counters do not match their classifier"
        assert recorded["payloadQueries"] == derived["payloadQueries"], "Payload counts cannot be corrected"
        for key in keys:
            assert after["derived"][key] - before["derived"][key] == derived[key]
            assert after["recorded"][key] - before["recorded"][key] == recorded[key]
        if case["name"] == "jdbc_view_rejected" and action["action"] == "first":
            assert "source\\_view" in text or "source_view" in text
            assert "pg_catalog.pg_class" in text, "Missing actual JDBC table-kind metadata SQL"
            assert "FATAL:" not in text and "ERROR:" not in text, "Database failure cannot prove a physical-table guard"
        actions.append(dict(action=action["action"], before=before, after=after,
                            recorded=recorded, derived=derived, suffixes=suffixes))
    for key in keys:
        for label in ("recorded", "derived"):
            assert snapshots[1][label][key] - snapshots[0][label][key] == sum(action[label][key] for action in actions)
    return dict(name=case["name"], snapshots=snapshots, actions=actions)


def verify_sql_correction(result):
    """Require explicit derived correction provenance and unchanged original case arrays."""
    from cdf_merge_live_checks import read_ref

    if "sqlCountCorrection" not in result:
        for case in result["cases"]:
            postgres_case_evidence(case)
        return
    correction = read_ref(result["sqlCountCorrection"])
    assert set(correction) == {"scope", "originalResult", "cases"}
    assert correction["scope"] == "registered-jdbc-sql-count-correction-v1"
    original = read_ref(correction["originalResult"])
    derived_fields = {"sqlCountCorrection", "revalidationSourceDelta", "supervisorExit"}
    assert not derived_fields.intersection(original)
    assert original == {key: value for key, value in result.items() if key not in derived_fields}, "Original result was relabeled"
    assert correction["cases"] == [postgres_case_evidence(case, original_classifier=True) for case in original["cases"]]


def verify_cell(result):
    """Recheck original executed package, services, SQL and native state without replay."""
    import json
    from pathlib import Path
    from cdf_merge_live_checks import read_ref
    from cdf_read_live_package import verify_runtime, _verify_runtime_artifacts, capture_sources, ROOT, sha
    from append_aqe_public import verify_public_transition

    verify_sql_correction(result)
    runtime = read_ref(result["runtime"])
    if result.get("revalidationSourceDelta"):
        verify_source_delta(runtime["sourceInputs"], capture_sources(ROOT), read_ref(result["revalidationSourceDelta"]))
        _verify_runtime_artifacts(runtime)
    else:
        verify_runtime(runtime)
    supervisor = read_ref(result["supervisorExit"])
    assert supervisor["exitCode"] == 0 and supervisor["timedOut"] is False
    verify_versions(runtime)
    pair = (result["spark"], result["delta"])
    assert pair in PAIRS and pair == (runtime["spark"], runtime["delta"])
    assert result["caseInventory"] == list(case_names(pair))
    assert [case["name"] for case in result["cases"]] == result["caseInventory"]
    plan = read_ref(result["fixturePlan"])
    source = read_ref(result["source"])
    expected = dict(plan["jdbcSource"], kind="TABLE")
    verify_source(source, expected)
    assert source["imageId"] == "sha256:33f923b05f64ca54ac4401c01126a6b92afe839a0aa0a52bc5aeb5cc958e5f20"
    assert source["physicalKinds"].splitlines() == ["source|BASE TABLE", "source_view|VIEW"]
    assert source["writeDenied"] is True and "f|t|f|f" in source["acl"]
    terminal = read_ref(result["sourceTerminal"])
    assert terminal["preservedData"] is True and terminal["state"]["Running"] is False and terminal["state"]["ExitCode"] == 0
    fixture = read_ref(result["fixtureExit"])
    assert fixture["exitCode"] == 0 and fixture["timedOut"] is False
    deployed = read_ref(result["deployedRanger"])
    assert deployed["associated"] is True and deployed["deployedRangerVersion"] == "2.8.0"
    fixture_directory = Path(result["fixturePlan"]["path"]).parent
    lifecycle = json.loads((fixture_directory / "lifecycle-exit.json").read_text())
    server_exit = json.loads((fixture_directory / "server/exit-1.json").read_text())
    assert lifecycle["stopRequested"] is True and lifecycle["failed"] is False
    assert lifecycle["serverPid"] == plan["serverPid"] == server_exit["pid"]
    assert lifecycle["serverExit"] == server_exit["exit"] == 143
    assembly = json.loads(Path(runtime["assemblyManifest"]).read_text())
    logs = (fixture_directory / "server/class-load-1.log").read_text()
    from urllib.parse import urlsplit, unquote
    for name in ("org.apache.gravitino.catalog.postgresql.PostgreSqlCatalog", "org.apache.gravitino.catalog.jdbc.JdbcCatalog",
                 "org.postgresql.Driver", "org.apache.gravitino.authorization.ranger.delta.RangerDeltaAuthorizationPlugin"):
        origins = {line.split(" source: ")[1] for line in logs.splitlines() if " " + name + " source: " in line}
        assert len(origins) == 1, "Missing or ambiguous isolated server provider origin"
        actual = Path(unquote(urlsplit(origins.pop()).path))
        relative = actual.relative_to(fixture_directory / "server")
        original = Path(runtime["assemblyManifest"]).parent / "server" / relative
        assert sha(actual) == sha(original) == assembly["artifactFiles"][str(original)]
    for case in result["cases"]:
        verify_case(case)
        jvm = Path(case["origins"]["path"]).parent
        observed = json.loads((jvm / "observed.json").read_text())
        assert observed["java"] == "17" and observed["scala"] == "2.12.21" and observed["spark"] == runtime["spark"]
        assert observed["subject"] == plan["subject"]
        verify_python_runtime(read_ref(case["pythonRuntime"]), runtime, plan)
        for action in case["actions"]:
            caller = read_ref(action["caller"])["before"]
            assert caller["gatewayType"] == "py4j.clientserver.ClientServer" and caller["clientType"] == "py4j.clientserver.JavaClient"
            assert caller["javaServerType"] == "py4j.JavaServer" and Path(caller["javaServerOrigin"]).resolve() == Path(runtime["providers"]["py4j.JavaServer"]).resolve()
            assert caller["autoReset"] == "false" and caller["nullDefaults"] == "true" and caller["transactionVersion"] == "1"
            observation = read_ref(action["observation"])
            assert observation["subject"] == plan["subject"]
            assert all(item["user"] == plan["subject"] for item in action["requests"])
        applied = read_ref(case["applied"])
        state = read_ref(applied["rangerState"])
        downloaded, roles = read_ref(applied["download"]), read_ref(applied["roleDownload"])
        stable = lambda value: {key: item for key, item in value.items() if key not in ("policyUpdateTime", "roleUpdateTime")}
        assert applied["sameJvm"] is True and stable(downloaded) == stable(state["policies"]) and stable(roles) == stable(state["roles"])
        assert downloaded["serviceName"] == roles["serviceName"] == plan["resourceService"]
        assert applied["policy"] == downloaded["policyVersion"] and applied["role"] == roles["roleVersion"]
        verify_source_policies(plan, downloaded["policies"])
        if case["name"] in ("jdbc_insert_into", "jdbc_mixed_save_as_table"):
            start = json.loads(Path(case["writerExit"]["path"]).with_name("process.json").read_text())["started"]
            end = read_ref(case["writerExit"])["finished"]
            rows = read_ref(case["after"])["rows"]
            verify_timestamp_bounds(rows, observed["jvmTimezone"], start, end)
            if case["name"] == "jdbc_mixed_save_as_table":
                expected_cache = [row[:3] + [timestamp_millis(value, observed["jvmTimezone"]) for value in row[3:]] for row in rows]
                assert sorted(read_ref(case["warm"])["targetRows"]) == sorted(expected_cache)
        metadata = json.loads((jvm / "metadata-identity-final.json").read_text())
        assert metadata["principal"] == plan["metadataPrincipal"] != plan["subject"]
        paths = metadata["successfulNamedGets"]
        source_prefix = "/api/metalakes/" + plan["metalake"] + "/catalogs/" + case["source"]["catalog"] + "/schemas/" + case["source"]["database"] + "/tables/"
        assert any(path.startswith(source_prefix) for path in paths)
        if case["name"] != "jdbc_column_composition":
            target = case["target"]
            assert "/api/metalakes/" + plan["metalake"] + "/catalogs/" + target["catalog"] + "/schemas/" + target["database"] + "/tables/" + target["table"] in paths or case["name"] == "jdbc_view_rejected"
        for original_exit in [case["writerExit"]["path"]] + [str(Path(read_ref(case[label])["nativeEvidence"]["path"]).parent / "process/exit.json") for label in ("before", "after")]:
            exit_path = Path(original_exit)
            exit_state = json.loads(exit_path.read_text())
            process_state = json.loads(exit_path.with_name("process.json").read_text())
            command_state = json.loads(exit_path.with_name("command.json").read_text())
            assert exit_state["pid"] == process_state["pid"] and exit_state["exitCode"] == 0 and exit_state["timedOut"] is False
            assert command_state["argv"] and command_state["started"] <= process_state["started"] <= exit_state["finished"]
        origins = read_ref(case["origins"])
        assert set(origins) == set(runtime["providers"])
        for name, observed in origins.items():
            assert observed["jar"] == runtime["providers"][name]
            assert observed["jarSha256"] == runtime["jarHashes"][observed["jar"]]
            assert observed["classSha256"] == runtime["providerClasses"][name]
        images = [read_ref(case[label]) for label in ("before", "after")]
        for image in images:
            native = read_ref(image["nativeEvidence"])
            table = next(t for t in native["tables"] if t["name"] == image["nativeTable"])
            assert image["nativeTable"] == case["target"]["table"]
            assert image["version"] == table["version"] == table["reopenVersion"]
            assert image["rows"] == table["rows"] == table["reopenRows"]
            assert image["nativeId"] == table["native"]["id"]
            commits = []
            for key, reference in image["objects"].items():
                verify_artifact(reference)
                if "/" + image["nativeTable"] + "/_delta_log/" in key and Path(key).suffix == ".json" and Path(key).stem.isdecimal():
                    commits.append(int(Path(key).stem))
                    [json.loads(line) for line in Path(reference["path"]).read_text().splitlines() if line]
            assert sorted(commits) == image["publications"] == list(range(image["version"] + 1))
        old, new = images
        # Archives use new retained paths; equality concerns original object content hashes.
        assert all(new["objects"][key]["sha256"] == value["sha256"] for key, value in old["objects"].items())
        added = set(new["objects"]) - set(old["objects"])
        if case["name"] in ("jdbc_source_select_denied", "jdbc_target_update_denied", "jdbc_column_composition", "jdbc_view_rejected"):
            assert not added
        assert all("/" + case["target"]["table"] + "/" in key for key in added)
        for reference in case["transitions"]:
            change = read_ref(reference)
            request = read_ref(change["publicRequest"])
            full = request["path"].split("/table/")[1].rsplit("/", 2)[0]
            from urllib.parse import unquote
            catalog, schema, table = unquote(full).split(".")
            descriptor = dict(catalog=catalog, database=schema, table=table)
            states = [dict(public=json.loads(Path(change[side]["publicStateFile"]).read_text()),
                           **json.loads(Path(change[side]["rangerStateFile"]).read_text())) for side in ("before", "after")]
            for state in states:
                assert state["policies"]["serviceName"] == plan["resourceService"]
                assert all(policy["service"] == plan["resourceService"] for policy in state["policies"]["policies"])
            verify_public_transition(plan, descriptor, change["privilege"], change["grant"], *states, read_ref(change["callbacks"]))
    return True


def verify_source_delta(before, after, delta):
    """Only finite checker/docs edits may revalidate exact original executed packages."""
    from cdf_read_live_package import canonical_digest

    allowed = {"delta/acceptance/registered_jdbc_checks.py", "delta/acceptance/test_registered_jdbc_checks.py",
               "docs/superpowers/specs/2026-11-05-delta-registered-jdbc-results.md", "docs/delta-spark-parity.md",
               "docs/superpowers/plans/2026-11-05-delta-registered-jdbc.md"}
    assert delta["scope"] == "registered-jdbc-read-only-v1"
    assert delta["beforeInventory"] == before and delta["afterInventory"] == after
    assert delta["beforeDigest"] == canonical_digest(before) and delta["afterDigest"] == canonical_digest(after)
    changed = {path for path in set(before) | set(after) if before.get(path) != after.get(path)}
    assert changed and changed <= allowed and set(delta["files"]) == changed
    assert all(delta["files"][path] == dict(before=before.get(path), after=after.get(path)) for path in changed)


def verify_versions(runtime):
    """Verify installed provider metadata, not only reported runtime labels."""
    from pathlib import Path
    import zipfile
    from read_parity_checks import verify_jar_property

    providers = runtime["providers"]
    for name, expected in {
        "org.apache.spark.sql.SparkSession": "spark-sql_2.12-" + runtime["spark"] + ".jar",
        "org.apache.spark.sql.delta.DeltaLog": "delta-spark_2.12-" + runtime["delta"] + ".jar",
        "scala.Predef$": "scala-library-2.12.21.jar",
        "org.postgresql.Driver": "postgresql-42.7.11.jar",
    }.items():
        assert Path(providers[name]).name == expected
    for artifact in ("ranger-plugins-common", "ranger-audit-core", "ranger-plugins-cred", "ranger-plugin-classloader", "ugsync-util", "ranger-authz-api"):
        verify_jar_property(providers["org.apache.ranger.plugin.service.RangerBasePlugin"],
                            "META-INF/maven/org.apache.ranger/" + artifact + "/pom.properties", "version", "2.8.0")
    # The stock PostgreSQL jar has manifest versions, not Maven pom.properties.
    with zipfile.ZipFile(providers["org.postgresql.Driver"]) as archive:
        lines = archive.read("META-INF/MANIFEST.MF").decode().splitlines()
        for key in ("Implementation-Version", "Bundle-Version"):
            assert [line for line in lines if line.startswith(key + ":")] == [key + ": 42.7.11"]


def verify_python_runtime(python, runtime, plan):
    """Join classic pinned-thread Python modules to original stock ZIP member bytes."""
    import hashlib
    import zipfile
    from pathlib import Path
    from cdf_merge_live_checks import file_ref

    assert python["spark"] == runtime["spark"] and python["py4j"] == "0.10.9.7" and python["pinEnvironment"] == "true"
    assert python["executable"] == plan["python"]
    assert set(python["providers"]) == {"py4j", "pyspark", "py4j.clientserver", "pyspark.java_gateway", "pyspark.sql.readwriter"}
    libraries = Path(runtime["sparkHome"]) / "python/lib"
    for name, provider in python["providers"].items():
        container = provider["container"]
        assert file_ref(container["path"]) == container and provider["loader"] == "zipimport.zipimporter"
        expected = libraries / "pyspark.zip" if name.startswith("pyspark") else next(libraries.glob("py4j*.zip"))
        assert Path(container["path"]).resolve() == expected.resolve()
        with zipfile.ZipFile(container["path"]) as archive:
            assert hashlib.sha256(archive.read(provider["member"])).hexdigest() == provider["sha256"]


def verify_source_policies(plan, policies):
    """Require both exact JDBC catalogs' distinct original masks and row filters."""
    source = plan["jdbcSource"]
    selected = [policy for policy in policies if policy["id"] in plan["jdbcPolicyIds"]]
    assert len(selected) == 4
    for catalog in (source["catalog"], source["isolationCatalog"]):
        for kind in (1, 2):
            resources = {key: dict(values=[value], isExcludes=False, isRecursive=False)
                         for key, value in dict(catalog=catalog, database=source["schema"], table=source["table"]).items()}
            if kind == 1:
                resources["column"] = dict(values=["secret"], isExcludes=False, isRecursive=False)
            matches = [policy for policy in selected if policy["policyType"] == kind and policy["resources"] == resources]
            assert len(matches) == 1
            policy = matches[0]
            assert policy["service"] == plan["resourceService"] and policy["isEnabled"] is True
            items = policy["dataMaskPolicyItems" if kind == 1 else "rowFilterPolicyItems"]
            assert len(items) == 1 and items[0]["roles"] == [plan["rangerAppRole"]]
            assert not items[0].get("users") and not items[0].get("groups")
            assert items[0]["accesses"] == [dict(type="select", isAllowed=True)]
            expected = (dict(dataMaskType="CUSTOM", valueExpr="'MASKED'" if catalog == source["catalog"] else "'ISOLATED'")
                        if kind == 1 else dict(filterExpr="region = 'allowed'" if catalog == source["catalog"] else "id = 2"))
            actual = items[0]["dataMaskInfo" if kind == 1 else "rowFilterInfo"]
            assert all(actual[key] == value for key, value in expected.items())


def timestamp_millis(value, timezone):
    """Convert retained native timestamps to the JVM cache observer's millisecond form."""
    from datetime import datetime
    from zoneinfo import ZoneInfo

    if value is None:
        return None
    timestamp = datetime.fromisoformat(value).replace(tzinfo=ZoneInfo(timezone))
    return int(timestamp.timestamp()) * 1000 + timestamp.microsecond // 1000


def verify_timestamp_bounds(rows, timezone, started, finished):
    """Require native timestamp values inside the original process lifetime."""
    values = [timestamp_millis(value, timezone) for row in rows for value in row[3:] if value is not None]
    assert values and all(started * 1000 <= value <= finished * 1000 for value in values)

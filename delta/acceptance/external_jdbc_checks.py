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

"""Fail-closed, finite actual-service external JDBC evidence contracts."""
from collections import Counter
from datetime import datetime
import json
import re
from registered_jdbc_checks import verify_artifact, verify_versions

LEAD = ("3.5.6", "3.3.2")
PAIRS = tuple((s, d) for s in ("3.5.4", "3.5.6") for d in ("3.3.0", "3.3.2"))
COMMON = ("external_insert_into", "external_partitioned_save_as_table", "external_target_update_denied")
EXTRA = ("external_hub_tracker", "external_mixed_governed", "external_named_source_denied", "external_scope_closed_before_publish", "external_target_revoked_before_publish", "external_unissued_rejected", "external_source_advances")
COMMITTED = {COMMON[0], COMMON[1], EXTRA[0], EXTRA[1], EXTRA[-1]}


def case_names(pair):
    assert tuple(pair) in PAIRS
    return COMMON + (EXTRA if tuple(pair) == LEAD else ())


def action_names(name):
    assert name in COMMON + EXTRA
    if name == "external_hub_tracker":
        return ("remote_count", "tracker_count", "append")
    if name == "external_source_advances":
        return ("count_before", "append", "count_after")
    return ("append",)


def verify_inventory(cells):
    assert Counter((c["spark"], c["delta"]) for c in cells) == Counter(PAIRS)
    for cell in cells:
        assert Counter(cell["cases"]) == Counter(case_names((cell["spark"], cell["delta"])))


def verify_actions(name, actions):
    assert list(actions) == list(action_names(name)), "Missing/extra/reordered declared action"


def verify_terminal(value):
    assert value["outcome"] in ("committed", "unknown", "denied")
    assert value["attempted"] is True and value["retryEligible"] is False, "Attempted target cannot become replay eligible"


def verify_summary(value):
    text = json.dumps(value).lower()
    assert not any(token in text for token in ("password", "jdbc:postgresql:", "jdbcoptions", "credential", "secret=")), "Sensitive connection material in summary"


def verify_hub(value):
    required = {"src.data_platform.sources.jdbc.v2.main", "src.shared.commands.progress_tracker", "src.shared.services.v2.spark", "src.shared.services.v3.gravitino_jdbc"}
    assert required <= set(value["modules"]), "Missing actual Hub caller"
    assert value["remoteCount"] == value["trackerCount"] == 3
    assert set(value["identities"]) == {"main", "tracker"}
    main, tracker = value["identities"]["main"], value["identities"]["tracker"]
    assert main["python"] != tracker["python"] and main["jvm"] != tracker["jvm"]
    assert main["session"] and main["ugi"]
    assert all(main[k] == tracker[k] for k in ("session", "ugi", "groups"))


def verify_dependencies(dependencies):
    assert dependencies
    for value in dependencies.values():
        assert value["version"] and value["files"]
        for path, digest in value["files"].items():
            verify_artifact(dict(path=path, sha256=digest))


def verify_hub_calls(calls):
    required = {
        ("src.data_platform.sources.jdbc.v2.main", "run_source_jdbc_v2"),
        ("src.shared.services.v2.spark", "query_jdbc"),
        ("src.shared.services.v3.gravitino_jdbc", "read"),
        ("src.shared.commands.progress_tracker", "_run"),
        ("src.shared.services.v2.spark", "init_df_tracking"),
    }
    assert required <= {(c["module"], c["function"]) for c in calls}


def verify_native(name, before, after):
    committed = name in COMMITTED
    assert before["version"] == 0 and before["publications"] == [0]
    assert after["version"] == int(committed)
    assert after["publications"] == ([0, 1] if committed else [0])
    assert before["nativeId"] and before["nativeId"] == after["nativeId"]
    old, new = Counter(map(tuple, before["rows"])), Counter(map(tuple, after["rows"]))
    assert not old - new
    added = list((new - old).elements())
    expected = ([(1, "MASKED", "allowed"), (3, "MASKED", "allowed")] if name == "external_mixed_governed" else
                [(1, "alpha", "allowed"), (2, "beta", "blocked"), (3, "gamma", "allowed")] + ([(4, "delta", "allowed")] if name == "external_source_advances" else []))
    assert Counter(row[:3] for row in added) == Counter(expected if committed else [])
    if committed:
        assert all(len(row) == 5 for row in added)
        values = [value for row in added for value in row[3:]]
        assert all(isinstance(v, str) and v for v in values) and len(set(values)) == 1
        datetime.fromisoformat(values[0])


def verify_denial(name, failures):
    if name in COMMITTED:
        assert failures == [], "Committed/unconfirmed cannot be recast as denied or retried"
        return
    assert failures
    frames = [frame for failure in failures for frame in failure.get("stack", [])]
    if name in ("external_target_update_denied", "external_named_source_denied"):
        assert any(f["className"] == "java.lang.SecurityException" and f["message"] == "Delta Ranger access denied" for f in failures)
        assert any("DeltaRangerPolicyEngine.authorize" in frame for frame in frames)
    elif name == "external_target_revoked_before_publish":
        from input_timestamp_checks import verify_timestamp_outcome
        verify_timestamp_outcome("after-helper-revoked", failures)
    elif name == "external_scope_closed_before_publish":
        assert any(f["className"] == "java.lang.SecurityException" for f in failures)
        assert any("DeltaRangerExternal" in frame and ("requireLive" in frame or "reject" in frame) for frame in frames)
    else:
        assert name == "external_unissued_rejected"
        assert any("DeltaRanger" in frame and ("Input" in frame or "Read" in frame or "Append" in frame) for frame in frames)
        assert any(f["className"] == "java.lang.SecurityException" for f in failures)


def postgres_counts(text):
    """Classify each source-role SQL execution once; schema probes are not payload."""
    timestamp = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)? \S+ "
    records = re.finditer(timestamp + r"\[\d+\] (?P<role>\S+)@\S+ (?P<message>.*?)" + r"(?=" + timestamp + r"|\Z)", text, re.MULTILINE | re.DOTALL)
    counts = dict(schemaQueries=0, remoteCountQueries=0, payloadQueries=0, mutationQueries=0)
    for record in records:
        if record["role"] != "extractor":
            continue
        message = re.match(r"LOG:\s+(?:statement:|execute [^:\n]+:)\s*(.*)", record["message"], re.DOTALL)
        if not message:
            continue
        sql = message[1]
        if re.match(r"(?:INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|TRUNCATE)\b", sql, re.I):
            counts["mutationQueries"] += 1
        elif re.match(r"SELECT\b", sql, re.I):
            source = re.search(r"\bsource_schema\s*\.\s*source\b", sql.replace('"', ''), re.I)
            schema = re.search(r"\bWHERE\s+1\s*=\s*0\b", sql, re.I)
            remote = re.search(r"SELECT\s+COUNT\s*\(\s*\*\s*\)", sql, re.I)
            key = "schemaQueries" if not source or schema else ("remoteCountQueries" if remote else "payloadQueries")
            counts[key] += 1
    return counts


def verify_identities(value):
    assert (value["spark"], value["delta"]) in PAIRS
    assert value["server"] == value["client"] == "2.8.0"
    assert value["java"] == "17" and value["scala"] == "2.12.21"


def verify_process(reference):
    from pathlib import Path
    from cdf_merge_live_checks import read_ref
    terminal = read_ref(reference)
    folder = Path(reference["path"]).parent
    process = json.loads((folder/"process.json").read_text())
    command = json.loads((folder/"command.json").read_text())
    assert terminal["pid"] == process["pid"] and terminal["exitCode"] == 0 and terminal["timedOut"] is False
    assert command["argv"] and command["started"] <= process["started"] <= terminal["finished"]


def verify_sql(action):
    from pathlib import Path
    from cdf_merge_live_checks import read_ref
    before, after = [read_ref(action[k]) for k in ("postgresBefore", "postgresAfter")]
    texts = []
    all_text = [[], []]
    for stream in ("logStdout", "logStderr"):
        for record in (before, after):
            verify_artifact(record[stream])
        old, new = [Path(record[stream]["path"]).read_bytes() for record in (before, after)]
        assert new.startswith(old), "Source log truncated/replaced"
        texts.append(new[len(old):].decode())
        all_text[0].append(old.decode())
        all_text[1].append(new.decode())
    derived = postgres_counts("\n".join(texts))
    assert derived == action["counts"]
    for record, text in zip((before, after), all_text):
        assert postgres_counts("\n".join(text)) == {key: record[key] for key in derived}
    assert all(after[key] - before[key] == derived[key] for key in derived)
    assert derived["mutationQueries"] == 0, "Extraction role performed mutation"
    return derived


def verify_native_table(name, image, table):
    assert image["nativeId"] == table["native"]["id"]
    assert image["version"] == table["version"] == table["reopenVersion"]
    assert image["rows"] == table["rows"] == table["reopenRows"]
    assert table["partitionColumns"] == (["region"] if name == "external_partitioned_save_as_table" else [])


def verify_partition_protocol(events, payload_events):
    assert payload_events == len(events) == 3
    initial = [e for e in events if not e["fetch"]]
    continuation = [e for e in events if e["fetch"]]
    assert len(initial) == 2 and len(continuation) == 1
    assert len({e["pid"] for e in initial}) == 2
    lower = [e for e in initial if e["sql"].endswith('WHERE "id" < 3 or "id" is null')]
    upper = [e for e in initial if e["sql"].endswith('WHERE "id" >= 3')]
    assert len(lower) == len(upper) == 1
    fetched = continuation[0]
    assert all(fetched[k] == lower[0][k] for k in ("pid", "portal", "sql"))
    assert events.index(lower[0]) < events.index(fetched)


def partition_protocol(action):
    from pathlib import Path
    from cdf_merge_live_checks import read_ref
    before, after = [read_ref(action[k]) for k in ("postgresBefore", "postgresAfter")]
    events = []
    for stream in ("logStdout", "logStderr"):
        old, new = [Path(v[stream]["path"]).read_bytes() for v in (before, after)]
        assert new.startswith(old)
        for line in new[len(old):].decode().splitlines():
            match = re.search(r'\[(\d+)\] extractor@\S+ LOG:\s+execute (fetch from )?([^:]+): (SELECT .*)', line)
            if match and "source_schema.source" in match[4] and not re.search(r"WHERE\s+1\s*=\s*0", match[4]):
                events.append(dict(pid=int(match[1]), portal=match[3], fetch=bool(match[2]), sql=" ".join(match[4].split())))
    return events


def verify_early_unissued_close(name, message, checkpoint, observation, before, after, counts, identity):
    """Accept only the expected metadata absence after a completed early source denial."""
    assert name == "external_unissued_rejected"
    assert message == "requirement failed: Separately authenticated metadata lookup was not observed"
    assert checkpoint == dict(checkpoint=name+"-append", verified=True)
    assert observation["failure"] and observation["rangerRequests"] == []
    assert any(f["className"] == "java.lang.SecurityException" and
               any("DeltaRangerExternalJdbcSources" in frame and ".resolve(" in frame for frame in f["stack"])
               for f in observation["failure"])
    verify_native(name, before, after)
    assert counts == dict(schemaQueries=2, remoteCountQueries=0, payloadQueries=0, mutationQueries=0)
    assert identity["origin"] == "executing-jvm-before-close" and identity["jvmPid"] > 0 and identity["subject"]
    assert identity["spark"] in ("3.5.4", "3.5.6") and identity["java"] == "17" and identity["scala"] == "2.12.21"


def verify_case(case):
    from pathlib import Path
    from cdf_merge_live_checks import read_ref, file_ref
    verify_summary(case)
    name = case["name"]
    assert name in COMMON + EXTRA
    verify_terminal(case)
    assert case["outcome"] == ("committed" if name in COMMITTED else "denied")
    verify_actions(name, [a["action"] for a in case["actions"]])
    assert read_ref(case["declaredActions"]) == list(action_names(name))
    before, after = [read_ref(case[k]) for k in ("before", "after")]
    verify_native(name, before, after)
    verify_process(case["writerExit"])
    request = read_ref(case["request"])
    assert request["externalJdbc"] is True and not request.get("registeredJdbc", False)
    assert request["caseName"] == name == case["target"]["table"]
    assert case["api"] == ("saveAsTable" if name == "external_partitioned_save_as_table" else "insertInto")
    for action in case["actions"]:
        observed, caller = read_ref(action["observation"]), read_ref(action["caller"])
        assert caller["before"] == caller["after"] and caller["pinnedEnvironment"] == "true"
        assert observed["writerThreadId"] == caller["before"]["jvmThread"]
        assert observed["aqe"] is True and observed["listenerQueuesDrained"] is True
        counts = verify_sql(action)
        if action["action"] == "append":
            verify_denial(name, observed["failure"])
            assert counts["remoteCountQueries"] == 0
            expected = (3 if name == "external_partitioned_save_as_table" else
                        int(name in COMMITTED or name in ("external_scope_closed_before_publish", "external_target_revoked_before_publish")))
            assert counts["payloadQueries"] == expected, "Wrong native payload query count"
            if name == "external_partitioned_save_as_table":
                verify_partition_protocol(partition_protocol(action), counts["payloadQueries"])
            if name in ("external_target_update_denied", "external_named_source_denied"):
                target = dict(case["target"])
                if name == "external_named_source_denied":
                    target["table"] = "source"
                assert any(all(r["resource"].get(k) == v for k, v in target.items()) and r["allowed"] is False
                           and r["accessType"] == ("select" if name == "external_named_source_denied" else "update")
                           for r in observed["rangerRequests"]), "Denial belongs to another resource"
            if name in COMMITTED:
                assert any(c["appendOwner"] and any(n["className"] == "org.apache.spark.sql.execution.RowDataSourceScanExec" for n in c["planNodes"])
                           for c in observed["callbacks"]), "Missing native owned JDBC payload"
        else:
            assert observed["failure"] == []
            assert caller["count"] == (4 if action["action"] == "count_after" else 3)
            assert counts["remoteCountQueries"] == int(action["action"] == "remote_count")
            assert counts["payloadQueries"] == int(action["action"] != "remote_count")
        assert not any(r["resource"].get("catalog") == "jdbc_source" for r in observed["rangerRequests"]), "External input acquired invented Ranger resource"
    for image in (before, after):
        native = read_ref(image["nativeEvidence"])
        table = next(t for t in native["tables"] if t["name"] == name)
        verify_native_table(name, image, table)
        verify_process(file_ref(Path(image["nativeEvidence"]["path"]).parent/"process/exit.json"))
        for ref in image["objects"].values():
            verify_artifact(ref)
    assert all(after["objects"][k]["sha256"] == ref["sha256"] for k,ref in before["objects"].items())
    added = set(after["objects"]) - set(before["objects"])
    assert all("/"+name+"/" in key for key in added)
    if name in ("external_target_update_denied", "external_named_source_denied", "external_unissued_rejected"):
        assert not added
    if "barrier" in case:
        barrier = {k: read_ref(v) for k,v in case["barrier"].items()}
        assert barrier["forwarded"]["status"] == 200 and barrier["forwarded"]["transparent"] is True
        assert barrier["forwarded"]["runId"] == barrier["held"]["runId"]
        assert barrier["held"]["writerThreadId"] == read_ref(case["actions"][0]["caller"])["before"]["jvmThread"]
        residue = {e["key"] for e in barrier["rawHeld"]} - {e["key"] for e in barrier["rawBefore"]}
        assert any(k.endswith(".parquet") for k in residue)
        assert not any(k.endswith("/00000000000000000001.json") for k in residue)
        if name == "external_scope_closed_before_publish":
            assert barrier["release"]["scopeClosed"] is True
            assert barrier["release"]["policy"] == barrier["release"]["initialPolicy"]
        else:
            assert barrier["release"]["updateAbsent"] is True and barrier["release"]["publicRevokeComplete"] is True
    if name == "external_hub_tracker":
        hub = read_ref(case["hubCaller"])
        verify_hub(hub)
        verify_hub_calls(hub["calls"])
        for call in hub["calls"]:
            identity = "tracker" if call["function"] in ("_run", "init_df_tracking") else "main"
            assert call["pythonThread"] == hub["identities"][identity]["python"]
        provenance = read_ref(case["hubProvenance"])
        assert set(hub["modules"]) == set(provenance["importPaths"])
    if name == "external_source_advances":
        advanced = read_ref(case["advancement"])
        assert advanced["rows"] == 4 and read_ref(advanced["command"])["exitCode"] == 0
    if name == "external_mixed_governed":
        warm = read_ref(case["warm"])
        assert warm["freshDataFrames"] is True and warm["targetRows"] == warm["repeatedTargetRows"]
        assert sorted(row[:3] for row in warm["targetRows"]) == sorted(row[:3] for row in after["rows"])
        assert sorted(warm["dependentRows"]) == sorted([row[0], row[2]] for row in after["rows"])
    return True


def local_table(plan, resource, expected_tables):
    assert resource["catalog"] == plan["catalog"] and resource["database"] == plan["database"]
    assert resource["table"] in expected_tables
    return resource["table"]


def fixture_identity(plan):
    """Project only public fixture identities; never digest credential-bearing state."""
    from pathlib import Path
    from urllib.parse import urlsplit
    endpoint = plan["primary"]["endpoint"]
    parsed = urlsplit(endpoint)
    assert parsed.scheme in ("http", "https") and parsed.hostname
    assert parsed.username is None and parsed.password is None and not parsed.query and not parsed.fragment
    assert parsed.path in ("", "/")
    keys = ("catalog", "database", "resourceService", "serverPid", "sourcePolicyIds", "subject", "python", "rangerAppRole", "fixtureDirectory")
    value = {key:plan[key] for key in keys}
    value["primary"] = dict(endpoint=endpoint)
    value["privateStatePath"] = str(Path(plan["fixtureDirectory"])/"state.private.json")
    return value


def load_fixture_identity(result):
    """Rejoin a safe projection to its owner-only original without computing a private digest."""
    from pathlib import Path
    from cdf_merge_live_checks import read_ref
    assert "fixturePlan" not in result, "Credential-state digests are not accepted in new qualification records"
    identity = read_ref(result["fixtureIdentity"])
    private = Path(identity["privateStatePath"])
    assert private.is_file() and not private.is_symlink()
    assert private.stat().st_mode & 0o077 == 0 and private.parent.stat().st_mode & 0o077 == 0
    assert identity == fixture_identity(json.loads(private.read_text()))
    return identity


def _verify_cell(result, inventory):
    """Join loaded original bytes, real services, native state, grants and all actions."""
    from pathlib import Path
    import zipfile
    import hashlib
    from urllib.parse import urlsplit, unquote
    from cdf_merge_live_checks import read_ref
    from cdf_read_live_package import _verify_runtime_artifacts, sha
    from registered_jdbc_checks import verify_python_runtime, verify_timestamp_bounds, timestamp_millis
    from append_aqe_profiles import verify_registration_transition
    from append_aqe_public import verify_public_transition

    verify_summary(result)
    runtime, plan = read_ref(result["runtime"]), load_fixture_identity(result)
    _verify_runtime_artifacts(runtime)
    verify_versions(runtime)
    pair = (result["spark"], result["delta"])
    assert pair == (runtime["spark"], runtime["delta"])
    assert result["caseInventory"] == list(inventory) == [c["name"] for c in result["cases"]]
    if "correction" in result:
        verify_correction(result)
    else:
        verify_process(result["supervisorExit"])
    verify_process(result["fixtureExit"])
    deployed = read_ref(result["deployedRanger"])
    assert deployed["associated"] is True and deployed["installedJarHashesRechecked"]
    assert sha(deployed["rootProof"]) == deployed["rootProofSha256"]
    source = read_ref(result["source"])
    assert source["imageId"] == "sha256:33f923b05f64ca54ac4401c01126a6b92afe839a0aa0a52bc5aeb5cc958e5f20"
    assert source["writeDenied"] is True and "f|t|f|f" in source["acl"] and source["upstreamRole"] == "extractor"
    assert source["driverVersion"] == "42.7.11" and source["driver"] == "org.postgresql.Driver"
    terminal = read_ref(result["sourceTerminal"])
    assert terminal["preservedData"] is True and terminal["state"]["Running"] is False and terminal["state"]["ExitCode"] == 0
    fixture = Path(plan["fixtureDirectory"])
    lifecycle = json.loads((fixture/"lifecycle-exit.json").read_text())
    server_exit = json.loads((fixture/"server/exit-1.json").read_text())
    assert lifecycle["stopRequested"] is True and lifecycle["failed"] is False
    assert lifecycle["serverPid"] == plan["serverPid"] == server_exit["pid"]
    assert lifecycle["serverExit"] == server_exit["exit"] == 143
    logs = (fixture/"server/class-load-1.log").read_text()
    assembly = json.loads(Path(runtime["assemblyManifest"]).read_text())
    for name in ("org.apache.gravitino.catalog.lakehouse.delta.DeltaCatalog", "org.apache.gravitino.authorization.ranger.delta.RangerDeltaAuthorizationPlugin"):
        origins = {line.split(" source: ")[1] for line in logs.splitlines() if " "+name+" source: " in line}
        assert len(origins) == 1, "Missing real server provider"
        actual = Path(unquote(urlsplit(origins.pop()).path))
        original = Path(runtime["assemblyManifest"]).parent/"server"/actual.relative_to(fixture/"server")
        assert sha(actual) == sha(original) == assembly["artifactFiles"][str(original)]
    verify_artifact(result["hubSource"])
    with zipfile.ZipFile(result["hubSource"]["path"]) as archive:
        hub_sources = {n: hashlib.sha256(archive.read(n)).hexdigest() for n in archive.namelist()}
    for case in result["cases"]:
        verify_case(case)
        jvm = Path(case["origins"]["path"]).parent
        if case["name"] == "external_unissued_rejected":
            witness = read_ref(case["metadataAbsentClose"])
            observed = read_ref(case["runtimeObserved"])
            assert witness["className"] == "pyspark.errors.exceptions.captured.IllegalArgumentException"
            action = case["actions"][0]
            verify_early_unissued_close(case["name"], witness["message"], read_ref(witness["checkpoint"]),
                                       read_ref(action["observation"]), read_ref(case["before"]), read_ref(case["after"]),
                                       verify_sql(action), observed)
            assert observed["jvmPid"] == read_ref(case["writerExit"])["pid"]
            assert witness["runtimeObserved"] == case["runtimeObserved"]
        else:
            observed = json.loads((jvm/"observed.json").read_text())
        verify_identities(dict(spark=observed["spark"], delta=runtime["delta"], server=deployed["deployedRangerVersion"], client="2.8.0", java=observed["java"], scala=observed["scala"]))
        assert observed["spark"] == result["spark"] and observed["subject"] == plan["subject"]
        verify_python_runtime(read_ref(case["pythonRuntime"]), runtime, plan)
        facade = read_ref(case["facade"])
        assert facade["container"]["sha256"] == runtime["pythonZipSha256"]
        assert facade["sha256"] == runtime["pythonZipMembers"][facade["member"]]
        origins = read_ref(case["origins"])
        assert set(origins) == set(runtime["providers"])
        for name, origin in origins.items():
            assert origin["jar"] == runtime["providers"][name]
            assert origin["jarSha256"] == runtime["jarHashes"][origin["jar"]] and origin["classSha256"] == runtime["providerClasses"][name]
        applied = json.loads((jvm/"initial-applied.json").read_text())
        state, downloaded, roles = [read_ref(applied[k]) for k in ("rangerState", "download", "roleDownload")]
        stable = lambda v: {k:i for k,i in v.items() if k not in ("policyUpdateTime", "roleUpdateTime")}
        assert applied["sameJvm"] is True and stable(downloaded) == stable(state["policies"]) and stable(roles) == stable(state["roles"])
        assert downloaded["serviceName"] == roles["serviceName"] == plan["resourceService"]
        assert set(plan["sourcePolicyIds"]) <= {p["id"] for p in downloaded["policies"]}
        for action in case["actions"]:
            observation = read_ref(action["observation"])
            assert observation["subject"] == plan["subject"] and all(r["user"] == plan["subject"] for r in observation["rangerRequests"])
        before, after = [read_ref(case[k]) for k in ("before", "after")]
        verify_registration_transition(read_ref(before["registrations"]), read_ref(after["registrations"]), case["name"], before["version"], after["version"])
        if case["name"] in COMMITTED:
            process = json.loads(Path(case["writerExit"]["path"]).with_name("process.json").read_text())
            verify_timestamp_bounds(after["rows"], observed["jvmTimezone"], process["started"], read_ref(case["writerExit"])["finished"])
            if "warm" in case:
                expected = [row[:3]+[timestamp_millis(v, observed["jvmTimezone"]) for v in row[3:]] for row in after["rows"]]
                assert sorted(read_ref(case["warm"])["targetRows"]) == sorted(expected)
        if "hubProvenance" in case:
            provenance = read_ref(case["hubProvenance"])
            verify_dependencies(provenance["dependencies"])
            assert provenance["dependencies"]["kafka-python"]["version"] == "2.0.2"
            assert provenance["sourceInputs"] and all(hub_sources[k] == v for k,v in provenance["sourceInputs"].items())
            assert all(path.startswith(result["hubSource"]["path"]+"/") for path in provenance["importPaths"].values())
        for reference in case["transitions"]:
            change = read_ref(reference)
            request = read_ref(change["publicRequest"])
            full = request["path"].split("/table/")[1].rsplit("/", 2)[0]
            catalog, schema, table = unquote(full).split(".")
            descriptor = dict(catalog=catalog, database=schema, table=table)
            states = [dict(public=json.loads(Path(change[side]["publicStateFile"]).read_text()), **json.loads(Path(change[side]["rangerStateFile"]).read_text())) for side in ("before", "after")]
            verify_public_transition(plan, local_table(plan, descriptor, (case["name"], "source")), change["privilege"], change["grant"], *states, read_ref(change["callbacks"]))
    return True


def verify_correction(result):
    """Validate explicitly retained checker/setup failures without replaying targets."""
    from pathlib import Path
    from cdf_merge_live_checks import read_ref
    correction = read_ref(result["correction"])
    expected_failure = {
        "external-jdbc-partition-field-read-only-v1": (["external_insert_into"], "KeyError: 'partitionColumns'"),
        "external-jdbc-cursor-fetch-read-only-v1": (["external_partitioned_save_as_table"], "AssertionError: Wrong native payload query count"),
        "external-jdbc-following-hub-import-failure-v1": (["external_target_update_denied"], "AssertionError: Original process failed before required evidence; no replay"),
    }
    expected_failure["external-jdbc-following-barrier-arm-failure-v1"] = (["external_hub_tracker", "external_mixed_governed", "external_named_source_denied"], "AssertionError: Original process failed before required evidence; no replay")
    expected_failure["external-jdbc-following-unissued-close-failure-v1"] = (["external_scope_closed_before_publish", "external_target_revoked_before_publish"], "AssertionError")
    assert correction["scope"] in expected_failure
    expected_cases, expected_error = expected_failure[correction["scope"]]
    assert result["caseInventory"] == expected_cases
    assert correction["originalSupervisor"] == result["supervisorExit"]
    terminal = read_ref(result["supervisorExit"])
    assert terminal["exitCode"] == 1 and terminal["timedOut"] is False
    log = correction["originalFailureLog"]
    verify_artifact(log)
    assert Path(log["path"]) == Path(result["supervisorExit"]["path"]).with_name("process.private.log")
    trace = Path(log["path"]).read_text()
    assert trace.rstrip().endswith(expected_error)
    if correction["scope"] == "external-jdbc-following-hub-import-failure-v1":
        failed = read_ref(correction["failedWriter"])
        assert failed["exitCode"] == 1 and failed["timedOut"] is False
        original = read_ref(correction["failedPython"])
        assert original["noReplay"] is True
        assert any(t.rstrip().endswith("ModuleNotFoundError: No module named 'kafka'") for t in original["trace"])
        native = read_ref(correction["failedNative"])
        assert native["nativeTable"] == "external_hub_tracker" and native["version"] == 0 and len(native["rows"]) == 1
        verify_sql(dict(postgresBefore=correction["failedPostgresBefore"], postgresAfter=correction["failedPostgresAfter"],
                        counts=dict(schemaQueries=0, remoteCountQueries=0, payloadQueries=0, mutationQueries=0)))
        marker = read_ref(correction["completedMarker"])
        assert marker["case"] == expected_cases[0] and marker["finished"] < terminal["finished"]
    elif correction["scope"] == "external-jdbc-following-barrier-arm-failure-v1":
        failed = read_ref(correction["failedWriter"])
        assert failed["exitCode"] == 1 and failed["timedOut"] is False
        original = read_ref(correction["failedPython"])
        assert original["noReplay"] is True
        assert any(t.rstrip().endswith("pyspark.errors.exceptions.captured.IllegalArgumentException: requirement failed: Invalid barrier arm") for t in original["trace"])
        native = read_ref(correction["failedNative"])
        assert native["nativeTable"] == "external_scope_closed_before_publish" and native["version"] == 0 and len(native["rows"]) == 1
        verify_sql(dict(postgresBefore=correction["failedPostgresBefore"], postgresAfter=correction["failedPostgresAfter"],
                        counts=dict(schemaQueries=2, remoteCountQueries=0, payloadQueries=0, mutationQueries=0)))
        markers = [read_ref(ref) for ref in correction["completedMarkers"]]
        assert [m["case"] for m in markers] == expected_cases
        assert all(m["finished"] < terminal["finished"] for m in markers)
    elif correction["scope"] == "external-jdbc-following-unissued-close-failure-v1":
        failed = read_ref(correction["failedWriter"])
        assert failed["exitCode"] == 1 and failed["timedOut"] is False
        log = correction["failedWriterLog"]
        verify_artifact(log)
        assert Path(log["path"]) == Path(correction["failedWriter"]["path"]).with_name("process.private.log")
        assert Path(log["path"]).read_text().rstrip().endswith("pyspark.errors.exceptions.captured.IllegalArgumentException: requirement failed: Separately authenticated metadata lookup was not observed")
        observed = read_ref(correction["failedObservation"])
        verify_denial("external_unissued_rejected", observed["failure"])
        assert observed["rangerRequests"] == []
        native = read_ref(correction["failedNative"])
        assert native["nativeTable"] == "external_unissued_rejected" and native["version"] == 0 and len(native["rows"]) == 1
        assert read_ref(correction["failedDeclaredActions"]) == ["append"]
        verify_sql(dict(postgresBefore=correction["failedPostgresBefore"], postgresAfter=correction["failedPostgresAfter"],
                        counts=dict(schemaQueries=2, remoteCountQueries=0, payloadQueries=0, mutationQueries=0)))
        markers = [read_ref(ref) for ref in correction["completedMarkers"]]
        assert [m["case"] for m in markers] == expected_cases
        assert all(m["finished"] < terminal["finished"] for m in markers)
    else:
        assert "external_jdbc_checks.py" in trace
    verify_artifact(correction["checkerSnapshot"])
    verify_artifact(correction["verifier"])
    verify_process(correction["verificationExit"])
    command = json.loads(Path(correction["verificationExit"]["path"]).with_name("command.json").read_text())
    assert command["argv"][1] == correction["verifier"]["path"]
    assert [read_ref(ref) for ref in correction["originalCases"]] == result["cases"]
    assert correction["noReplay"] is True


def verify_cell(result):
    """A standalone cell must contain every assigned case for its pinned pair."""
    return _verify_cell(result, case_names((result["spark"], result["delta"])))


def verify_segment(result):
    """Validate one immutable fixture segment; only the matrix can qualify a pair."""
    from pathlib import Path
    from cdf_merge_live_checks import read_ref
    from cdf_read_live_package import canonical_digest
    if "originalSegmentPath" in result:
        original = json.loads(Path(result["originalSegmentPath"]).read_text())
        private = original.pop("fixturePlan")
        identity = load_fixture_identity(result)
        assert identity["privateStatePath"] == private["path"]
        projected = {k:v for k,v in result.items() if k not in ("fixtureIdentity", "originalSegmentPath", "projectionKind")}
        assert projected == original and result["projectionKind"] == "external-fixture-nonsecret-v1"
    names = result["caseInventory"]
    assert names and len(set(names)) == len(names)
    assert set(names) <= set(case_names((result["spark"], result["delta"])))
    assert [read_ref(ref) for ref in result["originalCases"]] == result["cases"]
    _verify_cell(result, names)
    readiness = read_ref(result["serviceReadiness"])
    capacity = readiness.get("capacity", dict(minimumAvailableBytes=8*1024**3, profile="default-8GiB"))
    assert readiness["availableBytes"] > capacity["minimumAvailableBytes"]
    if capacity["profile"] == "task4-final-two-pairs-5GiB":
        assert capacity["pair"] == [result["spark"], result["delta"]]
        assert tuple(capacity["pair"]) in (("3.5.4", "3.3.2"), ("3.5.6", "3.3.0"))
        decision = read_ref(capacity["decision"])
        assert decision["minimumAvailableBytes"] == capacity["minimumAvailableBytes"] == 5*1024**3
        assert decision["scope"] == "task4-final-two-pairs-v1" and decision["preserveDefaultBytes"] == 8*1024**3
        verify_artifact(decision["measurement"])
    else:
        assert capacity["profile"] == "default-8GiB" and capacity["minimumAvailableBytes"] == 8*1024**3
    minio = readiness["identities"]["delta-live-20261010-minio-1"]
    assert minio["state"]["Running"] is True and minio["id"] and minio["image"]
    assert readiness["minioVersion"].startswith("minio version RELEASE.")
    from urllib.parse import urlsplit
    plan = load_fixture_identity(result)
    endpoint = urlsplit(plan["primary"]["endpoint"])
    assert endpoint.hostname in ("localhost", "127.0.0.1")
    assert any(int(b["HostPort"]) == endpoint.port for b in minio["ports"]["9000/tcp"])
    volume = read_ref(result["sourceVolume"])
    terminal = read_ref(result["sourceTerminal"])
    assert volume["name"] == terminal["container"]+"-data" and volume["owner"] == "external-jdbc-task4" and volume["preserveData"] is True
    runtime = read_ref(result["runtime"])
    product = dict(jars={Path(p).name: d for p,d in runtime["jarHashes"].items()}, python=runtime["pythonZipSha256"])
    return dict(spark=result["spark"], delta=result["delta"], cases=names,
                actions={c["name"]:[a["action"] for a in c["actions"]] for c in result["cases"]},
                producer=canonical_digest(product), hub=result["hubSource"]["sha256"], verified=True)


def verify_segment_inventory(segments):
    """Require precisely four pinned pairs/nineteen cases across verified segments."""
    assert segments and all(s["verified"] is True for s in segments)
    expected = Counter((s,d,name) for s,d in PAIRS for name in case_names((s,d)))
    actual = Counter((v["spark"],v["delta"],name) for v in segments for name in v["cases"])
    assert actual == expected, "Missing, extra or repeated matrix case"
    for pair in PAIRS:
        selected = [s for s in segments if (s["spark"],s["delta"]) == pair]
        assert len({s["producer"] for s in selected}) == 1, "Mixed producing bytes for pair"
        assert len({s["hub"] for s in selected}) == 1
    assert len({s["hub"] for s in segments}) == 1, "Different Hub producer in matrix"
    for segment in segments:
        assert set(segment["actions"]) == set(segment["cases"])
        for name, actions in segment["actions"].items():
            verify_actions(name, actions)


def verify_matrix(references):
    from cdf_merge_live_checks import read_ref
    segments = [verify_segment(read_ref(ref)) for ref in references]
    verify_segment_inventory(segments)
    return dict(pairs=len(PAIRS), cases=sum(len(s["cases"]) for s in segments),
                actions=sum(len(a) for s in segments for a in s["actions"].values()))

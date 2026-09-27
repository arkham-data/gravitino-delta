# Delta Retained Query Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Root reviews this plan before dispatch and retains source handoff, build, evidence, review, Git and live acceptance authority.

**Goal:** Extend the opt-in test application to retain one named Delta Dataset across downloaded-policy changes and record its actual collection outcome, without running a live service in this task.

**Architecture:** Preserve legacy fresh SQL and `{close:true}` commands. Add a strict retained-command parser, finite driver-local Dataset/root holder, one action executor shared by driver and real native fixture, and a downloader-only gateway metadata mode that binds completed response generations to applied engine generations. Keep evidence immutable and private.

**Tech Stack:** Java test application/JUnit 5, Scala native fixture, Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2, Scala 2.12, Ranger 2.8.0, JDK 17, Gradle Spotless/Javadoc.

**Spec:** `docs/superpowers/plans/2026-09-26-delta-retained-query-harness-design.md`

## Global Constraints

- Change only the optional `spark-delta-ranger-3.5` **test** application/tests. No production, core/API/server/client/storage-schema/default connector source, global registry or test hook.
- No external Ranger, Gravitino, MinIO, Docker, live deployment, mutation or private credential is needed for this implementation. Local fixture passes are not live downloaded-policy acceptance.
- Keep the existing absent-`operation` fresh command/result contract and exact `{close:true}` shutdown. Old 208 checks and frozen September 21 runner/verifier stay historical and untouched.
- Pin Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2, Scala 2.12, Ranger 2.8.0 and JDK 17. Use normal Java imports, strict Javadoc for public/protected declarations and Google Java style.
- Root owns later coordinator/live work, packaging acceptance, independent review and Git checkpoint. No outage fault support in this task.
- At most four active retained Datasets and 4096 retained commands per driver. Use exact Java reference comparison and private 0700/0600 evidence; no credential, header, downloaded body or policy model in a new result.

## File structure and interfaces

All paths below start at `spark-connector/v3.5/spark-delta-ranger/src/test/` and use package `org/apache/gravitino/spark/connector/delta/ranger/`.

| File | Responsibility and exact interface |
| --- | --- |
| `java/org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerDownloadedCommand.java` (new) | `static Parsed parse(String line, Ledger ledger)` with `Kind {FRESH, PREPARE, COLLECT, RELEASE, CLOSE}` and strict Jackson duplicate/trailing-token rejection. `Parsed.kind(): Kind`; `id(): @Nullable String` is null only for CLOSE; `retentionId(): String` applies to retained kinds; `sql(): String` to FRESH/PREPARE; `action(): String`, `expectedRows(): Optional<JsonNode>`, `deniedAtEntry(): boolean`, `expectedFailureContains(): Optional<String>`, `requiredDownloads(): DownloadRequirement` to COLLECT; `minPolicy(): long`, `minRole(): long` to FRESH/PREPARE/COLLECT. An accessor on an inapplicable kind throws `IllegalStateException`. `Ledger` reserves IDs and legacy output basenames. |
| `java/org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerRetainedQueries.java` (new) | `DeltaRangerRetainedQueries(SparkSession spark)`, `Slot prepare(String retentionId, String sql, long cursor, DeltaRangerPolicyEngine.Versions applied)`, `Slot require(String retentionId)`, `void release(String retentionId)`, `void close()`. `Slot` exposes `Dataset<Row> dataset()`, `SparkPlan root()`, `long cursor()`, `Versions versions()` and `void advance(long cursor, Versions applied)`. |
| `java/org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerRetainedAction.java` (new) | `static Result execute(SparkSession spark, DeltaRangerRetainedQueries.Slot slot, DeltaRangerDownloadedCommand.Parsed command)`; immutable `Result` has `boolean passed()`, `Map<String,Object> evidence()`, `@Nullable Throwable failure()`. Driver and native fixture call this one path. No download or slot advancement here. |
| `java/org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerCallbackGateway.java` (modify) | Add `DeltaRangerCallbackGateway(URI upstream, Path evidence, String downloadService)` used only by downloader; for exact secure GET HTTP 200 record decoded service/version and SHA-256 of forwarded response bytes. Existing constructors/forwarding stay unchanged. |
| `java/org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerDownloadedSecurity.java` (modify) | Nested `DownloadRequirement {NONE,POLICY,ROLE,BOTH}`, `long completedCursor() throws IOException, InterruptedException`, `DownloadEvidence completedAfter(long exclusiveCursor, DownloadRequirement effective, DeltaRangerPolicyEngine.Versions applied) throws IOException, InterruptedException`. `DownloadEvidence` has `Optional<CompletedDownload> policy()`, `Optional<CompletedDownload> role()`, `long cursor()`; `CompletedDownload` has `long attempt()`, `long version()`, `String responseSha256()`. Existing `recordInitialDownloads` also binds decoded initial response versions to passed applied versions. |
| `java/org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerDownloadedQuery.java` (modify) | Parse/dispatch commands, call shared executor, qualify actual downloads/versions independently, write immutable numbered result and pass marker, close slots in `finally`; retain current fresh path and terminal close behavior. |
| `java/org/apache/gravitino/spark/connector/delta/ranger/TestDeltaRangerDownloadedQuery.java`, `TestDeltaRangerDownloadedSecurity.java`, `TestDeltaRangerCallbackGateway.java` (modify) | Parser/close/path/evidence tests, wrong-version and malformed download tests, scoped response metadata tests. |
| `scala/org/apache/gravitino/spark/connector/delta/ranger/TestDeltaRangerRetainedQuery.scala` (new) | Actual named Delta native fixture calls `DeltaRangerRetainedAction.execute` for unchanged repeats and before-first/after-success decision and role drift. |

## Review Focus

- `{close:true}` with no other field must shut down and release slots; duplicate `close`, extra field or `close:false` must fail before an action. Parser/lifecycle tests own this.
- Unsafe, duplicate or filename-colliding IDs and reused retention IDs must fail without evidence-file overwrite or Dataset recreation. Parser/holder tests own this.
- Wrong-service, missing, fractional, duplicate-key or wrong-generation HTTP 200 and HTTP 304 must never qualify an applied publication. Gateway/security tests own this.
- A fixture that directly calls `frame.collect` could pass without exercising driver outcome/metrics code. Native test must call the exact shared executor; source review confirms driver invokes it.
- Expected entry denial advances a slot only after immutable result write; unexpected action/evidence failure does not advance and cleanup clears slots. Action/driver tests own this.

---


### Task 1: Test-only retained collection protocol

**Files:** exactly the ten test-source files in File structure. The accepted Python runner/verifier and all production source are excluded.

**Interfaces:** consume current `DeltaRangerDownloadedQuery.main(String[])`, `DeltaRangerDownloadedSecurity.recordInitialDownloads`, engine `versions()` and `NativeSelectFixture.withBoundSpark`; produce the exact `prepare`/`collect`/`release` commands, `{close:true}` continuity and private `retained-events/NNNNNN.json` plus `NNNNNN-passed.json` contract from the spec. The public coordinator remains a later live mutator.

- [ ] **Step 1: Write parser/close RED.** Extend `TestDeltaRangerDownloadedQuery` to parse accepted fresh positive/denied/hidden commands and exact `{close:true}`, then reject duplicate JSON properties, trailing tokens, `close:false`, close-plus-extra-field, null/unknown fields, missing floors, unsafe/reserved IDs, `x`/`x-passed` legacy output collision and unsupported retained action/outcome. Assert a parsed CLOSE has no command ID and cannot reserve or create a retention slot. Test shape:

  ```java
  DeltaRangerDownloadedCommand.Ledger ledger = new DeltaRangerDownloadedCommand.Ledger();
  assertEquals(DeltaRangerDownloadedCommand.Kind.CLOSE,
      DeltaRangerDownloadedCommand.parse("{\"close\":true}", ledger).kind());
  assertThrows(IllegalArgumentException.class,
      () -> DeltaRangerDownloadedCommand.parse("{\"close\":true,\"id\":\"x\"}", ledger));
  assertEquals(DeltaRangerDownloadedCommand.Kind.PREPARE,
      DeltaRangerDownloadedCommand.parse("{\"id\":\"p1\",\"operation\":\"prepare\",\"retentionId\":\"held\",\"sql\":\"SELECT secret FROM a.db.t\",\"minPolicy\":1,\"minRole\":1}", ledger).kind());
  ```

- [ ] **Step 2: Run parser RED on first pin.** Run `./gradlew :spark-connector:spark-delta-ranger-3.5:test --tests '*TestDeltaRangerDownloadedQuery' -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.0 -PskipITs -PskipDockerTests=true`. Save original exit/XML or compiler failure. Expected RED: parser absent.

- [ ] **Step 3: Implement parser and verify GREEN.** Create `DeltaRangerDownloadedCommand.java` using a Jackson mapper with `JsonParser.Feature.STRICT_DUPLICATE_DETECTION` and `DeserializationFeature.FAIL_ON_TRAILING_TOKENS`. Accept only exact close shape, then exact legacy fresh shapes (`id/sql/minPolicy/minRole` plus one typed outcome), and exact new shapes in the spec. Validate safe `[A-Za-z0-9_-]{1,64}` IDs, deny reserved basenames and names ending `-passed`, reserve both legacy result filenames atomically after validation, reject duplicate IDs. Parse `requiredDownloads` as the four enum values, not arbitrary text. Run Step 2 to GREEN.

  ```java
  if (node.has("close")) {
    if (node.size() != 1 || !node.path("close").isBoolean()
        || !node.path("close").booleanValue()) {
      throw new IllegalArgumentException("Only exact close:true is a shutdown command");
    }
    return Parsed.close();
  }
  ```

- [ ] **Step 4: Write gateway/download RED with real minimal response shapes.** Extend `TestDeltaRangerCallbackGateway` and `TestDeltaRangerDownloadedSecurity` in-process HTTP tests to use policy `{"serviceName":"owned","policyVersion":7}` and role `{"serviceName":"owned","roleVersion":9}` bytes for the existing initial-download assertions. In new generation-matching tests use `TestDeltaRangerNativeSelect.Fixture.engine.versions()` as the actual applied `Versions` object (its constructor is private), and server response generation `1` for matching or `2` for wrong-generation controls. Require an exact-service secure GET, original forwarded bytes/status, completed upstream/downstream HTTP 200, response SHA-256 and decoded integral generation, with no body/header/credential in event JSON. Then exercise wrong service, missing/fractional/nonintegral/out-of-range/duplicate version, malformed JSON, 304, incomplete completion and wrong-generation 200 versus applied `Versions`; each must fail qualification. A role-only response with matching applied role generation must work without inventing a policy download. Run both test selectors on first pin; expect RED before recorder implementation.

  ```java
  try (TestDeltaRangerNativeSelect.Fixture fixture = new TestDeltaRangerNativeSelect.Fixture()) {
    DeltaRangerPolicyEngine.Versions appliedVersions = fixture.engine.versions();
    long cursor = security.completedCursor();
    request(security.uri(),
        RangerRESTUtils.REST_URL_SERVICE_SERCURE_GET_USER_GROUP_ROLES + "owned");
    assertTrue(security.awaitDrained());
    assertEquals(1L, security.completedAfter(cursor,
        DeltaRangerDownloadedSecurity.DownloadRequirement.ROLE,
        appliedVersions).role().orElseThrow().version());
  }
  ```

- [ ] **Step 5: Implement scoped response metadata and generation-bound selection.** Add the downloader-only gateway constructor. At exact secure GET path with HTTP 200 parse `response.body()` in memory with strict duplicate/trailing checks, exact `serviceName`, integral nonnegative 64-bit version, and store only `downloadKind`, `downloadService`, `downloadVersion`, `responseSha256`; record sanitized invalid-metadata reason when parsing fails. Forward bytes/status unchanged and keep existing coordinator constructors untouched. In `DeltaRangerDownloadedSecurity`, use the new constructor; `completedAfter` chooses the **latest completed HTTP 200 per required kind after the cursor** and requires its exact path/service/decoded version to equal `applied.policy()` or `applied.role()`. A later malformed/wrong-generation 200 invalidates that kind; no earlier matching response is cherry-picked. Reject 304, invalid metadata, missing kind or mismatched generation. `NONE` returns empty optionals and no refresh qualification. Its returned cursor, for every requirement including `NONE`, is the highest completed attempt after drain, so an accepted action cannot reuse incidental earlier downloads. Make `recordInitialDownloads` require the decoded initial policy/role versions to match its passed engine versions while keeping the existing result schema; update its in-process fixture to valid minimal Ranger response shapes. Run Step 4 selectors to GREEN.

- [ ] **Step 6: Write holder/shared-action native RED.** Create `TestDeltaRangerRetainedQuery.scala` using `NativeSelectFixture.withBoundSpark` and actual named `a.db.t`. Drive the new `DeltaRangerRetainedAction.execute`, never direct `frame.collect`, through a parsed collect command. Test one retained Dataset/root unchanged `collect`, `collectAsList`, repeat; separate before-first and after-success `policy.denied.add("a:secret")` cases; role publication drift as in `TestDeltaRangerInstalledStaleness.scala:52-59`. Assert result map's exact rows or entry-denial, reference booleans, job IDs and zero named user-scan metrics on denial. Add four-slot cap, released/reused/missing ID and close-clears-state assertions. Run `./gradlew :spark-connector:spark-delta-ranger-3.5:test --tests '*TestDeltaRangerRetainedQuery' -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.0 -PskipITs -PskipDockerTests=true`; preserve expected missing-holder/executor RED.

  ```scala
  NativeSelectFixture.withBoundSpark { (spark, policy) =>
    val held = new DeltaRangerRetainedQueries(spark)
    val slot = held.prepare("one", "SELECT secret FROM a.db.t WHERE id = 1", 0L,
      policy.engine.versions())
    val command = DeltaRangerDownloadedCommand.parse(
      "{\"id\":\"c1\",\"operation\":\"collect\",\"retentionId\":\"one\",\"action\":\"collect\",\"expectedRows\":[[\"a-one-masked\"]],\"minPolicy\":1,\"minRole\":1,\"requiredDownloads\":\"none\"}",
      new DeltaRangerDownloadedCommand.Ledger())
    val result = DeltaRangerRetainedAction.execute(spark, slot, command)
    assertTrue(result.passed())
    assertEquals(true, result.evidence().get("sameRootAfter"))
    held.close()
  }
  ```

- [ ] **Step 7: Implement finite holder and shared executor; run native GREEN.** Add `DeltaRangerRetainedQueries.java` with exact constructor/methods in File structure and used-ID set. `prepare` calls `spark.sql(sql)` and captures `queryExecution().executedPlan()` once; return the same `Slot` and Java references on every `require`. Add `DeltaRangerRetainedAction.java`: verify command is COLLECT, use held Dataset for action, capture reference identity before/after, compare normalized rows or exact SecurityException with `DeltaRangerCollectGuardExec.executeCollect` but no `doExecute`, and return immutable `Result` even for unexpected action failure. Record action-specific job IDs and a **nonempty** list of named user-scan `numOutputRows`; zero user-scan rows on expected entry denial, without assuming all native metadata jobs are user work. It does not advance the slot. Run Step 6 selector until GREEN.

  ```java
  Dataset<Row> heldDataset = slot.dataset();
  SparkPlan heldRoot = slot.root();
  Row[] rows = command.action().equals("collect") ? heldDataset.collect()
      : heldDataset.collectAsList().toArray(new Row[0]);
  boolean sameDataset = heldDataset == slot.dataset();
  boolean sameRoot = heldRoot == heldDataset.queryExecution().executedPlan();
  ```

- [ ] **Step 8: Write driver/evidence RED.** Extend `TestDeltaRangerDownloadedQuery` to assert a 0700 `retained-events` directory, immutable 0600 `000001.json` and `000001-passed.json`, the exact PREPARE/COLLECT/RELEASE envelopes and pass marker from the spec, no complete pass marker for `Result.passed()==false`, 4097th command rejection before writing, and no secret-bearing fields. Test parser CLOSE acceptance, rejected close extras and malformed input together with holder `close()` clearing all slots; inspect the driver `finally` path to confirm it calls that same `close()` before Spark/recorder shutdown. Test `NONE` requires unchanged baseline generations, advances the attempt cursor past incidental completed requests and is labeled no refresh; POLICY/ROLE/BOTH require indicated advance and response-version match, with optional other-generation advance handled by requiring its corresponding download. Preserve old fresh `isSecurityFailure` behavior. Run first-pin query tests RED.

- [ ] **Step 9: Wire driver dispatch and state advancement; run driver GREEN.** Parse raw line once; CLOSE breaks and finally clears slots before Spark/recorder close; FRESH retains current refresh/await/new-SQL/result path. PREPARE refreshes/awaits, stores applied `Versions`, `completedCursor()` and exact Dataset/root. COLLECT refreshes/awaits, compares current applied versions with `slot.versions()`, derives an effective download requirement as the union of requested type and *every* generation that advanced, and calls `completedAfter(slot.cursor(), effective, applied)`. A requested type with no corresponding advance fails; `NONE` requires no advance and records no refresh qualification. Invoke the shared executor, write the exact COLLECT envelope from the spec with selected attempts/digests and applied versions. Use a private `versionRecord(Versions): Map<String,Object>` to emit only `service/definition/policy/role` and a private `downloadRecord(DownloadEvidence, DownloadRequirement requested, DownloadRequirement effective, long cursorBefore): Map<String,Object>` to emit exactly `requested/effective/cursorBefore/cursorAfter/policy/role`, with each nonnull download holding only attempt/version/digest. On `Result.passed()==true`, write the immutable result and complete pass marker, then advance the cursor/versions before accepting another command. `Slot.advance` performs only a prevalidated in-memory assignment. On false or any evidence-write failure, no advance/complete pass marker; propagate failure and clear slots in `finally`. RELEASE removes slot. Preserve terminal `completed.json` and `failure.json` and exact legacy filenames. The driver call is:

  ```java
  DeltaRangerRetainedAction.Result action =
      DeltaRangerRetainedAction.execute(spark, slot, command);
  Map<String, Object> record = new LinkedHashMap<>();
  record.put("schemaVersion", 1);
  record.put("sequence", sequence);
  record.put("id", command.id());
  record.put("operation", "collect");
  record.put("retentionId", command.retentionId());
  record.put("versionsBefore", versionRecord(slot.versions()));
  record.put("versionsAfter", versionRecord(applied));
  record.put("downloads", downloadRecord(downloads, command.requiredDownloads(),
      effective, slot.cursor()));
  record.put("refreshQualified", effective != DeltaRangerDownloadedSecurity.DownloadRequirement.NONE);
  record.put("actionResult", action.evidence());
  write(retainedEvents.resolve(String.format("%06d.json", sequence)), record);
  if (!action.passed()) {
    throw new AssertionError("Retained collection failed; private result preserved", action.failure());
  }
  Map<String, Object> marker = new LinkedHashMap<>();
  marker.put("schemaVersion", 1);
  marker.put("sequence", sequence);
  marker.put("id", command.id());
  marker.put("retentionId", command.retentionId());
  marker.put("passed", true);
  write(retainedEvents.resolve(String.format("%06d-passed.json", sequence)), marker);
  slot.advance(downloads.cursor(), applied);
  ```

- [ ] **Step 10: Verify the four pinned profiles and package exclusion.** Run focused `'*TestDeltaRangerDownloadedQuery'`, `'*TestDeltaRangerDownloadedSecurity'`, `'*TestDeltaRangerCallbackGateway'`, `'*TestDeltaRangerRetainedQuery'` selectors once per pair with `:spark-connector:spark-delta-ranger-3.5:test -PskipITs -PskipDockerTests=true` and respectively `-PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.0`, `3.5.4/3.3.2`, `3.5.6/3.3.0`, `3.5.6/3.3.2`. Preserve original XML/exits and compare testcase inventories. Run `./gradlew :spark-connector:spark-delta-ranger-3.5:spotlessCheck :spark-connector:spark-delta-ranger-3.5:javadoc -PskipITs -PskipDockerTests=true`. Under root's Gradle ownership/handoff build the optional companion with `./gradlew :spark-connector:spark-delta-ranger-runtime-3.5:shadowJar -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.0`; inspect its actual JAR entries and require all new/modified test application and fixture classes absent. The test application archive stays separate; no external endpoint flag is set.

- [ ] **Step 11: Report and root review gate.** Author reports exact file list, parsed original XML/exits, four-pair testcase multiset, hashes/pins, Javadoc/format, private output redaction, companion exclusion and explicit offline limits. Root checks scope, reviews close/parser, response-version binding, shared action and failure advancement, then owns any checkpoint commit and later fresh fixture continuity/live orchestration. Any correction requires a new complete focused four-pair matrix; old September 21 evidence is never relabeled.

## Self-review result

Every design contract maps to a step: close and legacy preservation (1-3,8-9); exact secure response service/version/digest and applied-generation matching (4-5,8-9); finite holder and a single driver/native action path (6-7,9); immutable result/failure/cursor lifecycle (8-9); all four pins and package exclusion (10); root review (11). Review Focus tests cover the five highest-risk inputs. All return types and method names are declared above. No production or live work is part of this task.

## Controller execution handoff

Accepted BASE: 55d8b746a2f31c1b185684e42c6c43ef8b1372b7. Root owns all Git mutations/checkpoints and live services. You own only the ten listed test-source files and exclusive Gradle use until handoff. No subagents. No commits/pushes or live endpoint flags. Preserve all original failures and final evidence in this plan workspace/evidence, never overwrite a completed run. Prefer offline Gradle with JDK17 as available locally. Root will coordinate packaging acceptance; you may build the optional companion after test validation while you own Gradle.

The full design is the binding contract and should be read alongside this brief. Fresh protocol uses existing id/sql/minPolicy/minRole plus expectedRows, denied:true, or hidden:true and catalog; add sensible package-private accessors for that existing shape rather than changing it. Runtime fixture and prior evidence are not implementation inputs. No source freeze changes while final focused four-pair qualification runs. Preserve final source hashes, original exits/XML, test case inventories and compiled artifacts per pair. Source/archive manifests should include all module sources/build wiring needed to bind the result, but do not copy private runtime/credentials. Report file: task-1-report.md next to this brief. Return DONE/DONE_WITH_CONCERNS/NEEDS_CONTEXT/BLOCKED, concise test totals, concerns and report path.

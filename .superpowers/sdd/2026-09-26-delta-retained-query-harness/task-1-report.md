# Task 1 report — retained downloaded-policy query harness

Status: **DONE_WITH_CONCERNS**. Source ownership and Gradle ownership are released. No active tool sessions remain. No Git mutation, production edit, private runtime read, live endpoint, service, or Docker operation was performed.

## Result

The test-only driver now parses strict fresh/prepare/collect/release/close commands; stores at most four actual named Dataset/root references; executes retained `collect` and `collectAsList` through the shared action used by the offline native fixture; records scoped HTTP response generation/digest metadata; binds latest completed downloads to applied policy/role versions; and writes immutable private numbered results and pass markers. Expected entry denial advances the slot only after both result and marker writes. The driver closes all slots in `finally` before engine, Spark, and recorder shutdown. The legacy fresh result filenames and exact `{close:true}` behavior remain.

Only these ten assigned test-source files changed:

1. `DeltaRangerDownloadedCommand.java` (new)
2. `DeltaRangerRetainedQueries.java` (new)
3. `DeltaRangerRetainedAction.java` (new)
4. `DeltaRangerCallbackGateway.java`
5. `DeltaRangerDownloadedSecurity.java`
6. `DeltaRangerDownloadedQuery.java`
7. `TestDeltaRangerDownloadedQuery.java`
8. `TestDeltaRangerDownloadedSecurity.java`
9. `TestDeltaRangerCallbackGateway.java`
10. `TestDeltaRangerRetainedQuery.scala` (new)

## Verification

Final frozen source manifest: `evidence/freeze2-source-sha256.txt`; all ten `shasum -c` checks passed after verification. The original 19-test-per-pin freeze and its source manifest remain in `evidence/final-matrix/` and `evidence/final-source-sha256.txt`. Freeze2 adds one recursive-immutability regression and supersedes it.

| Spark / Delta | Focused tests | Original exit | Dependencies | Compiled output |
| --- | ---: | ---: | ---: | --- |
| 3.5.4 / 3.3.0 | 20/20 | 0 | resolved, exit 0 | captured |
| 3.5.4 / 3.3.2 | 20/20 | 0 | resolved, exit 0 | captured |
| 3.5.6 / 3.3.0 | 20/20 | 0 | resolved, exit 0 | captured |
| 3.5.6 / 3.3.2 | 20/20 | 0 | resolved, exit 0 | captured |

Each `evidence/freeze2-matrix/<Spark>-<Delta>/` contains original Gradle log/exit, four original JUnit XML files, an identical 20-case inventory, `compiled-classes.tar.gz` containing test and main Java/Scala classes, a module JAR snapshot with SHA-256, and the resolved `testRuntimeClasspath` report/exit. The inventory is identical across all four pins: 7 query, 5 security, 6 gateway, and 2 native cases; no XML failure/error. Dependencies resolve Spark 3.5.4/3.5.6, Delta 3.3.0/3.3.2, Ranger 2.8.0, Scala 2.12.21; builds used local JDK 17.0.16. The source manifest binds all four executed rows; compiled class archives were captured immediately after each test process and before the next pin. The module JAR snapshots are supplementary; the compiled class archives are the exact per-pin binary evidence.

`evidence/freeze2-package-check.log`/`.exit` record passing `spotlessCheck`, `javadoc`, and optional companion `shadowJar` on the first pin. `evidence/freeze2-companion-entries.txt` is the actual JAR entry list; no new/modified test application or fixture class name is present. The companion hash is in `evidence/freeze2-companion-sha256.txt`. `git diff --check` passed.

The native fixture calls `DeltaRangerRetainedAction.execute` for repeated unchanged actions, before-first and after-success policy denial, and role-publication drift. It checks identical Dataset/root references, exact rows or entry denial, job observations, and zero named user-scan output on denial. It also covers four-slot capacity, released/reused/missing IDs, close cleanup, immutable pass-marker permissions, and write-failure/failed-result no-advance through the exact helper the driver calls. Query tests cover strict close, malformed/duplicate/trailing/null/unknown fields, filename safety, 4096 limit, and effective policy/role union. Gateway/security tests cover scoped forwarded bytes, SHA-256, service/version decoding, latest wrong-generation or malformed HTTP 200, 304/incomplete/wrong-service rejection, role-only evidence, and `NONE` cursor movement. Existing fresh `isSecurityFailure` tests remain green.

## RED evidence and limits

`evidence/parser-red.log`/`.exit` preserve the JDK 17 missing-parser compiler RED. `evidence/freeze2-immutable-red.log`/`.exit` show the new regression failing because a nested row changed from one value to two; `evidence/freeze2-immutable-green.log`/`.exit` show it passing after defensive recursive freezing. The initial JDK 25 configuration failure was environmental and is not counted as a test RED. The first gateway RED attempt stopped on formatting before reaching its intended missing-constructor assertion, and a separate native missing-holder RED was not captured; those are test-first process gaps, not passing evidence.

All acceptance here is offline. The native fixture uses the installed extension and actual named Delta table, but does not establish a live Gravitino/Ranger downloaded-policy transition. No current-artifact live verifier, public coordinator, or outage fault support is claimed. The existing September 21 evidence was not relabeled. One pre-existing Scala compiler warning in `TestDeltaRangerActionValidation.scala:399` remains outside this task's scope.

## Fix1 independent-review resolution

The earlier freeze2 evidence and review package remain intact. **Fix1 is the current source and test result.** The source manifest is `evidence/fix1-source-sha256.txt`; all ten files passed `shasum -c` after the matrix and package checks. `evidence/fix1-driver-red.log`/`.exit` show the new full driver-loop test failing to compile because `runCommands` did not exist. `evidence/fix1-driver-green-attempt1.log`/`.exit` show its first pinned GREEN run. `evidence/fix1-first-pin-attempt2.log`/`.exit` show all focused tests passing after policy/BOTH and cap assertions were added.

Finding 1 is covered by `TestDeltaRangerRetainedQuery.driverLoopWritesExactRetainedEnvelopesAndCleansOnCloseOrMalformedInput`: it calls the package-private `DeltaRangerDownloadedQuery.runCommands` that `main` calls, with an in-process security gateway and actual native named Dataset. It asserts the exact PREPARE, COLLECT and RELEASE field sets, version and download map schemas, normalized native row, numbered result/marker schemas and values, private 0700 directory and 0600 files, exact CLOSE termination without a fourth event, and malformed close cleanup. The same test invokes the driver loop at sequence 4096 with a valid held slot to assert that the 4097th retained command fails before a result write and clears the slot. `runCommands` receives a refresh callback solely to avoid a live Ranger service in the offline fixture; `main` supplies `SparkRangerAdminPlugin::refreshPoliciesAndTags`.

Finding 2 is covered by `TestDeltaRangerDownloadedSecurity.postCursorPolicyAndBothSelectExactAppliedResponseDigests`, which uses the real gateway and a fixture engine's actual `Versions` object for post-cursor POLICY and BOTH selections. `TestDeltaRangerRetainedQuery.driverUnionRequiresRealPolicyAndRoleDownloadsForAppliedVersions` drives the actual command loop: a policy-requested COLLECT observes applied policy/role generations 2/2, chooses effective BOTH, selects distinct completed policy and role HTTP 200 attempts, records their exact versions and SHA-256 digests, and writes a passing entry-denial result and marker. The earlier role-only, latest invalid 200, 304, missing and incomplete download controls remain green.

Finding 3 is addressed by computing SHA-256 from the forwarded body in `scopedSecureResponseRecordsOnlyMatchingGenerationAndDigest` and from selected response bodies in the post-cursor and driver-union tests, comparing exact hex digests. Finding 4 is addressed with normal `java.util.Locale` imports in both changed Java files.

| Fix1 Spark / Delta | Focused tests | Original test exit | Dependency exit | Compiled archive |
| --- | ---: | ---: | ---: | --- |
| 3.5.4 / 3.3.0 | 23/23 | 0 | 0 | captured |
| 3.5.4 / 3.3.2 | 23/23 | 0 | 0 | captured |
| 3.5.6 / 3.3.0 | 23/23 | 0 | 0 | captured |
| 3.5.6 / 3.3.2 | 23/23 | 0 | 0 | captured |

The 92 testcases have identical four-pin inventories: 7 query, 6 security, 6 gateway and 4 native cases, with no XML failures or errors. Each `evidence/fix1-matrix/<Spark>-<Delta>/` holds the original Gradle log/exit, four original XMLs, exact testcase inventory, compiled Java/Scala main/test class archive and hash, module JAR snapshot/hash, and resolved `testRuntimeClasspath` report/exit. Classes were captured at each completed test-process boundary before the next profile. `evidence/fix1-package-check.log`/`.exit` record passing Spotless, Javadoc and optional companion `shadowJar`; `evidence/fix1-companion-entries.txt` contains no test harness classes, and `evidence/fix1-companion-sha256.txt` hashes the archive. `git diff --check` passed. No live downloaded-policy acceptance is claimed by fix1.

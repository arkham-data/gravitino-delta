# Delta Shared Protocol Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox syntax for tracking.

**Goal:** Make the qualified registration protocol independently consumable by production server and Spark artifacts.

**Architecture:** Move the pure marker/action-reader code into `:delta:delta-common`, preserving wire/API behavior. The future server catalog depends on this artifact; existing native test consumers reference it directly.

**Tech Stack:** Java 8 compatible main code, JDK 17 build, Gradle 8.2, Jackson, JUnit 5.

**Spec:** `docs/superpowers/specs/2026-09-08-delta-production-runtime-design.md`

## Global Constraints

- Do not modify core, API, server, client, or storage-schema source.
- Preserve existing Java package names and wire encoding; no protocol behavior changes in this task.
- Shared runtime dependencies contain no Spark, Kernel, Hadoop, Gravitino core, or test fixtures.
- Preserve default connector dependency isolation and generic catalog behavior.
- Preserve existing evidence archives and original workspace. Work only in `/private/tmp/gravitino-delta-feasibility`.
- No subagents, external messages, shared deployment, remote push, merge, or PR creation by the implementer.

### Task 1: Extract the qualified protocol into its shared production artifact

**Files:**
- Move `catalogs/catalog-lakehouse-delta/src/main/java/org/apache/gravitino/catalog/lakehouse/delta/registration/*.java` into the identical package under `delta/delta-common/src/main/java/`.
- Move the two existing protocol test classes to `delta/delta-common/src/test/java/`.
- Create `delta/delta-common/build.gradle.kts` from the current pure protocol build, retaining strict Javadoc.
- Modify `catalogs/catalog-lakehouse-delta/build.gradle.kts`, `settings.gradle.kts`, root `build.gradle.kts`, `catalogs/catalog-lakehouse-generic/build.gradle.kts`, and `spark-connector/v3.5/spark/build.gradle.kts`.
- Create `delta/delta-common/README.md` documenting runtime isolation, stable package ownership and current artifact scope.

**Interfaces:**
- Produces project `:delta:delta-common` with unchanged `DeltaOperationMarker`, `DeltaCommitActionReader`, and `DeltaCommitEvidence` public APIs and existing package-private `DeltaJson`.
- `:catalogs:catalog-lakehouse-delta` declares `implementation(project(":delta:delta-common"))`; no duplicate classes or new provider claims.
- Existing generic and Spark native test consumers replace their direct protocol dependency with `testImplementation(project(":delta:delta-common"))`.
- Root Java 8 compatible prefixes include `:delta:delta-common`; the old provider-specific prefix can be removed since it now targets server code. No historical results document is rewritten to imply the old archive contained the new jar.

- [ ] **Step 1: Capture baseline and inspect consumers.** Save `git rev-parse HEAD`, existing source-file SHA256 hashes, and `rg -n 'catalog-lakehouse-delta|DeltaOperationMarker|DeltaCommitActionReader'` consumer results to this task's report/evidence directory. Read exact root build conventions for artifact naming before choosing any override. There is no new behavior, so no artificial failing unit test is required.
- [ ] **Step 2: Move code and wire dependencies.** Use filesystem moves retaining source content. Add `include("delta:delta-common")` next to the other shared format modules. The provider build becomes:

```kotlin
plugins { id("java") }
dependencies { implementation(project(":delta:delta-common")) }
```

Retain ASF headers. The shared build keeps the current Jackson/JUnit dependencies and strict Javadoc task. Existing public methods and tests are unchanged.

- [ ] **Step 3: Verify behavior and consumer linkage.** With `JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home`, run shared `test`, `javadoc`, `jar`, and scoped formatting checks; run generic `compileTestJava`; run Spark 3.5 `compileTestJava` once with the existing explicit Delta 3.3.0/Spark 3.5.4 test properties and once without opt-in. Use repository flags to skip unrelated integrations. Confirm 36 protocol tests execute, with zero failures/skips unexpected from baseline. Capture exact commands/exits and original XML. Resolve the actual Spark project path from settings rather than inventing it.
- [ ] **Step 4: Verify packaging isolation.** Capture shared `runtimeClasspath` dependency report; inspect jar entries and class-file major versions (52 for main classes). Confirm provider jar no longer contains protocol classes, shared jar does, and hashes of moved source match baseline. No production Spark server dependencies are introduced by this extraction. Document the artifact paths and hashes.
- [ ] **Step 5: Self-review and commit.** Check only intended files changed, run `git diff --check`, commit the extraction, and write the full report with status, commit, test commands/exits/counts, artifacts, and concerns. Return only a concise summary. Root will arrange independent review before the next implementation task.

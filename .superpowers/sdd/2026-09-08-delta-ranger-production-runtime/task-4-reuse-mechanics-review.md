# Independent review: collect boundary mechanics prototype

No P0–P2 findings. **Approve the bounded test-only mechanics characterization.**
This does not approve a production guard or complete the broader mechanics and
production qualification sequence in the design review. Task 4 remains open.

Reviewed on 2026-09-21 at HEAD `88478f00896c65d157f5f0d688bc0a59140bd543`:
the untracked `TestDeltaRangerCollectBoundary.scala`, the mechanics report, and
their original saved evidence, against `task-4-reuse-enforcement-design-review.md`.
Only this review file was written. No source edits, builds, test reruns, JVMs,
live queries, Docker operations, commits, or subagents were used. This is direct
source and artifact review; no GitNexus graph/taint coverage is claimed. No callable
claude-mem/mcp-search tool was available.

## Evidence verified

The current Scala source SHA-256 is
`5864db07d7489f9ceda1560ff6d93ba1cd3e1fee15d0d3605eafbfe3b63a10fc`.
It matches both GREEN source archives and their source manifests exactly.
The reviewed mechanics report SHA-256 is
`4f939f6022f74c3c60bbaded830a59d967f83908a3d31897a4161f94fe9efe45`;
the design review SHA-256 is
`3a9a68c0c6f01c4111a4e420e48652bdc3eb8e47a795c44f8f50e2baa1250311`.

Original command JSON, exit JSON, JUnit XML and Gradle logs agree:

| Evidence directory under private `task-4-evidence/` | Spark / Delta | Tests | Failures / errors / skipped | Exit |
| --- | --- | --- | --- | --- |
| `collect-boundary-red-compiled` | 3.5.4 / 3.3.0 | 3 | 3 / 0 / 0 | 1 |
| `collect-boundary-green-354-guard` | 3.5.4 / 3.3.0 | 3 | 0 / 0 / 0 | 0 |
| `collect-boundary-green-356` | 3.5.6 / 3.3.2 | 3 | 0 / 0 / 0 | 0 |

All three named test methods appear in each XML. The RED fails because stale
first and retained collection calls do not throw `CollectBoundaryDenied`, and
the downstream-failure test observes zero collection validations instead of one.
Its archived source hash is
`80065c8270c965ce9195e3a74ae88eb824434b7cc071adf22ab982db7bda5abe`.
The RED-to-GREEN source diff only installs and defines the collection guard;
the assertions remain unchanged. The misleadingly named `collect-boundary-green-354`
contains this same RED source and three failures, accurately disclosed in the report.
The earlier sandbox, formatting, and protected-cleanup compile failures were also
checked and are correctly excluded from behavioral RED evidence.

The saved capture script runs in `Test.doFirst`. I verified the Scala test-class
archive hashes and all collect-boundary class-entry hashes (10 RED entries, 12
per GREEN). I also rehashed the recorded Spark SQL/core/catalyst, Delta Spark/storage,
and Scala library inputs: all match their receipts, with Scala 2.12.21 and the
reported version pairs. External dependency JARs are hashed in place, whereas
test classes are archived; this is not a claim that every dependency was copied.
The GREEN XML runtime output identifies Spark 3.5.4 and 3.5.6 respectively.

Both GREEN logs finish successfully and include module Spotless checks. The
3.5.4 run executes Scala formatting and its check; the final 3.5.6 command explicitly
requests `spotlessScalaCheck`, reported UP-TO-DATE for the identical source.
The command and resource script specify one worker/fork, 768 MiB Gradle/Zinc heaps
and a 1 GiB test heap. These checks validate saved evidence, not a fresh replay.

## What the source and assertions establish

- At Scala line 46, the test retains a real Spark Range Dataset and its physical
  root, verifies exact rows through both `collect` and `collectAsList`, rejects
  both stale repeated actions, and restores successful collection on the same
  Dataset. Five collection validations distinguish actual action entry from
  testing a boolean helper. The retained root is asserted identical; the root
  name and visible `WholeStageCodegenExec` confirm the injected unary survives
  preparation passes with child codegen still present.
- At lines 228 and 235, the guard is an ordinary visible `UnaryExecNode`, without
  `CodegenSupport`. Collection validation precedes `super.executeCollect`.
  `doExecute` adds defense in depth and delegates without capturing validation
  state in an RDD transformation. Raw `execute` remains outside the pre-prepare claim.
- At line 74, stale first collection leaves prepare, execute and broadcast
  counters at zero. This matters because preparation is normally memoized, so
  unchanged preparation counters on an already prepared plan alone would not
  establish ordering. An admitted retry creates a real `SparkContext.broadcast`
  from the instrumented child's `doPrepare`; subsequent stale entry performs no
  additional child work. This models observable preparation work, not a real
  Spark broadcast-exchange join.
- At lines 101, 208 and 223, explicit root cleanup traverses the visible child
  after denial, success and admitted downstream failure, destroying the fixture
  broadcast. The helper exposes inherited cleanup without reflection or hidden
  delegation. Dataset automatic cleanup is not asserted.
- At line 120, each test owns fresh state, an extension installed through its
  builder, and a temporary warehouse. The finally block stops Spark, clears
  active/default sessions and deletes the directory. There is no global fixture
  map or production registration. Focused serial execution is supported by the
  evidence; parallel/full-suite isolation was not qualified.

## Remaining qualification boundary

The report accurately narrows the approved design's broader test sequence:
native row/vectorized scans, real broadcast-capable joins, and an explicit
AQE-negative test have not been completed. AQE is explicitly false in every test;
the unexercised extension precondition is not evidence of AQE rejection coverage.
Delta JAR presence establishes a classpath profile, not execution of a Delta scan.
Only two diagonal pairs ran, not the four-pair acceptance matrix.

Complete engine/subject/role/registration and decision receipts, typed native
provenance, user-cache rejection and production installation belong to the next
vertical slice. This test-owned mutable validator establishes none of those
security properties. It also does not qualify raw plan/RDD/iterator entry,
arbitrary Spark actions or revocation after action admission. Their absence is
not a defect in this expressly bounded test-only prototype. Keep these limits
when reusing the evidence or describing progress.

Raw evidence remains private and ignored; this review contains no credentials,
endpoint details, machine identifiers or copied runtime logs.

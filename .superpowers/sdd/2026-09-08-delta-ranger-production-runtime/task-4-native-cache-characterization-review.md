# Task 4 native cache characterization review

Review date: 2026-09-21. Target:
`TestDeltaRangerNativeCacheCapture.scala` at SHA-256
`500f9ddcfd42594a25075ba151ef1a7bb249e19472a4c561fa2b2a1fe5bee413`.
This is an independent read-only review of the bounded characterization, not a
review or qualification of a production physical authorization implementation.

## Findings

- **LOW** — `task-4-native-cache-characterization-report.md:43` says the
  `withStats` getter produced thirteen state-derived `resolution`/`check`
  callbacks. Both sealed final inventories contain fourteen at events 02-15:
  nine `resolution` and five `check` callbacks. The total of 28 events and the
  ordering conclusions are otherwise correct. Change “thirteen” to “fourteen”
  so the prose agrees with the archived evidence.

No source-code quality or behavioral finding was identified.

## Spec-compliance verdict

**PASS WITH MINOR REPORT CORRECTION.** The source and sealed evidence support the
brief's bounded conclusions:

- `captureGetter` enters capture before evaluating the public `snapshot.stateDF`
  or `snapshot.withStats` getter, obtains the actual returned frame, then derives
  and seals its analyzed `LogicalRDD` only after getter return.
- Resolution and check callbacks retain actual RDD and output objects. The
  assertions establish the design-critical finite order: returned-RDD
  resolution, nested same-schema decoy resolution/check, returned-RDD check,
  getter return, rejected decoy seal, exact returned-RDD/output seal, and only
  then explicit candidate planning.
- The decoy is constructed through public
  `SparkSession.createDataFrame(emptyRDD[Row], returned.schema)`. Its schema
  equals the returned schema while its RDD identity differs; sealing it throws,
  and it never appears in sealed physical callbacks.
- Pending physical callbacks are required to exist before returned-RDD
  resolution and are rejected if any `RDDScanExec` contains the eventual
  returned RDD. After sealing, an `RDDScanExec` over that exact RDD is required.
  This distinguishes reconstruction planning from candidate planning.
- Repeated public getters must return the exact captured backing RDD and equal
  output attributes for both state and stats caches.
- The second test installs the observers together with the existing production
  `DeltaRangerSparkExtension`, uses the real fixture snapshot and current native
  scope/registerSnapshot sequence, leaves scope, and then collects the expected
  three rows from a named `b.db.t` SELECT.
- The standalone test asserts AQE enabled and the expected Delta cache and
  columnar-compression settings. Both inventories record those same settings;
  no setting was disabled to obtain the result.

The report also keeps the result within its actual boundary: this is a test-only
capture protocol. It does not claim owner registration, sealed production native
tickets, native AQE authorization, collection enforcement, retained-cache
revocation, every Delta route or the full four-pair gate. In particular, the
successful named SELECT is a coexistence control, not proof of physical
authorization.

## Code-quality verdict

**READY.** The review-package diff reconstructs the working file byte-for-byte,
and the working file has the required SHA-256. The test uses no reflection or
private adapter to manufacture the returned cache result. The event recorder is
test-local, keeps raw identities in memory, emits only sanitized identity
categories, and traverses ordinary children plus the public AQE/stage plan
accessors with identity-cycle protection. Its phase and event mutations are
synchronized, while DataFrame construction for the nested decoy occurs outside
the monitor.

The helper is larger than an ordinary unit-test fixture, but each part serves the
bounded characterization: callback capture, exact identity checks, phase order,
AQE-aware physical traversal and sanitized evidence output. There is no unused
production abstraction or production behavior in this change.

## Evidence checked

- The review-package diff and working source match exactly; source SHA-256 is
  `500f9ddcfd42594a25075ba151ef1a7bb249e19472a4c561fa2b2a1fe5bee413`.
- The archived manifests and hashes identify Spark 3.5.4/Delta 3.3.0 and Spark
  3.5.6/Delta 3.3.2. Their final XML hashes match
  `native-cache-capture-root-verification.json`; each XML contains 2 tests with
  0 failures, 0 errors and 0 skips, and each archived command exited 0.
- For each runtime, standalone and production-scope inventories are byte-for-byte
  identical. They record 18 `stateDF` events and 28 `withStats` events and confirm
  that the returned candidate occurs in physical callbacks only after seal.
- Pinned `StateCache.scala` places reconstruction `qe.toRdd`, RDD map/persist and
  then `LogicalRDD`/`Dataset.ofRows` construction in the order described by the
  report. The archived Delta binaries expose public `Snapshot.stateDF()` and
  final public `Snapshot.withStats()` on both tested Delta versions.

The GitNexus index predates this new test and does not contain
`NativeSelectFixture`, so it could not provide complete symbol/flow coverage for
the target. Direct source, frozen diff, pinned dependency sources/binaries and
sealed runtime evidence were authoritative. No rerun was warranted for the
single prose-count correction.

## Overall verdict

**READY after the LOW report count correction.** The source needs no change.

## Closure

The report-only correction is verified at SHA-256
`1506ff640cc89e36d86ee9e4d582ba186c83b80fa97c18ef781bc31b99c9a0cc`:
line 43 now says fourteen callbacks, matching events 02-15 in both final
inventories. The sole finding is resolved. The reviewed source remains
`500f9ddcfd42594a25075ba151ef1a7bb249e19472a4c561fa2b2a1fe5bee413`.

Final spec-compliance verdict: **PASS**. Final code-quality verdict: **READY**.
Overall final verdict: **READY**.

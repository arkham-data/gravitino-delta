# Task 4 native cache capture characterization

Status: **DONE** for this bounded characterization. This does not complete Task 4 or qualify a production collection guard.

## Change

Added only `spark-connector/v3.5/spark-delta-ranger/src/test/scala/org/apache/gravitino/spark/connector/delta/ranger/TestDeltaRangerNativeCacheCapture.scala`.

Final SHA-256: `500f9ddcfd42594a25075ba151ef1a7bb249e19472a4c561fa2b2a1fe5bee413`. Both final archives and the working file match. No existing source/test/build file, service, Docker fixture, runtime JAR or checkpoint was changed; no commit was made.

The test uses real `NativeSelectFixture` tables and public `snapshot.stateDF` / `snapshot.withStats`. Test-only resolution, check and columnar callbacks retain actual `LogicalRDD.rdd`, output attributes and physical `RDDScanExec.rdd` references in memory. A private init script alone supplies an optional sanitized-inventory output path; ordinary test execution has no repository filesystem side effect.

## API findings

Pinned Delta `StateCache.CachedDS` first plans reconstruction through `qe.toRdd`, maps/persists the cache RDD, then constructs `LogicalRDD(qe.analyzed.output, rdd)`. This explains reconstruction physical callbacks before the returned candidate is analyzed without implying candidate physical planning.

`Snapshot.stateDF` publicly returns `cachedState.getDF`; final public `DataSkippingReader.withStats` returns its separate cached stats result. One exact public-API incompatibility was found: `Dataset.ofRows` is inaccessible as a value outside `org.apache.spark.sql`. The test uses public `SparkSession.createDataFrame(emptyRDD[Row], returned.schema)` for the real same-schema decoy; it uses no reflection/private adapter.

The physical observer follows normal children plus public `AdaptiveSparkPlanExec.inputPlan` and `QueryStageExec.plan`.

## Actual order and identity results

The standalone and production-scope inventories were identical. The order was also identical on Spark 3.5.4/Delta 3.3.0 and Spark 3.5.6/Delta 3.3.2.

`stateDF` produced 18 events:

1. getter entry;
2. pre/post physical `AdaptiveSparkPlanExec`;
3. pre/post physical `ShuffleExchangeExec`;
4. pre/post physical `SerializeFromObjectExec`;
5. resolution of the eventual returned RDD;
6. nested resolution/check of a distinct same-schema decoy;
7. check of the returned RDD;
8. getter return;
9. rejected decoy seal, then exact returned-RDD/output seal;
10. planning entry;
11. pre/post physical `RDDScanExec` containing the exact returned RDD;
12. planning exit.

`withStats` produced 28 events:

1. getter entry;
2. fourteen resolution/check callbacks over state-derived `other` RDD plans;
3. pre/post physical `ProjectExec` containing only an `other` state RDD;
4. resolution of the eventual returned stats RDD;
5. nested resolution/check of its distinct same-schema decoy;
6. check of the returned stats RDD;
7. getter return;
8. rejected decoy seal, then exact returned-RDD/output seal;
9. planning entry;
10. pre/post physical `RDDScanExec` containing the exact returned stats RDD;
11. planning exit.

No pending physical callback contained either eventual returned candidate. Candidate physical planning first occurred after return and seal. Reconstruction did plan while each getter was pending: state planned adaptive/exchange/serialization nodes; stats planned a project over the existing state RDD. Repeated getter calls reused the exact backing RDD objects and equal output attributes. Same-schema decoys had different RDD identity and could not seal.

The production coexistence control installed observers plus the existing `DeltaRangerSparkExtension`, called `stateDF`, exact `registerSnapshot(index, snapshot)`, and `withStats` inside `withNativeScans(Seq(index))`, then left scope. The subsequent real named `SELECT id, region FROM b.db.t` returned exactly `{(1, keep), (2, drop), (3, keep)}`.

Both runtimes recorded:

- `spark.sql.adaptive.enabled=true`
- `spark.databricks.delta.snapshotCache.storageLevel=MEMORY_AND_DISK_SER`
- `spark.sql.inMemoryColumnarStorage.compressed=true`

No AQE/cache setting was disabled.

## Verification

| Evidence | Runtime | Result |
| --- | --- | --- |
| `task-4-evidence/native-cache-capture-354-330-production` | Spark 3.5.4 / Delta 3.3.0 | exit 0; 2 tests; 0 failures/errors/skips; Spotless passed |
| `task-4-evidence/native-cache-capture-356-332-production` | Spark 3.5.6 / Delta 3.3.2 | exit 0; 2 tests; 0 failures/errors/skips; Spotless passed |

Each retains exact command/exit, full Gradle log, original XML, pre-run source/hash snapshot and artifacts. `native-provenance/` retains executed Spark/Delta JARs, pinned available Delta sources, normalized standalone/production inventories and hashes. `git diff --check` passed. No source changed after the final runs.

Original non-behavioral failures remain separate:

- `native-cache-capture-354-330`: sandbox denied the Gradle wrapper lock before compilation.
- `native-cache-capture-354-330-run1`: compilation rejected inaccessible `Dataset.ofRows`; no test ran.
- `native-cache-capture-354-330-final`: assertions passed, then optional inventory output failed from a wrong working-directory assumption.
- `native-cache-capture-354-330-final2`: private init-script configuration failed; no test ran.
- Intermediate green runs preceded the final production-scope control.

## Limits

This does not qualify owner registration, delayed native tickets, native AQE, every Delta route, collection enforcement, retained-cache revocation or the full four-pair gate. Current production admission can analyze a same-schema decoy while its native scope is active; only this test protocol proves the decoy is not sealed. Production must replace schema admission with owner-bearing exact RDD/output evidence.

The 3.3.2 provenance includes pinned `Snapshot.scala`/`StateCache.scala` and the exact executed Delta JAR, but no separate 3.3.2 `DataSkippingReader.scala`; the public `withStats` seam was compiled/executed against that archived runtime.

# Collection-boundary guard primitive review

Review scope: frozen `DeltaRangerCollectGuardExec.scala` and `TestDeltaRangerCollectGuardExec.scala`, the implementation brief, physical-provenance and logical-receipt design sections, author report, review-package diff, exact sources, and original diagonal evidence summarized by `collect-guard-root-verification.json`. No build was rerun and no source was changed.

## Specification verdict: PASS

No specification finding was identified.

- `DeltaRangerCollectGuardExec.scala:28-38` defines only the package-local driver validation seam and a final visible `UnaryExecNode`. The guard adds no `CodegenSupport` or columnar path, and its output, partitioning, and ordering delegate exactly to the current child (`:39-41`).
- `DeltaRangerCollectGuardExec.scala:47-55` checks canonicalization/context and validates the actual current child before entering `super.executeCollect`; the defense-in-depth `doExecute` check occurs before `child.execute`. Construction, copying, canonicalization, and rendering do not invoke either validator.
- `DeltaRangerCollectGuardExec.scala:43-45,60-71` preserves a live context through ordinary Spark clone and child-replacement paths, omits it from JSON, and rejects canonicalized or deserialized/contextless guards before child preparation or execution. Only `child` is present in the case-class Product surface, while the context field is private and transient.
- `DeltaRangerCollectGuardExec.scala:57-58` retains Spark's inherited visible-child cleanup traversal without reflection or a public test hook.
- The production class changes no Spark setting. The test harness explicitly disables AQE for this mechanics slice and verifies that the setting remains false.

The focused tests exercise the production guard rather than a parallel implementation. They prove repeated `collect`/`collectAsList` entry validation on one retained Dataset and root, unchanged rows, a visible code-generated descendant, stale denial before new child preparation/execution, preparation-time broadcast prevention, explicit inherited cleanup after a downstream failure, clone and replacement-child revalidation, canonicalized fail-closed ordering, Java serialization without the non-serializable context, deserialized denial before child work, and omission of sensitive context data from Product, plan, JSON, and explain output.

## Quality verdict: PASS

No HIGH, MEDIUM, or LOW quality finding was identified.

The implementation is small and uses only public/protected Spark extension points. Validation calls are centralized through `requiredDriverContext`, error messages are constant and contain no authority data, normal copies retain the context explicitly, and the transient field cannot enter Java serialization. The tests distinguish collection-entry validation from the potentially cached `doExecute` lifecycle and use observable preparation/execution counters rather than assuming each repeated collection rebuilds the child RDD. The cleanup test makes an explicit traversal call and does not claim automatic Dataset cleanup.

The current source hashes match the report and both archived pre-run snapshots:

| File | SHA256 |
| --- | --- |
| `DeltaRangerCollectGuardExec.scala` | `8a53473a76759878312ab36eeb08ee5b8e607ff9fbddd05044f2b88d2d30c5d1` |
| `TestDeltaRangerCollectGuardExec.scala` | `af4b042bbcbd5dffdb32f16520904607154dc29506d162ad43064fa4dd3f45b8` |

## Evidence checked

The preserved commands and exit records are successful, and the original XML hashes match `collect-guard-root-verification.json`:

| Spark / Delta / Ranger | XML tests / failures / errors / skips | XML SHA256 | Other gates |
| --- | --- | --- | --- |
| 3.5.4 / 3.3.0 / 2.8.0 | 4 / 0 / 0 / 0 | `8580f7df2e86ffa136390982d294f6597b33fd07eeb1918f3e5f9a57b61eed34` | exit 0; Spotless and Javadoc |
| 3.5.6 / 3.3.2 / 2.8.0 | 4 / 0 / 0 / 0 | `d72c73b9f00f4835e0dcf6b9ed633c2eb303a4395aea89e0970ce1d26df38b3f` | exit 0; Spotless |

Each run has 11 verified compiled-input archives and the expected pinned Spark SQL, Delta Spark, and Ranger plugins-common jars. The meaningful RED archive records 4/4 behavioral failures against the incomplete guard, while earlier setup/compile failures remain correctly separated.

## Review limits

- The guard is not installed by the production extension, and this change supplies no production validation-context implementation. The tests establish boundary mechanics only; they do not establish policy, registration, receipt, or native-provenance enforcement.
- The supported boundary is `executeCollect`/Dataset collection. Raw `execute`, `prepare`, RDD APIs, iterators, arbitrary actions, columnar collection, running-action cancellation, and task-closure safety are outside this verdict.
- The diagonal Spark/Delta runs meet this bounded brief. They are not the full four-profile Task 4 qualification matrix.
- AQE, user cache revocation, real native Delta physical classification, exchange/stage handling, prepared-index replacement, and retained-plan decision validation remain future integration gates.
- The available GitNexus index predates the new guard symbol, so structural conclusions come from the exact frozen source, design contract, diff, and archived execution evidence.

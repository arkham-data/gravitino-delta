# Task 4 live Spark, Delta and Ranger matrix — September 21, 2026

Task 4 remains active. Both the initial-deny and post-mask SELECT policy matrices passed on Ranger 2.8.0 with all four requested Spark/Delta combinations. This report does not qualify full Delta support or the remaining policy lifecycle tests.

| Spark | Delta | Initial exact row results | Initial Ranger denials | Post-mask exact row results | Driver exits |
| --- | --- | --- | --- | --- | --- |
| 3.5.4 | 3.3.0 | 6 passed | 4 passed | 10 passed | 0 / 0 |
| 3.5.4 | 3.3.2 | 6 passed | 4 passed | 10 passed | 0 / 0 |
| 3.5.6 | 3.3.0 | 6 passed | 4 passed | 10 passed | 0 / 0 |
| 3.5.6 | 3.3.2 | 6 passed | 4 passed | 10 passed | 0 / 0 |

Each driver started with an empty private policy cache and completed both authenticated policy and role HTTP 200 downloads. Observed generations were policy 41 / role 63 initially and policy 44 / role 63 after the masking transition. Across eight fresh drivers, all 80 assertions passed: 64 exact positive row results and 16 genuine policy denials, with sixteen completed initial policy/role downloads. Root independently checked exact results, runtime versions, identities, download completions, response hashes, genuine `DeltaRangerPolicyEngine.authorize` denial origins, and natural process shutdown. No known Admin, plugin or S3 credentials appeared in the inspected driver evidence.

The queries exercise CUSTOM uppercase masking, row filtering, NULL masking, quoted column names, permitted projections, star expansion, predicates on forbidden columns and cross-catalog joins. All initial cells used the same active `delta_b.secret` deny. Fresh-process observations do not establish same-JVM refresh or cached-plan revocation.

## Recovery after Docker stopped

Ranger reused its persistent database and the original pinned 2.8.0 image. Gravitino retained its verified process, configuration and provider artifacts. Its callback gateway was restored without replaying public role or registration operations.

The original MinIO `/data` mount was tmpfs, so stopping Docker removed the table objects. All twelve archived objects were restored byte-for-byte into a new named-volume MinIO instance. Native Delta table UUIDs, version-zero logs and Parquet bytes match the archived fixtures; S3 modification times have a new explicit baseline. This is backup restoration, not uninterrupted storage continuity. Other historical datasets were not qualified by this restoration.

The restored control plane passed 163 checks across 38 completed GET requests. Explicit Ranger restart generation/timestamp changes were recorded separately; existing roles, policy semantics, preserved lifecycle divergence and Gravitino registrations were not repaired.

## Transition from denial to masking

The fixture initially attempted to create a separate allow policy for the same column resource as the deny. Ranger 2.8.0 rejected that duplicate access-policy resource with HTTP 400 after the NULL mask had been created. The deny remained enabled.

The corrected fixture expands the existing same-principal allow policy to include `secret`, verifies that publication while the deny remains enabled, then disables the deny. The active mask covers the column throughout this sequence. A subsequent assertion flagged a Ranger-generated resource signature change on disabling the policy; exact readback confirmed that only the intended enabled flag and server metadata changed. Completed writes were never replayed. Private evidence preserves the original rejection and both assertion stops.

Final independent review confirmed all ten owned policies, twelve completed HTTP 200 readbacks, policy generation 44, unchanged role generation 63 and all unrelated policy models unchanged. The disabled deny is the sole policy omitted from the final plugin download. The post-mask matrix passed on this verified state with fresh driver caches. Direct secret reads and star projections returned NULL; predicates comparing the masked column to its original value returned no rows; cross-catalog joins returned NULL for the protected column. Existing masks and row filters remained effective.

## Evidence

Private runtime and query evidence remains outside version control. The root summaries are `task-4-evidence/initial-matrix-root-summary-20260921.json` and `task-4-evidence/live-matrices-root-summary-20260921.json`; each preparation contains its own `root-verification.json`, actual query results, transport records and process exit evidence. The immutable companion SHA-256 is `5d91d3aa6db812d4d5a6247c205d52c204e097d72acb3996517f17669bc19be7`.

After all eight drivers exited, read-only S3 verification confirmed the same twelve native objects, contents, sizes and restored modification times across all three tables. No native Delta object changed during either matrix.

The private execution runner passed fifteen offline tests. The corrected post-mask result verifier passed eleven tests and an all-four-pair audit of 600 recorded fields. Independent review closed its timestamp/schema findings before final acceptance; previous candidates and failures remain retained.

## Remaining gates

Group and visibility checks, grant/revoke and membership refresh, same-JVM refresh/cache behavior, public callback failure cases and final Task 4 review remain separate acceptance gates. Broader Delta operations and deployed catalog-demo integration remain unqualified. This continuation makes no Gravitino core changes.

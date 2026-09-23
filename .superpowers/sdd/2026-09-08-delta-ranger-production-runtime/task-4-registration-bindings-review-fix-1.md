# Registration-bindings review fix 1 closure

Scope: the two-file patch in `task-4-evidence/registration-bindings-review-fix-1-staged/fix.diff`, current exact sources, its source-hash manifest, and the four corrected matrix runs summarized by `registration-bindings-review-fix-1-root-verification.json`. This review addresses only the LOW test-portability finding in `task-4-registration-bindings-review.md`.

## Closure verdict: PASS — LOW finding resolved

No new specification, production-correctness, or quality finding was identified.

- `DeltaRangerRegistrationBindings.java:323-329` keeps the production two-argument `IdentityKey` constructor and delegates it directly to `System.identityHashCode(value)`. Both production insertion and lookup call sites (`:116,139`) still use only that constructor, so map authority and lookup behavior are unchanged.
- The new three-argument constructor is package-private and is used only by the focused test. It changes no public API and introduces no name, path, or structural-equality authority.
- `TestDeltaRangerRegistrationBindings.scala:84-108` deterministically gives two distinct live referents the same hash. It verifies same-referent equality, different-referent inequality, both colliding map lookups, stable hash after clearing, self-only equality for cleared keys, exact queued-key removal, and survival/lookup of the other colliding entry.
- The test no longer relies on JVM identity-hash distribution and no longer retains up to one million objects. The unchanged preceding test still verifies that the production constructor derives its hash from `System.identityHashCode`.

The corrected source hashes match current bytes, the staged manifest, and every final pre-run archive:

| File | SHA256 |
| --- | --- |
| `DeltaRangerRegistrationBindings.java` | `9dcc968f15f98baa6e5c957d1058d2747a1a5d8064b979fc6c136f344ba53bb7` |
| `TestDeltaRangerRegistrationBindings.scala` | `c7cac7413608379c7ec9a185bd1c359608ad70f8b8d2b1f8fb81970cb296949b` |

## Evidence checked

All four preserved commands exited 0. Each original run contains 10 registry tests plus 5 native-wrapper tests with no failures, errors, or skips:

| Spark / Delta / Ranger | Tests / failures / errors / skips | Result |
| --- | --- | --- |
| 3.5.4 / 3.3.2 / 2.8.0 | 15 / 0 / 0 / 0 | Spotless and Javadoc passed |
| 3.5.4 / 3.3.0 / 2.8.0 | 15 / 0 / 0 / 0 | Spotless passed |
| 3.5.6 / 3.3.0 / 2.8.0 | 15 / 0 / 0 / 0 | Spotless passed |
| 3.5.6 / 3.3.2 / 2.8.0 | 15 / 0 / 0 / 0 | Spotless passed |

The 60 corrected executions have zero failures/errors/skips; each run has 11 verified compiled-input archives and the expected pinned Spark SQL, Delta Spark, and Ranger plugins-common jars. The original 119-execution binding proof remains historical evidence for the pre-fix source.

## Documentation verdict: PASS

The root-owned README addition at `spark-delta-ranger/README.md:71-83` accurately describes the reviewed implementation and evidence. Ordinary named loads use the same metadata lease for both PUBLISHED observations and the base load; complete receipt/schema drift, expiry, and closure fail closed. Completed entries require exact descriptor/relation/index identities and retain the raw native schema, full protocol JSON, ordered partitions, and column-mapping evidence. The four-profile tests include positive partitioned and name-mapped fallback controls. The documented `NullType` rejection follows from Delta dropping those fields before the implementation's exact full-schema comparison. The final sentence correctly states that these bindings do not install action-time validation for retained Datasets. No documentation finding was identified.

## Limits

This closure does not expand the earlier registration-binding verdict. The registry still covers ordinary named-load binding and package-local resolution only; physical/retained action enforcement and the other limits recorded in `task-4-registration-bindings-review.md` remain open.

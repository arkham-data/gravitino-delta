# Task 1 fix1 scoped re-review

Scope: `evidence/task-1-fix1-review.diff` only, comparing the reconstructed freeze2 review package with frozen fix1. The original four findings are in `task-1-review.md`. I read the changed assertions and author report; I did not rerun tests or review unchanged files. Root's `evidence/root-fix1-matrix-verification.json` establishes four original 23/0/0/0 focused rows (92 cases); artifact/source integrity and later live acceptance remain root-owned.

## Original findings

1. **ADDRESSED — driver evidence contract coverage.** `DeltaRangerDownloadedQuery.main` now calls the extracted `runCommands` used by the native fixture (`DeltaRangerDownloadedQuery.java:135-143,163-298`). `TestDeltaRangerRetainedQuery.scala:44-214` drives PREPARE/COLLECT/RELEASE/CLOSE through that loop with a real named Dataset and in-process gateway. It checks numbered result key sets, version/download/action schemas, expected row, 0700 directory, 0600 files, marker key set, malformed CLOSE cleanup, and the 4096-to-4097 boundary before a result file. The existing `completeCollect` test continues to cover marker-write failure and failed-result no-advance. This closes the principal missing driver seam.
2. **ADDRESSED — post-cursor policy/BOTH and effective union.** `TestDeltaRangerDownloadedSecurity.java:55-111` selects POLICY and BOTH against the fixture engine's applied `Versions`, including exact policy/role generations and response digests. `TestDeltaRangerRetainedQuery.scala:219-277` runs a POLICY-requested COLLECT after both applied generations advance, verifies effective BOTH, selected policy/role attempts and versions, and a passing entry-denial result. The earlier role-only, later malformed/wrong-generation, 304 and incomplete controls remain in the frozen base.
3. **ADDRESSED — exact digest assertion.** `TestDeltaRangerCallbackGateway.java:83-86` compares the recorded digest to SHA-256 of the forwarded body. `TestDeltaRangerDownloadedSecurity.java:89-109` compares selected policy and role digests to their response bytes.
4. **ADDRESSED — `Locale` import style.** `DeltaRangerDownloadedCommand.java:28,97` and `DeltaRangerDownloadedQuery.java:37,322,413-414` use normal imports and `Locale.ROOT`.

## New breakage in fix diff

No Critical, Important or Minor new breakage found. The `main` callback remains `SparkRangerAdminPlugin::refreshPoliciesAndTags`; the extracted loop preserves the fresh branch, exact CLOSE branch, retained write/advance order and `finally` cleanup. The new tests use an in-process gateway and test-local callback; no live service or production source was added.

## Out-of-scope, nonblocking observations

- The new marker assertion checks the exact field set and `passed:true`, but does not compare each marker's `sequence`, `id` and `retentionId` to its result (`TestDeltaRangerRetainedQuery.scala:152-166`). The driver source supplies these fields from the same command and sequence. This is a narrow assertion opportunity, not a remaining original finding.
- The driver union test checks selected attempts and versions but does not itself compare the serialized `downloads.policy/role.responseSha256` values to the recorded gateway digests (`TestDeltaRangerRetainedQuery.scala:262-276`). Exact gateway and selected-download digests are checked in the dedicated tests above. This does not block the scoped fix verdict.
- This offline fixture does not establish live downloaded-policy publication or public phase attestation. No such claim is made by fix1.

**Final fix-round verdict: SPEC compliant; QUALITY approved for the scoped offline task.** All four original findings are addressed; the fix diff introduced no identified regression. This is not a whole-branch or live-acceptance verdict.

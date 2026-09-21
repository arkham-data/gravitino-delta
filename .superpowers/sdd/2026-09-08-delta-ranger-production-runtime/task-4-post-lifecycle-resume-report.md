# Task 4 post-lifecycle resume implementation — September 21, 2026

Implemented a separate test-only `post-role-lifecycle-resume` main route in `40731ce8bf49ad936a39de721ae2e83c09c637ea`, with the reviewed response-boundary fix in `c78df994832d8b9c9d518508e476582e19ae3a25`. The final focused suite passes 102 tests; the initial implementation evidence below remains historical. Existing `matrix-resume` and production code are unchanged. No service endpoint, Docker operation, live query, bootstrap, reconciliation, repair or replay was performed by this implementation worker. Root owns input preparation, gateway transfer, service lifecycle and live acceptance.

## Input and continuation contract

See `task-4-resume-input-schema.md` for the exact closed schema, 33 GET paths and command shapes. Three independently SHA-bound owner-only nonsymlink files are required: original checkpoint with fixed digest `90653533d333a59880459f9a88c6d4db236f38e2236abc13bffb444f6e088cda`, new root preflight, and new exact model baseline. Historical 40/63 stays bound separately from accepted current 44/63. Duplicate fields, trailing JSON, wrong stages/identity/preflight/generations and model drift reject.

The validator executes only GET, through the recording gateways. Public, Ranger Admin and owned downloader identities are separate; both secure downloads use the independently matched user 9 credential file. The final route compares all 34 complete responses over 33 distinct baseline keys before any validated event or command dispatch. Public metalake DTO lacks a numeric ID; persisted ID binds through full canonical role 13 and managed policy labels. Service 16 has no roleVersion field; current role version 63 comes from the completed secured role download.

Full JSON model equality preserves timestamps and unlisted fields. Only enumerated collection ordering, list-wrapper queryTimeMS, audit_cleanup 404 message/stack, and parsed side-service audit-filter quote/whitespace spelling normalize. Column order, resource values, policy item order and expressions remain exact. Admin has 19 policies (35–52 and 54); secured download has 18, explicitly excluding disabled policy 48 only. Every downloadable policy and all 19 roles match complete Admin models. Root receipt snapshots are checked against the original native input IDs/version 0/locations.

The validated event is forced to disk before the new guarded loop starts. Protected roles global_guard/audit_cleanup/metadata_schema/protocol and originalowner artifacts cannot be dispatched, recreated or reconciled. Ordinary/group mutations and visibilityOnly membership commands remain possible. Unexpected dispatched failures quarantine further mutations; snapshot/close remain permitted. Every new-route event identifies preservedDivergence=true and the protected objects, independently of new-failure quarantine.

Only the new route opts into complete upstream-response byte retention. Separate files are mode 0600; recorder events contain paths/completion facts without headers or credentials. Existing gateway close/drain/failure behavior and default body-free query recorder remain covered by regression tests.

## Focused offline verification

Evidence root: `task-4-evidence/post-lifecycle-resume-20260921/`.

- `red-command-formatted`: original exit 1/XML: 6 tests, 2 failures: protected/unknown/malformed/quarantined command gate absent.
- `red-input-model-executed`: original exit 1/XML: 8 tests, 4 failures: input digest/model validators absent, plus command REDs.
- `boundary-and-response-red`: original exit 1/XML: 60 tests, 1 failure: new complete-byte response recorder absent;55 resume tests and existing gateway checks passed.
- `red-strict-json`: exit 1/XML: 85 tests, 2 failures: numeric string accepted, and an overstrict test incorrectly expected arbitrary diagnostic timestamp text to reject. The latter was corrected to remove the required completion field; no blanket timestamp normalization was added.
- `red-trailing-json`: exit 1/XML: 85 tests, 1 failure: a second trailing command JSON object was accepted. Strict parser now rejects trailing tokens.
- `red-additional-protected-boundaries`: exit 1/XML: 94 tests, 2 failures: fixed protocol 14 could disappear from an internally consistent expected baseline; a cleaned audit filter could be reintroduced. Both explicit invariants now reject.
- `green-protected-and-regression`: original exit 0/XML: 98 tests, 0 failures/errors/skips; formatting and Javadoc checks succeeded.
- `frozen-green`: independent final exit 0/XML: 98 tests, 0 failures/errors/skips: resume 82, readback 4, gateway 5, downloaded security 4, downloaded query 3. Original command/environment/log/XML and `Test.doFirst` execution-classpath capture are retained. Source-before/source-after manifests match 24 files; `frozen-verification.json` and `owned-source-sha256.json` bind the final six owned files.

Earlier sandbox/cache-lock, formatting and stub compile failures are retained but are not claimed as executed-test RED evidence. Existing Git-worktree commit-ID warning and Gradle deprecation warnings remain unrelated to this change. Javadoc task was up-to-date for unchanged production classes; compileTestJava ran with existing warning-as-error settings. No broad native suite was run because production/native behavior did not change.

## Frozen runtime artifacts

`runtime/<spark>-<delta>/` contains four 259-entry classpath manifests. Each reuses 258 original immutable entries and changes only the separate test application. Every old/new artifact hash was checked. Companion/native/server archives were not rewritten or redeployed. New coordinator/gateway/validator classes come from the exact frozen test execution-input ZIP; existing downloaded query/security classes are byte-identical to the original test app. Per-pair `archive-comparison.json`, root `archive-verification.json`, the archive builder and invocation record bind the output. ZIP timestamps make the first test app archive hash differ from the other three; compiled class hashes are identical.

The checked-in offline JSON fixture preserves complete response shapes with password/secret values replaced, private paths removed and diagnostic error bodies trimmed. It is neither a live baseline nor a credential source. Root independently verified the frozen source hashes and found no known credential matches before both source-only commits. Root subsequently sealed the live inputs, transferred callback ownership and verified the successful live resume; see `task-4-lifecycle-report.md`.


## Final public read review correction

The initial source commit was `40731ce8bf49ad936a39de721ae2e83c09c637ea`. Independent review found a final public loadMetalake GET after the 33-read validation. The bounded correction and 102-test RED/GREEN/frozen evidence are documented in `task-4-evidence/post-lifecycle-resume-20260921/review-fix/report.md`. Use only the new review-fix runtime/coordinator archives after independent approval. The corrected boundary compares all 34 responses and validates the exact request multiset before validated/ready events; 33 unique baseline observations remain unchanged.

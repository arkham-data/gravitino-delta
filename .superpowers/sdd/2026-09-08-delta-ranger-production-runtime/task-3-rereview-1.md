# Task 3 fix round 1 narrow re-review

Reviewed `d5e35f6e17862b0a86c7090f87e49571e622f0b4..f7343d46a6ee39fd1f614e648c5b555d031974e7` against the original P2 and the root ruling, using the supplied 14,694-byte three-file package and retained fix evidence. No suite was replayed.

## Verdict

- **Original P2: ADDRESSED.** The wrapper now admits only effective SIMPLE Gravitino application/current-actor authentication before catalog visibility, actor transport, metadata configuration, or engine acquisition. The check reads the common `GRAVITINO_AUTH_TYPE` key, defaults absent configuration to `simple`, and compares case-insensitively. This matches the current common client factory, which uses the same key/default and `AuthProperties.isSimple`; SIMPLE binds the application client to the Spark application user, so the retained metadata-versus-Spark/current-UGI checks establish the required separation. Basic and every other unqualified configured mode are rejected before the original counterexample can acquire wrapper resources.
- **New blocking breakage in the fix: none identified.** Counts: **0 P0, 0 P1, 0 P2.** The README accurately limits this gate to the Gravitino application/current-actor client and does not narrow the independent Ranger downloader or generic metadata-builder authentication contracts.

## Evidence observations

- The RED wrapper XML records 13 tests, 7 failures, 0 errors/skips, exit 1. Each of `basic`, `BASIC`, `kerberos`, `token`, `oauth2`, `unknown`, and empty reaches the engine-acquisition assertion before the gate exists. The test runs under a `worker` UGI with Spark user `worker` and Basic username `metadata-reader`, reproducing the P2 identity counterexample.
- The GREEN wrapper XML records the same 13 tests with 0 failures/errors/skips and exit 0. It covers those seven early rejections plus absent, `simple`, and `SIMPLE` admitted-mode positives. The unsupported-mode seam also fails if actor transport is used, counts engine acquisitions, and verifies the metadata client remains unconfigured.
- The GREEN command includes the scoped wrapper test, SpotlessCheck, strict Javadoc, and JAR tasks; the retained log ends `BUILD SUCCESSFUL in 22s`. `git diff --check` for FIX_BASE/FIX_HEAD exits 0.
- Root execution verification reports, for both RED and GREEN, 203 bound inputs (11 archived mutable, 191 external, 1 absent), 68 verified archive entries, and no mismatches. Root source verification reports all 25 GREEN source inputs match commit `f7343d46a6ee39fd1f614e648c5b555d031974e7`; independent hashes of the three changed worktree files match the binding manifest.
- The copied 17-case common XML is stale and excluded. Native plans/rules, broader application-auth modes, the earlier 69-case Task 3 runs, and deferred M1-M4 were not reopened. The available GitNexus index predates this worktree class, so graph mapping could not contribute without the prohibited cache mutation.

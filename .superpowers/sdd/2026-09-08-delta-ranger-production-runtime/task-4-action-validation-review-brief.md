# Action-validator independent task review

Review this frozen component increment for SPEC and QUALITY, not whole-branch
merge or physical/action integration acceptance. Read the complete
`task-4-action-validation-implementation-brief.md`, its requirements map
`task-4-action-validation-verification-checklist.md`, and implementer report
`task-4-action-validation-report.md`. Use the SDD task-reviewer template.

Base is a1f2ce4b5e9d22d36f5e712312576e0837a3d999. Head is uncommitted frozen
source82filesSHA256917843641f3ea7ab95906c681614ca8361c0f1d7d069596130c23e871c2f9bf0.
Diff `task-4-evidence/action-validation-review-package-1.diff` includes six changed
or new module paths with45contextlines and complete new files. Read it once; read
a changed file separately only for a named truncated-function gap. Inspect
unchanged code only for a concrete named risk, and record the risk/check. No
repository crawl. No source/git/Gradle/tests/services mutations or subagents.
Write only `task-4-action-validation-review.md`.

Review actual same-receipt validation, exact retained native log/binding authority,
lease/engine/subject/configuration order, complete original occurrence inventory,
all-access-before-any-decision and common fresh time, semantic equality including
NONE/absence, separate original publication/snapshot lower bounds, observed drift
within native brackets and across the entire action, conservative schema/protocol/
mapping equality and serialization/rendering. Do not replace analysis validation
with action reevaluation, and do not infer installed collection enforcement.

Tests and evidence: meaningful missing-action-contract REDs characterize the old
analysis-only boundary, not a claimed regression in its accepted contract. Native
head regression has an explicit source guard; normal retained DeltaLog is
monotonic and author does not claim forcing a lower actual returned head. Assess
coverage and stated limits accurately. One earlier full-matrix attempt stopped
before tests with Gradle scheduler failure (0XML); the retry uses accepted command
order and a unique label. Read the report and original XML; do not rerun tests to
regenerate evidence. Root independently audits source/compiled hashes, original
XML and exact Spark/Delta/Ranger pins. First full final row passes before dispatch;
remaining profile rows may run while review proceeds. Missing proofs are pending,
not failed tests. Resolve task-scoped gaps explicitly before acceptance.

No Checkstyle task exists in baseline: Spotless/strict Javadoc/Java-Werror plus
explicit Java imports/member/Javadoc review are the available checks. Identify
actual new warnings accurately without calling baseline Gradle/JVM noise a new
production defect. All4Spark3.5.4/3.5.6 x Delta3.3.0/3.3.2, Ranger2.8/JDK17.
No core/API/server/client/storage-schema changes, new services, live policy writes,
Docker/deployment or catalog-demo modifications. Collection guard stays uninstalled.

Global constraints copied from plan:

- One Ranger resource service is provisioned for exactly one metalake. Server pushdown and Spark engine bind to the same service identity and metalake; canonical catalog is within that metalake. Policy labels are mutation ownership selectors, not an extra Ranger resource dimension. No global catalog-name uniqueness assumption.
- Validate a closed Ranger 2.8 service definition on startup and every observed definition refresh: every access/mask/filter resource uses RangerDefaultResourceMatcher with wildCard=true, ignoreCase=false, replaceTokens=false and replaceReqExpressions=false. Initial Ranger-integrated admission rejects literal `*` and `?` names before the first Admin mutation and before engine evaluation. Keep explicitly typed descendant wildcard selectors distinct from user literals. Braces, backslashes and token-looking text remain literal; do not invent SQL or backslash escaping. This scoped restriction does not change the base Delta provider and needs a later canonical-name/custom-matcher qualification before full name support.
- Preserve `catalog → database → table → column`. Access/mask requests include all four keys; filters end at table. Canonical names come from the registered Delta descriptor, never SQL parsing, current-catalog guesses, Spark aliases or path→name maps.
- Preserve native DeltaTableV2. Participating sessions maintain `spark.databricks.delta.catalog.update.enabled=false`; required runtime entry/commit checks and session-catalog sentinel qualification remain in force. This is a trusted session/deployment lifetime invariant, not protection from arbitrary later config mutation or cached-plan execution.
- Read-only metadata credentials may resolve registrations for users with Ranger-only column grants. They never become the registration mutation actor or authenticated Ranger query subject. Reuse the runtime's explicit read/mutation supplier seam, ordinary existing client authentication and lifecycle; no hard-coded metadata_service or new auth protocol.
- All code below is production `src/main`, with tests in `src/test`. No test classes/resources, LocalRangerPolicyClient, fake downloader, class shadowing, Kyuubi package overrides or reflection-based processor replacement in artifacts.
- Pinned Kyuubi privilege extraction reports top-level Attribute names and does not establish nested-field permissions. An allowed struct column covers that column; never broaden a leaf-only policy into a struct grant. A literal top-level column named `payload.secret` is not a nested path. Nested permissions and struct masking require their own qualification.
- First execution acceptance: fresh named SELECT with projection, predicates and two-catalog joins, CUSTOM/NULL masks and table row filters. Unsupported shapes/types must fail explicitly; they are not full Delta or full Ranger conformance. DML/streaming/maintenance/cached-plan and remaining masks are required follow-up gates before the user's larger goal is complete.
- Source work runs sequentially under root's ownership after the packaged Spark chain is accepted. Root owns deployment inputs/services/cleanup. This plan neither starts services nor authorizes changes to shared Ranger/demo state.


Return SPEC PASS/ISSUES and QUALITY READY/NEEDS FIXES with actionable file:line
findings, strengths, exact evidence limits and any pending final matrix gate.

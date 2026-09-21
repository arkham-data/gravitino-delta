# Complete engine publication stamp foundation

Base f1bf2f193. Production retained SELECT remains a later integration gate. User
has authorized implementation and local tests; no renewed permission or live action
is needed. No core/API/server/client/storage-schema changes. Preserve the immutable
Ranger 2.8.0 live checkpoint at56/81 and all existing archives.

Own only DeltaRangerPolicyEngine.java, TestDeltaRangerPolicyEngine.java (and a new
focused Java test in the same package if useful), plus task-4-engine-stamp-report.md
and new private engine-stamp-* evidence. No Scala/catalog/metadata source edits,
Gradle build-file changes, services/network/Docker/commits/subagents. Root owns
integration and commits. You own bounded offline Gradle runs; use existing private
run-gradle.py and resource limits, archive actual Test.doFirst inputs and original
RED/GREEN XML. Existing AGENTS formatting/Javadoc/import/tests rules apply.

Implement a package-private immutable, opaque, driver-owned stamp that supports
capture and comparison of the complete validated engine publication for a retained
analysis/decision batch. Include owning lease identity (a different compatible
lease is not interchangeable), immutable trusted service/definition/metalake
binding, completed policy publication identity/current context, and independently
completed role publication identity/version. Numeric versions alone do not detect
same-number replacements. Snapshot capture and validation must recheck both policy
and role state at the end, and capture after metadata/decision I/O must reject
observed drift. Do not expose mutable Ranger internals or tokens through logging,
serialization or plan text; do not use reflection, Kyuubi class shadowing or a
private-package bridge. Use the existing public Ranger 2.8 event/publication seam,
but first verify its concrete setRoles/setPolicies order against pinned local
bytecode/source. Do not assert a completion guarantee unsupported by that seam.

Suggested package-private API is SnapshotStamp snapshotStamp() and
void requireSnapshot(SnapshotStamp), exact names may be improved. Keep existing
public versions() contract as diagnostics; strengthen evaluate() to compare the
complete stamp around each individual evaluation. Later callers will compare stamps
around the whole original-access/mask/filter batch as well. Add semantic equals/
hashCode for MaskDecision kind plus optional expression; this is required for
later reauthorization comparison even when numeric versions do not change.

Before editing production code, write a concise exact publication protocol and
ruling/limitations in your report and send root the intended API. Then meaningful
TDD: unchanged reuse succeeds; same-version completed policy context replacement,
role-only completed refresh including same-version role replacement, role refresh
during snapshot capture/evaluation, incomplete publication, wrong lease/closed
lease, and ordinary policy/version/service mismatches reject. Retain existing
readiness/role-only listener tests and normal access/mask/filter decisions. Tests
must use actual Ranger objects and deterministic barriers/hooks, not merely compare
fabricated stamp booleans. Add MaskDecision value equality positive/negative tests.
If a public seam cannot guarantee role completion, report the exact source-backed
gap immediately; do not silently substitute object/version observation for completed
publication. No fake production readiness bypass. This foundation does not install
a physical guard or qualify reused native Delta, cache/AQE/outage or full Delta.

Root will review the precise diff and test evidence independently before accepting.

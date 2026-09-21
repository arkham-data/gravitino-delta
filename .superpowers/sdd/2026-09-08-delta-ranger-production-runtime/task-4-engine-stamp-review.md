# Review: engine publication stamp foundation

Base `f1bf2f193048771fb7e1dce0611daa5c344e9d09`. Review target is the frozen
`task-4-engine-stamp-review-package.diff` plus the four source files recorded in
`task-4-evidence/engine-stamp-review-freeze/freeze-manifest.json`. Their current
SHA-256 values match the manifest exactly. This was a read-only review; no build,
source edit, service, network, Docker action or subagent was used.

## Findings

No findings. I found no concrete defect, regression, security issue or material
coverage gap within this foundation's stated scope.

## Specification-compliance verdict: APPROVE

The implementation satisfies the bounded brief:

* `DeltaRangerPolicyEngine.java:190-237` adds a package-private, opaque,
  non-serializable driver stamp bound to the exact lease, immutable trusted
  `DeltaRangerConfig`, observed publication identity and diagnostic versions.
  Wrong/null stamps reject; a closed lease rejects before comparison. Rendering is
  constant and exposes no live Ranger object or identity.
* `DeltaRangerPolicyEngine.java:396-423` derives completion only from Ranger's
  public auth-context callback. A new policy context records policy identity,
  version and published metalake. A role record requires the current context's
  actual `RangerRolesUtil`, the exact raw role input used to build it, and a new
  context/mapping identity. Every observed notification creates a new publication,
  conservatively invalidating stamps even when versions and live identities are
  otherwise unchanged.
* `DeltaRangerPolicyEngine.java:537-593` validates trusted service, definition and
  metalake, policy context/version, role input/mapping/service/version and actual
  plugin pointers. Its final read repeats mutable bindings and versions, then
  compares the volatile publication identity last. This covers observed drift
  during capture and validation without claiming a linearizable fence after the
  final check.
* `DeltaRangerPolicyEngine.java:595-637` brackets every access, mask and filter
  evaluation with the complete publication stamp. `MaskDecision` value equality at
  lines 281-298 compares kind and optional expression as required.
* The focused tests use real `RangerBasePlugin`, `RangerAuthContext`,
  `RangerRolesUtil`, `ServicePolicies` and role-backed access decisions. They cover
  same-version policy and role replacement, role-only refresh, missing completion,
  wrong/closed lease, wrong bindings, drift during evaluation/capture/final reads,
  unchanged access-mask-filter batches and mask equality. The NativeSelect helper
  now wires a real context/mapping and exact raw role input.

The pinned Ranger 2.8 source supports the completion ruling. In
`RangerBasePlugin.java:496-509`, `setPolicies` constructs the engine, assigns service
configuration, engine and current context, then synchronously notifies. At lines
249-258, `setRoles` assigns the raw input, updates the policy engine, then notifies.
`RangerAuthContext.java:100-102` constructs a new `RangerRolesUtil` before assigning
it. The util builds its five final map objects and final role version during
construction. On the normal writer callback, the later volatile `Publication` write
publishes the preceding mapping assignment and volatile plugin role input. An
unrelated observer callback does not itself establish a constructor-thread
happens-before edge: the five maps/version rely on final-field initialization
safety, and completion additionally requires the exact non-null mapping input to
match the volatile plugin input before the observer republishes only that safely
observed state. Later readers do not rely on Ranger's optional lock path.

## Code-quality verdict: APPROVE

The change stays within the existing owner/backend seam and introduces no parallel
registry, reflection, shadow class, serialization or public API. The immutable
`Publication`, `CompletedPolicy` and `CompletedRoles` records keep the concurrency
contract explicit. Identity comparison is used only where replacement identity is
the authority; value equality is limited to mask decisions. Failure paths remain
closed and sanitized. The final repeated reads are intentional race checks, and the
tests name and deterministically place each mutation with hooks rather than sleeps.

## Change and blast-radius summary

The production change is confined to `DeltaRangerPolicyEngine`: readiness now
requires independently observed policy and role publication, each evaluation is
bracketed by the full stamp, and mask decisions gain semantic equality. Existing
callers continue using `requireReady`, `authorize`, `mask`, `rowFilter` and
`versions`; the new stamp methods are package-private for the later retained-batch
integration. Test-only changes update the policy-engine fixture, NativeSelect helper
and add the focused snapshot suite.

The repository GitNexus index predates this optional module and could not resolve
`DeltaRangerPolicyEngine`, so this review cannot claim graph-backed dependent or
PDG coverage. Direct repository search found the mask consumer in
`DeltaRangerMaskRules.scala` and no production consumer yet for the package-private
stamp, consistent with the brief's later-integration boundary.

## Coverage and residual risk

I inspected the original archived commands, XML and source/input hashes. The frozen
evidence records 46 tests across the engine, read-rule, snapshot, native mask and
native snapshot-admission suites, with zero failures, errors or skips, using Spark
3.5.4, Delta 3.3.2 and the inspected Ranger 2.8.0 jar. Root independently verified
those artifacts and all four frozen source hashes.

This approval does not qualify the future outer batch guard, retained physical
Delta scan provenance, cache/AQE behavior, outage behavior or a full production
Delta matrix. It also retains the documented Ranger boundary: arbitrary trusted
code can mutate maps returned by Ranger's public util getters, and a change after
the final validation boundary cannot cancel an already admitted operation.

## Verdict

**READY** for the scoped engine-publication foundation. No changes requested.

# Engine publication stamp foundation

## Publication protocol, before implementation

Pinned Ranger 2.8.0 source and local bytecode are archived in
`task-4-evidence/engine-stamp-publication-protocol/`, including jar SHA-256.
`RangerBasePlugin.setPolicies` constructs an engine (which publishes the plugin
auth context), writes service configs, assigns its engine and current auth
context, then synchronously invokes the public untyped auth-context listener.
The existing CompletedPolicy records a new matched current/plugin context only
on that callback and freezes the policy version and downloaded metalake binding.
A same-context callback cannot repair a rejected policy binding.

`RangerBasePlugin.setRoles` first writes its input roles field, calls
`RangerPolicyEngineImpl.setRoles`, then notifies. The engine requests its optional write
lock while `PolicyEngine.setRoles` calls `RangerAuthContext.setRoles`: that
constructs all five maps in a NEW `RangerRolesUtil` and only then assigns rolesUtil.
Even the same input object/version produces a new authoritative mapping identity.
The listener has no event kind and also serves tag/user-store changes.

Ruling agreed with root: completion means fully constructed authoritative role
mapping publication observed through the public callback seam. It does NOT mean
proof of callback origin, setRoles return, or cache persistence. An unrelated
callback may observe the already assigned mapping. Before assignment, the old
mapping remains authoritative. The listener-issued immutable CompletedRoles
records context, util identity, role body identity/service/version and is safely
published via volatile owner state. Live pointers are validation predicates only;
getters alone never mint completion. Readiness and stamp capture require the
listener token plus matching authoritative current context/util, policy completion
and trusted service/definition/metalake. End validation rechecks policy AND role
pointers/versions/tokens. No reflection, private package bridge or shadow classes.

Intended package-private API: immutable opaque `SnapshotStamp snapshotStamp()`
and `void requireSnapshot(SnapshotStamp)`. Stamp owns the exact lease identity,
immutable trusted binding, completed policy and role identities and diagnostic
versions. It exposes no Ranger objects, fields, serialization or diagnostic text.
`evaluate` compares the complete stamp around each decision. Later analysis
callers must compare around the entire original access/mask/filter batch and
metadata I/O. `versions()` remains diagnostics with full readiness validation.
MaskDecision gains value equality over kind plus optional expression.

Limits: no linearizable cancellation after admission; a publication after the
last check can affect subsequent operations. No physical guard or retained native
Delta/cache/AQE/outage/full-Delta qualification is supplied by this foundation.
Live checkpoint 56/81 and all prior archives remain untouched. No memory tools
were exposed, so claude-mem search/store could not be performed.

## Validation

JMM qualification: Ranger locks are no-ops unless policy deltas AND in-place
policy updates are enabled. No lock-based visibility is assumed. The util's
roleVersion and all five map references are final; all reachable map/set contents
are populated in the constructor, which does not expose this. Normal setRoles
replaces the util and never mutates its existing maps. Final-field initialization
safety therefore covers the authoritative mapping and role version. Non-final
raw roles references must be non-null and match the volatile plugin input.
An unrelated callback does not by itself establish constructor-thread happens-before.

The stamp also captures the identity of every observed auth-context notification,
including same-context/same-util notifications. This conservatively invalidates
retained stamps on completed observed enricher notifications without repairing a
rejected policy binding. It does not qualify unobserved mutable enricher state.

Implementation is complete in the engine Java source, its existing Java tests,
and the new focused `TestDeltaRangerSnapshotStamp`. Root additionally assigned
the narrow `TestDeltaRangerNativeSelect` Java helper fixture adjustment: use a real
RangerAuthContext/role util and bind plugin.getRoles to the current role input.
Its access/mask/filter transport doubles are unchanged. No Scala source was edited.

Owner now publishes one volatile immutable Publication containing completed
policy/role tokens; its object identity also denotes every observed notification.
Role token binding cannot be repaired by a later same-util notification. Capture
validates downloaded definition, trusted service/metalake, role input and util,
then rechecks current/plugin policy context, role util, both versions and finally
publication identity AFTER binding reads. requireSnapshot additionally enforces
exact lease identity and trusted binding equality. Closed leases remain rejected
with IllegalStateException; stale/wrong/null stamps reject with SecurityException.
Opaque stamp rendering is a constant string and the stamp is not Serializable.

Meaningful TDD originals retained without rewriting:
- `engine-stamp-red-4`: 4 tests, 4 expected assertion failures against the original
  engine: same-version role replacement during evaluation/readiness capture,
  assigned role mapping without owner completion callback, and mask value equality.
- `engine-stamp-red-final-read-3`: 16 tests, 4 expected assertion failures before
  the final fix: completed notification during final binding read, incomplete
  role/policy publication during that read, and identity-bearing stamp rendering.
- Earlier archives retain cache permission, formatting/compiler and shared-tree
  metadata compilation failures honestly; these are NOT behavioral RED evidence.

Tests exercise actual RangerBasePlugin/ServicePolicies/RangerAuthContext/
RangerRolesUtil objects. Deterministic public getter/callback hooks place refreshes
inside evaluations, readiness capture, final binding reads, and role construction.
Role-backed access policy assertions demonstrate actual same-version revocation.
Other coverage includes unchanged original-access/mask/filter batch reuse, wrong
compatible lease, closed lease, missing completion, same-version completed policy
context replacement, independent role version update, SAME INPUT role republish
producing a new util, wrong service/metalake, and unrelated notifications. Existing
readiness, completed-policy listener, role-only listener, mask and filter tests
remain passing in the engine suite. The fixture uses the production-accepted
policy-deltas=false configuration, hence Ranger's NO_OP lock path.

Final successful evidence (exact commands in each command.json):
- `engine-stamp-green-integration`: 39 tests, zero failures/errors/skips:
  PolicyEngine 18, SnapshotStamp 16, ReadRule 5. SpotlessApply/Check and Javadoc
  passed. NativeSelect is a helper class and is not counted as a JUnit suite.
- `engine-stamp-green-native`: existing TestDeltaRangerMaskRules native SELECT
  suite: 5 tests, zero failures/errors/skips.
- `engine-stamp-green-native-admission`: NativeSnapshotAdmission: 2 tests, zero
  failures/errors/skips. Final coverage totals 46 tests across five suites.

All runs use Spark 3.5.4/Delta 3.3.2, --offline, -PskipITs, --max-workers=2,
--no-parallel and a 2 GiB/two-processor Gradle JVM. The existing module uses one
test fork. No live or Docker tests/services were invoked. Gradle configuration
retains its existing read-only Docker environment probe and deprecation notice.
No commit was created.

Original XML, source archives, actual Test.doFirst classpath/compiled inputs, logs
and exact commands are retained per run. All four owned Java source hashes match
the pre-execution archive in green-integration, proving formatting did not alter
those inputs. Actual test classpath Ranger 2.8 jar SHA-256 matches the inspected
source/bytecode target:
`10ad212120866d79566a595ff1282e92a6e43e1460eab558ef0ad4ec1e27d59e`.
Final source/report/XML/evidence hashes are in
`task-4-evidence/engine-stamp-review-freeze/freeze-manifest.json`.


Public map getters can expose mutable maps to other trusted plugin code. Normal
refresh consumers were inspected and do not mutate the existing util maps. This
contract excludes arbitrary external mutation through those getters.

Status: implementation and focused verification complete; four owned Java files
and this report are frozen for root independent review. The Gradle slot has been
released. No claim of a production retained-plan guard or expanded Delta scope.

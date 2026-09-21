# Same-JVM fresh-query runner report

Status: frozen private candidate, offline verification passed, actual first-cell validation passed, no live JVM or service call made by this implementation task.

## Frozen sources

- `task-4-evidence/query-executor-same-jvm-20260921/run.py`: SHA-256 `9b296234a55f2f1fe98aa0a4cb26e9cff81b977be1c2b42e3d14a8e3866e7b30`, 38,694 bytes.
- `task-4-evidence/query-executor-same-jvm-20260921/test_runner.py`: SHA-256 `1fbd86a02a4221c5626ea262a5eced08470797a2c250ac2a37d57f5eb68eb1e8`, 28,895 bytes.
- Reviewed lifecycle source remains SHA-256 `0f17a2bf6f2ef69927a2594d981a4fe232b3f4be28a55c147ff24d8264689474`.

The runner owns one prepared child and one Spark/Delta pair for three ordered phases: `initial-denied`, `granted`, and `revoked`. It records one PID at launch and in all phase, attempt, response, close, and exit evidence. It sends twelve SQL commands once, then one close command, and accepts only twelve validated results, a complete marker, natural exit 0, and no cleanup error. It makes no public mutation or repair call.

## CLI

```text
python3 task-4-evidence/query-executor-same-jvm-20260921/run.py --validate-only PREPARATION WORKFLOW INITIAL_ATTESTATION
python3 task-4-evidence/query-executor-same-jvm-20260921/run.py --execute PREPARATION WORKFLOW INITIAL_ATTESTATION
```

`--validate-only` performs private ownership, nonsymlink, exact-schema, fixture, attestation, preparation, 259-archive, profile, launcher, and command checks. It returns `jvmLaunched:false` and does not create `execution`, `results`, or control commands. `--execute` repeats those checks, creates an exclusive execution directory, temporarily sets umask `077` for the child launch, and refuses replay after any attempted execution.

## Exact root schemas

Workflow:

```json
{
  "schemaVersion": 1,
  "pair": "3.5.4-3.3.0",
  "fixture": "/absolute/private/fixture.json",
  "fixtureSha256": "<sha256>",
  "initialAttestationSha256": "<sha256>",
  "phaseDirectory": "/absolute/private/root-controlled-directory",
  "phases": [
    {"phase": "initial-denied", "commands": [
      {"id": "initial-a", "sql": "<exact SQL>", "denied": true},
      {"id": "initial-b", "sql": "<exact SQL>", "denied": true},
      {"id": "initial-quoted", "sql": "<exact SQL>", "denied": true},
      {"id": "initial-join", "sql": "<exact SQL>", "denied": true}
    ]},
    {"phase": "granted", "envelope": "/absolute/.../granted-envelope.json", "commands": [
      {"id": "granted-a", "sql": "<exact SQL>", "expectedRows": [["<exact values>"]]},
      {"id": "granted-b", "sql": "<exact SQL>", "denied": true},
      {"id": "granted-quoted", "sql": "<exact SQL>", "denied": true},
      {"id": "granted-join", "sql": "<exact SQL>", "denied": true}
    ]},
    {"phase": "revoked", "envelope": "/absolute/.../revoked-envelope.json", "commands": [
      {"id": "revoked-a", "sql": "<exact SQL>", "denied": true},
      {"id": "revoked-b", "sql": "<exact SQL>", "denied": true},
      {"id": "revoked-quoted", "sql": "<exact SQL>", "denied": true},
      {"id": "revoked-join", "sql": "<exact SQL>", "denied": true}
    ]}
  ]
}
```

Each command has exact keys `id`, `sql`, and one typed outcome. A denial is `"denied":true`; a positive is `"expectedRows":[...]`. IDs match `[A-Za-z0-9_-]+` and are globally unique. Initial and revoked contain four denials. Granted contains exactly one positive and three denials. The pair is one of Spark 3.5.4/3.5.6 crossed with Delta 3.3.0/3.3.2. Both envelope paths are distinct direct children of the bound phase directory and must not exist at launch.

Initial attestation:

```json
{
  "schemaVersion": 1,
  "status": "passed",
  "phase": "initial-denied",
  "pair": "3.5.4-3.3.0",
  "fixture": "/absolute/private/fixture.json",
  "fixtureSha256": "<sha256>",
  "minPolicy": 48,
  "minRole": 73,
  "expectedSubject": "<fixture.subjects.freshCallback>",
  "expectedGroups": [],
  "expectedVisibleCatalogs": {"delta_a": true, "delta_b": true, "delta_quoted": true},
  "transitionVerification": "/absolute/private/root-continuity-verification.json",
  "transitionVerificationSha256": "<sha256>"
}
```

The initial attestation hash must equal the workflow binding. Its floors exactly equal the prepared request and its transition proof reports `status:"passed"` with matching strict integer `policyVersion` and `roleVersion`. The actual initial policy and role HTTP 200 downloads and ready/result versions must equal these floors; later generations are rejected as unreviewed mutation.

Post-mutation envelope and attestation:

```json
{
  "schemaVersion": 1,
  "phase": "granted",
  "attestation": "/absolute/private/granted-attestation.json",
  "attestationSha256": "<sha256>"
}
```

```json
{
  "schemaVersion": 1,
  "status": "passed",
  "phase": "granted",
  "workflow": "/absolute/private/workflow.json",
  "workflowSha256": "<sha256>",
  "pair": "3.5.4-3.3.0",
  "fixture": "/absolute/private/fixture.json",
  "fixtureSha256": "<sha256>",
  "policyVersion": 49,
  "roleVersion": 74,
  "transitionVerification": "/absolute/private/root-transition-verification.json",
  "transitionVerificationSha256": "<sha256>"
}
```

The same exact forms apply to `revoked`. Envelope, attestation, transition proof, workflow, fixture, pair, phase, and hashes must bind exactly. Each transition proof reports passed status and exact strict integer attested versions. All root files are owner-only regular nonsymlinks.

## Wait and transport evidence

After the previous phase passes, the runner exclusively creates `execution/awaiting-granted.json` or `execution/awaiting-revoked.json`; root waits for complete JSON:

```json
{
  "schemaVersion": 1,
  "status": "awaiting-root-transition",
  "phase": "granted",
  "pair": "3.5.4-3.3.0",
  "workflow": "/absolute/private/workflow.json",
  "workflowSha256": "<sha256>",
  "fixture": "/absolute/private/fixture.json",
  "fixtureSha256": "<sha256>",
  "pid": 12345,
  "completedPhase": "initial-denied",
  "completedCommands": 4,
  "completedAttemptCursor": 2,
  "expectedEnvelope": "/absolute/private/granted-envelope.json",
  "publishedAt": "<UTC timestamp>",
  "deadlineSeconds": 900
}
```

`completedAttemptCursor` is the causal floor captured before the root transition. A periodic new-version download that occurs while the runner waits remains eligible. `phase-<phase>-start.json` records the same `causalAttemptCursor` and a separate `observedAttemptCursorAtPhaseStart`. Before accepting the phase's first result, the runner requires the first policy and role GET 200 events after the causal floor for the exact service, with `upstreamAttempted:true`, a UTC completion timestamp, downstream completion, and distinct attempts. `phase-<phase>-downloads.json` preserves both cursors, the attempt range, exact raw download-log prefix byte count/hash, both qualifying event objects, exact attested versions, and the first result path/hash.

Every response record preserves command, passed-marker, and raw-result hashes. Results bind exact SQL, fresh callback subject, resource service/definition, and exact before/after versions. Positive rows compare exactly. Denials require `java.lang.SecurityException`, `authorizationDenied:true`, empty rows, `Delta Ranger access denied`, and a stack origin at `DeltaRangerPolicyEngine.authorize`.

## Verification

The RED run failed because `run.py` did not exist. The final GREEN run passed 16 offline tests in 20.520 seconds. Coverage includes one child across all phases; both wait barriers; downloads during the wait; old, 304, incomplete, and wrong-service download rejection; exact initial versions; wrong phase/hash/proof generation; phase order; duplicate IDs; strict schema and numeric-boolean rejection; fresh identity/groups/visibility; immutable input changes; precise authorization origin; partial JSON without replay; query failure and timeout cleanup; close timeout; missing completion; and validate-only subprocess exclusion.

Final checks also passed Python syntax compilation without bytecode output, `git diff --check`, source privacy modes, and validation against the root's actual 3.5.4/3.3.0 preparation. That validation bound workflow SHA-256 `1ec3e5b70e43852458c67b19eaa830d07cc321df283e576f3ec04342aafbbfc0` and initial-attestation SHA-256 `1819f9f5d5caeab91aa9ddcd9daa17a596e528c580fd378ff7577d649ab53514`; it launched no JVM and created no execution or results directory.

## Limitations

This runner establishes fresh `spark.sql` calls in one JVM after explicit root transitions. It makes no reused DataFrame, logical plan, physical plan, or cache claim. The root owns all public Gravitino mutations, transition readbacks, phase envelope publication, live service state, and final acceptance. The candidate was implemented and tested offline; live JVM, Docker, Gradle, and service execution were outside this task.

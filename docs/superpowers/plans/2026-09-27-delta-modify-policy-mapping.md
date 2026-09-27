# Delta MODIFY_TABLE policy mapping implementation plan

> **For agentic workers:** Use superpowers:subagent-driven-development with TDD and one independent SPEC/QUALITY review. This is the first independently testable task of the governed append design.

**Goal:** Propagate canonical table MODIFY_TABLE grants as Ranger update access while preserving SELECT, ownership and foreign policies.

**Architecture:** Extend only the existing optional Delta authorization plugin's allowlist and privilege translation. Keep whole-change prevalidation, current callback routing and exact managed policy selection. Spark continues rejecting unqualified write commands.

**Tech Stack:** Java, Gravitino authorization SPI, Ranger 2.8.0, JUnit 5, Gradle/JDK 17.

**Spec:** `docs/superpowers/specs/2026-09-27-delta-governed-append-design.md`, Policy contract.

## Global constraints

- Spark 3.5.4 and 3.5.6 × Delta 3.3.0 and 3.3.2; Scala 2.12; JDK 17 qualification.
- No Gravitino core/API/server/client/storage-schema changes; optional provider, authorization and Spark modules only. No Spark or Delta fork.
- No build/dependency/service-definition changes, external runtime actions, packaging or remote push in this task. Root owns Git and any later runtime qualification.
- New public/protected members require Javadoc; English and existing Google Java style. All new behavior needs tests.

## Review focus

- Mixed SELECT/MODIFY changes must remove only the intended access, including owner-bearing shared policies.
- A valid first change followed by invalid DENY/type/catalog/wildcard must fail before any Admin interaction.
- MODIFY must not inherit HadoopSQL's read/select/alter/write privilege bundle.
- Cross-catalog old role context must stay distinct from explicit changes and must not lose another catalog's contributions.
- Already converged repetition must not be mislabeled failed-callback reconciliation; this task has no live enforcement claim.

## Task 1: Exact translation and policy-preserving updates

**Files:**
- Modify `authorizations/authorization-ranger-delta/src/main/java/org/apache/gravitino/authorization/ranger/delta/RangerDeltaAuthorizationPlugin.java`.
- Modify `authorizations/authorization-ranger-delta/src/test/java/org/apache/gravitino/authorization/ranger/delta/TestRangerDeltaAuthorization.java`.
- If needed for an existing realistic local REST fixture, modify `authorizations/authorization-ranger-delta/src/test/java/org/apache/gravitino/authorization/ranger/delta/TestRangerDeltaProductionCallbacks.java`; no new endpoint fixture or live calls.

**Interfaces:** Consume existing `privilegesMappingRule`, `allowPrivilegesRule`, `translatePrivilege(SecurableObject)`, `onRoleCreated`, `onRoleUpdated` and ordinary `RoleChange` callbacks. Produce the same typed Delta resource shape with unique select/update privileges; no new public API.

- [ ] Add translation regressions first, using existing helpers and native privilege values:

```java
SecurableObject modify = SecurableObjects.parse(
    "delta_a.sales.orders", MetadataObject.Type.TABLE,
    List.of(Privileges.ModifyTable.allow()));
AuthorizationSecurableObject actual = plugin.translatePrivilege(modify).get(0);
assertEquals(List.of("delta_a", "sales", "orders", "*"), actual.names());
assertEquals(List.of("update"), actual.privileges().stream()
    .map(privilege -> privilege.getName()).collect(Collectors.toList()));
```

Also assert `privilegesMappingRule().get(MODIFY_TABLE)` contains exactly the native UPDATE enum and the allowlist contains MODIFY_TABLE, mixed/reversed/duplicate SELECT+MODIFY produces exactly two distinct accesses, empty produces none, visibility produces none, and illegal combinations still reject. Existing tests which use MODIFY_TABLE as an unsupported TABLE privilege must use an actually unsupported privilege; preserve their original validation purpose. Foreign old context may still contain MODIFY_TABLE but must remain untranslated.

- [ ] Add behavioral callback regressions using the existing Admin fixture: install owner and another ordinary role plus foreign access/mask/filter models; create/update a writer role, add SELECT beside MODIFY, revoke only MODIFY then only SELECT, and re-add. Compare complete detached serialized models for every unaffected policy/owner/foreign contribution and exact writer accesses for the affected one. Cover A/B same-name tables with one global role and a valid-first/invalid-second change array with zero Admin interactions. Reuse existing true policy simulation rather than asserting mock call counts as the sole oracle.

- [ ] Run focused tests before changing production code; archive actual failing XML/logs and classify failures. Command with JDK17:

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home ./gradlew :authorizations:authorization-ranger-delta:spotlessApply :authorizations:authorization-ranger-delta:test --tests '*TestRangerDeltaAuthorization' -PskipITs -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.0 --offline --console=plain
```

- [ ] Add MODIFY_TABLE to both the plugin allowlist and `privilegesMappingRule()` with only UPDATE. Fully validate all privileges before emitting a resource. Deduplicate by native access enum/name before constructing `RangerHivePrivilegeImpl(SELECT, ALLOW)` / `RangerHivePrivilegeImpl(UPDATE, ALLOW)` values (the wrapper has identity equality) for table SELECT_TABLE / MODIFY_TABLE, respectively; retain local visibility's empty output and existing canonical checks. Do not delegate to inherited broad MODIFY translation.

- [ ] Run the complete offline module suite and scoped formatting:

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home ./gradlew :authorizations:authorization-ranger-delta:spotlessApply :authorizations:authorization-ranger-delta:test :authorizations:authorization-ranger-delta:spotlessCheck -PskipITs -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.0 --offline --console=plain
```

Record every failure/warning and excluded Docker/native gate explicitly. No rerun of live SELECT fixtures is implied. Archive final source diff/hashes, XML and class outputs for review; root will commit only exact reviewed files.

- [ ] Independent SPEC/QUALITY review, covering all five review-focus conditions; fix grounded findings with focused reruns. Root commits the accepted local change and updates this task status. Full governed append remains pending native characterization, command enforcement and four-pair live qualification.

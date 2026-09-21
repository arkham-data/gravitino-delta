# Immutable published-registration reader review

Review scope: frozen `DeltaRangerRegistrationReader.java`, `TestDeltaRangerRegistrationReader.java`, and the single `compileOnly(project(":delta:delta-common"))` build dependency in `spark-delta-ranger`. The implementation brief, design, author report, review-package diff, exact sources, supporting receipt/identity and metadata-lease sources, and original final evidence were inspected independently. No tests were rerun because no finding called the archived result into question.

## Specification verdict: PASS

No specification findings at HIGH, MEDIUM, or LOW severity.

- `DeltaRangerRegistrationReader.java:49-100` binds the existing metadata client and trusted literal metalake/catalog, rejects any identifier other than one literal namespace plus one literal table before metadata I/O, and performs catalog verification, table load/name verification, receipt decoding, lease capture, and schema detachment inside `DeltaRangerMetadataClient.read`.
- `DeltaRangerRegistrationReader.java:68-84` checks the exact catalog name, `lakehouse-delta` provider, returned table name, PUBLISHED state, location, opaque native ID, and nonnegative native version. It uses `DeltaRegistrationReceipt.fromProperties`; it does not infer identity from the location or parse the native ID as a UUID.
- `DeltaRangerRegistrationReader.java:102-110,175-261` keeps the observation package-local and immutable, retains only scalar/value metadata plus the opaque lease stamp, rejects a null or foreign-reader observation, delegates expiry/closure validation to the owning metadata client, and renders a constant redacted description. The existing receipt and identity types are immutable, and the receipt property map is unmodifiable.
- `DeltaRangerRegistrationReader.java:113-168` uses the existing `SparkTypeConverter`, preserves ordered names, types, field nullability, array element nullability, and map value nullability, recursively removes Spark field metadata/comments, and rejects null, empty, unnamed, untyped, duplicate, or unsupported schema input rather than dropping or inventing fields.
- `DeltaRangerRegistrationReader.java:244-253` compares owner, canonical name, the complete immutable receipt properties (including native version), and logical schema for exact within-bracket publication equality. The comment correctly limits this method to publication equality; a future action-time compatibility check may allow native data-version advancement between validation brackets and must implement that rule separately.
- `spark-delta-ranger/build.gradle.kts:59` adds the requested production compile-only delta-common dependency without adding a runtime dependency.

The 19 focused reader tests exercise the required successful, malformed, mutation-detachment, schema, receipt-field, wrong-owner, expiry/closure-during-I/O, sanitization, rendering, and query-identity cases through a real `DeltaRangerMetadataClient` backed by fake Gravitino objects. Exact stubbing of `loadTable(NameIdentifier.of("sales", "orders"))` also makes an incorrect lookup fail closed.

## Quality verdict: PASS

No code-quality or test-quality findings at HIGH, MEDIUM, or LOW severity.

The API is deliberately package-private and small. The implementation has one read path, reuses the accepted metadata lease primitive and receipt parser, performs no reflection or credential/client construction, and sanitizes runtime and linkage failures to a constant exception with no attached upstream cause. Schema reconstruction is localized and recursive, and the observation exposes strings and existing immutable values instead of provider DTOs or Spark schema arrays. The tests are behavior-focused and cover the important race boundaries after table I/O and during conversion rather than mirroring private helpers.

The review package contains exactly the two new Java files and one dependency-line addition. Current bytes match the frozen hashes and the archived pre-run copies:

| File | SHA256 |
| --- | --- |
| `DeltaRangerRegistrationReader.java` | `2a019ec590b9c19acb9d9d38658d316859bf2f5868aed671b911ddb35a0ff8a7` |
| `TestDeltaRangerRegistrationReader.java` | `58d167f9f0c3396865787765f6ab8046110c59206a368f13819dd1cb0f44e894` |
| `spark-delta-ranger/build.gradle.kts` | `dfab7f89357f4347b6620825bbb2aec26839318ba005016f787df3d9f7fefe81` |

The preserved final command exited 0. Its original XML hashes match `registration-reader-root-verification.json`: reader `775bb9ea2a39405616ea28b026f64be506dc4f79251edb667f9ffb95c282b24b` with 19/0/0/0 and metadata client `995b3b480116f76e3250acb0d6b51178902abcb31ed6fa08a84284018bd3ef73` with 19/0/0/0. The same log records successful `spotlessCheck`, focused tests, and Javadoc (up-to-date after the prior successful generation).

## Review limits

- This verdict covers only the bounded immutable-reader foundation. It does not establish catalog wiring, named native binding, native log/index identity or head/schema/protocol checks, location policy, action-time comparison, physical-plan enforcement, or metadata/policy atomicity.
- Exact `samePublication` native-version equality is appropriate for detecting drift within one validation bracket. It is not the future retained-plan action comparator, where unchanged identity/schema/native ID may coexist with data-version advancement between actions.
- The reader deliberately does not synchronize arbitrary concurrent mutation of external provider DTOs during its callback; it guarantees that a completed observation is detached.
- Verification covers the archived Java 17 / Spark 3.5.4 / Delta 3.3.2 profile. Original evidence was inspected rather than rerun.
- The available GitNexus index predates these new reader/metadata symbols. Its `SparkTypeConverter` context confirms this is an established shared converter, but reports lower-bound reference completeness; exact review conclusions therefore come from the frozen source, diff, and preserved execution evidence.

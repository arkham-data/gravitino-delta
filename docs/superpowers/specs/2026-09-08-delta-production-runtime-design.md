# Delta production provider and Spark runtime

The user authorized continued sub-agent implementation after native-registration qualification at `6ecc79fc1`. This stage produces a real `lakehouse-delta` provider and optional Spark runtime. The prior test providers remain evidence fixtures, not deployment artifacts.

## Binding requirements

- Gravitino owns table registration. Native Delta metadata and data remain authoritative in S3.
- Qualify Spark 3.5.4 and 3.5.6 against Delta 3.3.0 and 3.3.2, all four combinations, with Scala 2.12.
- Ranger 2.8.0 is the required authorization target. Downloaded-policy Spark enforcement and catalog-demo integration follow production artifact qualification; a dependency report alone does not prove enforcement.
- Do not modify core, API, server, client, or storage-schema source. Build wiring outside those source trees is allowed.
- The platform prevents concurrent writer jobs for one dataset. This does not independently fence an old writer after failure.
- Preserve existing generic catalog behavior and default connector dependency isolation.
- No shared deployment, remote push, merge, or PR creation is included. Preserve local evidence and unrelated work.

## Component boundaries

1. `delta/delta-common` owns the pure Java registration marker and commit-action correlation protocol. It contains no Spark, Kernel, Hadoop, Gravitino core, or test-fixture dependency. Keep existing Java package names and wire encoding stable when moving the previously qualified implementation. Main bytecode remains Java 8 compatible.
2. `catalogs/catalog-lakehouse-delta` becomes the actual ServiceLoader provider, owning managed schema/table registration, immutable storage policy, native metadata access, durable commands and receipts. It depends on the shared protocol and server storage libraries, never Spark.
3. An optional Spark 3.5 Delta adapter composes native DeltaCatalog. Its runtime consumes the shared protocol and ordinary Gravitino client APIs without depending on server provider/test jars. Default Spark artifacts remain free of Delta dependencies.

## Server behavior

Use existing managed entity storage and complete-row optimistic comparison. Native I/O occurs outside entity update callbacks. Preserve registration ID, governance owner and audit history during publication/replacement. Never replay CREATE successfully on an already published row: the existing create hook would reset ownership.

Catalog administrators are trusted to configure immutable warehouse/adoption roots, credentials and recovery principals. Ordinary schema/table requests cannot override these. Use existing secret references and masked properties; disable credential backfill. Reader resources must be isolated per catalog even when classloaders are pooled.

Normal publication belongs to the current operation actor after its completed native transaction, with exact durable operation and native commit evidence. Recovery requires authenticated durable HANDOFF attestation tied to the operation and writer run; it records the platform controller's assertion that the writer was stopped and restart suppressed. It is not an independently verified S3 fence. Privileged paths reject anonymous identities and disabled authorization.

STATUS provides an authenticated receipt for a known identifier through the existing alter-table endpoint, including pending entries hidden from normal discovery. ADOPT is privileged, observational registration of a known admitted native location; it reads native schema/UUID/protocol and does not rewrite Delta history or mutate Glue. Automatic Glue enumeration is outside this increment. Adoption requires controlled cutover and platform admission for location ownership; per-row CAS does not enforce global alias uniqueness.

Checkpoint-aware metadata reading must replace the prototype's bounded JSON-history reader before production adoption. Native schema metadata is preserved losslessly alongside its Gravitino type projection. Kernel compatibility and feature coverage require explicit tests against both supported patches; unsupported protocols fail explicitly, never silently downgrade.

Choose a single isolated, package-private Kernel 3.3 metadata adapter. Public snapshots do not expose native table ID/configuration/protocol, while a public checkpoint parser would duplicate checkpoint discovery and replay. Guard exact API/defaults runtime versions and implementation class; copy values immediately without exposing internal types. This intentionally replaces the previous bounded milestone's public-Kernel-only design restriction, which was not a user restriction. Any Kernel upgrade requires renewed compatibility tests.

Own a fresh Hadoop UGI Subject per provider instance and run all eager and lazy Kernel I/O within its scope. Drain reads before closing only that Subject's cached filesystems. This addresses pooled catalog classloaders without core edits or global Hadoop mutation. Raw native configuration is internal and may contain sensitive operational values; publish only an explicit safe subset.

## Spark behavior

Keep native DeltaTableV2 and native write paths. An inert staged descriptor may expose schema and capabilities before reserving. At query writer construction, reserve with the resolved native query-schema normalization; at schema-only commit, reserve with the exact declared schema. Existing schema-only CREATE OR REPLACE uses native stageReplace after authoritative Gravitino identity lookup and expected-ID reservation. Do not blanket-normalize schema-only NOT NULL constraints.

Operator status/recovery must use production client transport, not raw-store fixtures. Resolve current request identity through the existing catalog manager, without closing shared clients or pinning the first actor. Qualify production jars in fresh Spark JVMs and a real provider server; previous test classpaths do not establish distribution correctness.

## Execution sequence

Execute independently reviewable subplans continuously: shared protocol extraction; native metadata/storage; durable provider; optional Spark adapter/runtime; packaged four-pair acceptance. Each subplan records exact interfaces and meaningful verification before its consumer begins. Follow with the remaining full Delta feature matrix, Ranger 2.8.0 downloaded-policy enforcement, then isolated catalog-demo integration. Do not label intermediate artifact qualification as full Delta support.

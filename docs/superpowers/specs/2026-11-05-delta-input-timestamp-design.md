<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements. See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License. You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Governed input timestamp projection

## Purpose and sequence

Spark Hub adds `__arkham_loaded_at = current_timestamp()` before writing a
dataset. The existing governed named APPEND rejects that expression. Enable
this required native Spark projection first, with its original source policies
and native Delta write behavior intact. This is the first deliverable of input
and transformation support, not completion of JDBC ingestion or Spark Hub M2.

The user confirmed that JDBC sources can be either registered in Gravitino or
externally authenticated by Spark Hub. Both routes remain in the roadmap:

1. This native timestamp projection over existing named Delta inputs.
2. Registered JDBC using Gravitino's existing JDBC catalog, `SparkJdbcTable`,
   configuration and credential vending, with explicit source authorization,
   filters/masks and native V2 scan continuation.
3. External JDBC using the existing trusted upstream source owner where possible,
   with an issued origin for the exact native Spark read and governed Delta sink.
4. Further input/transform families and overwrite/full refresh in separate plans.

Neither JDBC route is implemented by this plan. An arbitrary supplied DataFrame
does not acquire external-source authority through a boolean or matching schema.
Files/manual uploads, union/window/hash transforms, checkpoint materialization,
snapshot MERGE and CDF progress-state changes retain their separate contracts.

## Contract

Support a direct built-in `current_timestamp()` alias in Project over an already
supported named snapshot query, including multiple timestamp output columns.
Column names are unrestricted; `_arkham_loaded_at` and `__arkham_loaded_at` are
actual Hub examples. Native DataFrame `withColumn`/`select` and SQL expression
construction resolve to the same admitted expression. Existing append grammar,
required source column demand, target UPDATE/ALTER checks, row filters, masks,
partitioning and native write options remain binding.

All timestamp occurrences in one completed query use the same captured value.
Receipt rechecks, optimizer replay, native V1 input/helper/commit and maintenance
must never sample it again. A separately issued query uses its own capture;
repeated actions on a reused Spark QueryExecution must follow characterized
native Spark lifecycle, not an invented session-wide clock or freshness promise.

Only the exact stock zero-argument CurrentTimestamp class is admitted, directly
under an Alias in Project. Time predicates, nested time arithmetic/casts,
CurrentDate, LocalTimestamp, custom subclasses/UDFs and arbitrary foldable
expressions are outside this increment. Existing attributes referring to a
previously projected timestamp use the ordinary supported expression grammar.
The policy/CDF/MERGE expression grammar is unchanged.

## Implementation

Use a small input-projection helper shared by pending ordinary admission and
the append input admission. Keep shared policy expression validation closed by
default. The pending structural binding must represent exact CurrentTimestamp
without evaluating it; this structural case grants no executable authority.

OrdinaryRead.normalize already owns the transition from original pending intent
to a fully authorized decision receipt. After validating/cloning the original
and clearing pending tags, freeze query time with stock Spark ComputeCurrentTime
once, then authorize the resulting literal tree. Preserve output ExprIds,
metadata and exact original intent validation. The authored/frozen relationship
must be checked at issuance; the normal receipt owns the frozen executable tree.
Do not add an independently renewable permission or duplicate receipt framework.

AppendV1Input retains its original authored-demand extraction before normalization.
Hidden predicate/join column demand cannot disappear because of constant folding.
No new source kind or weakening of registration/native/physical proof is needed.
All later native preparation starts from the frozen analyzed query. Confirm the
stock rule and lifecycle using pinned engine code and executable tests.

## Verification and closure

Installed JVM tests prove actual named insertInto and existing-table saveAsTable
append masked/filtered rows with exact non-null timestamp fields, two equal
timestamp columns and one native target version. Include AQE/exchange and a
partitioned target using already qualified settings. Test hidden-column denial,
target denial, late source policy/registration changes, frozen-literal substitution,
foreign expression rejection and unchanged policy/CDF/MERGE grammar. Timestamp
capture adds no authorization bypass for columnless or unsupported inputs.

Test native query-time lifecycle without wall-clock sleeps or asserting distinct
timestamps from two back-to-back queries. Receipt replay must retain the literal;
a fresh owner must invoke the native capture independently.

Qualify all four Spark 3.5.4/3.5.6 and Delta 3.3.0/3.3.2 pairs using focused JVM
coverage and classic PySpark named append against actual Ranger server/client
2.8.0, Gravitino and MinIO. Reuse existing fixture and source-bound assembly
patterns; preserve original evidence. Lead pair proves masks/filters, repeated
timestamps, target denial and source revocation; remaining pairs prove the new
projection and a denial. A failure after native publication remains a committed
outcome and must not cause automatic replay.

No Gravitino core/API/server/storage-model changes, engine fork, manual
transaction, raw-path destination, Glue fallback or automatic write replay.
No new public DataFrame API is required. Do not disable required existing Spark
settings to turn a failed qualification green. Full Hub optimizeWrite/autoCompact
coverage is not implied by this expression addition. Preserve closed milestone
evidence; run relevant current controls rather than unrelated historical matrices.

## Acceptance boundary

Close and push only when implementation, task reviews and live version evidence
pass. Report the exact supported projection and remaining JDBC/transform work.
Catalog-demo integration remains deferred. This plan does not reopen ordinary
CREATE or implement overwrite/full refresh.

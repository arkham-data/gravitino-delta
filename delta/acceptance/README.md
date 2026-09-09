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

# Packaged Delta runtime acceptance

This opt-in harness starts an owned packaged Gravitino server and real `spark-submit`
processes against two existing, independently owned S3 endpoints. It does not start,
stop, or clean up containers. Every run needs a **new output directory** and unique
S3 prefix. Native objects and original failed process logs are retained.

```bash
python3 delta/acceptance/run.py --inputs /private/path/inputs.json --output /private/path/new-run
python3 -m unittest discover -s delta/acceptance -v
```

The private JSON input has this shape (placeholders are not usable credentials):

```json
{
  "javaHome": "/path/to/jdk17",
  "sparkEnvironmentsFile": "/private/path/spark-environments.json",
  "serverKernelVersion": "3.3.2",
  "prefix": "unique-owned-acceptance-prefix",
  "serviceAdmin": "owned-local-admin",
  "primary": {
    "endpoint": "http://127.0.0.1:9000",
    "region": "us-east-1",
    "bucket": "owned-bucket",
    "credentialsFile": "/private/path/primary.env"
  },
  "secondary": {
    "endpoint": "http://127.0.0.1:9001",
    "region": "us-east-1",
    "bucket": "owned-bucket",
    "credentialsFile": "/private/path/secondary.env"
  }
}
```

Credential files contain `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD` assignments.
They are parsed as data, never sourced by a shell. Output directories are private;
Spark properties and H2 state contain local credentials. Do not commit them.
Local simple authentication identifies test actors; this is not authentication
hardening or Ranger enforcement.

The Spark environments file contains `environments`, an array of exactly two
objects with `spark` (`3.5.4` or `3.5.6`), `spark_home`, `spark_submit`, and `venv`
(the Python 3.11 environment containing that distribution). Both distributions
must provide Hadoop 3.3.4 and Scala 2.12.18. The selected added jars are native
Delta Spark/storage 3.3.0 or 3.3.2, Hadoop AWS 3.3.4, its production-POM AWS SDK
bundle 1.12.262, and one common optional Gravitino runtime binary. Kernel 3.3.2
is supplied only by the isolated server provider package; its Hadoop is 3.3.6.

The focused preparation task copies the ordinary server `runtimeClasspath`,
server jar, production provider package, real launch scripts, logging template,
and shipped H2 schema scripts. It never invokes root `compileDistribution`,
fixture compilation, or test classpaths:

```bash
JAVA_HOME=/path/to/jdk17 ./gradlew -I delta/acceptance/prepare.gradle \
  prepareDeltaAcceptance -PdeltaAcceptanceOutput=/private/path/new-package \
  -PdeltaKernelVersion=3.3.2 -PdeltaVersion=3.3.0 -PdeltaSparkVersion=3.5.4 -PskipITs
```

For the second native package, `-PdeltaAcceptanceRuntime=/path/to/first/runtime.jar`
reuses the identical production runtime. The runner supplies this automatically.
The Hadoop AWS SDK version is taken from Hadoop 3.3.4's published production POM;
no fixture Hadoop or Scala jars replace distribution dependencies.

Development-only input options are `representativeOnly` (first pair), `processOnly`
(skip the writer matrix), `processStages` (a subset of `recovery`, `isolation`,
`authority`), `diagnosticOnly` (parser diagnostics with `processOnly`), and `packages`
(a mapping from each Delta version to an already prepared immutable package).
The first package's runtime is selected for every driver. These options narrow
execution and must not be described as a full matrix result.

The harness snapshots its own submitted sources, hashes actual driver and server
jars, records each child PID/exit, retains class-loading logs, and saves exact
object manifests and native operation actions. Matrix seed and writer JVMs are
separate; stopped-server, read-only H2 snapshots verify internal column IDs that
public `ColumnDTO` intentionally omits. SQL identifiers are individually quoted,
including the table named `history`, which is a Delta parser keyword.

Recovery uses the real journal, ordinary authenticated API, and an external
HTTP proxy. The adapter adds no application retry, but the underlying HTTP client
can retry PUT after connection loss. The proxy records every attempt. Reserve
response loss retains an original nonzero exit; publication blocking rejects all
PUT attempts until the original driver and proxy handlers drain. Only then does
an operator STATUS/reread/HANDOFF/PUBLISH or ABORT run. Completed publication loss
may recover through a transport PUT retry and a later STATUS-only driver. No
recovery step replays POST. Drain stops and joins the accept loop and waits for
all accepted connections, including partially parsed requests. A forwarded request
with unknown upstream completion prevents recovery even after its local handler
exits. Waiting for a timeout never establishes request drain.

Two same-session named catalogs use different prefixes on the primary endpoint.
Incompatible native Spark endpoint profiles run in separate drivers. Provider
isolation uses the same authority **and path** on different endpoints, simultaneous
loaded contexts, existing production cleanup logs, and a secondary read after
primary close completion. It does not claim session-global Spark routing isolation.

Native-ID quarantine appends a metadata-only identity fault to one sacrificial
owned table after its original driver exits; original bytes are archived and no
objects are deleted. It is external-log fault injection, not a successful native
lifecycle operation or observational adoption. Full Delta conformance, downstream
owner-hook fault injection, exact CAS barriers, Ranger 2.8 enforcement, and the
catalog demo retain their separately documented evidence and follow-up scopes.

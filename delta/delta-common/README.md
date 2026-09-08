<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Delta Common

`delta-common` owns the stable
`org.apache.gravitino.catalog.lakehouse.delta.registration` package. It is a small runtime artifact
for the qualified Delta registration protocol and can be shared by server and Spark components
without bringing either runtime into the other.

The artifact currently contains the operation marker wire format, Delta commit action reader,
immutable commit evidence, and their JSON support. Its production dependency is Jackson Databind.
It does not contain a catalog provider, Spark, Delta Kernel, Hadoop, Gravitino core, or test fixtures.

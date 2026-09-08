/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.catalog.lakehouse.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.junit.jupiter.api.Test;

class TestDeltaLocationPolicy {
  @Test
  void aliasesAndSegmentBoundaries() {
    DeltaLocationPolicy policy =
        new DeltaLocationPolicy("s3://WAREHOUSE/tables/", List.of("s3://WAREHOUSE/allowed/"));
    assertEquals(
        "s3a://warehouse/tables/sales/42",
        policy.newTableLocation(NameIdentifier.of("m", "c", "sales", "t"), 42));
    assertEquals(
        "s3a://warehouse/allowed/t", policy.admittedExistingLocation("s3://warehouse/allowed/t/"));
    assertThrows(
        IllegalArgumentException.class,
        () -> policy.admittedExistingLocation("s3a://warehouse/allowed2/t"));
    assertThrows(
        IllegalArgumentException.class,
        () -> policy.admittedExistingLocation("s3a://other/allowed/t"));
  }

  @Test
  void rejectsUntrustedUrisAndIdentifiers() {
    for (String uri :
        List.of(
            "file:/tmp/t",
            "s3a://user@bucket/t",
            "s3a://bucket:80/t",
            "s3a://bucket/a?b",
            "s3a://bucket/a#b",
            "s3a://bucket/a%2fb",
            "s3a://bucket/a/../b",
            "s3a://bucket/a/./b",
            "s3a://bucket/a//b",
            "s3a://bucket//",
            "s3a:///a",
            "s3a://bad_bucket/a")) {
      assertThrows(IllegalArgumentException.class, () -> new DeltaLocationPolicy(uri, List.of()));
    }
    DeltaLocationPolicy policy = new DeltaLocationPolicy("s3a://bucket/root", List.of());
    assertThrows(
        IllegalArgumentException.class,
        () -> policy.newTableLocation(NameIdentifier.of("m", "c", "../evil", "t"), 1));
    assertThrows(
        IllegalArgumentException.class,
        () -> policy.newTableLocation(NameIdentifier.of("m", "c", "s", "t"), 0));
    assertThrows(
        IllegalArgumentException.class,
        () -> policy.admittedExistingLocation("s3a://bucket/root/t"));
  }

  @Test
  void preservesSafeSchemaSegmentsWithoutNewIdentifierGrammar() {
    DeltaLocationPolicy policy =
        new DeltaLocationPolicy("s3a://bucket/root", List.of("s3a://bucket/root"));
    for (String schema :
        List.of(
            "sales.v2",
            "-sales",
            "sales~archive",
            "ventes-é",
            "销售",
            "sales😀",
            "e\u0301",
            "sales:archive",
            "sales@archive")) {
      String expected = "s3a://bucket/root/" + schema + "/42";
      assertEquals(expected, policy.newTableLocation(NameIdentifier.of("m", "c", schema, "t"), 42));
      assertEquals(expected, policy.admittedExistingLocation(expected));
    }
  }

  @Test
  void rejectsSchemaSeparatorsEscapesDotSegmentsAndMalformedText() {
    DeltaLocationPolicy policy =
        new DeltaLocationPolicy("s3a://bucket/root", List.of("s3a://bucket/root"));
    for (String schema :
        List.of(
            ".",
            "..",
            "/",
            "sales/part",
            "sales/",
            "/sales",
            "sales\\part",
            "sales%2fpart",
            "sales?part",
            "sales#part",
            "sales\u0000part",
            "sales\ud800",
            "sales\udc00",
            "sales\ud800part")) {
      assertThrows(
          IllegalArgumentException.class,
          () -> policy.newTableLocation(NameIdentifier.of("m", "c", schema, "t"), 42));
    }
  }
}

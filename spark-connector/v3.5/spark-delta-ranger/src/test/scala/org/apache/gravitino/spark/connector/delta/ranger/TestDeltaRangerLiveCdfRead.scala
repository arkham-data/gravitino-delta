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

package org.apache.gravitino.spark.connector.delta.ranger

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.collection.JavaConverters._

/** Evidence enrichment must support the immutable maps returned by actual collections. */
class TestDeltaRangerLiveCdfRead {

  /** Preserve the observed result while adding explicit requested bounds to a new document. */
  @Test
  def immutableCollectedEvidenceRetainsRowsAndOutcome(): Unit = {
    val original =
      Map[String, Any]("outcome" -> "success", "rows" -> Vector.empty[Any].asJava).asJava
    val enriched = DeltaRangerLiveCdfRead.withBounds(original, 4L, 4L)
    assertFalse(original.containsKey("bounds"))
    assertEquals("success", enriched.get("outcome"))
    assertSame(original.get("rows"), enriched.get("rows"))
    val json = new ObjectMapper()
    val document = json.readTree(json.writeValueAsBytes(enriched))
    assertEquals(4L, document.path("bounds").get(0).asLong())
    assertEquals(4L, document.path("bounds").get(1).asLong())
    assertEquals(0, document.path("rows").size())
  }
}

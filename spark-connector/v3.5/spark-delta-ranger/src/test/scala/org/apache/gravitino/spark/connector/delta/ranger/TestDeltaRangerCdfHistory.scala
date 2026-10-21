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

import org.apache.spark.sql.delta.actions.Action
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** Completeness must include the terminal version without reading beyond a fixed end. */
class TestDeltaRangerCdfHistory {
  @Test
  def completeInclusiveIntervalStopsBeforeAnotherHasNext(): Unit = {
    var position = 0
    val input = new Iterator[(Long, Seq[Action])] {
      override def hasNext: Boolean = {
        assertTrue(position < 3, "must not inspect history beyond requested end")
        true
      }
      override def next(): (Long, Seq[Action]) = {
        val current = position
        position += 1
        (10L + current, Seq.empty)
      }
    }
    assertEquals(Vector(10L, 11L, 12L), DeltaRangerCdfHistory.consume(input, 10L, 12L).map(_._1))
  }

  @Test
  def missingFirstIntermediateAndTerminalVersionsReject(): Unit = {
    Seq(Vector(11L, 12L), Vector(10L, 12L), Vector(10L, 11L)).foreach { versions =>
      assertThrows(
        classOf[SecurityException],
        () => {
          DeltaRangerCdfHistory.consume(versions.iterator.map(_ -> Seq.empty[Action]), 10L, 12L); ()
        }
      )
    }
  }

  @Test
  def terminalLongMaximumDoesNotOverflow(): Unit = {
    assertEquals(
      Vector(Long.MaxValue),
      DeltaRangerCdfHistory
        .consume(Iterator.single(Long.MaxValue -> Seq.empty[Action]), Long.MaxValue, Long.MaxValue)
        .map(_._1)
    )
  }
  @Test
  def requestOptionsAreClosedAndBoundsAreExplicit(): Unit = {
    val valid = Map("readChangeFeed" -> "true", "startingVersion" -> "0", "endingVersion" -> "10")
    assertEquals(DeltaRangerCdfRead.Request(0L, 10L), DeltaRangerCdfRead.parse(valid, 10L))
    assertEquals(
      DeltaRangerCdfRead.Request(0L, 10L),
      DeltaRangerCdfRead.parse(
        valid.map { case (key, value) =>
          key.toUpperCase(java.util.Locale.ROOT) -> value
        },
        10L
      )
    )
    val invalid = Seq(
      valid + ("READCHANGEFEED" -> "true"),
      valid - "startingVersion",
      valid - "endingVersion",
      valid + ("path" -> "/tmp/table"),
      valid + ("unknown" -> "true"),
      valid + ("versionAsOf" -> "1"),
      valid + ("timestampAsOf" -> "2026-10-20"),
      valid + ("startingTimestamp" -> "2026-10-20"),
      valid + ("endingTimestamp" -> "2026-10-20"),
      valid + ("startingVersion" -> "-1"),
      valid + ("startingVersion" -> "11"),
      valid + ("endingVersion" -> "11"),
      valid + ("endingVersion" -> "9223372036854775808"),
      valid + ("readChangeFeed" -> "false"),
      valid + ("startingVersion" -> "1.0")
    )
    invalid.foreach(options =>
      assertThrows(
        classOf[SecurityException],
        () => { DeltaRangerCdfRead.parse(options, 10L); () },
        options.toString
      )
    )
  }

}

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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import org.apache.spark.sql.Row
import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations}
import org.apache.spark.sql.delta.actions.{
  Action,
  AddCDCFile,
  AddFile,
  Metadata,
  Protocol,
  RemoveFile
}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
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

  @Test
  def completeInclusiveIntervalStopsAtEnd(): Unit = {
    import DeltaRangerCdfReadFixture._
    var version = -1L
    var expected = Vector.empty[Row]
    withGoverned(spark => {
      enable(spark)
      version = append(spark)
      expected = oracle(spark, version, version).collect().toVector
      append(spark, "SELECT 8L id, 'later' secret, 'EU' region")
      append(spark, "SELECT 9L id, 'head' secret, 'EU' region")
      // Keep authoritative current metadata reconstructible without the unrelated version 3 log.
      val log = DeltaLog.forTable(spark, location)
      log.checkpoint(log.unsafeVolatileSnapshot)
    }) { spark =>
      spark.table("a.db.t").collect()
      // Head 4 has a native checkpoint; version 3 is unrelated to the complete [2, 2] interval.
      Files.delete(logFile(version + 1L))
      val frame = read(spark, version, version)
      assertEquals(rowMultiset(expected), rowMultiset(frame.collect().toVector))
      assertTrue(frame.collect().forall(_.getLong(4) == version))
    }
  }

  @ParameterizedTest
  @ValueSource(strings =
    Array(
      "missing-start",
      "missing-middle",
      "missing-end",
      "corrupt-start",
      "corrupt-middle",
      "corrupt-end"
    )
  )
  def requestedLogFailuresAreNotEmptySuccess(fault: String): Unit = {
    import DeltaRangerCdfReadFixture._
    var start = -1L
    var end = -1L
    withGoverned(spark => {
      start = enable(spark)
      append(spark)
      end = append(spark)
    }) { spark =>
      spark.table("a.db.t").collect()
      val version =
        if (fault.endsWith("start")) start else if (fault.endsWith("middle")) start + 1L else end
      if (fault.startsWith("missing")) Files.delete(logFile(version))
      else {
        Files.write(logFile(version), "{broken-json\n".getBytes(StandardCharsets.UTF_8))
        Files.deleteIfExists(logFile(version).resolveSibling(f".$version%020d.json.crc"))
      }
      assertThrows(classOf[Exception], () => { read(spark, start, end).collect(); () }, fault)
    }
  }

  @ParameterizedTest
  @ValueSource(strings =
    Array(
      "missing-cdc",
      "corrupt-cdc",
      "missing-removed",
      "corrupt-removed",
      "missing-add",
      "corrupt-add"
    )
  )
  def historicalDataFailuresAreNotEmptySuccess(fault: String): Unit = {
    import DeltaRangerCdfReadFixture._
    var version = -1L
    var required: Path = null
    withGoverned(spark => {
      enable(spark)
      if (fault.endsWith("cdc"))
        spark.sql(s"UPDATE delta.`$location` SET secret = 'updated' WHERE id = 1").collect()
      else if (fault.endsWith("removed")) spark.sql(s"DELETE FROM delta.`$location`").collect()
      else append(spark)
      version = head(spark)
      val paths = actions(version).collect {
        case action: AddCDCFile if fault.endsWith("cdc")     => action.path
        case action: RemoveFile if fault.endsWith("removed") => action.path
        case action: AddFile if fault.endsWith("add")        => action.path
      }
      assertFalse(paths.isEmpty, fault)
      assertFalse(oracle(spark, version, version).collect().isEmpty, fault)
      required = Paths.get(location).resolve(paths.head)
    }) { spark =>
      // Prepare the actual installed producer first, proving action-time inventory validation too.
      val frame = read(spark, version, version)
      frame.queryExecution.executedPlan
      if (fault.startsWith("missing")) Files.delete(required)
      else Files.write(required, "not parquet".getBytes(StandardCharsets.UTF_8))
      assertThrows(classOf[Exception], () => { frame.collect(); () }, fault)
      assertThrows(classOf[Exception], () => { read(spark, version, version).collect(); () }, fault)
    }
  }

  @ParameterizedTest
  @ValueSource(strings =
    Array(
      "spark.sql.files.ignoreMissingFiles",
      "spark.sql.files.ignoreCorruptFiles",
      "spark.databricks.delta.changeDataFeed.timestampOutOfRange.enabled",
      "spark.databricks.delta.changeDataFeed.unsafeBatchReadOnIncompatibleSchemaChanges.enabled"
    )
  )
  def permissiveSettingsCannotTurnHistoryFailureIntoSuccess(setting: String): Unit = {
    import DeltaRangerCdfReadFixture._
    var version = -1L
    withGoverned(spark => { enable(spark); version = append(spark) }) { spark =>
      spark.conf.set(setting, "true")
      val failure = assertThrows(
        classOf[SecurityException],
        () => { read(spark, version, version).collect(); () }
      )
      assertTrue(failure.getMessage.contains("permissive session setting"), failure.getMessage)
    }
  }

  @Test
  def disabledCdfGapAndOutOfRangeFail(): Unit = {
    import DeltaRangerCdfReadFixture._
    var start = -1L
    var end = -1L
    withGoverned(spark => {
      start = enable(spark)
      append(spark)
      spark
        .sql(
          s"ALTER TABLE delta.`$location` SET TBLPROPERTIES ('delta.enableChangeDataFeed'='false')"
        )
        .collect()
      append(spark)
      enable(spark)
      end = append(spark)
    }) { spark =>
      val failure =
        assertThrows(classOf[SecurityException], () => { read(spark, start, end).collect(); () })
      assertTrue(
        failure.getMessage.contains("CDF disabled within requested interval"),
        failure.getMessage
      )
      assertThrows(classOf[SecurityException], () => { read(spark, end, end + 1L).collect(); () })
      assertEquals(1, read(spark, end, end).collect().length)
    }
  }

  @ParameterizedTest
  @ValueSource(strings =
    Array(
      "schema",
      "partition",
      "column-mapping",
      "deletion-vectors",
      "row-tracking",
      "protocol",
      "identity",
      "collision",
      "add-row-tracking",
      "remove-row-tracking",
      "add-deletion-vector",
      "remove-deletion-vector"
    )
  )
  def historicalProfileCannotHideBehindCurrentMetadata(change: String): Unit = {
    import DeltaRangerCdfReadFixture._
    var start = -1L
    var end = -1L
    var original: Metadata = null
    var originalProtocol: Protocol = null
    withGoverned(spark => {
      start = enable(spark)
      append(spark)
      spark.sql(s"DELETE FROM delta.`$location`").collect()
      end = append(spark)
      val snapshot = DeltaLog.forTable(spark, location).update()
      original = snapshot.metadata
      originalProtocol = snapshot.protocol
    }) { spark =>
      spark.table("a.db.t").collect()
      // Fault a genuine intermediate commit only after native current metadata has been observed.
      // The installed reader must inspect these historical actions, not just that current snapshot.
      val version = if (change.startsWith("remove")) start + 2L else start + 1L
      val entries = actions(version)
      val altered = change match {
        case "schema" =>
          entries :+ original.copy(schemaString = original.schema.add("extra", "string").json)
        case "partition" => entries :+ original.copy(partitionColumns = Seq("region"))
        case "identity"  => entries :+ original.copy(id = "historical-foreign-identity")
        case "collision" =>
          entries :+ original.copy(schemaString =
            original.schema.add("_COMMIT_VERSION", "long").json
          )
        case "protocol" =>
          entries :+ originalProtocol.copy(
            minWriterVersion = 7,
            writerFeatures =
              Some(Set("changeDataFeed", "appendOnly", "invariants", "domainMetadata"))
          )
        case "column-mapping" =>
          entries :+ original.copy(configuration =
            original.configuration + ("delta.columnMapping.mode" -> "name")
          )
        case "deletion-vectors" =>
          entries :+ original.copy(configuration =
            original.configuration + ("delta.enableDeletionVectors" -> "true")
          )
        case "row-tracking" =>
          entries :+ original.copy(configuration =
            original.configuration + ("delta.enableRowTracking" -> "true")
          )
        case "add-row-tracking" =>
          entries.map { case a: AddFile => a.copy(baseRowId = Some(0L)); case a => a }
        case "remove-row-tracking" =>
          entries.map {
            case r: RemoveFile => r.copy(defaultRowCommitVersion = Some(version)); case a => a
          }
        case "add-deletion-vector" =>
          entries.map {
            case a: AddFile =>
              a.copy(deletionVector =
                org.apache.spark.sql.delta.actions.DeletionVectorDescriptor("i", "abc", None, 1, 1L)
              ); case a => a
          }
        case "remove-deletion-vector" =>
          entries.map {
            case r: RemoveFile =>
              r.copy(deletionVector =
                org.apache.spark.sql.delta.actions.DeletionVectorDescriptor("i", "abc", None, 1, 1L)
              ); case a => a
          }
      }
      assertNotEquals(entries, altered)
      replaceActions(version, altered)
      val failure = assertThrows(
        classOf[SecurityException],
        () => { read(spark, start, end).collect(); () },
        change
      )
      assertTrue(failure.getMessage.contains("historical"), failure.getMessage)
    }
  }

  @Test
  def nativeSchemaChangeAndRestoreStillFails(): Unit = {
    import DeltaRangerCdfReadFixture._
    var start = -1L
    var end = -1L
    withGoverned(spark => {
      start = enable(spark)
      spark.sql(s"ALTER TABLE delta.`$location` ADD COLUMNS (extra STRING)").collect()
      spark
        .sql("SELECT 7L id, 'restored' secret, 'US' region")
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(location)
      end = head(spark)
    }) { spark =>
      val failure =
        assertThrows(classOf[SecurityException], () => { read(spark, start, end).collect(); () })
      assertTrue(
        failure.getMessage.contains("historical schema or protocol changed"),
        failure.getMessage
      )
      assertEquals(1, read(spark, end, end).filter("_change_type = 'insert'").collect().length)
    }
  }

  @ParameterizedTest
  @ValueSource(strings =
    Array("_change_type", "_commit_version", "_commit_timestamp", "_COMMIT_VERSION")
  )
  def nativeMetadataNameCollisionsFail(column: String): Unit = {
    import DeltaRangerCdfReadFixture._
    withGoverned(spark => {
      // Native Delta rejects enabling CDF with these columns. A malformed native metadata action
      // models an externally supplied log; admission must reject it even for an empty interval.
      val snapshot = DeltaLog.forTable(spark, location).update()
      replaceActions(
        0L,
        actions(0L).map {
          case m: Metadata =>
            m.copy(
              schemaString = m.schema.add(column, "string").json,
              configuration = m.configuration + ("delta.enableChangeDataFeed" -> "true")
            )
          case a => a
        }
      )
      Files.deleteIfExists(logFile(0L).resolveSibling("00000000000000000000.crc"))
    }) { spark =>
      assertThrows(classOf[Exception], () => { read(spark, 0L, 0L).collect(); () }, column)
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("append", "schema", "identity", "profile"))
  def retainedIntervalRevalidatesAfterHeadAdvances(change: String): Unit = {
    import DeltaRangerCdfReadFixture._
    var version = -1L
    var tail: Path = null
    var bytes: Array[Byte] = null
    var expected = Vector.empty[Row]
    withGoverned(spark => {
      enable(spark)
      version = append(spark)
      expected = oracle(spark, version, version).collect().toVector
      change match {
        case "append" => append(spark, "SELECT 8L id, 'later' secret, 'EU' region")
        case "schema" =>
          spark.sql(s"ALTER TABLE delta.`$location` ADD COLUMNS (extra STRING)").collect()
        case "identity" =>
          val txn = DeltaLog.forTable(spark, location).startTransaction(None)
          txn.commit(
            Seq(txn.metadata.copy(id = "changed-native-identity")),
            DeltaOperations.ManualUpdate
          )
        case "profile" =>
          spark
            .sql(s"ALTER TABLE delta.`$location` SET TBLPROPERTIES ('delta.appendOnly'='true')")
            .collect()
      }
      assertEquals(version + 1L, head(spark))
      tail = logFile(version + 1L)
      bytes = Files.readAllBytes(tail)
      Files.delete(tail)
      Files.deleteIfExists(tail.resolveSibling(f"${version + 1L}%020d.crc"))
      Files.deleteIfExists(tail.resolveSibling(f".${version + 1L}%020d.json.crc"))
    }) { spark =>
      val frame = read(spark, version, version)
      assertEquals(rowMultiset(expected), rowMultiset(frame.collect().toVector))
      // Publish exactly the previously generated native fixture commit; no governed writer exists.
      Files.write(tail, bytes)
      if (change == "append") {
        assertEquals(rowMultiset(expected), rowMultiset(frame.collect().toVector))
        assertEquals(
          rowMultiset(expected),
          rowMultiset(read(spark, version, version).collect().toVector)
        )
      } else assertThrows(classOf[SecurityException], () => { frame.collect(); () }, change)
    }
  }

}

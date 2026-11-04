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

import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import org.apache.ranger.plugin.service.RangerBasePlugin
import scala.collection.JavaConverters._
import org.apache.gravitino.rel.Column
import org.apache.gravitino.spark.connector.SparkTypeConverter
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{Action, Metadata}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** Actual named SQL and companion evolution through installed native ownership. */
class TestDeltaRangerMergeSchemaEvolution {
  private val input = "SELECT id, secret, region, concat(secret, '-extra') AS extra " +
    "FROM table_changes('a.db.t', 0, 4) WHERE _commit_version = 4 " +
    "AND _change_type IN ('insert', 'update_postimage')"

  @Test
  def installedSqlAddsPayloadInOneNativeCommit(): Unit = positive(api = false)

  @Test
  def companionStarsAddPayloadInOneNativeCommit(): Unit = positive(api = true)

  private def positive(api: Boolean): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies((spark, plugin) => {
      val settings = Map(
        "spark.databricks.delta.schema.autoMerge.enabled" -> "true",
        "spark.sql.constraintPropagation.enabled" -> "false"
      )
      val before = settings.keys.map(k => k -> spark.conf.getOption(k)).toMap
      settings.foreach { case (k, v) => spark.conf.set(k, v) }
      try {
        new TestDeltaRangerMergePolicies().publish(plugin, alter = true)
        refresh(spark)
        val log = DeltaLog.forTable(spark, NativeSelectFixture.locations("b"))
        val base = log.update().metadata
        if (api) {
          val source = spark.sql(input).as("s")
          GravitinoDeltaTable
            .forName(spark, "b.db.t")
            .alias("t")
            .merge(source, "t.id = s.id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        } else
          spark
            .sql(
              s"MERGE INTO b.db.t t USING ($input) s ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
            )
            .collect()
        val after = log.update()
        assertEquals(1L, after.version)
        assertEquals(Seq("id", "secret", "region", "extra"), after.schema.fieldNames.toSeq)
        assertEquals(base.copy(schemaString = after.metadata.schemaString), after.metadata)
        assertEquals(
          Set(
            Row(1L, "changed", "keep", "changed-extra"),
            Row(2L, "b-two", "drop", null),
            Row(3L, null, "keep", null),
            Row(4L, "new", "keep", "new-extra")
          ),
          spark.table("b.db.t").collect().toSet
        )
        val actions = PartitionedMergeFixture.actions(NativeSelectFixture.locations("b"), 1L)
        assertEquals(1, actions.count(_.isInstanceOf[Metadata]))
        assertEquals(after.metadata, actions.collect { case m: Metadata => m }.head)
        retain(spark, if (api) "companion-stars" else "sql-stars")
        assertFalse(
          Files.exists(
            Paths.get(NativeSelectFixture.locations("b"), "_delta_log", "00000000000000000002.json")
          )
        )
      } finally before.foreach { case (k, v) => v.fold(spark.conf.unset(k))(spark.conf.set(k, _)) }
    })

  private def query(payload: String = input, target: String = "b"): String =
    s"MERGE INTO $target.db.t t USING ($payload) s ON t.id = s.id " +
      "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"

  private def configured(
      test: (SparkSession, RangerBasePlugin) => Unit,
      prepare: SparkSession => Unit = _ => (),
      options: Map[String, String] = Map.empty,
      startup: Map[String, String] = Map.empty
  ): Unit = new TestDeltaRangerInstalledMerge().withPolicies(
    (spark, plugin) => {
      val settings = Map(
        "spark.databricks.delta.schema.autoMerge.enabled" -> "true",
        "spark.sql.constraintPropagation.enabled" -> "false"
      ) ++ options
      val old = settings.keys.map(k => k -> spark.conf.getOption(k)).toMap
      settings.foreach { case (k, v) => spark.conf.set(k, v) }
      try {
        new TestDeltaRangerMergePolicies().publish(plugin, alter = true)
        refresh(spark); refresh(spark, "a")
        test(spark, plugin)
      } finally old.foreach { case (k, v) => v.fold(spark.conf.unset(k))(spark.conf.set(k, _)) }
    },
    prepareTarget = prepare,
    startupOptions = startup
  )

  @Test
  def enabledMatchingSchemaNeedsNoAlter(): Unit = configured((spark, plugin) => {
    new TestDeltaRangerMergePolicies().publish(plugin)
    spark.sql(query(input.replace(", concat(secret, '-extra') AS extra", ""))).collect()
    assertEquals(1L, TestDeltaRangerMergeSource.version("b"))
    assertFalse(
      PartitionedMergeFixture
        .actions(NativeSelectFixture.locations("b"), 1L)
        .exists(_.isInstanceOf[Metadata])
    )
    assertEquals(3, spark.table("b.db.t").schema.length)
  })

  @Test
  def disabledUnusedExtraStillRequiresOriginalSelect(): Unit = configured(
    (spark, plugin) => {
      val payload = input.replace("concat(secret, '-extra')", "_commit_timestamp")
      val files = PartitionedMergeFixture.files()
      new TestDeltaRangerMergePolicies()
        .publish(plugin, sourceDeniedSelect = Seq("_commit_timestamp"))
      assertThrows(classOf[SecurityException], () => spark.sql(query(payload)))
      assertEquals(files, PartitionedMergeFixture.files())
      new TestDeltaRangerMergePolicies().publish(plugin)
      spark.sql(query(payload)).collect()
      assertEquals(1L, TestDeltaRangerMergeSource.version("b"))
      assertEquals(3, spark.table("b.db.t").schema.length)
      assertFalse(
        PartitionedMergeFixture
          .actions(NativeSelectFixture.locations("b"), 1L)
          .exists(_.isInstanceOf[Metadata])
      )
    },
    options = Map("spark.databricks.delta.schema.autoMerge.enabled" -> "false")
  )

  @Test
  def sourceAddedValueUpdateAndAlterDenialsPrecedeFiles(): Unit = configured((spark, plugin) => {
    val policies = new TestDeltaRangerMergePolicies
    val files = PartitionedMergeFixture.files()
    val cases = Seq[() => Unit](
      () => policies.publish(plugin, alter = true, sourceDeniedSelect = Seq("_commit_timestamp")),
      () => policies.publish(plugin, alter = true, deniedUpdate = Seq("extra")),
      () => policies.publish(plugin)
    )
    cases.foreach { publish =>
      publish()
      assertThrows(
        classOf[SecurityException],
        () => spark.sql(query(input.replace("concat(secret, '-extra')", "_commit_timestamp")))
      )
      assertEquals(0L, TestDeltaRangerMergeSource.version("b"))
      assertEquals(files, PartitionedMergeFixture.files())
    }
  })

  @Test
  def maskedAddedAliasAndHiddenTargetInsertPreserveOmittedRawFields(): Unit =
    configured((spark, plugin) => {
      val policies = new TestDeltaRangerMergePolicies
      policies.publish(
        plugin,
        alter = true,
        sourceMasks = Map("secret" -> "concat({col}, '-masked')"),
        filter = Some("region = 'drop'")
      )
      val payload = input.replace(
        "id, secret, region, concat(secret, '-extra')",
        "id, concat(secret, '-extra')"
      )
      spark.sql(query(payload)).collect()
      policies.publish(plugin, alter = true)
      assertEquals(
        PartitionedMergeFixture.bag(
          Seq(
            Row(1L, "b-one", "keep", null),
            Row(2L, "b-two", "drop", null),
            Row(3L, null, "keep", null),
            Row(1L, null, null, "changed-masked-extra"),
            Row(4L, null, null, "new-masked-extra")
          )
        ),
        PartitionedMergeFixture.bag(spark.table("b.db.t").collect().toSeq)
      )
    })

  @Test
  def emptySourceStillPublishesMetadataOnlyEvolution(): Unit = configured((spark, _) => {
    spark.sql(query(input + " AND id < 0")).collect()
    assertEquals(1L, TestDeltaRangerMergeSource.version("b"))
    assertEquals(4, spark.table("b.db.t").schema.length)
    val actions = PartitionedMergeFixture.actions(NativeSelectFixture.locations("b"), 1L)
    assertEquals(1, actions.count(_.isInstanceOf[Metadata]))
    assertFalse(actions.exists(_.isInstanceOf[org.apache.spark.sql.delta.actions.AddFile]))
    retain(spark, "empty-metadata-only")
  })

  @Test
  def partitionedAqeEvolutionProducesNativeCdf(): Unit = configured(
    (spark, _) => {
      spark.sql(query()).collect()
      val log = DeltaLog.forTable(spark, NativeSelectFixture.locations("b"))
      assertEquals(Seq("region"), log.update().metadata.partitionColumns)
      val cdf = spark
        .sql("SELECT id, extra, _change_type FROM table_changes('b.db.t', 1, 1)")
        .collect()
        .toSet
      assertEquals(
        Set(
          Row(1L, null, "update_preimage"),
          Row(1L, "changed-extra", "update_postimage"),
          Row(4L, "new-extra", "insert")
        ),
        cdf
      )
      retain(
        spark,
        "partitioned-aqe-cdf",
        artifacts = Map("cdf-rows.txt" -> cdf.toVector.map(_.toString).sorted.mkString("\n"))
      )
    },
    prepare = spark => PartitionedMergeFixture.reset(spark),
    options = Map("spark.sql.adaptive.enabled" -> "true")
  )

  @Test
  def distinctAndSameRegistrationRetriesNeverOpenSources(): Unit = {
    Seq(false, true).foreach { same =>
      configured((spark, plugin) => {
        val target = if (same) "a" else "b"
        new TestDeltaRangerMergePolicies().publish(plugin, alter = true, targetCatalog = target)
        spark.conf.set("spark.databricks.delta.write.txnAppId", "evolution-recovery")
        spark.conf.set("spark.databricks.delta.write.txnVersion", "4")
        spark.conf.set("spark.databricks.delta.write.txnVersion.autoReset.enabled", "false")
        val sql = query(target = target)
        spark.sql(sql).collect()
        val version = TestDeltaRangerMergeSource.version(target)
        val files = PartitionedMergeFixture.files(target)
        val registration = NativeSelectFixture.bindingFixture(spark, target)
        val refresh = registration.onRead
        registration.onRead = n => {
          assertNull(
            TestDeltaRangerMergeSource
              .field[ThreadLocal[DeltaRangerMergeSource.Attempt]](DeltaRangerMergeSource, "active")
              .get(),
            "recovery opened native source"
          )
          refresh(n)
        }
        spark.sql(sql).collect()
        assertEquals(version, TestDeltaRangerMergeSource.version(target))
        assertEquals(files, PartitionedMergeFixture.files(target))
        assertThrows(classOf[SecurityException], () => spark.sql(sql.replace("0, 4", "1, 4")))
        assertThrows(
          classOf[SecurityException],
          () => spark.sql(sql.replace("'-extra'", "'-changed'"))
        )
        new TestDeltaRangerMergePolicies().publish(plugin, targetCatalog = target)
        assertThrows(classOf[SecurityException], () => spark.sql(sql))
        new TestDeltaRangerMergePolicies()
          .publish(plugin, alter = true, targetCatalog = target, deniedUpdate = Seq("extra"))
        assertThrows(classOf[SecurityException], () => spark.sql(sql))
        assertEquals(version, TestDeltaRangerMergeSource.version(target))
        assertEquals(files, PartitionedMergeFixture.files(target))
        retain(
          spark,
          if (same) "same-registration-recovery" else "distinct-source-recovery",
          target
        )
      })
    }
  }

  @Test
  def laterMetadataRevertCannotBorrowExpectedSchemaRecovery(): Unit = configured((spark, _) => {
    spark.conf.set("spark.databricks.delta.write.txnAppId", "evolution-suffix")
    spark.conf.set("spark.databricks.delta.write.txnVersion", "4")
    spark.conf.set("spark.databricks.delta.write.txnVersion.autoReset.enabled", "false")
    spark.sql(query()).collect()
    val metadata = DeltaLog.forTable(spark, NativeSelectFixture.locations("b")).update().metadata
    val log = Paths.get(NativeSelectFixture.locations("b"), "_delta_log")
    // Adversarial native history: current schema is exact again, but a later Metadata action
    // must remain distinguishable from a contiguous data-only advancement.
    Files.write(
      log.resolve("00000000000000000002.json"),
      (metadata.copy(description = "foreign metadata").json + "\n").getBytes(StandardCharsets.UTF_8)
    )
    Files.write(
      log.resolve("00000000000000000003.json"),
      (metadata.json + "\n").getBytes(StandardCharsets.UTF_8)
    )
    DeltaLog.clearCache()
    val files = PartitionedMergeFixture.files()
    assertThrows(classOf[SecurityException], () => spark.sql(query()))
    assertEquals(3L, TestDeltaRangerMergeSource.version("b"))
    assertEquals(files, PartitionedMergeFixture.files())
  })

  @Test
  def exactNativeProofRejectsMetadataProtocolAndIdentityTampering(): Unit =
    configured((spark, _) => {
      val identity = Some("proof-evolution" -> 4L)
      spark.conf.set("spark.databricks.delta.write.txnAppId", identity.get._1)
      spark.conf.set("spark.databricks.delta.write.txnVersion", identity.get._2.toString)
      spark.conf.set("spark.databricks.delta.write.txnVersion.autoReset.enabled", "false")
      spark.sql(query()).collect()
      val entry = DeltaRangerMergeJournal
        .peek(
          NativeSelectFixture.bindingFixture(spark, "b").wrapper.mergeJournalDirectory(),
          identity
        )
        .get
      val path =
        Paths.get(NativeSelectFixture.locations("b"), "_delta_log", "00000000000000000001.json")
      val lines = Files.readAllLines(path).asScala.toVector
      assertTrue(lines(1).contains("metaData"))
      DeltaRangerMergeCommit.verifyPublication(Files.readAllBytes(path), entry.proof)
      val alternatives = Seq(
        lines.patch(1, Nil, 1),
        lines.patch(1, Seq(lines(1)), 0),
        Vector(lines.head) ++ lines.drop(2) ++ Vector(lines(1)),
        lines.updated(1, lines(1).replace("extra", "foreign")),
        lines.patch(2, Seq("{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}"), 0),
        lines.updated(0, lines.head.replace("MERGE", "WRITE"))
      )
      alternatives.foreach { changed =>
        assertThrows(
          classOf[SecurityException],
          () =>
            DeltaRangerMergeCommit.verifyPublication(
              (changed.mkString("\n") + "\n").getBytes(StandardCharsets.UTF_8),
              entry.proof
            )
        )
      }
      val mapper = new com.fasterxml.jackson.databind.ObjectMapper
      val proof =
        mapper.readTree(entry.proof).deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]()
      proof.put("nativeId", "foreign-transaction")
      assertThrows(
        classOf[SecurityException],
        () => DeltaRangerMergeCommit.verifyPublication(Files.readAllBytes(path), proof.toString)
      )
      val directory = Files.createTempDirectory("fixed-schema-proof")
      val fixed = new DeltaRangerMergeJournal(directory, None, entry.command)
      assertThrows(classOf[SecurityException], () => fixed.prepare(entry.proof, entry.version))
      retain(spark, "exact-publication-proof")
    })

  @Test
  def normalizedSourceStillRejectsCopiedAndChangedProjection(): Unit = configured((spark, _) => {
    import TestDeltaRangerMergeSource.field
    val registration = NativeSelectFixture.bindingFixture(spark, "b")
    val refresh = registration.onRead
    var inspected = false
    registration.onRead = n => {
      refresh(n)
      val attempt =
        field[ThreadLocal[DeltaRangerMergeSource.Attempt]](DeltaRangerMergeSource, "active").get()
      if (attempt != null && !inspected) {
        val root = field[DeltaRangerMergeSource.SourceExec](attempt, "root")
        if (root != null) {
          inspected = true
          val receipt = field[DeltaRangerMergeReceipt](attempt, "receipt")
          assertEquals(receipt.materializedColumns.map(_.exprId), root.child.output.map(_.exprId))
          assertTrue(root.child.isInstanceOf[DeltaRangerCdfProducer], "identity projection remains")
          val changed = org.apache.spark.sql.execution.ProjectExec(
            Seq(
              org.apache.spark.sql.catalyst.expressions
                .Alias(org.apache.spark.sql.catalyst.expressions.Literal(9L), "id")()
            ),
            root.child
          )
          assertThrows(classOf[SecurityException], () => attempt.replace(root, changed))
          assertThrows(
            classOf[SecurityException],
            () => attempt.replace(root.copy(root.child)(attempt), root.child)
          )
        }
      }
    }
    spark.sql(query()).collect()
    assertTrue(inspected)
    retain(spark, "owned-source-projection")
  })

  @Test
  def knownCommitAcknowledgementLossRecoversWithoutReplay(): Unit = configured((spark, _) => {
    spark.conf.set("spark.databricks.delta.write.txnAppId", "evolution-ack")
    spark.conf.set("spark.databricks.delta.write.txnVersion", "4")
    spark.conf.set("spark.databricks.delta.write.txnVersion.autoReset.enabled", "false")
    val registration = NativeSelectFixture.bindingFixture(spark, "b")
    val refresh = registration.onRead
    var failed = false
    var receipt: DeltaRangerMergeReceipt = null
    registration.onRead = n => {
      if (
        !failed && DeltaRangerMergeCommit.current.nonEmpty && TestDeltaRangerMergeSource
          .version("b") == 1L
      ) {
        failed = true
        receipt = TestDeltaRangerMergeSource
          .field[DeltaRangerMergeReceipt](DeltaRangerMergeCommit.current.get, "receipt")
        throw new IllegalStateException("lost evolving acknowledgement")
      }
      refresh(n)
    }
    val failure = assertThrows(classOf[Exception], () => spark.sql(query()))
    assertTrue(failure.isInstanceOf[SecurityException], failure.toString)
    assertTrue(failed)
    val outcome = TestDeltaRangerMergeSource.field[AnyRef](receipt, "outcome")
    assertEquals("committed", TestDeltaRangerMergeSource.field[String](outcome, "status"))
    assertEquals(1L, TestDeltaRangerMergeSource.version("b"))
    val entry = DeltaRangerMergeJournal
      .peek(registration.wrapper.mergeJournalDirectory(), Some("evolution-ack" -> 4L))
      .get
    assertEquals(DeltaRangerMergeJournal.Committed, entry.state)
    val files = PartitionedMergeFixture.files()
    registration.onRead = n => {
      assertNull(
        TestDeltaRangerMergeSource
          .field[ThreadLocal[DeltaRangerMergeSource.Attempt]](DeltaRangerMergeSource, "active")
          .get()
      )
      refresh(n)
    }
    spark.sql(query()).collect()
    assertEquals(1L, TestDeltaRangerMergeSource.version("b"))
    assertEquals(files, PartitionedMergeFixture.files())
    retain(spark, "lost-ack-recovered", outcome = Option(receipt))
  })

  @Test
  def versionTenEvolutionReconstructsNativeCheckpoint(): Unit = configured(
    (spark, _) => {
      spark.sql(query()).collect()
      assertEquals(10L, TestDeltaRangerMergeSource.version("b"))
      val directory = Paths.get(NativeSelectFixture.locations("b"), "_delta_log")
      assertTrue(Files.exists(directory.resolve("_last_checkpoint")))
      assertTrue(Files.exists(directory.resolve("00000000000000000010.checkpoint.parquet")))
      assertEquals(4, spark.table("b.db.t").schema.length)
      retain(spark, "version-ten-checkpoint")
    },
    prepare = spark => {
      (1 to 9).foreach(n =>
        spark
          .sql(
            s"ALTER TABLE delta.`${NativeSelectFixture.locations("b")}` SET TBLPROPERTIES ('comment' = 'version-$n')"
          )
          .collect()
      )
    }
  )

  @Test
  def pendingMetadataProtocolAndOwnerRemainExactAfterNativeStaging(): Unit =
    configured((spark, _) => {
      import TestDeltaRangerMergeSource.field
      val registration = NativeSelectFixture.bindingFixture(spark, "b")
      val refresh = registration.onRead
      var checked = false
      registration.onRead = n => {
        refresh(n)
        val write = field[ThreadLocal[DeltaRangerMergeWrite]](DeltaRangerMergeWrite, "active").get()
        if (write != null && !checked) {
          checked = true
          val receipt = field[DeltaRangerMergeReceipt](write, "receipt")
          val txn = field[org.apache.spark.sql.delta.OptimisticTransaction](write, "transaction")
          def change(name: String, value: AnyRef)(check: => Unit): Unit = {
            val member = Iterator
              .iterate[Class[_]](txn.getClass)(_.getSuperclass)
              .takeWhile(_ != null)
              .flatMap(_.getDeclaredFields)
              .find(f => f.getName == name || f.getName.endsWith("$$" + name))
              .get
            member.setAccessible(true)
            val before = member.get(txn)
            member.set(txn, value)
            try check
            finally member.set(txn, before)
          }
          assertEquals(receipt.schemaIntent.expectedMetadata, txn.metadata)
          change("newMetadata", Some(txn.metadata.copy(description = "foreign"))) {
            assertThrows(classOf[SecurityException], () => receipt.requireTransaction(txn))
            assertThrows(
              classOf[SecurityException],
              () =>
                receipt.requireSourceAttempt(
                  field[DeltaRangerMergeSource.Attempt](receipt, "sourceAttempt")
                )
            )
          }
          change("newMetadata", Some(txn.metadata.copy(id = "foreign-uuid"))) {
            assertThrows(classOf[SecurityException], () => receipt.requireTransaction(txn))
          }
          change(
            "newProtocol",
            Some(txn.protocol.copy(minReaderVersion = txn.protocol.minReaderVersion + 1))
          ) {
            assertThrows(classOf[SecurityException], () => receipt.requireTransaction(txn))
          }
          val foreign =
            org.mockito.Mockito.mock(classOf[org.apache.spark.sql.delta.OptimisticTransaction])
          assertThrows(classOf[SecurityException], () => receipt.requireTransaction(foreign))
          val command = field[DeltaRangerMergeCommand](receipt, "command")
          assertThrows(classOf[SecurityException], () => receipt.stageMetadata(command, txn) { () })
        }
      }
      spark.sql(query()).collect()
      assertTrue(checked)
      retain(spark, "pending-state-guards")
    })

  @Test
  def laterDataOnlyAdvancementAndUnavailableSourcePayloadStillReconcile(): Unit =
    configured((spark, _) => {
      spark.conf.set("spark.databricks.delta.write.txnAppId", "advance-evolution")
      spark.conf.set("spark.databricks.delta.write.txnVersion", "4")
      spark.conf.set("spark.databricks.delta.write.txnVersion.autoReset.enabled", "false")
      spark.sql(query()).collect()
      spark.conf.set("spark.databricks.delta.write.txnVersion", "8")
      spark.sql(query(input.replace("_commit_version = 4", "_commit_version = 1"))).collect()
      assertEquals(2L, TestDeltaRangerMergeSource.version("b"))
      val rows = spark.table("b.db.t").collect().toSet
      assertTrue(rows.contains(Row(11L, "history", "keep", "history-extra")))
      spark.conf.set("spark.databricks.delta.write.txnVersion", "4")
      PartitionedMergeFixture.files("a").foreach(p => Files.delete(Paths.get(p)))
      val files = PartitionedMergeFixture.files()
      spark.sql(query()).collect()
      assertEquals(files, PartitionedMergeFixture.files())
      assertEquals(rows, spark.table("b.db.t").collect().toSet)
      assertEquals(2L, TestDeltaRangerMergeSource.version("b"))
      retain(spark, "data-advancement-source-unavailable")
    })

  @Test
  def nativeDuplicateMatchAndConstraintFailurePublishNeitherSchemaNorData(): Unit = {
    configured((spark, _) => {
      val failure = assertThrows(
        classOf[Exception],
        () => spark.sql(query(input.replace("_commit_version = 4 AND ", "")))
      )
      assertTrue(
        PartitionedMergeFixture.messages(failure).contains("multiple source rows"),
        failure.toString
      )
      assertEquals(0L, TestDeltaRangerMergeSource.version("b"))
      assertEquals(3, spark.table("b.db.t").schema.length)
    })
    configured(
      (spark, _) => {
        val before = TestDeltaRangerMergeSource.version("b")
        val failure = assertThrows(
          classOf[Exception],
          () => spark.sql(query(input.replace("SELECT id,", "SELECT CAST(NULL AS BIGINT) AS id,")))
        )
        assertTrue(
          PartitionedMergeFixture.messages(failure).toLowerCase.contains("constraint"),
          failure.toString
        )
        assertEquals(before, TestDeltaRangerMergeSource.version("b"))
        assertEquals(3, spark.table("b.db.t").schema.length)
      },
      prepare = spark => {
        val location = NativeSelectFixture.locations("b")
        val paths = Files.walk(Paths.get(location))
        try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.delete)
        finally paths.close()
        DeltaLog.clearCache()
        spark
          .sql(
            s"CREATE TABLE delta.`$location` (id BIGINT NOT NULL, secret STRING, region STRING) USING DELTA"
          )
          .collect()
        spark
          .sql(
            s"INSERT INTO delta.`$location` VALUES (1L, 'b-one', 'keep'), (2L, 'b-two', 'drop'), (3L, NULL, 'keep')"
          )
          .collect()
      }
    )
  }

  @Test
  def missingNativeMarkerAndUnknownOrMalformedRecordsNeverOpenRecoverySource(): Unit =
    configured((spark, _) => {
      val identity = Some("refuse-evolution" -> 4L)
      spark.conf.set("spark.databricks.delta.write.txnAppId", identity.get._1)
      spark.conf.set("spark.databricks.delta.write.txnVersion", "4")
      spark.conf.set("spark.databricks.delta.write.txnVersion.autoReset.enabled", "false")
      spark.sql(query()).collect()
      val registration = NativeSelectFixture.bindingFixture(spark, "b")
      val entry =
        DeltaRangerMergeJournal.peek(registration.wrapper.mergeJournalDirectory(), identity).get
      val journalRoot = registration.wrapper.mergeJournalDirectory().resolve("merge")
      val listing = Files.list(journalRoot)
      val journal =
        try listing.iterator().asScala.find(_.toString.endsWith(".json")).get
        finally listing.close()
      val originalRecord = Files.readAllBytes(journal)
      val originalRead = registration.onRead
      registration.onRead = n => {
        assertNull(
          TestDeltaRangerMergeSource
            .field[ThreadLocal[DeltaRangerMergeSource.Attempt]](DeltaRangerMergeSource, "active")
            .get()
        )
        originalRead(n)
      }
      val files = PartitionedMergeFixture.files()
      val native =
        Paths.get(NativeSelectFixture.locations("b"), "_delta_log", "00000000000000000001.json")
      val originalNative = Files.readAllBytes(native)
      val withoutMarker =
        Files.readAllLines(native).asScala.filterNot(_.contains("\"txn\":")).mkString("\n") + "\n"
      def writeNative(bytes: Array[Byte]): Unit = {
        val path = new org.apache.hadoop.fs.Path(native.toUri)
        val output = path.getFileSystem(spark.sessionState.newHadoopConf()).create(path, true)
        try output.write(bytes)
        finally output.close()
      }
      writeNative(withoutMarker.getBytes(StandardCharsets.UTF_8))
      DeltaLog.clearCache()
      assertThrows(classOf[SecurityException], () => spark.sql(query()))
      writeNative(originalNative); DeltaLog.clearCache()
      Seq("{}", new String(originalRecord, StandardCharsets.UTF_8).replace("RECONCILED", "UNKNOWN"))
        .foreach { malformed =>
          Files.write(journal, malformed.getBytes(StandardCharsets.UTF_8))
          assertThrows(classOf[SecurityException], () => spark.sql(query()))
        }
      Files.delete(journal)
      assertThrows(classOf[SecurityException], () => spark.sql(query()))
      Files.write(journal, originalRecord)
      Files.setPosixFilePermissions(
        journal,
        java.nio.file.attribute.PosixFilePermissions.fromString("rw-------")
      )
      assertEquals(1L, TestDeltaRangerMergeSource.version("b"))
      assertEquals(files, PartitionedMergeFixture.files())
      assertTrue(entry.evolution.nonEmpty)
    })

  @Test
  def scheduledAlterExpiryAfterFilesRetainsOrphansWithoutCommit(): Unit =
    configured((spark, plugin) => {
      import java.time.{Instant, ZoneOffset}
      import java.time.format.DateTimeFormatter
      import org.apache.ranger.plugin.model.RangerValiditySchedule
      import TestDeltaRangerMergeSource.field
      val format = DateTimeFormatter
        .ofPattern(RangerValiditySchedule.VALIDITY_SCHEDULE_DATE_STRING_SPECIFICATION)
        .withZone(ZoneOffset.UTC)
      val expiry = Instant.now().plusSeconds(30)
      val schedule = new RangerValiditySchedule(
        format.format(expiry.minusSeconds(120)),
        format.format(expiry),
        "UTC",
        null
      )
      new TestDeltaRangerMergePolicies()
        .publish(plugin, alter = true, alterSchedule = Some(schedule))
      val registration = NativeSelectFixture.bindingFixture(spark, "b")
      val refresh = registration.onRead
      val files = PartitionedMergeFixture.files()
      var receipt: DeltaRangerMergeReceipt = null
      var crossed = false
      registration.onRead = n => {
        refresh(n)
        val writer =
          field[ThreadLocal[DeltaRangerMergeWrite]](DeltaRangerMergeWrite, "active").get()
        if (writer != null) receipt = field[DeltaRangerMergeReceipt](writer, "receipt")
        if (
          receipt != null && writer == null && !crossed && PartitionedMergeFixture
            .files()
            .diff(files)
            .nonEmpty
        ) {
          crossed = true
          assertTrue(Instant.now().isBefore(expiry), "ALTER expired before native file boundary")
          while (!Instant.now().isAfter(expiry.plusSeconds(1))) Thread.sleep(100)
        }
      }
      assertThrows(classOf[SecurityException], () => spark.sql(query()))
      assertTrue(crossed)
      assertEquals(0L, TestDeltaRangerMergeSource.version("b"))
      assertTrue(PartitionedMergeFixture.files().diff(files).nonEmpty)
      assertEquals("not-committed", field[String](field[AnyRef](receipt, "outcome"), "status"))
      retain(spark, "alter-expired-orphan-files", outcome = Option(receipt))
    })

  @Test
  def changedPublishedLogicalSchemaCannotAcknowledgeKnownCommit(): Unit = configured((spark, _) => {
    val registration = NativeSelectFixture.bindingFixture(spark, "b")
    val refresh = registration.onRead
    var receipt: DeltaRangerMergeReceipt = null
    registration.onRead = n => {
      refresh(n)
      if (
        TestDeltaRangerMergeSource.version("b") == 1L && DeltaRangerMergeCommit.current.nonEmpty
      ) {
        receipt = TestDeltaRangerMergeSource
          .field[DeltaRangerMergeReceipt](DeltaRangerMergeCommit.current.get, "receipt")
        registration.columns = registration.columns.filterNot(_.name() == "extra")
      }
    }
    assertThrows(classOf[SecurityException], () => spark.sql(query()))
    assertEquals(1L, TestDeltaRangerMergeSource.version("b"))
    assertNotNull(receipt)
    assertEquals(
      "committed",
      TestDeltaRangerMergeSource
        .field[String](TestDeltaRangerMergeSource.field[AnyRef](receipt, "outcome"), "status")
    )
    registration.onRead = refresh
    retain(spark, "committed-registration-schema-mismatch", outcome = Option(receipt))
  })

  @Test
  def targetMaskReadsDoNotOverwriteOmittedRawFields(): Unit = configured((spark, plugin) => {
    val policies = new TestDeltaRangerMergePolicies
    policies.publish(plugin, alter = true, masks = Map("secret" -> "concat({col}, '-masked')"))
    spark
      .sql(
        s"MERGE INTO b.db.t t USING ($input) s ON t.id = s.id " +
          "WHEN MATCHED THEN UPDATE SET extra = t.secret " +
          "WHEN NOT MATCHED THEN INSERT (id, extra) VALUES (s.id, s.extra)"
      )
      .collect()
    policies.publish(plugin, alter = true)
    assertEquals(
      Set(
        Row(1L, "b-one", "keep", "b-one-masked"),
        Row(2L, "b-two", "drop", null),
        Row(3L, null, "keep", null),
        Row(4L, null, null, "new-extra")
      ),
      spark.table("b.db.t").collect().toSet
    )
    retain(spark, "target-mask-raw-preservation")
  })

  @Test
  def recacheAcknowledgementLossRetainsCommittedOutcome(): Unit = configured((spark, _) => {
    import TestDeltaRangerMergeSource.field
    spark.conf.set("spark.databricks.delta.write.txnAppId", "recache-evolution")
    spark.conf.set("spark.databricks.delta.write.txnVersion", "4")
    spark.conf.set("spark.databricks.delta.write.txnVersion.autoReset.enabled", "false")
    val registration = NativeSelectFixture.bindingFixture(spark, "b")
    val refresh = registration.onRead
    var receipt: DeltaRangerMergeReceipt = null
    var failed = false
    registration.onRead = n => {
      refresh(n)
      DeltaRangerMergeCommit.current.foreach { commit =>
        val owned = field[DeltaRangerMergeReceipt](commit, "receipt")
        if (!failed && field[Boolean](owned, "recached")) {
          failed = true; receipt = owned
          throw new IllegalStateException("lost recache acknowledgement")
        }
      }
    }
    assertThrows(classOf[SecurityException], () => spark.sql(query()))
    assertTrue(failed)
    assertEquals("committed", field[String](field[AnyRef](receipt, "outcome"), "status"))
    val files = PartitionedMergeFixture.files()
    registration.onRead = refresh
    spark.sql(query()).collect()
    assertEquals(1L, TestDeltaRangerMergeSource.version("b"))
    assertEquals(files, PartitionedMergeFixture.files())
    retain(spark, "recache-ack-recovered", outcome = Option(receipt))
  })

  @Test
  def journalAcknowledgementLossRetainsExactCommitForRecovery(): Unit =
    commitIoFailure(journal = true)

  @Test
  def recoveryAcknowledgementLossRetainsVerifiedCommittedOutcome()
      : Unit = configured((spark, _) => {
    import TestDeltaRangerMergeSource.field
    import java.nio.file.attribute.PosixFilePermissions
    val identity = Some("recovery-ack-evolution" -> 4L)
    spark.conf.set("spark.databricks.delta.write.txnAppId", identity.get._1)
    spark.conf.set("spark.databricks.delta.write.txnVersion", "4")
    spark.conf.set("spark.databricks.delta.write.txnVersion.autoReset.enabled", "false")
    val registration = NativeSelectFixture.bindingFixture(spark, "b")
    val refresh = registration.onRead
    val directory = registration.wrapper.mergeJournalDirectory()
    val root = directory.resolve("merge")
    var permissions: java.util.Set[java.nio.file.attribute.PosixFilePermission] = null
    registration.onRead = n => {
      refresh(n)
      if (permissions == null && DeltaRangerMergeCommit.current.nonEmpty) {
        permissions = Files.getPosixFilePermissions(root)
        Files.setPosixFilePermissions(root, PosixFilePermissions.fromString("r-x------"))
      }
    }
    try assertThrows(classOf[Exception], () => spark.sql(query()))
    finally {
      registration.onRead = refresh
      if (permissions != null) Files.setPosixFilePermissions(root, permissions)
    }
    val prepared = DeltaRangerMergeJournal.peek(directory, identity).get
    val records = Files.list(root)
    val record =
      try
        records.iterator().asScala.filter(_.getFileName.toString.endsWith(".json")).toVector match {
          case Vector(path) => path
          case other        => fail("expected one durable recovery record: " + other)
        }
      finally records.close()
    val recordBytes = Files.readAllBytes(record)
    assertEquals(DeltaRangerMergeJournal.Prepared, prepared.state)
    assertTrue(prepared.evolution.nonEmpty)
    assertTrue(prepared.digest.isEmpty)
    val bytes = Files.readAllBytes(
      Paths.get(NativeSelectFixture.locations("b"), "_delta_log", "00000000000000000001.json")
    )
    DeltaRangerMergeCommit.verifyPublication(bytes, prepared.proof)
    val digest = DeltaRangerMergeJournal.hash(bytes)
    val files = PartitionedMergeFixture.files()
    val recovery = spark.sessionState.executePlan(spark.sessionState.sqlParser.parsePlan(query()))
    val receipt = recovery.analyzed.collectFirst { case command: DeltaRangerMergeCommand =>
      command.receipt
    }.get
    val injected = new java.io.IOException("injected recovery journal acknowledgement loss")
    var acknowledgements = 0
    var observed: AnyRef = null
    registration.onRead = n => {
      assertNull(
        field[ThreadLocal[DeltaRangerMergeSource.Attempt]](DeltaRangerMergeSource, "active").get()
      )
      assertNull(field[ThreadLocal[DeltaRangerMergeWrite]](DeltaRangerMergeWrite, "active").get())
      refresh(n)
    }
    val scope = org.mockito.Mockito.mockStatic(
      classOf[Files],
      org.mockito.Mockito
        .withSettings()
        .mockMaker("mock-maker-inline")
        .defaultAnswer(new org.mockito.stubbing.Answer[AnyRef] {
          override def answer(invocation: org.mockito.invocation.InvocationOnMock): AnyRef = {
            if (
              invocation.getMethod.getName == "createTempFile" && invocation.getArguments.length >= 2 &&
              invocation.getArgument[AnyRef](0) == root && invocation
                .getArgument[AnyRef](1) == ".merge-"
            ) {
              acknowledgements += 1
              observed = field[AnyRef](receipt, "outcome")
              throw injected
            }
            invocation.callRealMethod()
          }
        })
    )
    val failure =
      try assertThrows(classOf[java.io.IOException], () => recovery.executedPlan.executeCollect())
      finally { scope.close(); registration.onRead = refresh }
    assertSame(injected, failure)
    assertEquals(1, acknowledgements)
    assertEquals("committed", field[String](observed, "status"))
    assertSame(observed, field[AnyRef](receipt, "outcome"))
    assertEquals(Some(prepared.version), field[Option[Long]](observed, "version"))
    assertEquals(Some(digest), field[Option[String]](observed, "digest"))
    assertEquals(0, field[Int](receipt, "sourceAttemptNumber"))
    assertNull(field[DeltaRangerMergeSource.Attempt](receipt, "sourceAttempt"))
    assertTrue(field[Boolean](receipt, "closed"))
    assertEquals(files, PartitionedMergeFixture.files())
    assertEquals(1L, TestDeltaRangerMergeSource.version("b"))
    assertArrayEquals(recordBytes, Files.readAllBytes(record))
    retain(spark, "recovery-ack-failed-after-proof", outcome = Some(receipt))
  })

  @Test
  def nativeInvocationFailureRetainsUnknownWithoutReplay(): Unit = commitIoFailure(journal = false)

  private def commitIoFailure(journal: Boolean): Unit = configured((spark, _) => {
    import TestDeltaRangerMergeSource.field
    import java.nio.file.attribute.PosixFilePermissions
    spark.conf.set("spark.databricks.delta.write.txnAppId", "io-evolution")
    spark.conf.set("spark.databricks.delta.write.txnVersion", "4")
    spark.conf.set("spark.databricks.delta.write.txnVersion.autoReset.enabled", "false")
    val registration = NativeSelectFixture.bindingFixture(spark, "b")
    val refresh = registration.onRead
    val locked =
      if (journal) registration.wrapper.mergeJournalDirectory().resolve("merge")
      else Paths.get(NativeSelectFixture.locations("b"), "_delta_log")
    var previous: java.util.Set[java.nio.file.attribute.PosixFilePermission] = null
    var receipt: DeltaRangerMergeReceipt = null
    registration.onRead = n => {
      refresh(n)
      DeltaRangerMergeCommit.current.foreach { commit =>
        if (previous == null) {
          receipt = field[DeltaRangerMergeReceipt](commit, "receipt")
          previous = Files.getPosixFilePermissions(locked)
          Files.setPosixFilePermissions(locked, PosixFilePermissions.fromString("r-x------"))
        }
      }
    }
    try assertThrows(classOf[Exception], () => spark.sql(query()))
    finally {
      registration.onRead = refresh
      if (previous != null) Files.setPosixFilePermissions(locked, previous)
    }
    assertNotNull(receipt)
    assertEquals(
      if (journal) "committed" else "unknown",
      field[String](field[AnyRef](receipt, "outcome"), "status")
    )
    assertEquals(if (journal) 1L else 0L, TestDeltaRangerMergeSource.version("b"))
    val files = PartitionedMergeFixture.files()
    if (journal) spark.sql(query()).collect()
    else assertThrows(classOf[SecurityException], () => spark.sql(query()))
    assertEquals(files, PartitionedMergeFixture.files())
    retain(
      spark,
      if (journal) "journal-ack-recovered" else "unknown-native-invocation",
      outcome = Option(receipt)
    )
  })

  @Test
  def committedSourceLossRetainsOriginalFailureAcrossNativeOuterRetry(): Unit =
    sourceLossAfterInvocation(true)

  @Test
  def unknownSourceLossRetainsOriginalFailureAcrossNativeOuterRetry(): Unit =
    sourceLossAfterInvocation(false)

  private def sourceLossAfterInvocation(published: Boolean): Unit = {
    try
      configured(
        (spark, _) => {
          import TestDeltaRangerMergeSource.field
          spark.conf.set("spark.databricks.delta.write.txnAppId", "source-loss-evolution")
          spark.conf.set("spark.databricks.delta.write.txnVersion", "4")
          spark.conf.set("spark.databricks.delta.write.txnVersion.autoReset.enabled", "false")
          var receipt: DeltaRangerMergeReceipt = null
          var injected: org.apache.spark.SparkException = null
          MergeEvolutionFailureLogStore.boundary = after => {
            if (after == published && injected == null && DeltaRangerMergeCommit.current.nonEmpty) {
              receipt =
                field[DeltaRangerMergeReceipt](DeltaRangerMergeCommit.current.get, "receipt")
              val command = field[DeltaRangerMergeCommand](receipt, "command")
              val rdd =
                field[Option[org.apache.spark.rdd.RDD[org.apache.spark.sql.catalyst.InternalRow]]](
                  command,
                  "materializedSourceRDD"
                ).get
              injected =
                new org.apache.spark.SparkException(s"Checkpoint block rdd_${rdd.id}_0 not found!")
              throw injected
            }
          }
          val failure = assertThrows(classOf[Exception], () => spark.sql(query()))
          assertSame(injected, failure)
          assertEquals(
            if (published) "committed" else "unknown",
            field[String](field[AnyRef](receipt, "outcome"), "status")
          )
          assertTrue(field[Boolean](receipt, "sourceClosed"))
          assertEquals(1, field[Int](receipt, "sourceAttemptNumber"))
          assertEquals(2, field[Int](field[DeltaRangerMergeCommand](receipt, "command"), "attempt"))
          assertTrue(failure.getSuppressed.exists(_.isInstanceOf[SecurityException]))
          assertEquals(if (published) 1L else 0L, TestDeltaRangerMergeSource.version("b"))
          retain(
            spark,
            if (published) "committed-source-loss-no-replay" else "unknown-source-loss-no-replay",
            outcome = Option(receipt),
            observeRows = false
          )
        },
        startup =
          Map("spark.delta.logStore.class" -> classOf[MergeEvolutionFailureLogStore].getName)
      )
    finally MergeEvolutionFailureLogStore.boundary = _ => ()
  }

  @Test
  def evolvingTargetCacheIsRefusedBeforeNativeMutation(): Unit = targetCache(true)

  @Test
  def matchingSchemaTargetCacheRetainsExistingBehavior(): Unit = targetCache(false)

  private def targetCache(evolving: Boolean): Unit = configured((spark, _) => {
    val extensions = new org.apache.spark.sql.SparkSessionExtensions
    new io.delta.sql.DeltaSparkSessionExtension().apply(extensions)
    val constructor =
      classOf[SparkSession].getDeclaredConstructors.find(_.getParameterCount == 5).get
    constructor.setAccessible(true)
    val native = constructor
      .newInstance(
        spark.sparkContext,
        Some(spark.sharedState),
        None,
        extensions,
        Map.empty[String, String]
      )
      .asInstanceOf[SparkSession]
    SparkSession.setActiveSession(native)
    val frame =
      try {
        val value = native.read.format("delta").load(NativeSelectFixture.locations("b"))
        value.cache(); assertEquals(3L, value.count()); value
      } finally SparkSession.setActiveSession(spark)
    val files = PartitionedMergeFixture.files()
    var retired: DeltaRangerMergeReceipt = null
    val registration = NativeSelectFixture.bindingFixture(spark, "b")
    val refresh = registration.onRead
    registration.onRead = n => {
      refresh(n)
      DeltaRangerMergeCommit.current.foreach(commit =>
        retired = TestDeltaRangerMergeSource.field[DeltaRangerMergeReceipt](commit, "receipt")
      )
    }
    try {
      if (evolving) {
        val failure = assertThrows(classOf[SecurityException], () => spark.sql(query()))
        assertTrue(failure.getMessage.contains("target cache"), failure.toString)
        assertEquals(0L, TestDeltaRangerMergeSource.version("b"))
        assertEquals(files, PartitionedMergeFixture.files())
      } else {
        val failure = assertThrows(
          classOf[SecurityException],
          () => spark.sql(query(input.replace(", concat(secret, '-extra') AS extra", "")))
        )
        assertTrue(failure.getMessage.contains("Raw native observation rejected"), failure.toString)
        assertEquals(1L, TestDeltaRangerMergeSource.version("b"))
        assertEquals(
          "committed",
          TestDeltaRangerMergeSource
            .field[String](TestDeltaRangerMergeSource.field[AnyRef](retired, "outcome"), "status")
        )
      }
    } finally {
      registration.onRead = refresh
      frame.unpersist(blocking = true)
    }
    retain(
      spark,
      if (evolving) "target-cache-refused" else "matching-target-cache",
      outcome = Option(retired)
    )
  })

  @Test
  def retainedRecoveryOwnerCannotOpenOrReadHistory(): Unit = configured((spark, _) => {
    spark.conf.set("spark.databricks.delta.write.txnAppId", "closed-recovery-source")
    spark.conf.set("spark.databricks.delta.write.txnVersion", "4")
    spark.conf.set("spark.databricks.delta.write.txnVersion.autoReset.enabled", "false")
    spark.sql(query()).collect()
    val plan =
      spark.sessionState.executePlan(spark.sessionState.sqlParser.parsePlan(query())).analyzed
    val command = plan.collectFirst { case value: DeltaRangerMergeCommand => value }.get
    val receipt = command.receipt
    assertTrue(receipt.recoveryCandidate.nonEmpty)
    val source = receipt.cdf
    assertThrows(classOf[SecurityException], () => source.openMergeExecution(receipt))
    assertThrows(classOf[SecurityException], () => source.mergeRecipe(source.carrier.output))
    assertThrows(classOf[SecurityException], () => source.plan(source.carrier))
    assertThrows(classOf[SecurityException], () => source.validateCollection(null))
    assertThrows(classOf[SecurityException], () => source.validateExecution(null))
    assertEquals(1L, TestDeltaRangerMergeSource.version("b"))
    spark.sql(query()).collect()
    retain(spark, "recovery-owner-cannot-materialize")
  })

  @Test
  def retainedBaseStateRejectsForeignBindingsAndTransaction(): Unit = configured((spark, _) => {
    import TestDeltaRangerMergeSource.field
    import org.apache.spark.sql.execution.LogicalRDD
    import org.apache.spark.sql.delta.OptimisticTransaction
    val registration = NativeSelectFixture.bindingFixture(spark, "b")
    val refresh = registration.onRead
    var checked = false
    registration.onRead = n => {
      refresh(n)
      DeltaRangerMergeCommit.current.foreach { commit =>
        if (!checked) {
          checked = true
          val receipt = field[DeltaRangerMergeReceipt](commit, "receipt")
          val txn = field[OptimisticTransaction](commit, "transaction")
          val predicate = field[java.util.function.Predicate[LogicalRDD]](receipt, "baseState")
          val cache = field[AnyRef](predicate, "arg$1")
          val original = field[LogicalRDD](cache, "original")
          assertTrue(receipt.containsBaseState(commit, txn, original))
          val changed = original.copy(output =
            original.output.map(
              _.withExprId(org.apache.spark.sql.catalyst.expressions.NamedExpression.newExprId)
            )
          )(spark, None, None)
          assertFalse(receipt.containsBaseState(commit, txn, changed))
          val foreign = LogicalRDD(original.output, original.rdd.map(_.copy()))(spark)
          assertFalse(receipt.containsBaseState(commit, txn, foreign))
          assertThrows(
            classOf[SecurityException],
            () =>
              receipt.containsBaseState(
                commit,
                org.mockito.Mockito.mock(classOf[OptimisticTransaction]),
                original
              )
          )
          assertThrows(
            classOf[SecurityException],
            () => receipt.containsBaseState(null, txn, original)
          )
          val active = field[ThreadLocal[OptimisticTransaction]](OptimisticTransaction, "active")
          val previous = active.get()
          active.remove()
          try
            assertThrows(
              classOf[SecurityException],
              () => receipt.requireCommitContext(commit, txn, false)
            )
          finally active.set(previous)
          active.set(org.mockito.Mockito.mock(classOf[OptimisticTransaction]))
          try
            assertThrows(
              classOf[SecurityException],
              () => receipt.requireCommitContext(commit, txn, true)
            )
          finally active.set(previous)
        }
      }
    }
    spark.sql(query()).collect()
    assertTrue(checked)
    retain(spark, "base-state-guards")
  })

  private def retain(
      spark: SparkSession,
      label: String,
      target: String = "b",
      outcome: Option[DeltaRangerMergeReceipt] = None,
      artifacts: Map[String, String] = Map.empty,
      observeRows: Boolean = true
  ): Unit =
    sys.env.get("GRAVITINO_DELTA_MERGE_EVOLUTION_EVIDENCE_DIR").foreach { root =>
      val destination = Files.createDirectory(Paths.get(root).resolve(label))
      val logRoot = Paths.get(NativeSelectFixture.locations(target), "_delta_log")
      val logPaths = Files.list(logRoot)
      val jsonFiles =
        try
          logPaths
            .iterator()
            .asScala
            .filter(_.getFileName.toString.matches("[0-9]{20}\\.json"))
            .toVector
            .sortBy(_.getFileName.toString)
        finally logPaths.close()
      val actions = jsonFiles.flatMap(p => Files.readAllLines(p).asScala.map(Action.fromJson))
      val metadata = actions.collect { case m: Metadata => m }.last
      val protocol = actions.collect { case p: org.apache.spark.sql.delta.actions.Protocol =>
        p
      }.last
      val nativeVersion = jsonFiles.last.getFileName.toString.stripSuffix(".json").toLong
      def write(name: String, value: String): Unit =
        Files.write(destination.resolve(name), value.getBytes(StandardCharsets.UTF_8))
      artifacts.foreach { case (name, value) => write(name, value) }
      write("metadata.json", metadata.json)
      write("protocol.json", protocol.json)
      write("version.txt", nativeVersion.toString)
      write(
        "data-files.txt",
        PartitionedMergeFixture
          .files(target)
          .toVector
          .sorted
          .map(p =>
            Paths.get(NativeSelectFixture.locations(target)).relativize(Paths.get(p)).toString
          )
          .mkString("\n")
      )
      outcome.foreach { receipt =>
        val value = TestDeltaRangerMergeSource.field[AnyRef](receipt, "outcome")
        write("outcome.txt", value.toString)
      }
      if (observeRows) {
        val frame = spark.table(target + ".db.t")
        val fields = frame.schema.fieldNames
        val mapper = new com.fasterxml.jackson.databind.ObjectMapper
        val rows = frame.collect().map { row =>
          val values = new java.util.TreeMap[String, Object]
          fields.zipWithIndex.foreach { case (name, index) =>
            values.put(name, row.get(index).asInstanceOf[Object])
          }
          mapper.writeValueAsString(values)
        }
        write("logical-schema.json", frame.schema.json)
        write("rows.jsonl", rows.sorted.mkString("\n"))
      } else
        write(
          "rows-unconfirmed.txt",
          "Named readback remains unconfirmed after the injected native source-loss failure; native log and outcome are retained without another read."
        )
      val native = Files.list(Paths.get(NativeSelectFixture.locations(target), "_delta_log"))
      try
        native
          .iterator()
          .asScala
          .filter(p => !p.getFileName.toString.startsWith("."))
          .foreach(p =>
            if (Files.isRegularFile(p)) Files.copy(p, destination.resolve(p.getFileName))
          )
      finally native.close()
      val journalPath = NativeSelectFixture
        .bindingFixture(spark, target)
        .wrapper
        .mergeJournalDirectory()
        .resolve("merge")
      if (Files.isDirectory(journalPath)) {
        val journals = Files.list(journalPath)
        try
          journals
            .iterator()
            .asScala
            .foreach(p => Files.copy(p, destination.resolve("journal-" + p.getFileName)))
        finally journals.close()
      } else
        write(
          "journal-absent.txt",
          "No durable transaction-identity journal directory exists; native log and outcome determine publication."
        )
      val origins = Seq(
        classOf[SparkSession],
        classOf[DeltaLog],
        classOf[scala.Option[_]],
        classOf[RangerBasePlugin]
      ).map { c =>
        val path = Paths.get(c.getProtectionDomain.getCodeSource.getLocation.toURI)
        val hash = java.security.MessageDigest
          .getInstance("SHA-256")
          .digest(Files.readAllBytes(path))
          .map(b => f"${b & 0xff}%02x")
          .mkString
        c.getName + " " + path + " " + hash
      }
      write(
        "runtime.txt",
        s"Spark=${spark.version} Scala=${scala.util.Properties.versionNumberString} Java=${System.getProperty("java.version")}\n" + origins
          .mkString("\n")
      )
    }

  private def refresh(spark: SparkSession, name: String = "b"): Unit = {
    val registration = NativeSelectFixture.bindingFixture(spark, name)
    val original = registration.onRead
    registration.onRead = n => {
      original(n)
      val directory = Paths.get(NativeSelectFixture.locations(name), "_delta_log")
      val stream = Files.list(directory)
      val metadata =
        try
          stream
            .iterator()
            .asScala
            .toVector
            .filter(_.getFileName.toString.matches("[0-9]{20}\\.json"))
            .sortBy(_.getFileName.toString)
            .flatMap(p => Files.readAllLines(p).asScala.map(Action.fromJson))
            .collect { case m: Metadata => m }
            .last
        finally stream.close()
      val converter = new SparkTypeConverter
      registration.columns = metadata.schema.fields.map(f =>
        Column.of(
          f.name,
          converter.toGravitinoType(f.dataType),
          null,
          f.nullable,
          false,
          Column.DEFAULT_VALUE_NOT_SET
        )
      )
    }
  }
}

/** Injects a failure at the stock log-store publication boundary for outcome tests only. */
class MergeEvolutionFailureLogStore(
    sparkConf: org.apache.spark.SparkConf,
    hadoopConf: org.apache.hadoop.conf.Configuration
) extends org.apache.spark.sql.delta.storage.HDFSLogStore(sparkConf, hadoopConf) {

  /** Preserves native bytes and delegates successful writes to the stock store. */
  override def write(
      path: org.apache.hadoop.fs.Path,
      actions: Iterator[String],
      overwrite: Boolean,
      conf: org.apache.hadoop.conf.Configuration
  ): Unit = {
    MergeEvolutionFailureLogStore.boundary(false)
    super.write(path, actions, overwrite, conf)
    MergeEvolutionFailureLogStore.boundary(true)
  }
}

private[ranger] object MergeEvolutionFailureLogStore {
  var boundary: Boolean => Unit = (_: Boolean) => ()
}

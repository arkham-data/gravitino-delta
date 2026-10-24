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
import scala.collection.JavaConverters._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.ranger.plugin.service.RangerBasePlugin
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{Action, AddFile, AddCDCFile, RemoveFile}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.functions.input_file_name
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory

/** Installed partitioned MERGE keeps native file routing and exact writer authority. */
class TestDeltaRangerPartitionedMerge {
  import TestDeltaRangerMergeSource.{field, version}
  private val policies = new TestDeltaRangerMergePolicies
  private val mapper = new ObjectMapper
  private val sql = TestDeltaRangerMergeSource.sql
  private val initial = """SELECT 1L id, 'visible-raw' secret, 'old' region
    UNION ALL SELECT 2L, 'delete-raw', 'old'
    UNION ALL SELECT 3L, 'unchanged-raw', 'old'
    UNION ALL SELECT 8L, 'hidden-raw', 'old'
    UNION ALL SELECT 9L, 'untouched-raw', 'other'"""
  private val expected = Vector(
    Row(1L, "changed", "keep"),
    Row(3L, "unchanged-raw", "old"),
    Row(4L, "new", "keep"),
    Row(8L, "hidden-raw", "old"),
    Row(9L, "untouched-raw", "other")
  )
  private def bag(rows: Seq[Row]): Map[Row, Int] = rows.groupBy(identity).map {
    case (row, copies) => row -> copies.size
  }

  @Test
  def nativePartitionMovePreservesTouchedFileRows(): Unit =
    scenario(cdf = false, repartition = true, tableStats = true)

  @Test
  def partitionedCdcUsesOutputStatistics(): Unit =
    scenario(cdf = true, repartition = true, tableStats = false)

  @Test
  def orderedPartitionsWithoutRepartitionUseTableStatistics(): Unit =
    scenario(cdf = true, repartition = false, tableStats = true, multiple = true)

  @Test
  def orderedPartitionsWithoutCdcUseOutputStatistics(): Unit =
    scenario(cdf = false, repartition = false, tableStats = false, multiple = true)

  @Test
  def legacyUnpartitionedTargetRecoversBothSourceLayouts(): Unit = {
    Seq(false, true).foreach { partitionedSource =>
      new TestDeltaRangerInstalledMerge().withPolicies(
        (spark, _) => {
          spark.conf.set("spark.databricks.delta.write.txnAppId", "legacy-partition-consumer")
          spark.conf.set("spark.databricks.delta.write.txnVersion", "7")
          spark.conf.set("spark.databricks.delta.write.txnVersion.autoReset.enabled", "false")
          val query = spark.sessionState.executePlan(spark.sessionState.sqlParser.parsePlan(sql))
          val receipt = query.analyzed.collectFirst { case c: DeltaRangerMergeCommand =>
            c.receipt
          }.get
          val frozen = LegacyUnpartitionedMergeFingerprint(receipt)
          query.executedPlan.executeCollect()
          val journal = field[Path](receipt.journal, "file")
          val record = mapper.readTree(Files.readAllBytes(journal)).asInstanceOf[ObjectNode]
          assertEquals(frozen, record.get("command").textValue())
          // Model a retained pre-change COMMITTED record, including its full publication proof.
          // The fingerprint is emitted by the frozen baseline encoder, never by new production.
          record.put("command", frozen); record.put("state", "COMMITTED")
          Files.write(journal, record.toString.getBytes(StandardCharsets.UTF_8))
          val before = version("b")
          val files = parquetFiles("b")
          val rows = bag(spark.table("b.db.t").collect().toVector)
          val registration = NativeSelectFixture.bindingFixture(spark, "b")
          val refresh = registration.onRead
          registration.onRead = n => {
            assertNull(
              field[ThreadLocal[DeltaRangerMergeSource.Attempt]](DeltaRangerMergeSource, "active")
                .get()
            )
            assertNull(
              field[ThreadLocal[DeltaRangerMergeWrite]](DeltaRangerMergeWrite, "active").get()
            )
            refresh(n)
          }
          spark.sql(sql).collect()
          assertEquals(before, version("b")); assertEquals(files, parquetFiles("b"))
          assertEquals(rows, bag(spark.table("b.db.t").collect().toVector))
          assertEquals(
            "RECONCILED",
            mapper.readTree(Files.readAllBytes(journal)).get("state").textValue()
          )
          LoggerFactory
            .getLogger(getClass)
            .warn(
              "Frozen legacy recovery sourcePartitioned=" + partitionedSource + " command=" + frozen + " journal=" + record.toString
            )
        },
        prepareTarget = spark => if (partitionedSource) setupSource(spark)
      )
    }
  }

  @Test
  def partitionedIntentRecoversAndRejectsChangedOrderedLayouts(): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies(
      (spark, plugin) => {
        spark.conf.set("spark.databricks.delta.write.txnAppId", "partitioned-recovery")
        spark.conf.set("spark.databricks.delta.write.txnVersion", "7")
        spark.conf.set("spark.databricks.delta.write.txnVersion.autoReset.enabled", "false")
        val query = spark.sessionState.executePlan(spark.sessionState.sqlParser.parsePlan(sql))
        val receipt = query.analyzed.collectFirst { case c: DeltaRangerMergeCommand =>
          c.receipt
        }.get
        assertNotEquals(
          LegacyUnpartitionedMergeFingerprint(receipt),
          field[String](receipt.journal, "command"),
          "partitioned targets must never use the legacy encoding"
        )
        query.executedPlan.executeCollect()
        val journal = field[Path](receipt.journal, "file")
        val record = mapper.readTree(Files.readAllBytes(journal)).asInstanceOf[ObjectNode]
        record.put("state", "COMMITTED")
        Files.write(journal, record.toString.getBytes(StandardCharsets.UTF_8))
        val before = version("b"); val files = parquetFiles("b")
        val rows = bag(spark.table("b.db.t").collect().toVector)
        val registration = NativeSelectFixture.bindingFixture(spark, "b")
        val refresh = registration.onRead
        registration.onRead = n => {
          assertNull(
            field[ThreadLocal[DeltaRangerMergeSource.Attempt]](DeltaRangerMergeSource, "active")
              .get()
          )
          assertNull(
            field[ThreadLocal[DeltaRangerMergeWrite]](DeltaRangerMergeWrite, "active").get()
          )
          refresh(n)
        }
        spark.sql(sql).collect()
        assertEquals(
          "RECONCILED",
          mapper.readTree(Files.readAllBytes(journal)).get("state").textValue()
        )
        assertEquals(before, version("b")); assertEquals(files, parquetFiles("b"))
        assertEquals(rows, bag(spark.table("b.db.t").collect().toVector))
        // Change only the frozen ordered layout fact in a newly analyzed receipt. This isolates
        // durable binding from the separate active-metadata guards and keeps all other fields fixed.
        Seq("source-reordered", "target-reordered", "source-layout", "target-layout").foreach {
          mutation =>
            val retry = spark.sessionState.executePlan(spark.sessionState.sqlParser.parsePlan(sql))
            val fresh = retry.analyzed.collectFirst { case c: DeltaRangerMergeCommand =>
              c.receipt
            }.get
            val bound =
              if (mutation.startsWith("source")) fresh.source.binding.anchor() else fresh.target
            val binding = field[AnyRef](bound, "binding")
            val member = binding.getClass.getDeclaredField("partitions"); member.setAccessible(true)
            val original = member.get(binding)
            val changed =
              if (mutation.endsWith("reordered")) partitionStamp(Seq("secret", "region"))
              else partitionStamp(Seq("region"))
            assertNotEquals(original, changed)
            member.set(binding, changed)
            try {
              val failure = assertThrows(classOf[SecurityException], () => fresh.journal)
              assertTrue(
                failure.getMessage.contains("transaction identity intent changed"),
                failure.getMessage
              )
            } finally member.set(binding, original)
            assertEquals(before, version("b")); assertEquals(files, parquetFiles("b"))
        }
        policies.publish(plugin, deniedUpdate = Seq("secret"))
        assertThrows(classOf[SecurityException], () => spark.sql(sql))
        assertEquals(before, version("b")); assertEquals(files, parquetFiles("b"))
      },
      prepareTarget = spark => {
        setupSource(spark, Seq("region", "secret"))
        resetTarget(spark, Seq("region", "secret"), cdf = true)
      }
    )

  @Test
  def emptyStringNormalizesBeforeSuccessfulNativeCheck(): Unit = constraint(rejectNull = false)

  @Test
  def emptyStringViolatesNativeNotNullCheck(): Unit = constraint(rejectNull = true)

  private def constraint(rejectNull: Boolean): Unit = {
    val clause = if (rejectNull) "region IS NOT NULL" else "region IS NULL OR length(region) > 0"
    val query = sql
      .replace("region = s.region", "region = ''")
      .replace("VALUES (s.id, s.secret, s.region)", "VALUES (s.id, s.secret, NULL)")
    var oracleRows = Vector.empty[Row]
    var oracleFailed = false
    new TestDeltaRangerInstalledMerge().withPolicies(
      (spark, _) => {
        val before = version("b")
        if (rejectNull) {
          val failure = assertThrows(classOf[Exception], () => spark.sql(query).collect())
          assertTrue(
            messages(failure).contains("DELTA_VIOLATE_CONSTRAINT_WITH_VALUES"),
            messages(failure)
          )
          assertFalse(messages(failure).contains("writer profile"), messages(failure))
          assertTrue(oracleFailed); assertEquals(before, version("b"))
        } else {
          spark.sql(query).collect()
          assertEquals(before + 1, version("b"))
          assertEquals(
            bag(oracleRows),
            bag(spark.sql("SELECT id, secret, region FROM b.db.t").collect().toVector)
          )
          assertTrue(oracleRows.contains(Row(1L, "changed", null)))
          assertTrue(oracleRows.contains(Row(4L, "new", null)))
          val adds = commitActions("b", before + 1).collect { case a: AddFile => a }
          assertTrue(adds.exists(_.partitionValues == Map("region" -> null)))
          adds.foreach(a => assertEquals(Set("region"), a.partitionValues.keySet))
        }
      },
      prepareTarget = spark => {
        resetTarget(spark)
        val location = NativeSelectFixture.locations("b")
        spark
          .sql(s"ALTER TABLE delta.`$location` ADD CONSTRAINT partition_check CHECK ($clause)")
          .collect()
        val oracle = Paths.get(location).resolveSibling("native-constraint-oracle").toString
        spark.sql(initial).coalesce(1).write.format("delta").partitionBy("region").save(oracle)
        spark
          .sql(s"ALTER TABLE delta.`$oracle` ADD CONSTRAINT partition_check CHECK ($clause)")
          .collect()
        val native = query
          .replace("b.db.t", s"delta.`$oracle`")
          .replace(
            "table_changes('a.db.t'",
            s"table_changes_by_path('${NativeSelectFixture.locations("a")}'"
          )
        if (rejectNull) {
          val failure = assertThrows(classOf[Exception], () => spark.sql(native).collect())
          assertTrue(
            messages(failure).contains("DELTA_VIOLATE_CONSTRAINT_WITH_VALUES"),
            messages(failure)
          )
          oracleFailed = true
        } else spark.sql(native).collect()
        oracleRows = spark.read
          .format("delta")
          .load(oracle)
          .select("id", "secret", "region")
          .collect()
          .toVector
      }
    )
  }

  @Test
  def ownedRepartitionRejectsForeignExpressionsChildrenAndCarriers(): Unit = {
    var expired: DeltaRangerMergeWrite.Carrier = null
    var probed = false
    new TestDeltaRangerInstalledMerge().withPolicies(
      (spark, _) => {
        val registration = NativeSelectFixture.bindingFixture(spark, "b")
        val refresh = registration.onRead
        registration.onRead = n => {
          refresh(n)
          val owner =
            field[ThreadLocal[DeltaRangerMergeWrite]](DeltaRangerMergeWrite, "active").get()
          if (
            owner != null && !probed && field[DeltaRangerMergeWrite.Exec](
              owner,
              "writerRoot"
            ) != null
          ) {
            probed = true
            val original = field[LogicalPlan](owner, "writerOriginal")
            val carrier = field[DeltaRangerMergeWrite.Carrier](owner, "writerCarrier")
            expired = carrier
            val partition = original.collectFirst {
              case p: org.apache.spark.sql.catalyst.plans.logical.RepartitionByExpression => p
            }.get
            assertEquals(
              Seq("region", "secret"),
              partition.partitionExpressions.map(
                _.asInstanceOf[org.apache.spark.sql.catalyst.expressions.Attribute].name
              )
            )
            val changes = Seq(
              partition.copy(partitionExpressions = partition.partitionExpressions.reverse),
              partition.copy(partitionExpressions =
                Seq(org.apache.spark.sql.catalyst.expressions.Literal("foreign"))
              ),
              partition.copy(child =
                org.apache.spark.sql.catalyst.plans.logical.LocalRelation(partition.child.output)
              )
            )
            changes.foreach { changed =>
              val foreign = original.transformDown { case p if p eq partition => changed }
              assertFalse(owner.analysis(foreign, spark))
              assertTrue(owner.normalize(foreign, spark).isEmpty)
            }
            assertThrows(classOf[SecurityException], () => carrier.copy()(owner).owner)
            val root = field[DeltaRangerMergeWrite.Exec](owner, "writerRoot")
            assertThrows(classOf[SecurityException], () => root.copy()(owner).verify())
            assertThrows(classOf[SecurityException], () => root.executeCollect())
            val foreignChild =
              org.apache.spark.sql.execution.LocalTableScanExec(root.child.output, Nil)
            assertThrows(classOf[SecurityException], () => root.withNewChildren(Seq(foreignChild)))
            val captured = spark.sessionState.conf.getAllConfs
            Seq(
              DeltaSQLConf.MERGE_REPARTITION_BEFORE_WRITE.key,
              DeltaSQLConf.DELTA_COLLECT_STATS_USING_TABLE_SCHEMA.key
            ).foreach { key =>
              // RuntimeConfig.getOption includes defaults; restore exact explicit presence instead.
              val before = captured.get(key)
              spark.conf.set(key, "false")
              try assertThrows(classOf[SecurityException], () => carrier.owner)
              finally
                before match {
                  case Some(value) => spark.conf.set(key, value); case None => spark.conf.unset(key)
                }
              assertEquals(captured, spark.sessionState.conf.getAllConfs)
            }
            assertEquals(0L, version("b"))
          }
        }
        spark.sql(sql).collect()
        assertTrue(probed)
        assertThrows(classOf[SecurityException], () => expired.owner)
      },
      prepareTarget = spark => {
        spark.conf.set(DeltaSQLConf.MERGE_REPARTITION_BEFORE_WRITE.key, "true")
        spark.conf.set(DeltaSQLConf.DELTA_COLLECT_STATS_USING_TABLE_SCHEMA.key, "true")
        resetTarget(spark, Seq("region", "secret"), cdf = true)
      }
    )
  }

  @Test
  def changedCapturedRepartitionSettingCannotCommit(): Unit = changedCapturedSetting(
    DeltaSQLConf.MERGE_REPARTITION_BEFORE_WRITE.key
  )

  @Test
  def changedCapturedStatisticsSettingCannotCommit(): Unit = changedCapturedSetting(
    DeltaSQLConf.DELTA_COLLECT_STATS_USING_TABLE_SCHEMA.key
  )

  private def changedCapturedSetting(key: String): Unit = {
    var changed = false
    new TestDeltaRangerInstalledMerge().withPolicies(
      (spark, _) => {
        spark.conf.set(key, "true")
        val registration = NativeSelectFixture.bindingFixture(spark, "b")
        val refresh = registration.onRead
        val before = version("b")
        registration.onRead = n => {
          refresh(n)
          val owner =
            field[ThreadLocal[DeltaRangerMergeWrite]](DeltaRangerMergeWrite, "active").get()
          if (owner != null && !changed && field[Boolean](owner, "writing")) {
            changed = true
            spark.conf.set(key, "false")
          }
        }
        val failure = assertThrows(classOf[SecurityException], () => spark.sql(sql).collect())
        assertTrue(changed);
        assertTrue(messages(failure).contains("session/configuration changed"), messages(failure))
        assertEquals(before, version("b"))
      },
      prepareTarget = spark => {
        spark.conf.set(key, "true")
        resetTarget(spark)
      }
    )
  }

  @Test
  def partitionActionMapsRemainBoundToSubmittedAndPublishedIntent(): Unit = {
    var inspected = false
    var proof: String = null
    new TestDeltaRangerInstalledMerge().withPolicies(
      (spark, _) => {
        val registration = NativeSelectFixture.bindingFixture(spark, "b")
        val refresh = registration.onRead
        registration.onRead = n => {
          refresh(n)
          DeltaRangerMergeCommit.current.foreach { owner =>
            if (!inspected) {
              inspected = true
              val actions = field[Seq[Action]](owner, "actions")
              val intent = field[AnyRef](owner, "intent")
              val operation =
                field[org.apache.spark.sql.delta.DeltaOperations.Merge](owner, "nativeOperation")
              proof = field[String](intent, "encoded")
              val method =
                intent.getClass.getDeclaredMethods.find(_.getName == "requireArguments").get
              method.setAccessible(true)
              assertTrue(actions.exists(_.isInstanceOf[AddFile]))
              assertTrue(actions.exists(_.isInstanceOf[RemoveFile]))
              assertTrue(actions.exists(_.isInstanceOf[AddCDCFile]))
              actions.zipWithIndex.foreach { case (action, index) =>
                val maps = Seq(
                  Map("region" -> "forged"),
                  Map.empty[String, String],
                  Map("region" -> "old", "__is_cdc" -> "true")
                )
                val changed = maps.flatMap { values =>
                  action match {
                    case a: AddFile    => Some(a.copy(partitionValues = values))
                    case a: RemoveFile => Some(a.copy(partitionValues = values))
                    case a: AddCDCFile => Some(a.copy(partitionValues = values))
                    case _             => None
                  }
                }
                changed.foreach { candidate =>
                  val failure = assertThrows(
                    classOf[java.lang.reflect.InvocationTargetException],
                    () => {
                      method.invoke(
                        intent,
                        actions.updated(index, candidate),
                        operation,
                        Map.empty[String, String]
                      ); ()
                    }
                  )
                  assertTrue(failure.getCause.isInstanceOf[SecurityException])
                }
              }
              assertEquals(0L, version("b"))
            }
          }
        }
        spark.sql(sql).collect()
        assertTrue(inspected)
        val bytes = Files.readAllBytes(
          Paths.get(NativeSelectFixture.locations("b"), "_delta_log", "00000000000000000001.json")
        )
        DeltaRangerMergeCommit.verifyPublication(bytes, proof)
        Seq("add", "remove", "cdc").foreach { kind =>
          Seq("changed", "missing", "extra", "path").foreach { mutation =>
            val lines = new String(bytes, StandardCharsets.UTF_8).split("\n").map(mapper.readTree)
            val action = lines.find(_.has(kind)).get.get(kind).asInstanceOf[ObjectNode]
            val values = action.get("partitionValues").asInstanceOf[ObjectNode]
            mutation match {
              case "changed" => values.put("region", "forged")
              case "missing" => values.remove("region")
              case "extra"   => values.put("__is_cdc", "true")
              case "path"    => action.put("path", "foreign.parquet")
            }
            val changed =
              (lines.map(_.toString).mkString("\n") + "\n").getBytes(StandardCharsets.UTF_8)
            assertThrows(
              classOf[SecurityException],
              () => DeltaRangerMergeCommit.verifyPublication(changed, proof)
            )
          }
        }
      },
      prepareTarget = spark => resetTarget(spark, cdf = true)
    )
  }

  @Test
  def optimizedPartitionedWriterRemainsUnqualified(): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies(
      (spark, _) => {
        spark.conf.set(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key, "true")
        val before = version("b"); val files = parquetFiles("b")
        val failure = assertThrows(classOf[SecurityException], () => spark.sql(sql).collect())
        assertTrue(
          failure.getMessage.contains("optimized partitioned writer is not qualified"),
          failure.getMessage
        )
        assertEquals(before, version("b")); assertEquals(files, parquetFiles("b"))
      },
      prepareTarget = spark => resetTarget(spark)
    )

  private def messages(error: Throwable): String =
    Iterator.iterate(error)(_.getCause).takeWhile(_ != null).map(_.toString).mkString("\n")
  private def partitionStamp(names: Seq[String]): String =
    names.map(n => n.length + ":" + n).mkString

  private def scenario(
      cdf: Boolean,
      repartition: Boolean,
      tableStats: Boolean,
      multiple: Boolean = false
  ): Unit = {
    val keys = if (multiple) Seq("region", "secret") else Seq("region")
    val start =
      if (multiple)
        initial
          .replace("visible-raw", "shared-raw")
          .replace("delete-raw", "shared-raw")
          .replace("unchanged-raw", "shared-raw")
          .replace("hidden-raw", "shared-raw")
      else initial
    val finalRows = if (multiple) expected.map { row =>
      if (Set(3L, 8L)(row.getLong(0))) Row(row.getLong(0), "shared-raw", "old") else row
    }
    else expected
    val events = Vector(
      Row(1L, if (multiple) "shared-raw" else "visible-raw", "old", "update_preimage"),
      Row(1L, "changed", "keep", "update_postimage"),
      Row(2L, if (multiple) "shared-raw" else "delete-raw", "old", "delete"),
      Row(4L, "new", "keep", "insert")
    )
    var oracleRows = Vector.empty[Row]
    var oracleCdf = Vector.empty[Row]
    var oracleMaps = Set.empty[Map[String, String]]
    var oracleCdcMaps = Set.empty[Map[String, String]]
    var before = Vector.empty[AddFile]
    var membership = Map.empty[Long, String]
    new TestDeltaRangerInstalledMerge().withPolicies(
      (spark, plugin) => {
        spark.conf.set(DeltaSQLConf.MERGE_REPARTITION_BEFORE_WRITE.key, repartition.toString)
        spark.conf.set(DeltaSQLConf.DELTA_COLLECT_STATS_USING_TABLE_SCHEMA.key, tableStats.toString)
        assertEquals(
          repartition,
          spark.sessionState.conf.getConf(DeltaSQLConf.MERGE_REPARTITION_BEFORE_WRITE)
        )
        assertEquals(
          tableStats,
          spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_COLLECT_STATS_USING_TABLE_SCHEMA)
        )
        assertEquals(
          cdf,
          DeltaLog
            .forTable(spark, NativeSelectFixture.locations("b"))
            .unsafeVolatileSnapshot
            .metadata
            .configuration
            .getOrElse("delta.enableChangeDataFeed", "false")
            .toBoolean
        )
        LoggerFactory
          .getLogger(getClass)
          .warn(
            s"Partition Task1 scenario effective CDF=$cdf repartition=$repartition tableStats=$tableStats orderedKeys=$keys"
          )
        val registration = NativeSelectFixture.bindingFixture(spark, "b")
        val refresh = registration.onRead
        var observedWriter = false
        registration.onRead = n => {
          refresh(n)
          val owner =
            field[ThreadLocal[DeltaRangerMergeWrite]](DeltaRangerMergeWrite, "active").get()
          if (
            owner != null && !observedWriter && field[LogicalPlan](owner, "writerOriginal") != null
          ) {
            observedWriter = true
            val plan = field[LogicalPlan](owner, "writerOriginal")
            val partitions = plan.collect {
              case p: org.apache.spark.sql.catalyst.plans.logical.RepartitionByExpression => p
            }
            assertEquals(if (repartition) 1 else 0, partitions.size)
            partitions.foreach(p =>
              assertEquals(
                keys,
                p.partitionExpressions.map(
                  _.asInstanceOf[org.apache.spark.sql.catalyst.expressions.Attribute].name
                )
              )
            )
            assertEquals(cdf, plan.output.exists(_.name == "__is_cdc"))
          }
        }
        logOrigins(spark)
        assertEquals(
          membership(1L),
          membership(8L),
          "hidden row must share the actual touched file"
        )
        assertEquals(membership(1L), membership(3L)); assertEquals(membership(1L), membership(2L))
        assertNotEquals(membership(1L), membership(9L))
        policies.publish(
          plugin,
          filter = Some("id <> 8L"),
          masks = Map("secret" -> "concat({col}, '-masked')")
        )
        assertEquals(Vector(Row(3L, 1L, 1L, 1L)), spark.sql(sql).collect().toVector)
        assertTrue(observedWriter)
        policies.publish(plugin)
        assertEquals(bag(finalRows), bag(oracleRows))
        assertEquals(
          bag(oracleRows),
          bag(spark.sql("SELECT id, secret, region FROM b.db.t").collect().toVector)
        )
        val actions = commitActions("b", 1L)
        val adds = actions.collect { case a: AddFile => a }
        val removed = actions.collect { case r: RemoveFile => r }
        assertEquals(
          before
            .filter(_.partitionValues("region") == "old")
            .map(a => a.path -> a.partitionValues)
            .toMap,
          removed.map(a => a.path -> a.partitionValues).toMap
        )
        val untouched = before.find(_.partitionValues("region") == "other").get
        assertFalse(removed.exists(_.path == untouched.path))
        val active =
          before.map(a => a.path -> a).toMap -- removed.map(_.path) ++ adds.map(a => a.path -> a)
        assertEquals(untouched.json, active(untouched.path).json)
        assertEquals(oracleMaps, adds.map(_.partitionValues).toSet)
        adds.foreach { add =>
          assertEquals(keys.toSet, add.partitionValues.keySet)
          val stats = mapper.readTree(add.stats)
          Seq("minValues", "maxValues", "nullCount").foreach { key =>
            (keys :+ "__is_cdc").foreach(p => assertFalse(stats.path(key).has(p), add.stats))
            assertTrue(stats.path(key).has("id"), add.stats)
          }
        }
        val cdcActions = actions.collect { case a: AddCDCFile => a }
        if (cdf) {
          assertFalse(cdcActions.isEmpty)
          cdcActions.foreach { a =>
            assertEquals(keys.toSet, a.partitionValues.keySet)
            assertTrue(a.path.startsWith("_change_data/"), a.path)
          }
          assertEquals(oracleCdcMaps, cdcActions.map(_.partitionValues).toSet)
          assertEquals(bag(events), bag(oracleCdf))
          val actual = spark
            .sql("SELECT id, secret, region, _change_type FROM table_changes('b.db.t', 1, 1)")
            .collect()
            .toVector
          assertEquals(bag(oracleCdf), bag(actual))
        } else assertTrue(cdcActions.isEmpty)
      },
      prepareTarget = spark => {
        spark.conf.set(DeltaSQLConf.MERGE_REPARTITION_BEFORE_WRITE.key, repartition.toString)
        spark.conf.set(DeltaSQLConf.DELTA_COLLECT_STATS_USING_TABLE_SCHEMA.key, tableStats.toString)
        resetTarget(spark, keys, cdf, start)
        val location = NativeSelectFixture.locations("b")
        before = DeltaLog.forTable(spark, location).update().allFiles.collect().toVector
        membership = spark.read
          .format("delta")
          .load(location)
          .select(org.apache.spark.sql.functions.col("id"), input_file_name())
          .collect()
          .map(row => row.getLong(0) -> row.getString(1))
          .toMap
        val oracle = Paths.get(location).resolveSibling("native-partition-oracle").toString
        spark
          .sql(start)
          .coalesce(1)
          .write
          .format("delta")
          .partitionBy(keys: _*)
          .option("delta.enableChangeDataFeed", cdf.toString)
          .save(oracle)
        val listener = new org.apache.spark.sql.util.QueryExecutionListener {
          override def onSuccess(
              name: String,
              query: org.apache.spark.sql.execution.QueryExecution,
              nanos: Long
          ): Unit = {
            if (
              query.analyzed.output.map(_.name).contains("id") && query.logical.exists(
                _.isInstanceOf[org.apache.spark.sql.catalyst.plans.logical.RepartitionByExpression]
              )
            )
              LoggerFactory
                .getLogger(getClass)
                .warn(
                  "Stock native partition writer logical=" + query.analyzed.treeString + " physical=" + query.executedPlan.treeString
                )
          }
          override def onFailure(
              name: String,
              query: org.apache.spark.sql.execution.QueryExecution,
              error: Exception
          ): Unit = ()
        }
        spark.listenerManager.register(listener)
        try
          spark
            .sql(
              sql
                .replace("b.db.t", s"delta.`$oracle`")
                .replace(
                  "table_changes('a.db.t'",
                  s"table_changes_by_path('${NativeSelectFixture.locations("a")}'"
                )
                .replace("ON t.id = s.id", "ON t.id = s.id AND t.id <> 8L")
            )
            .collect()
        finally spark.listenerManager.unregister(listener)
        oracleRows = spark.read
          .format("delta")
          .load(oracle)
          .select("id", "secret", "region")
          .collect()
          .toVector
        val nativeActions = Files
          .readAllLines(Paths.get(oracle, "_delta_log", "00000000000000000001.json"))
          .asScala
          .map(Action.fromJson)
        oracleMaps = nativeActions.collect { case a: AddFile => a.partitionValues }.toSet
        oracleCdcMaps = nativeActions.collect { case a: AddCDCFile => a.partitionValues }.toSet
        if (cdf)
          oracleCdf = spark.read
            .format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", "1")
            .option("endingVersion", "1")
            .load(oracle)
            .select("id", "secret", "region", "_change_type")
            .collect()
            .toVector
      }
    )
  }

  private def resetTarget(
      spark: SparkSession,
      keys: Seq[String] = Seq("region"),
      cdf: Boolean = false,
      rows: String = initial
  ): Unit = {
    replace("b")
    spark
      .sql(rows)
      .coalesce(1)
      .write
      .format("delta")
      .partitionBy(keys: _*)
      .option("delta.enableChangeDataFeed", cdf.toString)
      .save(NativeSelectFixture.locations("b"))
  }

  private def setupSource(spark: SparkSession, keys: Seq[String] = Seq("region")): Unit = {
    replace("a")
    val location = NativeSelectFixture.locations("a")
    spark
      .sql("SELECT 1L id, 'old' secret, 'keep' region UNION ALL SELECT 2L, 'old', 'drop'")
      .coalesce(1)
      .write
      .format("delta")
      .partitionBy(keys: _*)
      .option("delta.enableChangeDataFeed", "true")
      .save(location)
    (1 to 3).foreach { n =>
      spark
        .sql(s"SELECT ${10 + n}L id, 'history' secret, 'keep' region")
        .write
        .format("delta")
        .mode("append")
        .save(location)
    }
    spark
      .sql(s"""MERGE INTO delta.`$location` t USING
      (SELECT 1L id, 'changed' secret UNION ALL SELECT 2L, 'delete' UNION ALL SELECT 4L, 'new') s
      ON t.id = s.id WHEN MATCHED AND s.id = 2 THEN DELETE
      WHEN MATCHED THEN UPDATE SET secret = s.secret
      WHEN NOT MATCHED THEN INSERT (id, secret, region) VALUES (s.id, s.secret, 'keep')""")
      .collect()
  }
  private def replace(name: String): Unit = {
    val paths = Files.walk(Paths.get(NativeSelectFixture.locations(name)))
    try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.delete)
    finally paths.close()
    DeltaLog.clearCache()
  }
  private def commitActions(name: String, version: Long): Vector[Action] =
    Files
      .readAllLines(
        Paths.get(NativeSelectFixture.locations(name), "_delta_log", f"$version%020d.json")
      )
      .asScala
      .toVector
      .map(Action.fromJson)
  private def parquetFiles(name: String): Set[String] = {
    val files = Files.walk(Paths.get(NativeSelectFixture.locations(name)))
    try files.iterator().asScala.filter(_.toString.endsWith(".parquet")).map(_.toString).toSet
    finally files.close()
  }
  private def logOrigins(spark: SparkSession): Unit = {
    val log = LoggerFactory.getLogger(getClass)
    Seq(
      classOf[SparkSession],
      classOf[DeltaLog],
      classOf[RangerBasePlugin],
      classOf[scala.Option[_]]
    )
      .foreach(c =>
        log.warn(
          "Partition Task1 origin: " + c.getName + " " + c.getProtectionDomain.getCodeSource.getLocation + " version=" + c.getPackage.getImplementationVersion
        )
      )
    log.warn(
      "Partition Task1 runtime Spark=" + spark.version + " Scala=" + scala.util.Properties.versionNumberString + " Java=" + System
        .getProperty("java.version")
    )
  }
}

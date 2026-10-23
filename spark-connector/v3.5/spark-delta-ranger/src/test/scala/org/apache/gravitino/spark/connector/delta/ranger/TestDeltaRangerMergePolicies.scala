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
import java.util.Collections
import java.util.concurrent.ConcurrentLinkedQueue
import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters._
import org.apache.hadoop.security.UserGroupInformation
import org.apache.ranger.plugin.model.{RangerPolicy, RangerRole, RangerValiditySchedule}
import org.apache.ranger.plugin.model.RangerPolicy._
import org.apache.ranger.plugin.service.RangerBasePlugin
import org.apache.ranger.plugin.util.{RangerRoles, ServicePolicies}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart
import org.slf4j.LoggerFactory
import org.apache.spark.sql.delta.DeltaLog
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** Real Ranger policies over named SQL and the native executor MERGE path. */
class TestDeltaRangerMergePolicies {
  private val source = """(SELECT * FROM table_changes('a.db.t', 0, 4)
    WHERE _commit_version = 4 AND _change_type = 'update_postimage') s"""
  private def merge(clauses: String, on: String = "t.id = s.id"): String =
    s"MERGE INTO b.db.t t USING $source ON $on $clauses"
  private val insert =
    "WHEN NOT MATCHED THEN INSERT (id, secret, region) VALUES (s.id, s.secret, s.region)"

  @Test
  def hiddenRowsAndUnassignedMaskedFieldsSurviveRewrite(): Unit = fixture { (spark, plugin) =>
    publish(
      plugin,
      filter = Some("region = 'keep'"),
      masks = Map("secret" -> "concat({col}, '-masked')")
    )
    spark.sql(merge("WHEN MATCHED THEN UPDATE SET region = 'changed' " + insert)).collect()
    publish(plugin)
    assertEquals(
      Set(Row(1L, "b-one", "changed"), Row(2L, "b-two", "drop"), Row(3L, null, "keep")),
      rows(spark)
    )
    publish(plugin, masks = Map("secret" -> "concat({col}, '-masked')"))
    spark.sql(merge("WHEN MATCHED THEN UPDATE SET secret = t.secret " + insert)).collect()
    publish(plugin)
    assertEquals(
      Set(Row(1L, "b-one-masked", "changed"), Row(2L, "b-two", "drop"), Row(3L, null, "keep")),
      rows(spark)
    )
  }

  @Test
  def targetMatchUsesPoliciesAndNativeMultiplicity(): Unit = fixture { (spark, plugin) =>
    publish(plugin, masks = Map("id" -> "CAST(1 AS BIGINT)"))
    spark.sql(merge("WHEN MATCHED THEN UPDATE SET region = 'all' " + insert)).collect()
    publish(plugin)
    assertEquals(
      Set(Row(1L, "b-one", "all"), Row(2L, "b-two", "all"), Row(3L, null, "all")),
      rows(spark)
    )
  }

  @Test
  def columnOnlyUpdatePreservesDeniedUnassignedFields(): Unit = fixture { (spark, plugin) =>
    publish(
      plugin,
      selected = Seq("id"),
      updated = Seq("region"),
      deniedSelect = Seq("secret"),
      deniedUpdate = Seq("secret"),
      filter = Some("region = 'keep'"),
      masks = Map("secret" -> "concat({col}, '-masked')")
    )
    spark.sql(merge("WHEN MATCHED THEN UPDATE SET region = 'narrow'")).collect()
    publish(plugin)
    assertEquals(
      Set(Row(1L, "b-one", "narrow"), Row(2L, "b-two", "drop"), Row(3L, null, "keep")),
      rows(spark)
    )
  }

  @Test
  def originalColumnDemandsSurviveUnreachableClausesAndEmptySources(): Unit = fixture {
    (spark, plugin) =>
      publish(plugin, deniedUpdate = Seq("secret"))
      Seq(
        "WHEN MATCHED THEN UPDATE SET secret = 'denied'",
        "WHEN MATCHED THEN UPDATE SET secret = t.secret",
        "WHEN MATCHED AND false THEN UPDATE SET secret = 'denied' WHEN MATCHED THEN UPDATE SET region = 'ok'",
        "WHEN MATCHED THEN UPDATE SET *",
        "WHEN MATCHED THEN DELETE",
        "WHEN MATCHED THEN UPDATE SET region = 'ok' WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)"
      ).foreach { clause =>
        reject(spark, merge(clause))
        reject(spark, merge(clause).replace("_commit_version = 4", "_commit_version = -1"))
      }
      publish(plugin, deniedSelect = Seq("secret"))
      Seq(
        "WHEN MATCHED THEN UPDATE SET region = t.secret",
        "WHEN MATCHED AND false THEN UPDATE SET region = t.secret WHEN MATCHED THEN UPDATE SET region = 'ok'",
        "WHEN MATCHED AND t.secret = 'b-one' THEN UPDATE SET region = 'ok'"
      ).foreach { clause =>
        reject(spark, merge(clause))
        reject(spark, merge(clause).replace("_commit_version = 4", "_commit_version = -1"))
      }
      val deniedOn = merge("WHEN MATCHED THEN UPDATE SET region = 'ok'", "t.secret = s.secret")
      reject(spark, deniedOn)
      reject(spark, deniedOn.replace("_commit_version = 4", "_commit_version = -1"))
      reject(spark, merge("WHEN NOT MATCHED BY SOURCE THEN DELETE"))
  }

  @Test
  def hiddenKeyCreatesUnmatchedInsertOutsideVisibility(): Unit = fixture { (spark, plugin) =>
    publish(plugin, filter = Some("region = 'drop'"))
    spark.sql(merge("WHEN MATCHED THEN UPDATE SET region = 'changed' " + insert)).collect()
    publish(plugin)
    val actual = spark.table("b.db.t").collect().toVector
    assertEquals(4, actual.size)
    assertEquals(
      Set(
        Row(1L, "b-one", "keep"),
        Row(1L, "changed", "keep"),
        Row(2L, "b-two", "drop"),
        Row(3L, null, "keep")
      ),
      actual.toSet
    )
  }

  @Test
  def nativeMultipleSourceAmbiguityAndUnconditionalDeleteException(): Unit = fixture {
    (spark, plugin) =>
      publish(
        plugin,
        masks = Map("id" -> "CAST(1 AS BIGINT)"),
        sourceMasks = Map("id" -> "CAST(1 AS BIGINT)")
      )
      val update = merge("WHEN MATCHED THEN UPDATE SET region = 'ambiguous' " + insert)
        .replace("_change_type = 'update_postimage'", "_change_type <> 'update_preimage'")
      val failure = reject(spark, update)
      assertTrue(
        messages(failure).contains("DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE"),
        messages(failure)
      )
      assertEquals(
        Vector(Row(3L, 0L, 3L, 0L)),
        spark.sql(update.replace("UPDATE SET region = 'ambiguous'", "DELETE")).collect().toVector
      )
      publish(plugin)
      assertTrue(rows(spark).isEmpty)
  }

  @Test
  def nullMasksFollowSqlAndNullSafeEquality(): Unit = fixture { (spark, plugin) =>
    publish(plugin, masks = Map("id" -> "MASK_NULL"), sourceMasks = Map("id" -> "MASK_NULL"))
    spark
      .sql(merge("WHEN MATCHED THEN UPDATE SET region = 'nullsafe' " + insert, "t.id <=> s.id"))
      .collect()
    publish(plugin)
    assertEquals(
      Set(Row(1L, "b-one", "nullsafe"), Row(2L, "b-two", "nullsafe"), Row(3L, null, "nullsafe")),
      rows(spark)
    )
    publish(plugin, masks = Map("id" -> "MASK_NULL"), sourceMasks = Map("id" -> "MASK_NULL"))
    spark.sql(merge("WHEN MATCHED THEN UPDATE SET region = 'wrong' " + insert)).collect()
    publish(plugin)
    assertEquals(4, rows(spark).size)
    assertTrue(rows(spark).contains(Row(null, "changed", "keep")))
  }

  @Test
  def sourceMasksGovernKeysMetadataAndAuthoredTargetExpressionsOnce(): Unit = fixture {
    (spark, plugin) =>
      publish(
        plugin,
        masks = Map("secret" -> "concat({col}, '-target')"),
        sourceMasks = Map(
          "secret" -> "concat({col}, '-source')",
          "_change_type" -> "upper({col})",
          "_commit_version" -> "CAST(44 AS BIGINT)"
        )
      )
      val sql = merge(
        "WHEN MATCHED AND t.secret = 'b-one-target' THEN UPDATE SET secret = s.secret " + insert
      )
        .replace("_commit_version = 4", "_commit_version = 44")
        .replace("'update_postimage'", "'UPDATE_POSTIMAGE'")
      spark.sql(sql).collect()
      publish(plugin)
      assertTrue(rows(spark).contains(Row(1L, "changed-source", "keep")))
  }

  @Test
  def sourceScheduledMaskAfterSourceClosurePreventsCommit(): Unit =
    scheduledAfterClosure(sourcePolicy = true)

  @Test
  def targetScheduledFilterAfterSourceClosurePreventsCommit(): Unit =
    scheduledAfterClosure(sourcePolicy = false)

  private def scheduledAfterClosure(sourcePolicy: Boolean): Unit = fixture { (spark, plugin) =>
    val format = DateTimeFormatter
      .ofPattern(RangerValiditySchedule.VALIDITY_SCHEDULE_DATE_STRING_SPECIFICATION)
      .withZone(ZoneOffset.UTC)
    val activation = Instant.now().plusSeconds(30)
    val schedule = new RangerValiditySchedule(
      format.format(activation),
      format.format(activation.plusSeconds(120)),
      "UTC",
      null
    )
    if (sourcePolicy)
      publish(
        plugin,
        sourceMasks = Map("_change_type" -> "MASK_NULL"),
        sourceSchedule = Some(schedule)
      )
    else publish(plugin, filter = Some("region = 'drop'"), filterSchedule = Some(schedule))
    val registration = NativeSelectFixture.bindingFixture(spark, "b")
    val refresh = registration.onRead
    var receipt: DeltaRangerMergeReceipt = null
    var crossed = false
    registration.onRead = n => {
      refresh(n)
      val writer = field[ThreadLocal[DeltaRangerMergeWrite]](DeltaRangerMergeWrite, "active").get()
      if (writer != null) receipt = field[DeltaRangerMergeReceipt](writer, "receipt")
      if (receipt != null && field[Boolean](receipt, "sourceClosed") && !crossed) {
        crossed = true
        assertTrue(
          Instant.now().isBefore(activation),
          "schedule activated before the closed-source probe"
        )
        while (!Instant.now().isAfter(activation.plusSeconds(1))) Thread.sleep(100)
        assertThrows(classOf[SecurityException], () => receipt.cdf.validateAction(spark))
      }
    }
    val failure =
      reject(spark, merge("WHEN MATCHED THEN UPDATE SET region = 'scheduled' " + insert))
    assertTrue(crossed)
    assertTrue(
      messages(failure).contains(
        if (sourcePolicy) "CDF policy changed" else "target policies changed"
      ),
      messages(failure)
    )
  }

  @Test
  def roleChangeAfterSourceClosureInvalidatesReceipt(): Unit = fixture { (spark, plugin) =>
    publish(plugin, useRole = true)
    val registration = NativeSelectFixture.bindingFixture(spark, "b")
    val refresh = registration.onRead
    var receipt: DeltaRangerMergeReceipt = null
    var revoked = false
    registration.onRead = n => {
      refresh(n)
      val writer = field[ThreadLocal[DeltaRangerMergeWrite]](DeltaRangerMergeWrite, "active").get()
      if (writer != null) receipt = field[DeltaRangerMergeReceipt](writer, "receipt")
      if (receipt != null && field[Boolean](receipt, "sourceClosed") && !revoked) {
        revoked = true
        val roles = new RangerRoles
        roles.setServiceName("service"); roles.setRoleVersion(3L);
        roles.setRangerRoles(Collections.emptySet())
        plugin.setRoles(roles)
      }
    }
    reject(spark, merge("WHEN MATCHED THEN UPDATE SET region = 'role' " + insert))
    assertTrue(revoked)
  }

  @Test
  def sameTableMasksPreserveSourceAndTargetOccurrences(): Unit = fixture { (spark, plugin) =>
    publish(plugin, masks = Map("secret" -> "concat({col}, '-masked')"), targetCatalog = "a")
    spark
      .sql(
        merge("WHEN MATCHED THEN UPDATE SET REGION = t.SeCrEt", "t.ID = s.ID")
          .replace("MERGE INTO b.db.t", "MERGE INTO a.db.t")
      )
      .collect()
    publish(plugin)
    assertTrue(spark.table("a.db.t").collect().contains(Row(1L, "changed", "changed-masked")))
  }

  @Test
  def unconditionalDeleteKeepsNativeCdfDeduplication(): Unit = fixture(
    { (spark, plugin) =>
      publish(
        plugin,
        filter = Some("region = 'keep'"),
        masks = Map("id" -> "CAST(1 AS BIGINT)"),
        sourceMasks = Map("id" -> "CAST(1 AS BIGINT)")
      )
      val result = spark
        .sql(
          merge("WHEN MATCHED THEN DELETE " + insert)
            .replace("_change_type = 'update_postimage'", "_change_type <> 'update_preimage'")
        )
        .collect()
      assertEquals(Vector(Row(2L, 0L, 2L, 0L)), result.toVector)
      publish(plugin)
      assertEquals(Set(Row(2L, "b-two", "drop")), rows(spark))
      val changes = spark
        .sql("SELECT id, secret, region, _change_type FROM table_changes('b.db.t', 1, 1)")
        .collect()
        .toVector
      assertEquals(2, changes.size)
      assertEquals(
        Set(
          Row(1L, "b-one", "keep", "delete"),
          Row(3L, null, "keep", "delete")
        ),
        changes.toSet
      )
    },
    cdf = true
  )

  @Test
  def nativeCdfDeleteDeduplicationRetainsUnmatchedInserts(): Unit = fixture(
    (spark, plugin) => {
      publish(plugin, filter = Some("region = 'keep'"), masks = Map("id" -> "CAST(1 AS BIGINT)"))
      assertEquals(
        Vector(Row(4L, 0L, 2L, 2L)),
        spark
          .sql(
            merge("WHEN MATCHED THEN DELETE " + insert)
              .replace(" AND _change_type = 'update_postimage'", "")
          )
          .collect()
          .toVector
      )
      publish(plugin)
      assertEquals(
        Set(Row(2L, "b-two", "drop"), Row(2L, "old", "drop"), Row(4L, "new", "keep")),
        rows(spark)
      )
      val changes = spark
        .sql("SELECT id, secret, region, _change_type FROM table_changes('b.db.t', 1, 1)")
        .collect()
        .toVector
      assertEquals(4, changes.size)
      assertEquals(
        Set(
          Row(1L, "b-one", "keep", "delete"),
          Row(3L, null, "keep", "delete"),
          Row(2L, "old", "drop", "insert"),
          Row(4L, "new", "keep", "insert")
        ),
        changes.toSet
      )
    },
    cdf = true
  )

  @Test
  def strictSourceConsistencyMatchesStockNativeConstantKeyJoin(): Unit = {
    val setting = "spark.databricks.delta.merge.failIfSourceChanged"
    def observe(spark: SparkSession, label: String): ConcurrentLinkedQueue[String] = {
      val plans = new ConcurrentLinkedQueue[String]()
      spark.sparkContext.addSparkListener(new SparkListener {
        override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
          case start: SparkListenerSQLExecutionStart
              if start.physicalPlanDescription.contains("BroadcastNestedLoopJoin") =>
            plans.add(start.physicalPlanDescription)
            LoggerFactory
              .getLogger(getClass)
              .warn(label + " physical plan: " + start.physicalPlanDescription)
          case _ =>
        }
      })
      LoggerFactory
        .getLogger(getClass)
        .warn(
          label + " materializeSource=" +
            spark.conf.get(
              "spark.databricks.delta.merge.materializeSource",
              "auto"
            ) + " strict=" + spark.conf.get(setting)
        )
      plans
    }
    def requireOuter(plans: ConcurrentLinkedQueue[String]): Unit = {
      val deadline = System.nanoTime() + 5000000000L
      while (!plans.asScala.exists(_.contains("FullOuter")) && System.nanoTime() < deadline)
        Thread.sleep(20)
      assertTrue(
        plans.asScala.exists(plan =>
          plan.contains("BroadcastNestedLoopJoin FullOuter BuildRight") &&
            plan.contains("Scan ExistingRDD mergeMaterializedSource")
        ),
        plans.toString
      )
    }
    fixture(
      (spark, plugin) => {
        spark.conf.set(setting, "true")
        val plans = observe(spark, "governed-strict")
        publish(
          plugin,
          filter = Some("region = 'keep'"),
          masks = Map("id" -> "CAST(1 AS BIGINT)"),
          sourceMasks = Map("id" -> "CAST(1 AS BIGINT)")
        )
        val failure = reject(
          spark,
          merge("WHEN MATCHED THEN DELETE " + insert)
            .replace("_change_type = 'update_postimage'", "_change_type <> 'update_preimage'")
        )
        assertTrue(
          messages(failure).contains("source dataset is not deterministic"),
          messages(failure)
        )
        requireOuter(plans)
      },
      cdf = true,
      beforeGovernance = spark => {
        val target =
          Paths.get(NativeSelectFixture.locations("b")).resolveSibling("native-oracle").toString
        spark.read
          .format("delta")
          .load(NativeSelectFixture.locations("b"))
          .coalesce(1)
          .write
          .format("delta")
          .option("delta.enableChangeDataFeed", "true")
          .save(target)
        spark.read
          .format("delta")
          .option("readChangeFeed", "true")
          .option("startingVersion", "0")
          .option("endingVersion", "4")
          .load(NativeSelectFixture.locations("a"))
          .createOrReplaceTempView("native_oracle_source")
        spark.conf.set(setting, "true")
        val plans = observe(spark, "stock-native-strict")
        val failure = assertThrows(
          classOf[Exception],
          () =>
            spark
              .sql(s"""MERGE INTO delta.`$target` t USING
          |(SELECT CAST(1 AS BIGINT) id, secret, region FROM native_oracle_source
          | WHERE _commit_version = 4 AND _change_type <> 'update_preimage') s
          |ON t.region = 'keep' AND CAST(1 AS BIGINT) = s.id
          |WHEN MATCHED THEN DELETE
          |WHEN NOT MATCHED THEN INSERT (id, secret, region) VALUES (s.id, s.secret, s.region)
          |""".stripMargin)
              .collect()
        )
        assertTrue(
          messages(failure).contains("source dataset is not deterministic"),
          messages(failure)
        )
        assertEquals(0L, DeltaLog.forTable(spark, target).update().version)
        requireOuter(plans)
        spark.catalog.dropTempView("native_oracle_source")
        spark.conf.unset(setting)
      }
    )
  }

  @Test
  def updateStarAndOmittedInsertFieldsUseNativeAssignments(): Unit = fixture { (spark, plugin) =>
    publish(
      plugin,
      masks = Map("secret" -> "concat({col}, '-target')"),
      sourceMasks = Map("secret" -> "concat({col}, '-source')")
    )
    spark.sql(merge("WHEN MATCHED THEN UPDATE SET *")).collect()
    publish(plugin)
    assertTrue(rows(spark).contains(Row(1L, "changed-source", "keep")))
    spark
      .sql(
        merge(
          "WHEN MATCHED THEN UPDATE SET region = 'unreached' WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)"
        )
          .replace("_change_type = 'update_postimage'", "_change_type = 'insert'")
      )
      .collect()
    assertTrue(rows(spark).contains(Row(4L, null, null)))
    assertEquals(4, rows(spark).size)
  }

  @Test
  def matchedOnlyPredicateUsesMaskAndRawFilter(): Unit = fixture { (spark, plugin) =>
    publish(
      plugin,
      filter = Some("region = 'keep'"),
      masks = Map("secret" -> "concat({col}, '-masked')"),
      selected = Seq("id", "secret"),
      updated = Seq("region")
    )
    spark
      .sql(merge("WHEN MATCHED AND t.secret = 'b-one-masked' THEN UPDATE SET region = 'outside'"))
      .collect()
    publish(plugin)
    assertTrue(rows(spark).contains(Row(1L, "b-one", "outside")))
    assertTrue(rows(spark).contains(Row(2L, "b-two", "drop")))
  }

  @Test
  def unsupportedTargetAndSourceMasksRejectBeforeMutation(): Unit = fixture { (spark, plugin) =>
    Seq("sha2({col}, 256)", "CAST({col} AS INT)", "secret").foreach { expression =>
      publish(plugin, masks = Map("id" -> expression))
      val failure = reject(spark, merge("WHEN MATCHED THEN UPDATE SET region = 'bad' " + insert))
      assertTrue(messages(failure).contains("Ranger"), messages(failure))
    }
    publish(plugin, sourceMasks = Map("id" -> "sha2({col}, 256)"))
    reject(spark, merge("WHEN MATCHED THEN UPDATE SET region = 'bad' " + insert))
  }

  private def field[A](value: AnyRef, name: String): A = {
    val member = value.getClass.getDeclaredFields
      .find(f => f.getName == name || f.getName.endsWith("$$" + name))
      .get
    member.setAccessible(true); member.get(value).asInstanceOf[A]
  }

  private def messages(error: Throwable): String =
    Iterator.iterate(error)(_.getCause).takeWhile(_ != null).map(_.toString).mkString("\n")
  private def reject(spark: SparkSession, sql: String): Exception = {
    val before = DeltaLog.forTable(spark, NativeSelectFixture.locations("b")).update().version
    val failure = assertThrows(classOf[Exception], () => spark.sql(sql).collect())
    assertEquals(
      before,
      DeltaLog.forTable(spark, NativeSelectFixture.locations("b")).update().version
    )
    failure
  }

  private def rows(spark: SparkSession): Set[Row] = spark.table("b.db.t").collect().toSet
  private def fixture(
      test: (SparkSession, RangerBasePlugin) => Unit,
      cdf: Boolean = false,
      beforeGovernance: SparkSession => Unit = _ => ()
  ): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies(
      test,
      prepareTarget = spark => {
        val path = Paths.get(NativeSelectFixture.locations("b"))
        val files = Files.walk(path)
        try files.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.delete)
        finally files.close()
        DeltaLog.clearCache()
        spark
          .sql(
            "SELECT 1L id, 'b-one' secret, 'keep' region UNION ALL SELECT 2L, 'b-two', 'drop' UNION ALL SELECT 3L, CAST(NULL AS STRING), 'keep'"
          )
          .coalesce(1)
          .write
          .format("delta")
          .option("delta.enableChangeDataFeed", cdf.toString)
          .save(path.toString)
        assertEquals(1, DeltaLog.forTable(spark, path.toString).update().allFiles.count())
        beforeGovernance(spark)
      }
    )

  private var version = 10L
  private def publish(
      plugin: RangerBasePlugin,
      filter: Option[String] = None,
      masks: Map[String, String] = Map.empty,
      selected: Seq[String] = Seq("*"),
      updated: Seq[String] = Seq("*"),
      deniedSelect: Seq[String] = Nil,
      deniedUpdate: Seq[String] = Nil,
      sourceMasks: Map[String, String] = Map.empty,
      sourceSchedule: Option[RangerValiditySchedule] = None,
      useRole: Boolean = false,
      targetCatalog: String = "b",
      filterSchedule: Option[RangerValiditySchedule] = None
  ): Unit = {
    if (useRole) {
      val role = new RangerRole; role.setName("merge-role")
      role.setUsers(
        Collections.singletonList(
          new RangerRole.RoleMember(UserGroupInformation.getCurrentUser.getShortUserName, false)
        )
      )
      val roles = new RangerRoles; roles.setServiceName("service"); roles.setRoleVersion(2L)
      roles.setRangerRoles(Collections.singleton(role)); plugin.setRoles(roles)
    }
    val policies = scala.collection.mutable.ArrayBuffer.empty[RangerPolicy]
    def resource(catalog: String, kind: Int, columns: Seq[String]): RangerPolicy = {
      val p = new RangerPolicy
      p.setId(policies.size.toLong + 1); p.setName("merge-policy-" + policies.size)
      p.setService("service"); p.setPolicyType(kind); p.setIsEnabled(true)
      p.setResources(
        (Map(
          "catalog" -> new RangerPolicyResource(catalog),
          "database" -> new RangerPolicyResource("db"),
          "table" -> new RangerPolicyResource("t")
        ) ++
          (if (columns.isEmpty) Map.empty[String, RangerPolicyResource]
           else Map("column" -> new RangerPolicyResource(columns.asJava, false, false)))).asJava
      )
      policies += p; p
    }
    def access(item: RangerPolicyItem, kind: String): Unit = {
      if (useRole) item.setRoles(Collections.singletonList("merge-role"))
      else
        item.setUsers(
          Collections.singletonList(UserGroupInformation.getCurrentUser.getShortUserName)
        )
      item.setAccesses(Collections.singletonList(new RangerPolicyItemAccess(kind, true)))
    }
    def grant(catalog: String, kind: String, columns: Seq[String], deny: Boolean = false): Unit =
      if (columns.nonEmpty) {
        val p = resource(catalog, 0, columns); val item = new RangerPolicyItem; access(item, kind)
        if (deny) p.setDenyPolicyItems(Collections.singletonList(item))
        else p.setPolicyItems(Collections.singletonList(item))
      }
    grant("a", "select", Seq("*")); grant(targetCatalog, "select", selected);
    grant(targetCatalog, "update", updated)
    grant(targetCatalog, "select", deniedSelect, true);
    grant(targetCatalog, "update", deniedUpdate, true)
    filter.foreach { value =>
      val p = resource(targetCatalog, 2, Nil); val item = new RangerRowFilterPolicyItem;
      access(item, "select")
      item.setRowFilterInfo(new RangerPolicyItemRowFilterInfo(value));
      p.setRowFilterPolicyItems(Collections.singletonList(item))
      filterSchedule.foreach(s => p.setValiditySchedules(Collections.singletonList(s)))
    }
    Seq("a" -> sourceMasks, targetCatalog -> masks).foreach { case (catalog, values) =>
      values.foreach { case (column, value) =>
        val p = resource(catalog, 1, Seq(column)); val item = new RangerDataMaskPolicyItem;
        access(item, "select")
        item.setDataMaskInfo(
          new RangerPolicyItemDataMaskInfo(
            if (value == "MASK_NULL") "MASK_NULL" else "CUSTOM",
            null,
            value
          )
        )
        p.setDataMaskPolicyItems(Collections.singletonList(item))
        if (catalog == "a")
          sourceSchedule.foreach(s => p.setValiditySchedules(Collections.singletonList(s)))
      }
    }
    val published = new ServicePolicies
    published.setServiceName("service"); published.setServiceDef(plugin.getServiceDef)
    version += 1; published.setPolicyVersion(version); published.setPolicies(policies.asJava)
    published.setServiceConfig(
      Collections.singletonMap("ranger.plugin.spark.gravitino.metalake", "lake")
    )
    plugin.setPolicies(published)
  }
}

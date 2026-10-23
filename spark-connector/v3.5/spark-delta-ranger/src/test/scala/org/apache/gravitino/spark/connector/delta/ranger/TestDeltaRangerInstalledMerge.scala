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
import org.apache.hadoop.security.UserGroupInformation
import org.apache.ranger.plugin.model.RangerPolicy
import org.apache.ranger.plugin.model.RangerPolicy._
import org.apache.ranger.plugin.service.RangerBasePlugin
import org.apache.ranger.plugin.util.{RangerRoles, ServicePolicies}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.delta.DeltaLog
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._

/** Installed named MERGE must consume bounded executor CDF with native row semantics. */
class TestDeltaRangerInstalledMerge {
  private val distinctSql = """MERGE INTO b.db.t t USING
      |(SELECT * FROM table_changes('a.db.t', 0, 4)
      | WHERE _commit_version = 4 AND _change_type <> 'update_preimage') s
      |ON t.id = s.id
      |WHEN MATCHED AND s._change_type = 'delete' THEN DELETE
      |WHEN MATCHED THEN UPDATE SET secret = s.secret, region = s.region
      |WHEN NOT MATCHED AND s._change_type <> 'delete'
      |THEN INSERT (id, secret, region) VALUES (s.id, s.secret, s.region)
      |""".stripMargin

  @Test
  def namedSqlReachesOwnedNativeMerge(): Unit = withFixture { spark =>
    val log = LoggerFactory.getLogger(getClass)
    Seq(
      classOf[SparkSession],
      classOf[DeltaLog],
      classOf[RangerBasePlugin],
      classOf[scala.Option[_]]
    )
      .foreach(c =>
        log.warn(
          "Task 1 installed native origin: " + c.getName + " " +
            c.getProtectionDomain.getCodeSource.getLocation + " version=" + c.getPackage.getImplementationVersion
        )
      )
    log.warn(
      "Task 1 installed runtime: Spark=" + spark.version + " Scala=" + scala.util.Properties.versionNumberString +
        " Java=" + System.getProperty("java.version")
    )
    spark.sql(distinctSql).collect()
    verifyTarget(spark)
  }

  @Test
  def sameTableSqlUsesNativeCdcRows(): Unit = withFixture { spark =>
    var inspected = false
    var receipt: DeltaRangerMergeReceipt = null
    var source: DeltaRangerMergeSource.Attempt = null
    var expiredBeforeCommit = false
    val registration = NativeSelectFixture.bindingFixture(spark, "a")
    val refresh = registration.onRead
    registration.onRead = n => {
      refresh(n)
      val owner = field[ThreadLocal[DeltaRangerMergeWrite]](DeltaRangerMergeWrite, "active").get()
      if (owner != null && receipt == null) {
        receipt = field[DeltaRangerMergeReceipt](owner, "receipt")
        source = field[DeltaRangerMergeSource.Attempt](owner, "source")
      }
      if (receipt != null && field[Boolean](receipt, "sourceClosed") && !expiredBeforeCommit) {
        expiredBeforeCommit = true
        val rejection =
          assertThrows(classOf[SecurityException], () => receipt.cdf.validateAction(spark))
        assertTrue(rejection.getMessage.contains("expired MERGE CDF source"), rejection.getMessage)
        assertThrows(classOf[SecurityException], () => source.logical)
      }
      if (owner != null && field[Boolean](owner, "constructing") && !inspected) {
        inspected = true
        val anchor = field[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan](owner, "anchor")
        val foreign = org.apache.spark.sql.catalyst.plans.logical.Project(
          Seq(
            org.apache.spark.sql.catalyst.expressions
              .Alias(org.apache.spark.sql.catalyst.expressions.Literal("foreign"), "injected")()
          ),
          anchor
        )
        val denied = assertThrows(
          classOf[SecurityException],
          () => TestDeltaRangerNativeSelect.rows(spark, foreign)
        )
        assertTrue(
          denied.getMessage.contains("action during native row construction"),
          denied.getMessage
        )
      }
    }
    spark
      .sql("""MERGE INTO a.db.t t USING
      |(SELECT * FROM table_changes('a.db.t', 0, 4)
      | WHERE _commit_version = 4 AND _change_type = 'update_postimage') s
      |ON t.id = s.id
      |WHEN MATCHED THEN UPDATE SET region = 'writeback'
      |WHEN NOT MATCHED THEN INSERT (id, secret, region) VALUES (s.id, s.secret, s.region)
      |""".stripMargin)
      .collect()
    assertTrue(inspected, "native construction action probe did not run")
    assertTrue(expiredBeforeCommit, "same-table source was not closed before native commit")
    assertThrows(classOf[SecurityException], () => receipt.cdf.validateAction(spark))
    assertThrows(classOf[SecurityException], () => source.logical)
    assertEquals(5L, DeltaLog.forTable(spark, NativeSelectFixture.locations("a")).update().version)
    val changes = spark
      .sql("""SELECT _change_type, region
      |FROM table_changes('a.db.t', 5, 5) WHERE id = 1""".stripMargin)
      .collect()
      .toSet
    assertEquals(Set(Row("update_preimage", "keep"), Row("update_postimage", "writeback")), changes)
  }

  private def verifyTarget(spark: SparkSession): Unit = {
    assertEquals(
      Set(Row(1L, "changed", "keep"), Row(3L, null, "keep"), Row(4L, "new", "keep")),
      spark.table("b.db.t").collect().toSet
    )
    val commit =
      Paths.get(NativeSelectFixture.locations("b"), "_delta_log", "00000000000000000001.json")
    assertTrue(Files.readAllLines(commit).asScala.exists(_.contains("\"operation\":\"MERGE\"")))
  }

  private def withFixture(test: SparkSession => Unit): Unit =
    withPolicies((spark, _) => test(spark))

  private[ranger] def withPolicies(
      test: (SparkSession, RangerBasePlugin) => Unit,
      aliasTarget: Boolean = false,
      rowTracking: Boolean = false,
      prepareTarget: SparkSession => Unit = _ => ()
  ): Unit = {
    val transport = new TestDeltaRangerNativeSelect.Fixture
    val nativePlugin = new RangerBasePlugin(transport.plugin.getConfig)
    val backend = new DeltaRangerPolicyEngine.Backend {
      override def plugin(): RangerBasePlugin = pluginRef
      private val pluginRef = nativePlugin
      override def start(): Unit = {
        val roles = new RangerRoles
        roles.setServiceName("service"); roles.setRoleVersion(1L)
        roles.setRangerRoles(Collections.emptySet())
        pluginRef.setRoles(roles)
        publish(pluginRef, transport.plugin.getServiceDef, Set("select", "update"), version = 1L)

      }
      override def close(): Unit = pluginRef.cleanup()
    }
    val engine = new DeltaRangerPolicyEngine.Owner(() => backend)
      .acquire(new DeltaRangerConfig("service", "spark", "lake", nativePlugin.getConfig))
    try {
      NativeSelectFixture.catalogEngines = Map("a" -> engine, "b" -> engine)
      NativeSelectFixture.withSpark(
        spark => {
          if (aliasTarget)
            NativeSelectFixture.locations =
              NativeSelectFixture.locations.updated("b", NativeSelectFixture.locations("a"))
          Seq("a", "b").foreach { name =>
            val registration = NativeSelectFixture.bindingFixture(spark, name)
            registration.onRead = _ => {
              // Production named load refreshes its persisted native observation. Model that
              // server behavior from this fixture's completed native log files, without SQL.
              val stream = Files.list(Paths.get(NativeSelectFixture.locations(name), "_delta_log"))
              val version =
                try
                  stream
                    .iterator()
                    .asScala
                    .map(_.getFileName.toString)
                    .filter(_.matches("[0-9]{20}\\.json"))
                    .map(_.stripSuffix(".json").toLong)
                    .max
                finally stream.close()
              registration.properties
                .put("delta.registration.receipt.nativeVersion", version.toString)
            }
          }
          test(spark, nativePlugin)
        },
        Some(NativeSelectFixture.extension(transport)),
        beforeExtension = spark => {
          setup(spark)
          prepareTarget(spark)
          if (rowTracking)
            spark
              .sql(
                s"ALTER TABLE delta.`${NativeSelectFixture.locations("b")}` SET TBLPROPERTIES ('delta.enableRowTracking' = 'true')"
              )
              .collect()
        }
      )
    } finally { engine.close(); transport.engine.close() }
  }

  private def publish(
      plugin: RangerBasePlugin,
      serviceDef: org.apache.ranger.plugin.model.RangerServiceDef,
      accesses: Set[String],
      version: Long,
      filter: Boolean = false,
      mask: Boolean = false
  ): Unit = {
    def resource(id: Long, kind: Int, columns: Boolean): RangerPolicy = {
      val policy = new RangerPolicy
      policy.setId(id); policy.setName("merge-" + id); policy.setService("service")
      policy.setPolicyType(kind); policy.setIsEnabled(true)
      val resources = Map(
        "catalog" -> new RangerPolicyResource("*"),
        "database" -> new RangerPolicyResource("db"),
        "table" -> new RangerPolicyResource("t")
      ) ++
        (if (columns) Map("column" -> new RangerPolicyResource("*"))
         else Map.empty[String, RangerPolicyResource])
      policy.setResources(resources.asJava); policy
    }
    def access(item: RangerPolicyItem): Unit = {
      item.setUsers(Collections.singletonList(UserGroupInformation.getCurrentUser.getShortUserName))
      item.setAccesses(accesses.toSeq.map(new RangerPolicyItemAccess(_, true)).asJava)
    }
    val grant = resource(1L, 0, columns = true)
    val item = new RangerPolicyItem; access(item)
    grant.setPolicyItems(Collections.singletonList(item))
    val values = scala.collection.mutable.ArrayBuffer(grant)
    if (filter) {
      val policy = resource(2L, 2, columns = false)
      val filtered = new RangerRowFilterPolicyItem; access(filtered)
      filtered.setRowFilterInfo(new RangerPolicyItemRowFilterInfo("region = 'keep'"))
      policy.setRowFilterPolicyItems(Collections.singletonList(filtered)); values += policy
    }
    if (mask) {
      val policy = resource(3L, 1, columns = true)
      val masked = new RangerDataMaskPolicyItem; access(masked)
      masked.setDataMaskInfo(new RangerPolicyItemDataMaskInfo("CUSTOM", null, "sha2({col}, 256)"))
      policy.setDataMaskPolicyItems(Collections.singletonList(masked)); values += policy
    }
    val policies = new ServicePolicies
    policies.setServiceName("service"); policies.setServiceDef(serviceDef)
    policies.setPolicyVersion(version); policies.setPolicies(values.asJava)
    policies.setServiceConfig(
      Collections.singletonMap("ranger.plugin.spark.gravitino.metalake", "lake")
    )
    plugin.setPolicies(policies)
  }

  private def version(name: String): Long = {
    val paths = Files.list(Paths.get(NativeSelectFixture.locations(name), "_delta_log"))
    try
      paths
        .iterator()
        .asScala
        .map(_.getFileName.toString)
        .filter(_.matches("[0-9]{20}\\.json"))
        .map(_.stripSuffix(".json").toLong)
        .max
    finally paths.close()
  }
  private def dataFiles(name: String): Set[String] = {
    val paths = Files.walk(Paths.get(NativeSelectFixture.locations(name)))
    try
      paths
        .iterator()
        .asScala
        .filter(_.getFileName.toString.endsWith(".parquet"))
        .map(_.toString)
        .toSet
    finally paths.close()
  }
  private def rejectWithoutMutation(spark: SparkSession, sql: String): Unit = {
    val before = version("b"); val files = dataFiles("b")
    assertThrows(classOf[Exception], () => spark.sql(sql).collect())
    assertEquals(before, version("b")); assertEquals(files, dataFiles("b"))
  }

  @Test
  def nativePhasesRetainNamedOwnership(): Unit = withFixture { spark =>
    val registration = NativeSelectFixture.bindingFixture(spark, "b")
    val refresh = registration.onRead
    var receipt: DeltaRangerMergeReceipt = null
    var source: DeltaRangerMergeSource.Attempt = null
    var inspected = false; var expired = false; var committed = false
    registration.onRead = n => {
      refresh(n)
      val write = field[ThreadLocal[DeltaRangerMergeWrite]](DeltaRangerMergeWrite, "active").get()
      if (write != null && !inspected) {
        inspected = true
        receipt = field[DeltaRangerMergeReceipt](write, "receipt")
        source = field[DeltaRangerMergeSource.Attempt](write, "source")
        val logical = source.logical.asInstanceOf[org.apache.spark.sql.execution.LogicalRDD]
        assertTrue(logical.rdd.isCheckpointed)
        val foreign = logical.copy(rdd =
          spark.sparkContext.emptyRDD[org.apache.spark.sql.catalyst.InternalRow]
        )(spark, None, None)
        assertFalse(source.ownsRetained(foreign))
        assertFalse(source.ownsRetained(logical.copy()(spark, None, None)))
        val command = field[DeltaRangerMergeCommand](receipt, "command")
        val native = field[org.apache.spark.sql.delta.commands.MergeIntoCommand](command, "native")
        assertThrows(classOf[SecurityException], () => command.copy(native)(receipt).receipt)
        val physical = field[DeltaRangerMergeCommand.Exec](receipt, "physical")
        assertThrows(
          classOf[SecurityException],
          () => receipt.requirePhysical(physical.copy()(receipt))
        )
      }
      if (receipt != null && field[Boolean](receipt, "sourceClosed") && !expired) {
        expired = true
        val rejection =
          assertThrows(classOf[SecurityException], () => receipt.cdf.validateAction(spark))
        assertTrue(rejection.getMessage.contains("expired MERGE CDF source"), rejection.getMessage)
        assertThrows(classOf[SecurityException], () => source.logical)
      }
      committed ||= DeltaRangerMergeCommit.current.nonEmpty
    }
    val result = spark.sql(distinctSql)
    assertEquals(Seq(Row(3L, 1L, 1L, 1L)), result.collect().toSeq)
    assertEquals(Seq(Row(3L, 1L, 1L, 1L)), result.collect().toSeq)
    assertTrue(inspected && expired && committed)
    val nativeResult = result.queryExecution.commandExecuted
      .asInstanceOf[org.apache.spark.sql.catalyst.plans.logical.CommandResult]
    val changed =
      nativeResult.copy(rows = Seq(org.apache.spark.sql.catalyst.InternalRow(99L, 99L, 99L, 99L)))
    assertThrows(classOf[SecurityException], () => receipt.result(changed))
    assertThrows(classOf[SecurityException], () => receipt.cdf.validateAction(spark))
    assertThrows(classOf[SecurityException], () => receipt.validate())
    verifyTarget(spark)
  }

  @Test
  def missingSelectOrUpdateAndUnsupportedMasksRejectBeforeFiles(): Unit = withPolicies {
    (spark, plugin) =>
      val service = plugin.getServiceDef
      publish(plugin, service, Set("select"), 2L)
      rejectWithoutMutation(spark, distinctSql)
      publish(plugin, service, Set("update"), 3L)
      rejectWithoutMutation(spark, distinctSql)
      publish(plugin, service, Set("select", "update"), 5L, mask = true)
      rejectWithoutMutation(spark, distinctSql)
  }

  @Test
  def rawTargetAndRawCdfRejectBeforeFiles(): Unit = withFixture { spark =>
    rejectWithoutMutation(
      spark,
      distinctSql.replace(
        "MERGE INTO b.db.t",
        s"MERGE INTO delta.`${NativeSelectFixture.locations("b")}`"
      )
    )
    rejectWithoutMutation(
      spark,
      distinctSql.replace(
        "table_changes('a.db.t'",
        s"table_changes_by_path('${NativeSelectFixture.locations("a")}'"
      )
    )
  }

  @Test
  def distinctRegistrationAliasCannotBorrowSameDataset(): Unit =
    withPolicies((spark, _) => rejectWithoutMutation(spark, distinctSql), aliasTarget = true)

  @Test
  def rowTrackingProtocolRejectsBeforeFiles(): Unit = withPolicies(
    (spark, _) => {
      val before = version("b"); val files = dataFiles("b")
      val failure = assertThrows(classOf[SecurityException], () => spark.sql(distinctSql))
      assertTrue(failure.getMessage.contains("row tracking is not qualified"), failure.getMessage)
      assertEquals(before, version("b")); assertEquals(files, dataFiles("b"))
    },
    rowTracking = true
  )

  @Test
  def selectRevocationAfterNativeFilesPreventsCommit(): Unit = revokedAfterFiles("select")
  @Test
  def updateRevocationAfterNativeFilesPreventsCommit(): Unit = revokedAfterFiles("update")
  @Test
  def sourceRegistrationRevocationAfterFilesPreventsCommit(): Unit = revokedAfterFiles(
    "registration"
  )
  private def revokedAfterFiles(access: String): Unit = withPolicies { (spark, plugin) =>
    val registration = NativeSelectFixture.bindingFixture(spark, "b")
    val refresh = registration.onRead
    val before = dataFiles("b"); val head = version("b")
    var writing = false; var revoked = false
    registration.onRead = n => {
      refresh(n)
      val active = field[ThreadLocal[DeltaRangerMergeWrite]](DeltaRangerMergeWrite, "active").get()
      writing ||= active != null
      if (writing && active == null && !revoked && dataFiles("b").diff(before).nonEmpty) {
        revoked = true
        if (access == "registration")
          NativeSelectFixture.bindingFixture(spark, "a").properties.clear()
        else publish(plugin, plugin.getServiceDef, Set("select", "update") - access, 2L)
      }
    }
    assertThrows(classOf[SecurityException], () => spark.sql(distinctSql))
    assertTrue(revoked); assertTrue(dataFiles("b").diff(before).nonEmpty)
    assertEquals(head, version("b"))
  }

  @Test
  def staleNamedReadbackCannotReportSuccessAfterNativeCommit(): Unit = withFixture { spark =>
    NativeSelectFixture.bindingFixture(spark, "b").onRead = _ => ()
    val rejected = assertThrows(classOf[SecurityException], () => spark.sql(distinctSql))
    assertTrue(rejected.getMessage.contains("publication is not reconciled"), rejected.getMessage)
    assertEquals(1L, version("b"))
  }

  private def field[A](value: AnyRef, name: String): A = {
    val member = value.getClass.getDeclaredFields
      .find(f => f.getName == name || f.getName.endsWith("$$" + name))
      .get
    member.setAccessible(true)
    member.get(value).asInstanceOf[A]
  }

  private def setup(spark: SparkSession): Unit = {
    val location = NativeSelectFixture.locations("a")
    // Replace only this fixture's temporary source so CDF is enabled at native version zero.
    val paths = Files.walk(Paths.get(location))
    try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.delete)
    finally paths.close()
    DeltaLog.clearCache()
    spark
      .sql("SELECT 1L id, 'old' secret, 'keep' region UNION ALL SELECT 2L, 'old', 'drop'")
      .coalesce(1)
      .write
      .format("delta")
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
      |(SELECT 1L id, 'changed' secret UNION ALL SELECT 2L, 'delete' UNION ALL SELECT 4L, 'new') s
      |ON t.id = s.id WHEN MATCHED AND s.id = 2 THEN DELETE
      |WHEN MATCHED THEN UPDATE SET secret = s.secret
      |WHEN NOT MATCHED THEN INSERT (id, secret, region) VALUES (s.id, s.secret, 'keep')
      |""".stripMargin)
      .collect()
    assertEquals(4L, DeltaLog.forTable(spark, location).update().version)
  }
}

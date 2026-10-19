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
import java.security.{MessageDigest, PrivilegedExceptionAction}
import java.util.UUID
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.example.GroupReadSupport
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import scala.collection.JavaConverters._
import org.apache.hadoop.security.UserGroupInformation
import org.apache.gravitino.spark.connector.authorization.{
  GravitinoAuthorizationSparkSessionExtensions,
  RequiredPrivilegesCheck
}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, ExprId, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.delta.{DeltaLog, OptimisticTransaction}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** Characterizes the constant mask boundary before any production compatibility change. */
class TestDeltaRangerAppendSourceShape {

  private val schema = StructType(
    Seq(
      StructField("id", LongType, nullable = true),
      StructField("secret", StringType, nullable = true),
      StructField("region", StringType, nullable = true)
    )
  )

  private val shapeTargets = Map(
    "nonnull" -> "id BIGINT NOT NULL, secret STRING, region STRING",
    "shorter" -> "id BIGINT, secret STRING",
    "longer" -> "id BIGINT, secret STRING, region STRING, extra STRING",
    "renamed" -> "id BIGINT, renamed STRING, region STRING",
    "cased" -> "id BIGINT, Secret STRING, region STRING",
    "reordered" -> "secret STRING, id BIGINT, region STRING",
    "typed" -> "id INT, secret STRING, region STRING",
    "nestedtyped" -> "id BIGINT, secret STRUCT<value: BIGINT>, region STRING",
    "nestedwiden" -> "id BIGINT, secret STRUCT<value: STRING>, region STRING",
    "nestednullable" -> "id BIGINT, secret STRUCT<value: STRING NOT NULL>, region STRING"
  )

  /** The actual installed insertInto supports a non-null constant mask into a nullable target. */
  @Test
  def installedConstantMaskInsertIntoCommits(): Unit = governed("insertInto")

  /** The actual installed existing-table saveAsTable preserves constant-mask semantics. */
  @Test
  def installedConstantMaskSaveAsTableCommits(): Unit = governed("saveAsTable")

  /** Native insertInto preserves the intentional mask project and commits without normalization. */
  @Test
  def nativeConstantShapeInsertIntoPreservesSource(): Unit = native("insertInto")

  /** Native existing-table saveAsTable preserves the source through its analyzed command. */
  @Test
  def nativeConstantShapeSaveAsTablePreservesSource(): Unit = native("saveAsTable")

  /** UPDATE denial must reach its real authorization boundary after initial shape admission. */
  @Test
  def installedConstantMaskTargetUpdateDenialPreservesStorage(): Unit =
    Seq("insertInto", "saveAsTable").foreach(frontend => governed(frontend, "denied-update"))

  /** A changed mask at actual V1 input cannot reuse an earlier constant-mask receipt. */
  @Test
  def installedConstantMaskChangeAtActualInputIsRejected(): Unit =
    governed("insertInto", "changed-mask")

  /** Direction, column structure and nested types remain strict over real native relations. */
  @Test
  def installedDirectionalAndShapeMismatchesRemainRejected(): Unit = governed("component", "shape")

  /** Admitting destination nullability never weakens retained source or target authority. */
  @Test
  def installedConstantMaskRetainsSourceAndTargetAuthority(): Unit =
    governed("component", "authority")

  private def seed(spark: SparkSession): Unit = {
    val rows = Seq(Row(1L, "one", "allowed"), Row(2L, "two", "blocked"), Row(3L, null, "allowed"))
    Seq("a", "b").foreach { catalog =>
      val values = if (catalog == "a") Seq.empty[Row] else rows
      spark
        .createDataFrame(values.asJava, schema)
        .write
        .format("delta")
        .mode("overwrite")
        .save(NativeSelectFixture.locations(catalog))
      val stored = spark.read.format("delta").load(NativeSelectFixture.locations(catalog))
      assertEquals(schema, stored.schema)
      assertEquals(values.toVector, stored.orderBy("id").collect().toVector)
    }
  }

  private def seedShapes(spark: SparkSession): Unit = {
    seed(spark)
    val root = Paths.get(NativeSelectFixture.locations("a")).getParent
    shapeTargets.foreach { case (name, columns) =>
      val location = root.resolve(name).toString
      NativeSelectFixture.locations = NativeSelectFixture.locations.updated(name, location)
      spark.sql(s"CREATE TABLE delta.`$location` ($columns) USING delta").collect()
    }
  }

  private def governed(frontend: String, scenario: String = "append"): Unit = {
    val trace =
      InstalledAppendBoundaryProbe.fresh("source-shape-governed-" + frontend + "-" + scenario)
    val target = new TestDeltaRangerNativeSelect.Fixture
    val source = new TestDeltaRangerNativeSelect.Fixture
    val previous = NativeSelectFixture.catalogPolicies
    val previousPolicy = NativeSelectFixture.policy
    source.customCatalog = "b"
    source.mask = "'MASKED'"
    source.filter = "region = 'allowed'"
    NativeSelectFixture.catalogPolicies = Map("a" -> target, "b" -> source)
    val install = NativeSelectFixture.extension(target)
    try
      NativeSelectFixture.withSpark(
        spark => {
          runtime(spark, trace)
          val rules = spark.sessionState.analyzer.postHocResolutionRules
          assertEquals(12, rules.size)
          assertEquals(classOf[RequiredPrivilegesCheck], rules(7).getClass)
          DeltaRangerAppendInventory.qualify(spark)
          val root = Paths.get(NativeSelectFixture.locations("a")).getParent
          Files.createDirectories(
            Paths.get(new java.net.URI(spark.conf.get("spark.sql.warehouse.dir")))
          )
          asApp {
            val data = spark.table("b.db.t")
            val original = data.queryExecution.analyzed
            val receipt =
              DeltaRangerDecisionReceipt.consumeOwned(original, spark, optimized = false)
            assertEquals(1, receipt.obligations.size)
            val obligation = receipt.obligations.head
            assertEquals("b", obligation.scan.catalog)
            assertEquals(Set("id", "secret", "region"), obligation.columns)
            val mask = obligation.decisions.masks("secret")
            assertEquals(DeltaRangerPolicyEngine.MaskDecision.Kind.EXPRESSION, mask.kind())
            assertEquals("'MASKED'", mask.expression().get())
            assertEquals("region = 'allowed'", obligation.decisions.filter.get())
            val catalog = spark.sessionState.catalogManager.catalog("a").asInstanceOf[TableCatalog]
            val id = Identifier.of(Array("db"), "t")
            val relation =
              DataSourceV2Relation.create(catalog.loadTable(id), Some(catalog), Some(id))
            assertEquals(schema, relation.schema)
            assertFields(
              original.output,
              Vector(
                ("id", "bigint", true),
                ("secret", "string", false),
                ("region", "string", true)
              )
            )
            assertFields(
              relation.output,
              Vector(("id", "bigint", true), ("secret", "string", true), ("region", "string", true))
            )
            trace.record("source-output", fields(original.output))
            trace.record("target-output", fields(relation.output))
            trace.record(
              "mask-obligation",
              "selected=true column=secret kind=EXPRESSION nonNullConstant=true rowFilterSelected=true allOriginalColumns=true"
            )
            val sourceProperties =
              NativeSelectFixture.bindingFixture(spark, "b").properties.asScala.toMap
            val targetProperties =
              NativeSelectFixture.bindingFixture(spark, "a").properties.asScala.toMap
            val before =
              InstalledAppendBoundaryProbe.manifest(root, trace.directory.resolve("before"))
            val version =
              DeltaLog.forTable(spark, NativeSelectFixture.locations("a")).update().version
            if (scenario == "shape") checkShapes(spark, original, trace)
            else if (scenario == "authority")
              checkAuthority(spark, original, receipt, relation, trace)
            else {
              val completed = new LinkedBlockingQueue[QueryExecution]()
              val listener = new QueryExecutionListener {
                override def onSuccess(name: String, qe: QueryExecution, duration: Long): Unit = {
                  if (qe.analyzed.isInstanceOf[AppendData]) completed.add(qe)
                }
                override def onFailure(name: String, qe: QueryExecution, failure: Exception): Unit =
                  ()
              }
              var actualInputObserved = false
              source.onRequest = _ => {
                if (!actualInputObserved && atInsert) {
                  actualInputObserved = true
                  val qe = SQLExecution.getQueryExecution(
                    spark.sparkContext.getLocalProperty("spark.sql.execution.id").toLong
                  )
                  val command = qe.analyzed.asInstanceOf[AppendData]
                  assertSame(original, command.query)
                  assertSame(original, command.analyzedQuery.get)
                  val guard = qe.executedPlan.collectFirst { case g: DeltaRangerAppendInputExec =>
                    g
                  }.get
                  val optimized = qe.optimizedPlan.asInstanceOf[AppendData]
                  val planned = qe.sparkPlan.collectFirst { case g: DeltaRangerAppendInputExec =>
                    g
                  }.get
                  trace.record(
                    "identity-diagnostic",
                    s"original=${trace.id(original)} analyzedQuery=${trace.id(command.query)} analyzedStored=${trace.id(command.analyzedQuery.get)} optimizedQuery=${trace.id(optimized.query)} optimizedStored=${trace.id(optimized.analyzedQuery.get)} plannedFallback=${trace.id(planned.delegate.plan)} executedFallback=${trace.id(guard.delegate.plan)} fallbackEqOriginal=${guard.delegate.plan eq original} fallbackEqOptimizedStored=${guard.delegate.plan eq optimized.analyzedQuery.get} plannedDelegateEqExecuted=${planned.delegate eq guard.delegate} sourceSchemaSame=${guard.delegate.plan.schema == original.schema} sourceOutputSame=${guard.delegate.plan.output == original.output}"
                  )
                  trace.record("retained-fallback-output", fields(guard.delegate.plan.output))
                  trace
                    .record("optimized-stored-output", fields(optimized.analyzedQuery.get.output))
                  assertSame(optimized.analyzedQuery.get, guard.delegate.plan)
                  assertSame(planned.delegate, guard.delegate)
                  assertSame(planned.delegate.plan, guard.delegate.plan)
                  assertEquals(original.output, guard.delegate.plan.output)
                  assertEquals(original.schema.json, guard.delegate.plan.schema.json)
                  receipt.consume(guard.delegate.plan, spark, optimized = false)
                  assertTrue(command.writeOptions.isEmpty)
                  clean()
                  trace.record(
                    "actual-input-observation",
                    "terminalInsert=true analyzedSourceSame=true nativeFallbackEqOptimizedStored=true originalOutputSchemaReceiptSame=true unchangedInputValidators=true noNativeOwner=true noTransaction=true"
                  )
                  if (scenario == "changed-mask") source.mask = "'CHANGED'"
                }
              }
              if (scenario == "denied-update") target.denied.add("a:*")
              spark.listenerManager.register(listener)
              try {
                def write(): Unit = {
                  if (frontend == "insertInto") data.write.mode("append").insertInto("a.db.t")
                  else data.write.mode("append").saveAsTable("a.db.t")
                }
                if (scenario == "append") {
                  write()
                  val qe = completed.poll(10, TimeUnit.SECONDS)
                  assertNotNull(qe)
                  val command = qe.analyzed.asInstanceOf[AppendData]
                  assertSame(original, command.query)
                  assertSame(original, command.analyzedQuery.get)
                  assertTrue(actualInputObserved)
                  trace.record("executed-source-output", fields(command.query.output))
                  trace.record("executed-target-output", fields(command.table.output))
                  committedRows(spark, version)
                  trace.record(
                    "append-rows",
                    "expectedRows=2 originalNullSecretMaskedNonNull=true filteredOutRows=1 nativeCommits=1 exactAddCommitActions=true"
                  )
                } else {
                  val failure = assertThrows(classOf[Exception], () => write())
                  if (scenario == "denied-update") {
                    assertReason(failure, "denied")
                    assertTrue(
                      target.requests.asScala.exists(r =>
                        r.getAccessType == "update" && r.getUser == "APP" && r.getResource.getValue(
                          "catalog"
                        ) == "a"
                      )
                    )
                    assertFalse(
                      causes(failure).exists(e =>
                        Option(e.getMessage).exists(
                          _.contains("native source normalization is not qualified")
                        )
                      )
                    )
                    trace.record(
                      "target-denial",
                      "actualUpdateRequest=true userAPP=true inspectNormalizationRejected=false"
                    )
                  } else {
                    assertTrue(actualInputObserved)
                    assertReason(failure, "action decision changed")
                    trace.record(
                      "retained-mask-denial",
                      "mutationAtActualInput=true originalMaskReceiptRejected=true"
                    )
                  }
                }
              } finally {
                source.onRequest = _ => ()
                source.mask = "'MASKED'"
                target.denied.clear()
                spark.listenerManager.unregister(listener)
              }
            }
            val after =
              InstalledAppendBoundaryProbe.manifest(root, trace.directory.resolve("after"))
            val afterVersion =
              DeltaLog.forTable(spark, NativeSelectFixture.locations("a")).update().version
            if (scenario == "append") {
              assertNotEquals(before, after)
              assertEquals(
                before.filter(_._1.startsWith("b/")),
                after.filter(_._1.startsWith("b/"))
              )
              assertEquals(version + 1, afterVersion)
            } else {
              assertEquals(before, after)
              assertEquals(version, afterVersion)
            }
            assertEquals(
              sourceProperties,
              NativeSelectFixture.bindingFixture(spark, "b").properties.asScala.toMap
            )
            assertEquals(
              targetProperties,
              NativeSelectFixture.bindingFixture(spark, "a").properties.asScala.toMap
            )
            clean()
            trace.record(
              "governed-result",
              s"frontend=$frontend scenario=$scenario sourceNullInputRows=1 targetVersionBefore=$version targetVersionAfter=$afterVersion sourceStorageUnchanged=true registrationsUnchanged=true cleanup=true"
            )
          }
        },
        Some(extensions => {
          new GravitinoAuthorizationSparkSessionExtensions().apply(extensions)
          install(extensions)
        }),
        startupOptions =
          Map("spark.databricks.delta.write.txnVersion.autoReset.enabled" -> "false"),
        beforeExtension = if (scenario == "shape") seedShapes else seed
      )
    finally {
      NativeSelectFixture.catalogPolicies = previous
      NativeSelectFixture.policy = previousPolicy
      target.engine.close()
      source.engine.close()
      trace.close()
    }
  }

  private def committedRows(spark: SparkSession, beforeVersion: Long): Unit = {
    val location = Paths.get(NativeSelectFixture.locations("a"))
    val log = location.resolve("_delta_log")
    val commit = log.resolve(f"${beforeVersion + 1}%020d.json")
    assertFalse(Files.exists(log.resolve(f"${beforeVersion + 2}%020d.json")))
    val mapper = new ObjectMapper()
    val actions = Files.readAllLines(commit).asScala.map(mapper.readTree(_)).toVector
    assertEquals(1, actions.count(_.has("commitInfo")))
    val info = actions.find(_.has("commitInfo")).get.get("commitInfo")
    assertEquals("WRITE", info.get("operation").asText())
    assertEquals("Append", info.get("operationParameters").get("mode").asText())
    assertTrue(actions.forall(a => a.has("commitInfo") || a.has("add")))
    val adds = actions.filter(_.has("add")).map(_.get("add"))
    assertTrue(adds.nonEmpty)
    assertTrue(adds.forall(_.get("dataChange").asBoolean()))
    val rows = adds.flatMap { add =>
      val reader = ParquetReader
        .builder[Group](
          new GroupReadSupport(),
          new HadoopPath(location.resolve(add.get("path").asText()).toUri)
        )
        .withConf(spark.sparkContext.hadoopConfiguration)
        .build()
      try
        Iterator
          .continually(reader.read())
          .takeWhile(_ != null)
          .map { group =>
            Row(
              group.getLong("id", 0),
              if (group.getFieldRepetitionCount("secret") == 0) null
              else group.getString("secret", 0),
              group.getString("region", 0)
            )
          }
          .toVector
      finally reader.close()
    }
    assertEquals(
      Vector(Row(1L, "MASKED", "allowed"), Row(3L, "MASKED", "allowed")),
      rows.sortBy(_.getLong(0))
    )
  }

  private def checkShapes(
      spark: SparkSession,
      original: LogicalPlan,
      trace: InstalledAppendBoundaryProbe.Trace
  ): Unit = {
    def target(name: String): DataSourceV2Relation = {
      val catalog = new NativeSelectTestCatalog
      catalog.initialize(name, new CaseInsensitiveStringMap(Map.empty[String, String].asJava))
      val id = Identifier.of(Array("db"), "t")
      DataSourceV2Relation.create(catalog.loadTable(id), Some(catalog), Some(id))
    }
    val nested = spark
      .sql("SELECT id, CAST(NULL AS STRUCT<value: STRING>) secret, region FROM b.db.t")
      .queryExecution
      .analyzed
    assertEquals(
      StructType(Seq(StructField("value", StringType, nullable = true))),
      nested.output(1).dataType
    )
    assertTrue(DeltaRangerDecisionReceipt.present(nested))
    val nestedNonNull = spark
      .sql("SELECT id, CAST(NULL AS STRUCT<value: STRING NOT NULL>) secret, region FROM b.db.t")
      .queryExecution
      .analyzed
    assertFalse(nestedNonNull.output(1).dataType.asInstanceOf[StructType].fields.head.nullable)
    assertTrue(DeltaRangerDecisionReceipt.present(nestedNonNull))
    shapeTargets.keys.toVector.sorted.foreach { name =>
      val to = target(name)
      val from =
        if (name == "nestedwiden") nestedNonNull
        else if (name.startsWith("nested")) nested
        else original
      if (name == "nonnull") {
        assertTrue(from.output.head.nullable)
        assertFalse(to.output.head.nullable)
      }
      if (name == "cased") assertEquals("Secret", to.output(1).name)
      if (name == "nestedwiden")
        assertTrue(to.output(1).dataType.asInstanceOf[StructType].fields.head.nullable)
      if (name == "nestednullable")
        assertFalse(to.output(1).dataType.asInstanceOf[StructType].fields.head.nullable)
      val command = AppendData.byPosition(to, from, Map.empty)
      val failure = assertThrows(
        classOf[SecurityException],
        () => DeltaRangerAppendV1Input.inspect(command, spark)
      )
      assertReason(failure, "native source normalization is not qualified")
      trace.record(
        "shape-negative",
        s"case=$name realNamedNativeTarget=true sourceReceipt=true rejected=true"
      )
    }
  }

  private def checkAuthority(
      spark: SparkSession,
      original: LogicalPlan,
      receipt: DeltaRangerDecisionReceipt,
      relation: DataSourceV2Relation,
      trace: InstalledAppendBoundaryProbe.Trace
  ): Unit = {
    val command = AppendData.byPosition(relation, original, Map.empty)
    DeltaRangerAppendV1Input.inspect(command, spark)
    assertSame(
      command,
      DeltaRangerAppendV1Input.resolve(command, spark, NativeSelectFixture.wrapper(spark, "a"))
    )
    val tag = TreeNodeTag[DeltaRangerAppendV1Input.Reference]("gravitino.delta.ranger.append-input")
    val owner = command.getTagValue(tag).get.get
    owner.analyzed(command.copy(analyzedQuery = Some(original)), spark)
    val nullable =
      Project(original.output.updated(1, original.output(1).withNullability(true)), original)
    assertTrue(nullable.output(1).nullable)
    assertReason(
      assertThrows(
        classOf[SecurityException],
        () => owner.analyzed(command.copy(query = nullable, analyzedQuery = Some(nullable)), spark)
      ),
      "command shell changed: source schema"
    )
    val attribute = original.output(1)
    val changedJvmId = attribute.withExprId(ExprId(attribute.exprId.id, UUID.randomUUID()))
    assertEquals(attribute.exprId.id, changedJvmId.exprId.id)
    assertNotEquals(attribute.exprId.jvmId, changedJvmId.exprId.jvmId)
    val mutations = Seq(
      "nullability" -> nullable,
      "exprid-jvm" -> Project(original.output.updated(1, changedJvmId), original),
      "exprid-number" -> Project(
        original.output.updated(1, attribute.withExprId(NamedExpression.newExprId)),
        original
      ),
      "producer" -> Filter(Literal(true), original)
    )
    mutations.foreach { case (name, changed) =>
      assertThrows(
        classOf[SecurityException],
        () => receipt.consume(changed, spark, optimized = false)
      )
      trace.record(
        "retained-source-negative",
        s"case=$name originalPairAdmitted=true rejected=true receiptCopied=false"
      )
    }
    val producer = Filter(Literal(true), original)
    assertEquals(original.output, producer.output)
    assertReason(
      assertThrows(
        classOf[SecurityException],
        () => owner.analyzed(command.copy(query = producer, analyzedQuery = Some(producer)), spark)
      ),
      "native source normalization is not qualified"
    )
    val table = relation.table.asInstanceOf[DeltaTableV2]
    val replacement = table.copy()
    assertNotSame(table, replacement)
    val changedTarget = relation.copy(table = replacement)
    assertEquals(relation.schema, changedTarget.schema)
    assertEquals(relation.output, changedTarget.output)
    assertReason(
      assertThrows(
        classOf[SecurityException],
        () =>
          owner.analyzed(command.copy(table = changedTarget, analyzedQuery = Some(original)), spark)
      ),
      "command shell changed: table"
    )
    receipt.consume(original, spark, optimized = false)
    owner.analyzed(command.copy(analyzedQuery = Some(original)), spark)
    trace.record(
      "retained-target-negative",
      "equalLookingTarget=true sameAttributes=true differentTableIdentity=true rejected=true originalStillValid=true"
    )
  }

  private def causes(error: Throwable): Vector[Throwable] =
    Iterator.iterate(error)(_.getCause).takeWhile(_ != null).toVector

  private def assertReason(error: Throwable, expected: String): Unit = assertTrue(
    causes(error).exists(e => Option(e.getMessage).exists(_.contains(expected))),
    "Expected retained guard: " + expected
  )

  private def atInsert: Boolean = Thread
    .currentThread()
    .getStackTrace
    .exists(frame =>
      frame.getClassName
        .endsWith("DeltaRangerAppendV1Input$TerminalRelation") && frame.getMethodName == "insert"
    )

  private def native(frontend: String): Unit = {
    val trace = InstalledAppendBoundaryProbe.fresh("source-shape-native-" + frontend)
    try
      NativeSelectFixture.withSpark(
        spark => {
          runtime(spark, trace)
          val completed = new LinkedBlockingQueue[QueryExecution]()
          val listener = new QueryExecutionListener {
            override def onSuccess(name: String, qe: QueryExecution, duration: Long): Unit = {
              if (qe.analyzed.isInstanceOf[AppendData]) completed.add(qe)
            }
            override def onFailure(name: String, qe: QueryExecution, failure: Exception): Unit = ()
          }
          spark.listenerManager.register(listener)
          try {
            val data = spark
              .table("b.db.t")
              .where("region = 'allowed'")
              .selectExpr("id", "'MASKED' AS secret", "region")
            val original = data.queryExecution.analyzed
            assertFields(
              original.output,
              Vector(
                ("id", "bigint", true),
                ("secret", "string", false),
                ("region", "string", true)
              )
            )
            val version =
              DeltaLog.forTable(spark, NativeSelectFixture.locations("a")).update().version
            trace.record("source-output", fields(original.output))
            if (frontend == "insertInto") data.write.mode("append").insertInto("a.db.t")
            else data.write.mode("append").saveAsTable("a.db.t")
            val qe = completed.poll(10, TimeUnit.SECONDS)
            assertNotNull(qe, "actual public writer did not provide its executed AppendData")
            val command = qe.analyzed.asInstanceOf[AppendData]
            trace.record("command-output", fields(command.query.output))
            trace.record("target-output", fields(command.table.output))
            trace.record(
              "source-continuity",
              s"querySame=${command.query eq original} analyzedQueryPresent=${command.analyzedQuery.nonEmpty} analyzedQuerySame=${command.analyzedQuery.exists(_ eq original)} originalProjects=${original.collect { case p: Project => p }.size} commandProjects=${command.query.collect { case p: Project => p }.size} queryClass=${command.query.getClass.getName}"
            )
            assertSame(original, command.query)
            assertSame(original, command.analyzedQuery.get)
            assertFields(
              command.table.output,
              Vector(("id", "bigint", true), ("secret", "string", true), ("region", "string", true))
            )
            assertEquals(1, command.query.collect { case p: Project => p }.size)
            assertEquals(
              Vector(Row(1L, "MASKED", "allowed"), Row(3L, "MASKED", "allowed")),
              spark.read
                .format("delta")
                .load(NativeSelectFixture.locations("a"))
                .orderBy("id")
                .collect()
                .toVector
            )
            assertEquals(
              version + 1,
              DeltaLog.forTable(spark, NativeSelectFixture.locations("a")).update().version
            )
            clean()
            trace.record(
              "native-result",
              s"frontend=$frontend publicWriterReturned=true sourceSame=true analyzedSourceSame=true normalizationProject=false cast=false reorder=false intentionalMaskProjects=1 committedRows=2 nullInputMasked=true targetVersionBefore=$version targetVersionAfter=${version + 1} cleanup=true"
            )
          } finally spark.listenerManager.unregister(listener)
        },
        beforeExtension = seed
      )
    finally trace.close()
  }

  private def runtime(spark: SparkSession, trace: InstalledAppendBoundaryProbe.Trace): Unit = {
    assertEquals("17", System.getProperty("java.specification.version"))
    val requestedSpark = Option(System.getProperty("bare.source.spark"))
    val requestedDelta = Option(System.getProperty("bare.source.delta"))
    val deltaOrigin = classOf[DeltaLog].getProtectionDomain.getCodeSource.getLocation.toString
    requestedSpark.foreach(expected => assertEquals(expected, spark.version))
    requestedDelta.foreach(expected =>
      assertTrue(deltaOrigin.endsWith("delta-spark_2.12-" + expected + ".jar"))
    )
    Seq(
      classOf[SparkSession],
      classOf[DeltaLog],
      classOf[RequiredPrivilegesCheck],
      classOf[DeltaRangerSparkExtension],
      DeltaRangerAppendV1Input.getClass
    ).foreach { clazz =>
      val input = clazz.getResourceAsStream("/" + clazz.getName.replace('.', '/') + ".class")
      assertNotNull(input)
      val digest =
        try MessageDigest.getInstance("SHA-256").digest(input.readAllBytes())
        finally input.close()
      val sha = digest.map(b => f"${b & 0xff}%02x").mkString
      trace.record("loaded-class", "class=" + clazz.getName + " sha256=" + sha)
    }
    trace.record(
      "loaded-runtime",
      s"spark=${spark.version} deltaOrigin=$deltaOrigin requestedSpark=${requestedSpark.getOrElse("absent")} requestedDelta=${requestedDelta.getOrElse("absent")} java=17"
    )
  }

  private def fields(output: Seq[Attribute]): String = output
    .map(a =>
      s"${a.name}|${a.dataType.catalogString}|nullable=${a.nullable}|exprId=${a.exprId.id}@${a.exprId.jvmId}"
    )
    .mkString("\n")

  private def assertFields(
      output: Seq[Attribute],
      expected: Vector[(String, String, Boolean)]
  ): Unit =
    assertEquals(expected, output.map(a => (a.name, a.dataType.catalogString, a.nullable)).toVector)

  private def clean(): Unit = {
    assertNull(DeltaRangerNativeOperation.current())
    assertTrue(OptimisticTransaction.getActive.isEmpty)
    assertTrue(DeltaRangerAppendCommit.current.isEmpty)
  }

  private def asApp(body: => Unit): Unit = UserGroupInformation
    .createUserForTesting("APP", Array.empty[String])
    .doAs(new PrivilegedExceptionAction[Unit] {
      override def run(): Unit = body
    })
}

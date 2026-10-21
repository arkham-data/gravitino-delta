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

import java.nio.file.Files

import org.apache.hadoop.fs.Path
import org.apache.kyuubi.plugin.spark.authz.PrivilegesBuilder
import org.apache.kyuubi.plugin.spark.authz.rule.Authorization.KYUUBI_AUTHZ_TAG
import org.apache.kyuubi.plugin.spark.authz.rule.RuleEliminateMarker
import org.apache.kyuubi.plugin.spark.authz.rule.datamasking.{
  DataMaskingStage0Marker,
  RuleApplyDataMaskingStage1
}
import org.apache.kyuubi.plugin.spark.authz.rule.rowfilter.RowFilterMarker
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project, SubqueryAlias}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.{Row, SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class TestDeltaRangerReadRule {
  @Test
  def nativeWrapperPreservesDescriptorAndCollidingSessionCatalog(): Unit = {
    NativeSelectFixture.withSpark { spark =>
      val location = NativeSelectFixture.locations("a")
      TestDeltaRangerNativeSelect.verifyWrapperDescriptor(
        spark,
        location,
        DeltaTableV2(spark, new Path(location)).update()
      )
    }
  }

  @Test
  def startupRequiresOneCompanionBeforeNativeAndRejectsStockRules(): Unit = {
    val companion = classOf[DeltaRangerSparkExtension].getName
    val native = "io.delta.sql.DeltaSparkSessionExtension"
    DeltaRangerSparkExtension.validateExtensions(s"$companion,$native")
    Seq(
      s"$companion,$native,example.Compatible",
      s"$native,$companion",
      s"$companion,$companion,$native",
      s"$companion,$native,org.apache.kyuubi.plugin.spark.authz.ranger.RangerSparkExtension",
      companion
    ).foreach { value =>
      assertThrows(
        classOf[IllegalArgumentException],
        () => DeltaRangerSparkExtension.validateExtensions(value)
      )
    }
  }

  @Test
  def nativeScopeIsSynchronousTypedAndCannotEscapeOrAdmitOtherPlans(): Unit = {
    NativeSelectFixture.withSpark { spark =>
      import org.apache.spark.sql.catalyst.InternalRow
      import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal}
      import org.apache.spark.sql.catalyst.plans.logical.{DeserializeToObject, LocalRelation}
      import org.apache.spark.sql.delta.actions.{AddFile, SingleAction}
      import org.apache.spark.sql.execution.LogicalRDD
      import org.apache.spark.sql.types.{ObjectType, StringType}
      val schema = org.apache.spark.sql.Encoders.product[SingleAction].schema
      val attributes =
        schema.fields.map(f => AttributeReference(f.name, f.dataType, f.nullable)()).toSeq
      val rdd = LogicalRDD(attributes, spark.sparkContext.emptyRDD[InternalRow])(spark)
      assertFalse(DeltaRangerPlanAdmission.internalMetadata(rdd))
      assertThrows(
        classOf[SecurityException],
        () => DeltaRangerPlanAdmission.withNativeMetadata { () }
      )
      assertThrows(
        classOf[SecurityException],
        () => DeltaRangerPlanAdmission.withNativeScans(Seq.empty) { () }
      )
      assertFalse(DeltaRangerPlanAdmission.internalMetadata(rdd))
      val objectType = ObjectType(classOf[SingleAction])
      val binding = DeserializeToObject(
        Literal.create(null, objectType),
        AttributeReference("obj", objectType)(),
        LocalRelation(attributes)
      )
      assertTrue(DeltaRangerPlanAdmission.internalMetadata(binding))
      assertFalse(
        DeltaRangerPlanAdmission.internalMetadata(
          binding.copy(child = LocalRelation(attributes, Seq(InternalRow.empty)))
        )
      )
      assertFalse(DeltaRangerPlanAdmission.internalMetadata(binding.copy(child = rdd)))
      val addType = ObjectType(classOf[AddFile])
      val addBinding = binding.copy(
        deserializer = Literal.create(null, addType),
        outputObjAttr = AttributeReference("obj", addType)()
      )
      assertFalse(DeltaRangerPlanAdmission.internalMetadata(addBinding))
      assertFalse(DeltaRangerPlanAdmission.internalMetadata(addBinding))
      val rowType = ObjectType(classOf[org.apache.spark.sql.Row])
      val rowBinding = binding.copy(
        deserializer = Literal.create(null, rowType),
        outputObjAttr = AttributeReference("obj", rowType)()
      )
      assertTrue(DeltaRangerPlanAdmission.internalMetadata(rowBinding))
      assertFalse(DeltaRangerPlanAdmission.internalMetadata(rowBinding.copy(child = rdd)))
      assertFalse(
        DeltaRangerPlanAdmission.internalMetadata(
          rowBinding.copy(child = LocalRelation(attributes, Seq(InternalRow.empty)))
        )
      )
      val udf = org.apache.spark.sql.catalyst.expressions.ScalaUDF(() => null, rowType, Seq.empty)
      assertFalse(DeltaRangerPlanAdmission.internalMetadata(rowBinding.copy(deserializer = udf)))
      val subquery = org.apache.spark.sql.catalyst.expressions
        .ScalarSubquery(LocalRelation(Seq(AttributeReference("sub", StringType)())))
      val conditional = org.apache.spark.sql.catalyst.expressions.If(
        org.apache.spark.sql.catalyst.expressions.IsNull(subquery),
        Literal.create(null, rowType),
        Literal.create(null, rowType)
      )
      assertFalse(
        DeltaRangerPlanAdmission.internalMetadata(rowBinding.copy(deserializer = conditional))
      )
      val foreignType = ObjectType(classOf[String])
      assertFalse(
        DeltaRangerPlanAdmission.internalMetadata(
          binding.copy(
            deserializer = Literal.create(null, foreignType),
            outputObjAttr = AttributeReference("obj", foreignType)()
          )
        )
      )
    }
  }

  @Test
  def pinnedExtractionPreservesBothCatalogsAndOriginalPredicateAndJoinColumns(): Unit = {
    NativeSelectFixture.withSpark { spark =>
      Seq(("a", "b"), ("b", "a")).foreach { case (left, right) =>
        val plan = spark
          .sql(
            s"SELECT x.secret, y.secret FROM $left.db.t x JOIN $right.db.t y ON x.id = y.id WHERE x.region = 'keep'"
          )
          .queryExecution
          .analyzed
        val (inputs, outputs, _) = PrivilegesBuilder.build(plan, spark)
        assertTrue(outputs.isEmpty)
        assertTrue(
          inputs.forall(_.catalog.isEmpty),
          "Pinned V1 extraction is catalogless; never use it for request identity"
        )
        val columns = DeltaRangerReadRule
          .extract(plan, spark)
          .map { case (scan, columns) => scan.catalog -> columns }
          .toMap
        assertEquals(Set("a", "b"), columns.keySet)
        assertEquals(Set("secret", "id", "region"), columns(left))
        assertEquals(Set("secret", "id"), columns(right))
      }
    }
  }

  /** Bare and passthrough roots must demand their original full output identities. */
  @Test
  def bareNativeRootsPreserveFullOutputDemand(): Unit = {
    NativeSelectFixture.withSpark { spark =>
      val bare = spark.table("a.db.t").queryExecution.analyzed
      assertTrue(bare.isInstanceOf[SubqueryAlias], bare.treeString)
      assertFalse(bare.exists(_.isInstanceOf[Project]), bare.treeString)
      val leaf = bare.collectFirst { case relation: LogicalRelation => relation }.get
      assertEquals(leaf.output.map(_.exprId), bare.output.map(_.exprId))
      val filtered = spark
        .sql("SELECT * FROM a.db.t x WHERE id = 1")
        .queryExecution
        .analyzed
        .asInstanceOf[Project]
        .child
      val explicit = spark.sql("SELECT id, secret, region FROM a.db.t").queryExecution.analyzed
      Seq(bare, leaf, SubqueryAlias("fresh", leaf), filtered, explicit).foreach { plan =>
        val originalLeaves = plan.collect { case relation: LogicalRelation => relation }
        val (inputs, outputs, _) = PrivilegesBuilder.build(plan, spark)
        assertTrue(outputs.isEmpty)
        assertEquals(1, inputs.size)
        assertEquals(Set("id", "secret", "region"), inputs.head.columns.toSet)
        val extracted = DeltaRangerReadRule.extract(plan, spark)
        assertEquals(1, extracted.size)
        assertSame(originalLeaves.head, extracted.head._1.relation)
        assertEquals(
          originalLeaves.head.output.map(_.exprId),
          extracted.head._1.relation.output.map(_.exprId)
        )
        assertEquals(Set("id", "secret", "region"), extracted.head._2)
      }
    }
  }

  /** Full join output must retain each independent native occurrence and its columns. */
  @Test
  def passthroughJoinRequiresEveryReturnedColumnPerOccurrence(): Unit = {
    NativeSelectFixture.withSpark { spark =>
      Seq(("a", "b"), ("a", "a")).foreach { case (left, right) =>
        val plan = spark
          .sql(s"SELECT * FROM $left.db.t x JOIN $right.db.t y ON x.id = y.id")
          .queryExecution
          .analyzed
          .asInstanceOf[Project]
          .child
        val leaves = plan.collect { case relation: LogicalRelation => relation }
        val scans = DeltaRangerReadRule.extract(plan, spark)
        assertEquals(2, scans.size)
        assertTrue(scans.forall(_._2 == Set("id", "secret", "region")))
        assertTrue(leaves.forall(leaf => scans.count(s => s._1.relation eq leaf) == 1))
        assertTrue(
          leaves.head.output
            .map(_.exprId)
            .toSet
            .intersect(leaves.last.output.map(_.exprId).toSet)
            .isEmpty
        )
      }
    }
  }

  /** A real pinned-builder skipped occurrence must not disappear from source obligations. */
  @Test
  def taggedNativeOccurrenceCannotSilentlyDisappear(): Unit = {
    NativeSelectFixture.withSpark { spark =>
      val plan = spark
        .sql("SELECT x.secret, y.secret FROM a.db.t x JOIN b.db.t y ON x.id = y.id")
        .queryExecution
        .analyzed
      val leaves = plan.collect { case relation: LogicalRelation => relation }
      assertEquals(2, leaves.size)
      leaves.head.setTagValue(KYUUBI_AUTHZ_TAG, ())
      val (inputs, _, _) = PrivilegesBuilder.build(plan, spark)
      assertEquals(1, inputs.size)
      val failure =
        assertThrows(classOf[SecurityException], () => DeltaRangerReadRule.extract(plan, spark))
      assertEquals("Unsupported or ambiguous Delta SELECT extraction", failure.getMessage)
    }
  }

  /** Constant-only native projections remain rejected when pinned extraction skips the leaf. */
  @Test
  def constantNativeProjectionCannotSubstituteAllLeafColumns(): Unit = {
    NativeSelectFixture.withSpark { spark =>
      val plan = spark.sql("SELECT 1 AS constant FROM a.db.t").queryExecution.analyzed
      assertEquals(1, plan.collect { case relation: LogicalRelation => relation }.size)
      val (inputs, _, _) = PrivilegesBuilder.build(plan, spark)
      assertTrue(inputs.isEmpty)
      val failure =
        assertThrows(classOf[SecurityException], () => DeltaRangerReadRule.extract(plan, spark))
      assertEquals("Unsupported or ambiguous Delta SELECT extraction", failure.getMessage)
    }
  }

  @Test
  def pinnedMarkersResolveFreshAliasIdsAndFilterBeforeMasks(): Unit = {
    NativeSelectFixture.withSpark { spark =>
      val original = spark
        .sql("SELECT x.secret, y.secret FROM a.db.t x JOIN b.db.t y ON x.id = y.id")
        .queryExecution
        .analyzed
      val marked = original.transformUp { case scan: LogicalRelation =>
        val filtered =
          Filter(spark.sessionState.sqlParser.parseExpression("id <> 2"), RowFilterMarker(scan))
        val aliases = scan.output.map { attr =>
          if (attr.name == "secret")
            Alias(
              spark.sessionState.sqlParser.parseExpression("concat(secret, '-masked')"),
              attr.name
            )()
          else attr
        }
        DataMaskingStage0Marker(Project(aliases, filtered), scan)
      }
      val resolved = spark.sessionState.analyzer.execute(marked)
      val stage1 = RuleApplyDataMaskingStage1(spark)(resolved)
      val rewritten = RuleEliminateMarker(stage1)
      assertTrue(rewritten.resolved)
      assertTrue(
        rewritten.output.zip(original.output).forall { case (a, b) => a.exprId != b.exprId }
      )
      val rows = TestDeltaRangerNativeSelect.rows(spark, rewritten).toSet
      assertEquals(Set(Row("a-one-masked", "b-one-masked"), Row(null, null)), rows)
      assertEquals(stage1, RuleApplyDataMaskingStage1(spark)(stage1))
    }
  }

}

/** Native named scans with explicit descriptors; no registration or policy transport is simulated
  * here.
  */
class NativeSelectTestCatalog extends TableCatalog {
  private var catalogName: String = _
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    catalogName = name
  }
  override def name(): String = catalogName
  override def listTables(namespace: Array[String]): Array[Identifier] = Array(
    Identifier.of(namespace, "t")
  )
  override def loadTable(id: Identifier): Table = {
    val spark = SparkSession.active
    val path = new Path(NativeSelectFixture.locations(catalogName))
    val native = DeltaTableV2(spark, path)
    val descriptor = CatalogTable(
      TableIdentifier(id.name(), Some(id.namespace().head), Some(catalogName)),
      CatalogTableType.EXTERNAL,
      CatalogStorageFormat.empty.copy(locationUri = Some(path.toUri)),
      native.schema(),
      provider = Some("delta"),
      owner = "same-owner"
    )
    native.copy(catalogTable = Some(descriptor))
  }
  override def createTable(
      id: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: java.util.Map[String, String]
  ): Table = throw new UnsupportedOperationException
  override def alterTable(id: Identifier, changes: TableChange*): Table =
    throw new UnsupportedOperationException
  override def dropTable(id: Identifier): Boolean = throw new UnsupportedOperationException
  override def renameTable(oldId: Identifier, newId: Identifier): Unit =
    throw new UnsupportedOperationException
}

class NativeSelectInstalledExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit =
    NativeSelectFixture.installer.get(extensions)
}

private[ranger] object NativeSelectFixture {
  var installer: Option[SparkSessionExtensions => Unit] = None
  var locations: Map[String, String] = Map.empty
  var policy: Option[TestDeltaRangerNativeSelect.Fixture] = None
  var catalogEngines: Map[String, DeltaRangerPolicyEngine] = Map.empty
  var catalogPolicies: Map[String, TestDeltaRangerNativeSelect.Fixture] = Map.empty
  private val bound = new java.util.IdentityHashMap[
    SparkSession,
    scala.collection.mutable.Map[String, NativeBoundCatalogFixture]
  ]()

  def withNativeOperation[T](spark: SparkSession, catalog: String = "b")(
      body: org.apache.spark.sql.delta.files.TahoeLogFileIndex => T
  ): T = {
    val plan = spark.sql(s"SELECT id, region FROM $catalog.db.t").queryExecution.analyzed
    val owner = DeltaRangerNativeScans.consume(plan, spark, optimized = false)
    owner.run { _ => body(owner.obligations.head.binding.index()) }
  }

  def bindingFixture(spark: SparkSession, catalog: String): NativeBoundCatalogFixture = {
    wrapper(spark, catalog)
    bound.get(spark)(catalog)
  }
  def withBoundSpark(test: (SparkSession, TestDeltaRangerNativeSelect.Fixture) => Unit): Unit = {
    val fixture = new TestDeltaRangerNativeSelect.Fixture
    try withSpark(spark => test(spark, fixture), Some(extension(fixture)))
    finally fixture.engine.close()
  }
  def wrapper(spark: SparkSession, catalog: String): DeltaRangerCatalogSpark35 = {
    var entries = bound.get(spark)
    if (entries == null) {
      entries = scala.collection.mutable.Map.empty[String, NativeBoundCatalogFixture]
      bound.put(spark, entries)
    }
    entries
      .getOrElseUpdate(
        catalog,
        new NativeBoundCatalogFixture(
          spark,
          catalogPolicies.getOrElse(catalog, policy.get),
          catalog,
          catalogEngines.get(catalog)
        )
      )
      .wrapper
  }

  // Component-only native row controls deliberately do not install production admission.
  // Every source still comes from an actual wrapper and every decision from its owning engine.
  def componentRewriter(
      spark: SparkSession,
      fixture: TestDeltaRangerNativeSelect.Fixture
  ): org.apache.spark.sql.catalyst.plans.logical.LogicalPlan => org.apache.spark.sql.catalyst.plans.logical.LogicalPlan = {
    import scala.collection.JavaConverters._
    configure(spark, fixture)
    plan => {
      val subject = DeltaRangerQuerySubject.current(spark)
      val instant = java.time.Instant.now()
      val bound = DeltaRangerReadRule.extract(plan, spark).map { case (scan, columns) =>
        val binding = wrapper(spark, scan.catalog).registrationBindings().resolve(scan.relation)
        (scan, columns, binding, binding.engine())
      }
      bound.foreach { case (scan, columns, _, engine) =>
        DeltaRangerResourceKey
          .select(scan.catalog, scan.database, scan.table, columns.asJava, scan.owner.orNull)
          .asScala
          .foreach(key => engine.authorize(subject, key, instant))
      }
      val replacements = new java.util.IdentityHashMap[
        LogicalRelation,
        org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
      ]()
      bound.foreach { case (scan, columns, _, engine) =>
        val decisions = DeltaRangerMaskRules.capture(scan, columns, engine, subject, instant)
        replacements.put(
          scan.relation,
          DeltaRangerMaskRules.rewrite(scan, columns, decisions, spark)
        )
      }
      val stage0 = plan.transformUp { case relation: LogicalRelation => replacements.get(relation) }
      val rewritten = RuleEliminateMarker(RuleApplyDataMaskingStage1(spark)(stage0))
      bound.foreach(_._3.recheckPublication())
      rewritten
    }
  }

  def rule(
      spark: SparkSession,
      fixture: TestDeltaRangerNativeSelect.Fixture
  ): DeltaRangerReadRule = {
    configure(spark, fixture)
    new DeltaRangerReadRule(spark, scan => wrapper(spark, scan.catalog))
  }

  def extension(fixture: TestDeltaRangerNativeSelect.Fixture): SparkSessionExtensions => Unit = {
    policy = Some(fixture)
    extensions =>
      new DeltaRangerSparkExtension((spark, scan) => wrapper(spark, scan.catalog))(extensions)
  }

  private def configure(spark: SparkSession, fixture: TestDeltaRangerNativeSelect.Fixture): Unit = {
    policy = Some(fixture)
    Seq("a", "b", "q").foreach(c =>
      spark.conf.set("spark.sql.catalog." + c, classOf[NativeBoundForwardingCatalog].getName)
    )
    val configured = spark.conf.get("spark.sql.extensions")
    if (!configured.contains(DeltaRangerCatalogSpark35.EXTENSION))
      spark.sessionState.conf.setConfString(
        "spark.sql.extensions",
        DeltaRangerCatalogSpark35.EXTENSION + "," + configured
      )
  }
  def withSpark(
      test: SparkSession => Unit,
      extension: Option[SparkSessionExtensions => Unit] = None,
      startupOptions: Map[String, String] = Map.empty,
      beforeExtension: SparkSession => Unit = _ => (),
      clearNativeCacheBeforeExtension: Boolean = true
  ): Unit = {
    val directory = Files.createTempDirectory("delta-ranger-native")
    locations = Seq("a", "b", "q").map(c => c -> directory.resolve(c).toString).toMap
    val metadataConfig = directory.resolve("metadata.properties")
    Files.write(
      metadataConfig,
      "auth-type=simple\nprincipal=separate-metadata-reader\nlifetime-seconds=300\n".getBytes(
        java.nio.charset.StandardCharsets.UTF_8
      )
    )
    Files.setPosixFilePermissions(
      metadataConfig,
      java.nio.file.attribute.PosixFilePermissions.fromString("rw-------")
    )
    val builder = SparkSession
      .builder()
      .master("local[2]")
      .appName("delta-ranger-native")
      .config("spark.gravitino.enableDeltaSupport", "true")
      .config("spark.gravitino.enableDeltaRangerSupport", "true")
      .config("spark.sql.gravitino.uri", "http://127.0.0.1:1")
      .config("spark.sql.gravitino.metalake", "lake")
      .config("spark.gravitino.deltaRanger.metadata.config", metadataConfig.toString)
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      // Installed user collection is qualified with AQE off. Dedicated native tests enable it.
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.databricks.delta.snapshotPartitions", "2")
      .config("spark.sql.warehouse.dir", directory.resolve("warehouse").toString)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.databricks.delta.catalog.update.enabled", "false")
      .config("spark.sql.catalog.a", classOf[NativeSelectTestCatalog].getName)
      .config("spark.sql.catalog.b", classOf[NativeSelectTestCatalog].getName)
      .config("spark.sql.catalog.q", classOf[NativeSelectTestCatalog].getName)
    startupOptions.foreach { case (key, value) => builder.config(key, value) }
    var spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try {
      locations.foreach { case (catalog, location) =>
        val data = spark.sql(
          s"SELECT CAST(1 AS BIGINT) id, '$catalog-one' secret, 'keep' region UNION ALL SELECT 2L, '$catalog-two', 'drop' UNION ALL SELECT 3L, CAST(NULL AS STRING), 'keep'"
        )
        if (catalog == "q")
          data
            .withColumn("odd` name", org.apache.spark.sql.functions.col("secret"))
            .write
            .format("delta")
            .option("delta.columnMapping.mode", "name")
            .save(location)
        else
          data.write
            .format("delta")
            .option(
              "delta.enableRowTracking",
              startupOptions.getOrElse("spark.test.delta.rowTracking", "false")
            )
            .save(location)
      }
      beforeExtension(spark)
      extension.foreach { install =>
        if (clearNativeCacheBeforeExtension) org.apache.spark.sql.delta.DeltaLog.clearCache()
        val conf = spark.sparkContext.getConf
        spark.stop()
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
        installer = Some(install)
        spark = SparkSession
          .builder()
          .config(conf)
          .config(
            "spark.sql.extensions",
            classOf[
              NativeSelectInstalledExtension
            ].getName + ",io.delta.sql.DeltaSparkSessionExtension"
          )
          .getOrCreate()
      }
      if (extension.nonEmpty && policy.nonEmpty) configure(spark, policy.get)
      test(spark)
    } finally {
      import scala.collection.JavaConverters._
      bound.values().asScala.foreach(_.values.foreach(_.close()))
      bound.clear()
      policy = None
      catalogPolicies = Map.empty
      catalogEngines = Map.empty
      spark.stop()
      installer = None
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }
}

/** Test transport forwards every named load through the actual registration-owning wrapper. */
class NativeBoundForwardingCatalog extends NativeSelectTestCatalog {
  override def loadTable(id: Identifier): Table =
    NativeSelectFixture.wrapper(SparkSession.active, name()).loadTable(id)
}

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

import java.lang.reflect.InvocationTargetException
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.nio.file.attribute.PosixFilePermissions
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import org.apache.spark.sql.{DataFrame, SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.delta.DeltaLog
import io.delta.sql.DeltaSparkSessionExtension
import org.apache.spark.sql.delta.files.{ShallowSnapshotDescriptor, TahoeLogFileIndex}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.execution.QueryExecution
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.Mockito._
import org.apache.spark.SparkContext
import org.apache.spark.sql.delta.{OptimisticTransaction, Snapshot}
import org.apache.spark.sql.types.{
  Metadata => FieldMetadata,
  DataType,
  UserDefinedType,
  ArrayType,
  MapType,
  MetadataBuilder,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.Row
import java.util.Collections

class TestDeltaRangerRawNativeObservation {
  import RawNativeObservationFixture._

  @Test def ordinaryReadFirstOldAnalysisNewHeadAndNoExecution(): Unit = withEnabled() {
    (spark, native, policy) =>
      assertFalse(native.sessionState.conf.getAllConfs == spark.sessionState.conf.getAllConfs)
      val location = NativeSelectFixture.locations("a")
      val raw = active(native) { native.read.format("delta").load(location) }
      val index = rawIndex(raw)
      assertEquals(
        "scala.collection.immutable.MapLike$$anon$1",
        index.deltaLog.options.getClass.getName
      )
      assertEquals(Map("path" -> location), index.deltaLog.allOptions)
      measured(spark) { denied(DeltaRangerRawNativeObservation.observe(spark, index)) }
      val fixture = new NativeBoundCatalogFixture(spark, policy)
      try {
        assertSame(
          index.deltaLog,
          DeltaRangerRawDeltaLogCache
            .observe(
              spark,
              DeltaRangerRawMaintenanceDeployment.observeRegistrations(spark).get(0).publication()
            )
            .log()
        )
        val foreign = classOf[SparkSession]
          .getConstructor(classOf[SparkContext])
          .newInstance(spark.sparkContext)
        denied(DeltaRangerRawNativeObservation.observe(foreign, index))
        val transaction = active(native) { index.deltaLog.startTransaction() }
        OptimisticTransaction.withActive(transaction) {
          measured(spark) { denied(DeltaRangerRawNativeObservation.observe(spark, index)) }
        }
        val first = measured(spark) { DeltaRangerRawNativeObservation.observe(spark, index) }
        assertEquals(first.analysisVersion, first.installedVersion)
        measured(spark) { first.requireCurrent(spark) }
        active(native) {
          native
            .sql("SELECT 9L id, 'new' secret, 'keep' region")
            .write
            .format("delta")
            .mode("append")
            .save(location)
        }
        denied(first.requireCurrent(spark))
        val fresh = measured(spark) { DeltaRangerRawNativeObservation.observe(spark, index) }
        assertTrue(fresh.analysisVersion < fresh.installedVersion)
        evidence(
          "semantics.txt",
          s"backend=local analysis=${fresh.analysisVersion} installed=${fresh.installedVersion} sameIndex=true sameDescriptor=true\n"
        )
        assertSame(index, rawIndex(raw))
        assertSame(index.snapshotAtAnalysis, rawIndex(raw).snapshotAtAnalysis)
        assertTrue(spark.sharedState.cacheManager.lookupCachedData(raw).isEmpty)
        denied(fresh.requireCurrent(spark.newSession()))
        val peerSetting = native.conf.get("spark.sql.shuffle.partitions")
        native.conf.set("spark.sql.shuffle.partitions", "7")
        denied(fresh.requireCurrent(spark))
        native.conf.set("spark.sql.shuffle.partitions", peerSetting)
        val old = spark.conf.getOption("spark.sql.shuffle.partitions")
        spark.conf.set("spark.sql.shuffle.partitions", "3")
        denied(fresh.requireCurrent(spark))
        old.foreach(spark.conf.set("spark.sql.shuffle.partitions", _))
        measured(spark) { fresh.requireCurrent(spark) }
        val previous = spark.sparkContext.hadoopConfiguration.get("raw.observation.marker")
        spark.sparkContext.hadoopConfiguration.set("raw.observation.marker", "changed")
        denied(fresh.requireCurrent(spark))
        if (previous == null) spark.sparkContext.hadoopConfiguration.unset("raw.observation.marker")
        else spark.sparkContext.hadoopConfiguration.set("raw.observation.marker", previous)
        policy.start()
        denied(fresh.requireCurrent(spark))
        val current = DeltaRangerRawNativeObservation.observe(spark, index)
        fixture.clock.set(TimeUnit.MINUTES.toNanos(6))
        denied(current.requireCurrent(spark))
      } finally fixture.close()
  }

  @Test def ordinaryWriteFirstAndNativeInvalidationCannotBindOldIndex(): Unit = withEnabled() {
    (spark, native, policy) =>
      val location = Files.createTempDirectory("raw-write-first").resolve("table").toString
      active(native) {
        native.sql("SELECT 1L id, 'new' secret, 'keep' region").write.format("delta").save(location)
      }
      NativeSelectFixture.locations += "a" -> location
      val raw = active(native) { native.read.format("delta").load(location) }
      val index = rawIndex(raw)
      assertEquals(Map("path" -> location), index.deltaLog.allOptions)
      val fixture = new NativeBoundCatalogFixture(spark, policy)
      try {
        val first = DeltaRangerRawNativeObservation.observe(spark, index)
        DeltaLog.invalidateCache(native, new HadoopPath(location))
        denied(first.requireCurrent(spark))
        val replacement = active(native) { rawIndex(native.read.format("delta").load(location)) }
        assertNotSame(index.deltaLog, replacement.deltaLog)
        denied(DeltaRangerRawNativeObservation.observe(spark, index))
        measured(spark) { DeltaRangerRawNativeObservation.observe(spark, replacement) }
      } finally fixture.close()
  }

  @Test def aliasAmbiguityAndUnloadedUnrelatedRegistration(): Unit = withEnabled(Seq("a", "b")) {
    (spark, native, policy) =>
      val index = active(native) {
        rawIndex(native.read.format("delta").load(NativeSelectFixture.locations("a")))
      }
      val a = new NativeBoundCatalogFixture(spark, policy)
      val bPolicy = new TestDeltaRangerNativeSelect.Fixture
      val b = new NativeBoundCatalogFixture(spark, bPolicy, "b")
      try {
        // The fixture's catalog initialization legitimately loaded b. Public eviction leaves a
        // registered cold candidate, and observation must not reconstruct it.
        DeltaLog.invalidateCache(native, new HadoopPath(NativeSelectFixture.locations("b")))
        val before = DeltaRangerRawMaintenanceDeployment.observeRegistrations(spark)
        assertNull(DeltaRangerRawDeltaLogCache.observe(spark, before.get(1).publication()).log())
        val unique = measured(spark) { DeltaRangerRawNativeObservation.observe(spark, index) }
        assertNull(DeltaRangerRawDeltaLogCache.observe(spark, before.get(1).publication()).log())
        b.close()
        NativeSelectFixture.locations += "b" -> NativeSelectFixture.locations("a")
        val alias = new NativeBoundCatalogFixture(spark, bPolicy, "b")
        try {
          denied(unique.requireCurrent(spark))
          denied(DeltaRangerRawNativeObservation.observe(spark, index))
        } finally alias.close()
      } finally { a.close(); b.close(); bPolicy.engine.close() }
  }

  @Test def nestedCallbacksTimeTravelAndFiltersRejectedBeforeInvocation(): Unit = withEnabled() {
    (spark, native, policy) =>
      val index = active(native) {
        rawIndex(native.read.format("delta").load(NativeSelectFixture.locations("a")))
      }
      val fixture = new NativeBoundCatalogFixture(spark, policy)
      val calls = new AtomicInteger()
      val evil =
        new scala.collection.AbstractSeq[org.apache.spark.sql.catalyst.expressions.Expression] {
          override def iterator = { calls.incrementAndGet(); Iterator.empty }
          override def length: Int = { calls.incrementAndGet(); 0 }
          override def apply(i: Int) = { calls.incrementAndGet(); Literal(true) }
          override def isEmpty: Boolean = { calls.incrementAndGet(); true }
        }
      try {
        val malformed = new TahoeLogFileIndex(
          native,
          index.deltaLog,
          index.path,
          index.snapshotAtAnalysis,
          evil,
          false
        )
        denied(DeltaRangerRawNativeObservation.observe(spark, malformed))
        assertEquals(0, calls.get())
        val filtered = new TahoeLogFileIndex(
          native,
          index.deltaLog,
          index.path,
          index.snapshotAtAnalysis,
          Seq(Literal(true)),
          false
        )
        denied(DeltaRangerRawNativeObservation.observe(spark, filtered))
        val historical = new TahoeLogFileIndex(
          native,
          index.deltaLog,
          index.path,
          index.deltaLog.unsafeVolatileSnapshot,
          Nil,
          true
        )
        denied(DeltaRangerRawNativeObservation.observe(spark, historical))
        val evilPath = new HadoopPath(index.path.toString) {
          override def toUri = { calls.incrementAndGet(); super.toUri }
        }
        val pathCandidate = new TahoeLogFileIndex(
          native,
          index.deltaLog,
          evilPath,
          index.snapshotAtAnalysis,
          Nil,
          false
        )
        denied(DeltaRangerRawNativeObservation.observe(spark, pathCandidate))
        assertEquals(0, calls.get())
        val snapshot = mock(classOf[Snapshot])
        when(snapshot.deltaLog).thenReturn(index.deltaLog)
        when(snapshot.version).thenReturn(index.snapshotAtAnalysis.version)
        when(snapshot.protocol).thenReturn(index.snapshotAtAnalysis.protocol)
        val unsafeConfiguration = new scala.collection.immutable.Map[String, String] {
          override def get(key: String) = { calls.incrementAndGet(); None }
          override def iterator = { calls.incrementAndGet(); Iterator.empty }
          override def +[B1 >: String](kv: (String, B1)) = { calls.incrementAndGet(); Map(kv) }
          override def -(key: String) = { calls.incrementAndGet(); this }
        }
        when(snapshot.metadata).thenReturn(
          index.snapshotAtAnalysis.metadata.copy(configuration = unsafeConfiguration)
        )
        val malformedMetadata = new TahoeLogFileIndex(
          native,
          index.deltaLog,
          index.path,
          new ShallowSnapshotDescriptor(snapshot),
          Nil,
          false
        )
        denied(DeltaRangerRawNativeObservation.observe(spark, malformedMetadata))
        assertEquals(0, calls.get())
        val number = new Number {
          override def longValue(): Long = { calls.incrementAndGet(); 0L }
          override def intValue(): Int = { calls.incrementAndGet(); 0 }
          override def floatValue(): Float = { calls.incrementAndGet(); 0f }
          override def doubleValue(): Double = { calls.incrementAndGet(); 0d }
        }
        when(snapshot.metadata).thenReturn(
          index.snapshotAtAnalysis.metadata
            .copy(createdTime = Some(number).asInstanceOf[Option[Long]])
        )
        val malformedTime = new TahoeLogFileIndex(
          native,
          index.deltaLog,
          index.path,
          new ShallowSnapshotDescriptor(snapshot),
          Nil,
          false
        )
        denied(DeltaRangerRawNativeObservation.observe(spark, malformedTime))
        assertEquals(0, calls.get())
        val inner = mock(classOf[scala.collection.immutable.HashMap[String, String]])
        when(inner.iterator).thenAnswer(_ => { calls.incrementAndGet(); Iterator.empty })
        val array = Array(inner)
        val trieClass = Class.forName("scala.collection.immutable.HashMap$HashTrieMap")
        val trie = trieClass
          .getConstructor(classOf[Int], array.getClass, classOf[Int])
          .newInstance(Int.box(1), array, Int.box(1))
          .asInstanceOf[Map[String, String]]
        when(snapshot.metadata).thenReturn(
          index.snapshotAtAnalysis.metadata.copy(configuration = trie)
        )
        val nestedNode = new TahoeLogFileIndex(
          native,
          index.deltaLog,
          index.path,
          new ShallowSnapshotDescriptor(snapshot),
          Nil,
          false
        )
        denied(DeltaRangerRawNativeObservation.observe(spark, nestedNode))
        assertEquals(0, calls.get(), "A stock trie must not invoke a supplied inner iterator")
        when(snapshot.metadata).thenReturn(
          index.snapshotAtAnalysis.metadata.copy(description = "different metadata")
        )
        val drift = new TahoeLogFileIndex(
          native,
          index.deltaLog,
          index.path,
          new ShallowSnapshotDescriptor(snapshot),
          Nil,
          false
        )
        denied(DeltaRangerRawNativeObservation.observe(spark, drift))
        when(snapshot.metadata).thenReturn(index.snapshotAtAnalysis.metadata)
        when(snapshot.protocol).thenReturn(
          index.snapshotAtAnalysis.protocol.copy(minWriterVersion = 4)
        )
        val protocolDrift = new TahoeLogFileIndex(
          native,
          index.deltaLog,
          index.path,
          new ShallowSnapshotDescriptor(snapshot),
          Nil,
          false
        )
        denied(DeltaRangerRawNativeObservation.observe(spark, protocolDrift))
      } finally fixture.close()
  }

  @Test def nestedSchemaMetadataAndNullabilityRemainStructural(): Unit = withEnabled() {
    (spark, native, policy) =>
      val metadata = new MetadataBuilder().putString("source", "retained-field-metadata").build()
      val nested = StructType(Seq(StructField("leaf", StringType, nullable = false, metadata)))
      val schema = StructType(
        Seq(
          StructField("nested", nested, nullable = false, metadata),
          StructField("items", ArrayType(nested, containsNull = false), nullable = true, metadata),
          StructField(
            "mapped",
            MapType(StringType, nested, valueContainsNull = false),
            nullable = true,
            metadata
          )
        )
      )
      val location = Files.createTempDirectory("raw-nested-schema").resolve("table").toString
      active(native) {
        native
          .createDataFrame(Collections.emptyList[Row](), schema)
          .write
          .format("delta")
          .save(location)
      }
      active(native) {
        native
          .sql(
            s"ALTER TABLE delta.`$location` SET TBLPROPERTIES ('Aa'='one', 'BB'='two', 'm3'='three', 'm4'='four', 'm5'='five', 'AaAa'='six', 'BBBB'='seven')"
          )
          .collect()
      }
      NativeSelectFixture.locations += "a" -> location
      val index = active(native) { rawIndex(native.read.format("delta").load(location)) }
      val fixture = new NativeBoundCatalogFixture(spark, policy)
      try measured(spark) { DeltaRangerRawNativeObservation.observe(spark, index) }
      finally fixture.close()
  }

  @Test def schemaJsonPreservesNullabilityAndNeverInstantiatesUDT(): Unit = {
    val module = DeltaRangerRawNativeObservation
    val method = module.getClass.getDeclaredMethods.find(_.getName.endsWith("logicalSchema")).get
    method.setAccessible(true)
    def normalized(json: String): AnyRef = method.invoke(module, json)
    val metadata = new MetadataBuilder().putString("comment", "synthetic metadata").build()
    def schema(
        fieldNullable: Boolean,
        arrayNullable: Boolean,
        mapNullable: Boolean,
        tags: FieldMetadata
    ): StructType = {
      val leaf = StructType(Seq(StructField("leaf", StringType, fieldNullable, tags)))
      StructType(
        Seq(
          StructField(
            "value",
            ArrayType(MapType(StringType, leaf, mapNullable), arrayNullable),
            nullable = true,
            tags
          )
        )
      )
    }
    val baseline = normalized(schema(false, false, false, metadata).json)
    assertEquals(
      baseline,
      normalized(schema(false, false, false, FieldMetadata.empty).json)
    )
    assertNotEquals(baseline, normalized(schema(true, false, false, metadata).json))
    assertNotEquals(baseline, normalized(schema(false, true, false, metadata).json))
    assertNotEquals(baseline, normalized(schema(false, false, true, metadata).json))
    val udt = s"""{"type":"struct","fields":[{"name":"v","type":{"type":"udt","class":"${classOf[
        RawNativeCountingUDT
      ].getName}","pyClass":null,"sqlType":"string"},"nullable":true,"metadata":{}}]}"""
    val failure = assertThrows(classOf[InvocationTargetException], () => { normalized(udt); () })
    assertTrue(failure.getCause.isInstanceOf[SecurityException])
    assertEquals(0, RawNativeCountingUDT.constructions.get())
  }

  @Test def metadataPolicyAndPublicationBracketInvalidations(): Unit = withEnabled() {
    (spark, native, policy) =>
      val index = active(native) {
        rawIndex(native.read.format("delta").load(NativeSelectFixture.locations("a")))
      }
      val fixture = new NativeBoundCatalogFixture(spark, policy)
      try {
        val original = DeltaRangerRawNativeObservation.observe(spark, index)
        Seq("id", "generation", "token", "nativeId", "location").foreach { field =>
          val key = "delta.registration.receipt." + field
          val old = fixture.properties.put(
            key,
            if (field == "id" || field == "generation") "2" else "changed"
          )
          denied(original.requireCurrent(spark))
          fixture.properties.put(key, old)
        }
        val next = fixture.reads.get() + 4
        fixture.onRead = n =>
          if (n == next)
            DeltaLog.invalidateCache(native, new HadoopPath(NativeSelectFixture.locations("a")))
        denied(DeltaRangerRawNativeObservation.observe(spark, index))
        fixture.onRead = _ => ()
        fixture.close()
        denied(original.requireCurrent(spark))
        denied(DeltaRangerRawNativeObservation.observe(spark, index))
      } finally fixture.close()
  }
}

private[ranger] object RawNativeObservationFixture {
  def denied(body: => Any): Unit = {
    val failure = assertThrows(classOf[SecurityException], () => { body; () })
    assertNull(failure.getCause)
    assertFalse(failure.getMessage.contains("file:"))
    assertFalse(failure.getMessage.contains("s3a:"))
  }

  def rawIndex(frame: DataFrame): TahoeLogFileIndex = frame.queryExecution.analyzed.collectFirst {
    case relation: LogicalRelation if relation.relation.isInstanceOf[HadoopFsRelation] =>
      relation.relation.asInstanceOf[HadoopFsRelation].location.asInstanceOf[TahoeLogFileIndex]
  }.get

  def active[A](session: SparkSession)(body: => A): A = {
    val previous = SparkSession.getActiveSession
    SparkSession.setActiveSession(session)
    try body
    finally {
      previous match {
        case Some(value) => SparkSession.setActiveSession(value);
        case None        => SparkSession.clearActiveSession()
      }
    }
  }

  def nativeSession(spark: SparkSession): SparkSession = {
    val extensions = new SparkSessionExtensions
    new DeltaSparkSessionExtension().apply(extensions)
    val constructor =
      classOf[SparkSession].getDeclaredConstructors.find(_.getParameterCount == 5).get
    constructor.setAccessible(true)
    constructor
      .newInstance(
        spark.sparkContext,
        Some(spark.sharedState),
        None,
        extensions,
        Map.empty[String, String]
      )
      .asInstanceOf[SparkSession]
  }

  def withEnabled(catalogs: Seq[String] = Seq("a"), storage: Map[String, String] = Map.empty)(
      test: (SparkSession, SparkSession, TestDeltaRangerNativeSelect.Fixture) => Unit
  ): Unit = {
    val path = Files.createTempFile("raw-native-deployment", ".properties")
    val rows = catalogs.zipWithIndex.map { case (catalog, i) =>
      s"table.$i.metalake=lake\ntable.$i.catalog=$catalog\ntable.$i.database=db\ntable.$i.table=t\n"
    }.mkString
    Files.write(
      path,
      (s"version=1\nprofile=raw-current-head-filter-project-v1\ntable.count=${catalogs.size}\n" + rows)
        .getBytes(StandardCharsets.UTF_8)
    )
    Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-------"))
    val policy = new TestDeltaRangerNativeSelect.Fixture
    try
      NativeSelectFixture.withSpark(
        spark => { origins(); test(spark, nativeSession(spark), policy) },
        Some(NativeSelectFixture.extension(policy)),
        storage + ("spark.gravitino.deltaRanger.rawCacheMaintenance.config" -> path.toString)
      )
    finally { policy.engine.close(); Files.deleteIfExists(path) }
  }

  def evidence(name: String, value: String): Unit = {
    val directory = System.getProperty("raw.observation.evidence", "")
    if (directory.nonEmpty)
      Files.write(
        Paths.get(directory, name),
        value.getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.CREATE,
        StandardOpenOption.APPEND
      )
  }

  def origins(): Unit = {
    val names = Seq(
      "org.apache.spark.sql.SparkSession",
      "org.apache.spark.sql.delta.DeltaLog",
      "org.apache.ranger.plugin.service.RangerBasePlugin",
      "org.apache.hadoop.fs.FileSystem",
      "org.apache.hadoop.fs.Path",
      "com.google.common.cache.CacheBuilder"
    )
    val entries = names.map { name =>
      val cls = Class.forName(name, false, classOf[DeltaLog].getClassLoader)
      val source = cls.getProtectionDomain.getCodeSource
      assertNotNull(source)
      name + "=" + source.getLocation.toExternalForm + "\n"
    }
    evidence("loaded-origins.txt", entries.mkString)
  }

  def measured[A](spark: SparkSession)(body: => A): A = {
    val executions = new AtomicInteger()
    val ids = Seq(
      "org.apache.spark.sql.execution.QueryExecution$",
      "org.apache.spark.sql.execution.SQLExecution$"
    ).map { name =>
      val companion = Class.forName(name)
      val field = companion.getDeclaredField("_nextExecutionId")
      field.setAccessible(true)
      field.get(companion.getField("MODULE$").get(null)).asInstanceOf[AtomicLong]
    }
    val before = ids.map(_.get())
    val listener = new QueryExecutionListener {
      override def onSuccess(name: String, execution: QueryExecution, duration: Long): Unit =
        executions.incrementAndGet()
      override def onFailure(name: String, execution: QueryExecution, failure: Exception): Unit =
        executions.incrementAndGet()
    }
    spark.listenerManager.register(listener)
    val jobs = spark.sparkContext.statusTracker.getJobIdsForGroup(null).toSet
    try {
      val result = body
      assertNull(DeltaRangerNativeOperation.current())
      assertEquals(jobs, spark.sparkContext.statusTracker.getJobIdsForGroup(null).toSet)
      assertEquals(
        before,
        ids.map(_.get()),
        "Observation must not construct QueryExecution or start SQL execution"
      )
      assertEquals(0, executions.get())
      evidence(
        "measurements.txt",
        "jobsDelta=0 queryExecutionDelta=0 sqlExecutionDelta=0 NativeOperation=absent\n"
      )
      result
    } finally spark.listenerManager.unregister(listener)
  }
}

private[ranger] object RawNativeCountingUDT { val constructions = new AtomicInteger() }

/** Test-only UDT whose constructor must never run during observation schema compatibility. */
private[ranger] class RawNativeCountingUDT extends UserDefinedType[AnyRef] {
  RawNativeCountingUDT.constructions.incrementAndGet()
  override def sqlType: DataType = StringType
  override def serialize(value: AnyRef): Any = value
  override def deserialize(value: Any): AnyRef = value.asInstanceOf[AnyRef]
  override def userClass: Class[AnyRef] = classOf[AnyRef]
}

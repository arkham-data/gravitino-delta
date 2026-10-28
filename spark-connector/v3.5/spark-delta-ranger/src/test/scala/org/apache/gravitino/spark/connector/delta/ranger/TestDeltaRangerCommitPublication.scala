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

import java.net.{URI, URLClassLoader}
import java.lang.reflect.InvocationTargetException
import java.util.jar.{JarEntry, JarFile, JarOutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.nio.file.attribute.PosixFilePermissions
import java.security.{MessageDigest, PrivilegedExceptionAction}
import java.util.UUID

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.apache.gravitino.rel.Column
import org.apache.gravitino.spark.connector.SparkTypeConverter
import org.apache.gravitino.spark.connector.authorization.{
  GravitinoAuthorizationSparkSessionExtensions,
  RequiredPrivilegesCheck
}
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.delta.{DeltaLog, OptimisticTransaction}
import org.apache.spark.sql.types.{DataType, LongType, StringType, StructField, StructType}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** Qualifies finite native commit continuation with storage-derived receipt refresh. */
class TestDeltaRangerCommitPublication {
  private val mapper = new ObjectMapper
  private val versionKey = "delta.registration.receipt.nativeVersion"
  private val nativeIdKey = "delta.registration.receipt.nativeId"
  private val schema = StructType(
    Seq(
      StructField("id", LongType, nullable = true),
      StructField("secret", StringType, nullable = true),
      StructField("region", StringType, nullable = true)
    )
  )
  private val sourceRows =
    Vector(Row(1L, "one", "keep"), Row(2L, "two", "drop"), Row(3L, null, "keep"))
  private val sentinel = Row(99L, "sentinel", "keep")

  /** A static receipt permits the installed public insertInto to return after publication. */
  @Test
  def staticReceiptInsertIntoReturns(): Unit = characterize("insertInto", refresh = false)

  /** A static receipt permits the installed public saveAsTable to return after publication. */
  @Test
  def staticReceiptSaveAsTableReturns(): Unit = characterize("saveAsTable", refresh = false)

  /** A proven own receipt advance permits insertInto to complete native maintenance. */
  @Test
  def refreshedReceiptInsertIntoReturns(): Unit =
    characterize("insertInto", refresh = true)

  /** A proven own receipt advance permits saveAsTable to complete native maintenance. */
  @Test
  def refreshedReceiptSaveAsTableReturns(): Unit =
    characterize("saveAsTable", refresh = true)

  /** Qualifies representative proof, receipt, content and context rejection on every pin. */
  @ParameterizedTest(name = "publication rejects {0}")
  @ValueSource(strings =
    Array(
      "uuid",
      "changed-add",
      "duplicate-key",
      "same-stat-content",
      "jump",
      "regression",
      "settings",
      "foreign-next-file"
    )
  )
  def representativePublicationDriftFailsClosed(fault: String): Unit =
    characterize("insertInto", refresh = true, fault = fault)

  /** Exercises all remaining submitted action and full receipt fields at the real boundary. */
  @ParameterizedTest(name = "publication rejects {0}")
  @ValueSource(strings =
    Array(
      "app-version",
      "missing-action",
      "extra-action",
      "reordered-actions",
      "mixed-action",
      "tags",
      "unknown-envelope",
      "multiple-envelope",
      "token",
      "generation",
      "native-id",
      "location",
      "schema",
      "lease",
      "policy",
      "last-setting",
      "early-refresh",
      "receipt-id",
      "receipt-runId",
      "receipt-operation",
      "receipt-evidence",
      "receipt-state",
      "receipt-reason",
      "receipt-marker",
      "read-version",
      "operation",
      "operation-parameters"
    )
  )
  def publicationDriftFailsClosed(fault: String): Unit =
    characterize("insertInto", refresh = true, fault = fault)

  /** The full observed file status remains fixed even when replacement bytes are identical. */
  @Test
  def sameContentChangedStatusFails(): Unit =
    characterize("insertInto", refresh = true, fault = "same-content-status")

  /** Rechecks exact native settings before a metadata callback can return authority. */
  @Test
  def metadataCallbackRechecksExactSettings(): Unit =
    characterize("insertInto", refresh = true, fault = "nested-last-setting")

  /** Prepublication metadata cannot introduce the future native last-commit output. */
  @Test
  def prepublicationCallbackRechecksExactSettings(): Unit =
    characterize("insertInto", refresh = true, fault = "nested-published-setting")

  /** The accepted raw profile requires stable current-user wrapper identity. */
  @Test
  def rawWarmProfileRequiresStableLoginUgi(): Unit = {
    val trace = InstalledAppendBoundaryProbe.fresh("raw-warm-ugi-profile")
    try {
      assertSame(UserGroupInformation.getLoginUser(), UserGroupInformation.getCurrentUser())
      assertSame(UserGroupInformation.getCurrentUser(), UserGroupInformation.getCurrentUser())
      val app = UserGroupInformation.createUserForTesting("APP", Array.empty[String])
      app.doAs(new PrivilegedExceptionAction[Unit] {
        override def run(): Unit = {
          val first = UserGroupInformation.getCurrentUser()
          val second = UserGroupInformation.getCurrentUser()
          assertEquals(first, second)
          assertNotSame(first, second)
          trace.record(
            "ugi-wrapper-diagnostic",
            "loginReferenceStable=true doAsSameSubject=true doAsWrapperReferenceStable=false"
          )
        }
      })
    } finally trace.close()
  }

  /** Exercises the real packaged phase-pin inspector on exact current compiled class bytes. */
  @Test
  def actualCompiledCoordinatorRetainsPostHelperPhasePin(): Unit = {
    val trace = InstalledAppendBoundaryProbe.fresh("local-phase-pin-regression")
    val archive = trace.directory.resolve("compiled-phase-pin-test-only.jar")
    val classes: Seq[Class[_]] = Seq(
      classOf[DeltaRangerRegistrationBindings],
      DeltaRangerAppendCoordinator.getClass,
      classOf[TestDeltaRangerCommitPublication]
    )
    val origins = classes
      .map(cls => Paths.get(cls.getProtectionDomain.getCodeSource.getLocation.toURI))
      .distinct
    val contents = scala.collection.mutable.Map.empty[String, Array[Byte]]
    origins.foreach { origin =>
      assertTrue(Files.isDirectory(origin))
      val stream = Files.walk(origin)
      try
        stream
          .iterator()
          .asScala
          .filter(path => Files.isRegularFile(path) && path.toString.endsWith(".class"))
          .foreach { path =>
            val name = origin.relativize(path).toString
            val bytes = Files.readAllBytes(path)
            contents.get(name).foreach(previous => assertArrayEquals(previous, bytes))
            contents.put(name, bytes)
          }
      finally stream.close()
    }
    val output = new JarOutputStream(Files.newOutputStream(archive))
    try
      contents.toVector.sortBy(_._1).foreach { case (name, bytes) =>
        output.putNextEntry(new JarEntry(name)); output.write(bytes); output.closeEntry()
      }
    finally output.close()
    val jar = new JarFile(archive.toFile)
    try
      contents.foreach { case (name, bytes) =>
        val input = jar.getInputStream(jar.getJarEntry(name))
        try assertArrayEquals(bytes, input.readAllBytes())
        finally input.close()
      }
    finally jar.close()
    val loader = new URLClassLoader(Array(archive.toUri.toURL), getClass.getClassLoader) {

      /** Loads this test-only archive before delegating external runtime classes. */
      override protected def loadClass(name: String, resolve: Boolean): Class[_] = synchronized {
        if (!name.startsWith("org.apache.gravitino.spark.connector.delta.ranger."))
          return super.loadClass(name, resolve)
        val loaded: Class[_] = Option(findLoadedClass(name)).getOrElse {
          try findClass(name)
          catch { case _: ClassNotFoundException => super.loadClass(name, false) }
        }
        if (resolve) resolveClass(loaded)
        loaded
      }
    }
    try {
      val module = Class
        .forName(
          "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerLivePhasePin$",
          true,
          loader
        )
        .getField("MODULE$")
        .get(null)
      val pin =
        try module.getClass.getMethod("inspect", classOf[Path]).invoke(module, trace.directory)
        catch { case failure: InvocationTargetException => throw failure.getCause }
      val coordinatorName =
        "org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerAppendCoordinator$.class"
      assertEquals(
        sha256(contents(coordinatorName)),
        pin.getClass.getMethod("classSha256").invoke(pin)
      )
      assertEquals(
        sha256(Files.readAllBytes(archive)),
        pin.getClass.getMethod("artifactSha256").invoke(pin)
      )
      assertTrue(pin.getClass.getMethod("line").invoke(pin).asInstanceOf[Integer] > 0)
      trace.record(
        "phase-pin-inspected",
        s"compiledClassSha256=${sha256(contents(coordinatorName))} testJarSha256=${sha256(Files.readAllBytes(archive))} classes=${contents.size} localRegressionOnly=true"
      )
    } finally { loader.close(); trace.close() }
  }

  /** A nonzero native baseline reaches a real checkpoint with a storage-refreshed receipt. */
  @Test
  def refreshedNonzeroBaselineProducesCheckpoint(): Unit =
    characterize("insertInto", refresh = true, seedCommits = 9)

  /** Native cache maintenance and authorized duplicate handling survive both warm orders. */
  @ParameterizedTest(name = "warm dependent first={0}")
  @ValueSource(booleans = Array(false, true))
  def refreshedWarmCachesAndDuplicate(dependentFirst: Boolean): Unit =
    characterize(
      if (dependentFirst) "saveAsTable" else "insertInto",
      refresh = true,
      warmOrder = Some(dependentFirst)
    )

  /** Empty input without an application marker preserves the native skipped-commit result. */
  @Test
  def refreshedEmptyInputSkipsCommit(): Unit =
    characterize("insertInto", refresh = true, emptySource = true, transactionMarker = false)

  /** Empty input with an application marker publishes the native SetTransaction-only commit. */
  @Test
  def refreshedEmptyInputPublishesTransactionOnly(): Unit =
    characterize("saveAsTable", refresh = true, emptySource = true)

  private case class NativeObservation(
      version: Long,
      actions: Vector[JsonNode],
      headJsonSha256: String,
      headJsonLength: Int
  ) {
    val metadata: JsonNode = actions.filter(_.has("metaData")).last.get("metaData")
    val protocol: JsonNode = actions.filter(_.has("protocol")).last.get("protocol")
    val nativeId: String = metadata.get("id").asText()
    val schema: StructType =
      DataType.fromJson(metadata.get("schemaString").asText()).asInstanceOf[StructType]
  }

  private case class ReadObservation(
      catalog: String,
      nativeVersion: Long,
      receiptVersion: String,
      headJsonSha256: String,
      nativeId: String,
      schemaJson: String,
      properties: Map[String, String],
      commitOwner: Boolean,
      transactionId: Option[String],
      stack: String
  )

  private def characterize(
      frontend: String,
      refresh: Boolean,
      fault: String = "",
      seedCommits: Int = 0,
      warmOrder: Option[Boolean] = None,
      emptySource: Boolean = false,
      transactionMarker: Boolean = true
  ): Unit = {
    val app = UserGroupInformation.createUserForTesting("APP", Array.empty[String])
    def asApp[A](body: => A): A = if (warmOrder.nonEmpty) body
    else
      app.doAs(new PrivilegedExceptionAction[A] {
        override def run(): A = body
      })
    val beforePublicationFault =
      Set("early-refresh", "nested-published-setting", "foreign-next-file")(fault)
    val nextVersion = seedCommits.toLong + 1
    val baselineRows = Vector.fill(seedCommits + 1)(sentinel)
    val trace =
      InstalledAppendBoundaryProbe.fresh(s"commit-publication-$frontend-refresh-$refresh-$fault")
    val rawConfig = trace.directory.resolve("raw-maintenance.properties")
    if (warmOrder.nonEmpty) {
      Files.write(
        rawConfig,
        ("version=1\nprofile=raw-current-head-filter-project-v1\ntable.count=1\n" +
          "table.0.metalake=lake\ntable.0.catalog=a\ntable.0.database=db\ntable.0.table=t\n")
          .getBytes(StandardCharsets.UTF_8)
      )
      Files.setPosixFilePermissions(rawConfig, PosixFilePermissions.fromString("rw-------"))
    }
    val target = new TestDeltaRangerNativeSelect.Fixture
    val source = new TestDeltaRangerNativeSelect.Fixture
    val previousPolicies = NativeSelectFixture.catalogPolicies
    val previousPolicy = NativeSelectFixture.policy
    source.customCatalog = "b"
    source.mask = "'MASKED'"
    source.filter = if (emptySource) "false" else "region = 'keep'"
    NativeSelectFixture.catalogPolicies = Map("a" -> target, "b" -> source)
    val install = NativeSelectFixture.extension(target)
    val reads = ArrayBuffer.empty[ReadObservation]
    try
      NativeSelectFixture.withSpark(
        spark => {
          runtime(spark, trace)
          if (warmOrder.nonEmpty) {
            val current = UserGroupInformation.getCurrentUser()
            assertSame(current, UserGroupInformation.getCurrentUser())
            assertSame(UserGroupInformation.getLoginUser(), current)
            trace.record(
              "warm-subject-profile",
              s"profile=accepted-login-context subject=${current.getUserName} repeatedUgiReference=true"
            )
          }
          assertFalse(spark.sessionState.conf.adaptiveExecutionEnabled)
          assertEquals(12, spark.sessionState.analyzer.postHocResolutionRules.size)
          assertEquals(
            classOf[RequiredPrivilegesCheck],
            spark.sessionState.analyzer.postHocResolutionRules(7).getClass
          )
          DeltaRangerAppendInventory.qualify(spark)
          val targetPath = Paths.get(NativeSelectFixture.locations("a"))
          val sourcePath = Paths.get(NativeSelectFixture.locations("b"))
          val root = targetPath.getParent
          val beforeTarget = observe(targetPath)
          val beforeSource = observe(sourcePath)
          assertEquals(seedCommits.toLong, beforeTarget.version)
          assertEquals(0L, beforeSource.version)
          assertEquals(baselineRows, rows(spark, targetPath, beforeTarget))
          assertEquals(sourceRows, rows(spark, sourcePath, beforeSource))
          val targetFixture = NativeSelectFixture.bindingFixture(spark, "a")
          val sourceFixture = NativeSelectFixture.bindingFixture(spark, "b")
          val targetProperties = targetFixture.properties.asScala.toMap
          val sourceProperties = sourceFixture.properties.asScala.toMap
          assertEquals(beforeTarget.nativeId, targetProperties(nativeIdKey))
          assertEquals(seedCommits.toString, targetProperties(versionKey))
          val originalLastCommit =
            spark.conf.getOption("spark.databricks.delta.lastCommitVersionInSession")
          var injected = false
          var committedReads = 0
          val nextFile = targetPath.resolve("_delta_log/00000000000000000001.json")
          installRefresh(
            targetFixture,
            targetPath,
            "a",
            refresh,
            reads,
            trace,
            native => {
              if (native.version == 1) committedReads += 1
              val eligible =
                if (beforePublicationFault)
                  native.version == 0 &&
                  DeltaRangerAppendCommit.current.nonEmpty
                else native.version == 1
              if (
                fault.nonEmpty && !injected && eligible &&
                (fault != "regression" || committedReads == 2)
              ) {
                injected = true
                if (Files.exists(nextFile))
                  Files.copy(nextFile, trace.directory.resolve("original-published.json"))
                fault match {
                  case "jump"          => targetFixture.properties.put(versionKey, "2")
                  case "regression"    => targetFixture.properties.put(versionKey, "0")
                  case "early-refresh" => targetFixture.properties.put(versionKey, "1")
                  case "foreign-next-file" =>
                    val bytes =
                      Files.readAllBytes(targetPath.resolve("_delta_log/00000000000000000000.json"))
                    val path = new HadoopPath(nextFile.toUri)
                    val output =
                      path.getFileSystem(spark.sessionState.newHadoopConf()).create(path, false)
                    try output.write(bytes)
                    finally output.close()
                    trace.record(
                      "synthetic-foreign-file",
                      "copiedVersionZero=true ownNativeInvocation=false"
                    )
                  case value if value.startsWith("receipt-") =>
                    val changes = Map(
                      "id" -> "2",
                      "runId" -> "changed-run",
                      "operation" -> "RESERVE",
                      "evidence" -> "EXACT_NATIVE_COMMIT",
                      "state" -> "QUARANTINED",
                      "reason" -> "NATIVE_ID_CHANGED",
                      "marker" -> "foreign-marker"
                    )
                    val field = value.stripPrefix("receipt-")
                    targetFixture.properties
                      .put("delta.registration.receipt." + field, changes(field))
                  case "token" =>
                    targetFixture.properties
                      .put("delta.registration.receipt.token", "replaced-token")
                  case "generation" =>
                    targetFixture.properties.put("delta.registration.receipt.generation", "2")
                  case "native-id" =>
                    targetFixture.properties.put(nativeIdKey, UUID.randomUUID().toString)
                  case "location" =>
                    targetFixture.properties.put(
                      "delta.registration.receipt.location",
                      targetPath.resolve("foreign").toUri.toString
                    )
                  case "schema"   => targetFixture.columns = targetFixture.columns.reverse
                  case "lease"    => targetFixture.metadata.close()
                  case "policy"   => target.engine.close()
                  case "settings" => spark.conf.set("spark.sql.shuffle.partitions", "7")
                  case "last-setting" =>
                    spark.conf.set("spark.databricks.delta.lastCommitVersionInSession", "0")
                  case "nested-last-setting" | "nested-published-setting" =>
                    val retained = DeltaRangerAppendCommit.current.get
                    val originalCallback = targetFixture.onRead
                    var nestedCallback = false
                    targetFixture.onRead = _ => {
                      nestedCallback = true
                      if (beforePublicationFault)
                        spark.conf.set("spark.databricks.delta.lastCommitVersionInSession", "1")
                      else
                        originalLastCommit match {
                          case Some(value) =>
                            spark.conf
                              .set("spark.databricks.delta.lastCommitVersionInSession", value)
                          case None =>
                            spark.conf.unset("spark.databricks.delta.lastCommitVersionInSession")
                        }
                    }
                    try {
                      assertThrows(
                        classOf[SecurityException],
                        () => retained.requireLive(),
                        "The same requireLive invocation must reject callback settings drift"
                      )
                      assertTrue(nestedCallback, "Nested actual metadata callback must execute")
                    } finally targetFixture.onRead = originalCallback
                  case "same-content-status" =>
                    val retained = DeltaRangerAppendCommit.current.get
                    val originalCallback = targetFixture.onRead
                    targetFixture.onRead = _ => ()
                    try {
                      retained.requireLive()
                      corrupt(spark, nextFile, fault, trace)
                      assertThrows(
                        classOf[SecurityException],
                        () => retained.requireLive(),
                        "Previously validated status must remain latched across requireLive calls"
                      )
                    } finally targetFixture.onRead = originalCallback
                  case corruption => corrupt(spark, nextFile, corruption, trace)
                }
                trace.record(
                  "injected-drift",
                  s"fault=$fault nativeVersion=${native.version} committedReads=$committedReads"
                )
              }
            }
          )
          installRefresh(sourceFixture, sourcePath, "b", refresh = false, reads, trace)
          val before =
            InstalledAppendBoundaryProbe.manifest(root, trace.directory.resolve("before"))
          Files.createDirectories(Paths.get(new URI(spark.conf.get("spark.sql.warehouse.dir"))))
          val appId = "characterization-" + UUID.randomUUID()
          if (transactionMarker) {
            spark.conf.set("spark.databricks.delta.write.txnAppId", appId)
            spark.conf.set("spark.databricks.delta.write.txnVersion", "1")
          }
          val native = warmOrder.map(_ => RawNativeObservationFixture.nativeSession(spark))
          val cachedFrames = asApp {
            native.toVector.flatMap(session =>
              RawNativeObservationFixture.active(session) {
                val raw = session.read.format("delta").load(targetPath.toString)
                val dependent = raw.select("id", "region")
                val ordered = if (warmOrder.get) Vector(dependent, raw) else Vector(raw, dependent)
                ordered.foreach(frame => {
                  frame.cache(); assertEquals(baselineRows.size, frame.collect().length)
                })
                ordered
              }
            )
          }
          def entries =
            cachedFrames.map(frame => spark.sharedState.cacheManager.lookupCachedData(frame).get)
          var priorCaches = entries
          def assertRebuilt(): Unit = asApp {
            entries.zip(priorCaches).foreach { case (current, previous) =>
              assertSame(previous.plan, current.plan)
              assertNotSame(
                previous.cachedRepresentation.cacheBuilder,
                current.cachedRepresentation.cacheBuilder
              )
              assertFalse(previous.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded)
              assertFalse(current.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded)
              assertTrue(
                current.cachedRepresentation.cacheBuilder.cachedPlan
                  .isInstanceOf[DeltaRangerRawProducer]
              )
            }
            native.foreach(session =>
              RawNativeObservationFixture.active(session) {
                // A populated DataFrame retains its old QueryExecution. Resolve fresh reads
                // through the current cache entries, while keeping the original cache keys above.
                val raw = session.read.format("delta").load(targetPath.toString)
                val dependent = raw.select("id", "region")
                val ordered = if (warmOrder.get) Vector(dependent, raw) else Vector(raw, dependent)
                val expected =
                  Vector(Row(1L, "MASKED", "keep"), Row(3L, "MASKED", "keep")) ++ baselineRows
                def counted(rows: Vector[Row]) =
                  rows.groupBy(identity).map { case (row, values) => row -> values.size }
                ordered.zip(entries).foreach { case (frame, entry) =>
                  assertSame(
                    entry.cachedRepresentation.cacheBuilder,
                    spark.sharedState.cacheManager
                      .lookupCachedData(frame)
                      .get
                      .cachedRepresentation
                      .cacheBuilder
                  )
                  val expectedRows =
                    if (frame.columns.length == 2)
                      expected.map(row => Row(row.getLong(0), row.getString(2)))
                    else expected
                  assertEquals(counted(expectedRows), counted(frame.collect().toVector))
                }
              }
            )
            assertTrue(
              entries.forall(_.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded)
            )
            priorCaches = entries
          }
          asApp {
            val data = spark.table("b.db.t")
            var sourceObserved = false
            val preparingField = DeltaRangerAppendCoordinator.getClass.getDeclaredFields
              .find(field => field.getName == "preparing" || field.getName.endsWith("$$preparing"))
              .get
            preparingField.setAccessible(true)
            val preparing = preparingField
              .get(DeltaRangerAppendCoordinator)
              .asInstanceOf[ThreadLocal[DeltaRangerAppendCoordinator.Source]]
            val originalSourceRead = sourceFixture.onRead
            sourceFixture.onRead = index => {
              originalSourceRead(index)
              if (!sourceObserved && preparing.get() != null) {
                val owner = preparing.get()
                val inputField = owner.getClass.getDeclaredFields
                  .find(field => field.getName == "input" || field.getName.endsWith("$$input"))
                  .get
                inputField.setAccessible(true)
                val input =
                  inputField.get(owner).asInstanceOf[DeltaRangerAppendV1Input.RetainedV1Input]
                val receipt = input.receipt
                sourceObserved = true
                assertEquals(1, receipt.obligations.size)
                val obligation = receipt.obligations.head
                assertEquals(Set("id", "secret", "region"), obligation.columns)
                assertEquals("'MASKED'", obligation.decisions.masks("secret").expression().get())
                assertEquals(
                  if (emptySource) "false" else "region = 'keep'",
                  obligation.decisions.filter.get()
                )
                trace.source(receipt)
              }
            }
            clean()
            val started = System.nanoTime()
            var failure: Option[Throwable] = None
            try {
              if (frontend == "insertInto") data.write.mode("append").insertInto("a.db.t")
              else data.write.mode("append").saveAsTable("a.db.t")
            } catch {
              case error: Throwable =>
                failure = Some(error)
                trace.failed("original-public-writer", error)

            }
            sourceFixture.onRead = originalSourceRead
            assertTrue(sourceObserved, "actual retained append receipt must be observed")
            trace.record(
              "writer-terminal",
              s"frontend=$frontend refresh=$refresh elapsedNanos=${System.nanoTime() - started} clientReturned=${failure.isEmpty} attempts=1"
            )
            clean()
            if (fault.isEmpty)
              assertTrue(
                failure.isEmpty,
                "Public writer must return normally; see original throwable"
              )
            else {
              assertTrue(injected, "Fault must reach the actual finite commit boundary")
              assertTrue(failure.isDefined, "Drift must fail the public writer")
              assertTrue(failure.get.isInstanceOf[SecurityException], failure.get.toString)
              assertTrue(
                failure.get.getStackTrace.exists(frame =>
                  frame.getClassName.contains("DeltaRangerAppendCommit") ||
                    frame.getClassName.contains("BoundAppendCommit")
                )
              )
            }
          }
          if (emptySource) {
            val afterTarget = observe(targetPath)
            assertEquals(
              if (transactionMarker) nextVersion else seedCommits.toLong,
              afterTarget.version
            )
            assertEquals(baselineRows, rows(spark, targetPath, afterTarget))
            assertEquals(beforeSource, observe(sourcePath))
            if (transactionMarker) {
              val actions = commitActions(nextFile)
              assertEquals(Vector("commitInfo", "txn"), actions.map(_.fieldNames().next()))
              assertEquals(appId, actions.last.get("txn").get("appId").asText())
              val crc = mapper.readTree(
                Files.readAllBytes(targetPath.resolve("_delta_log/00000000000000000001.crc"))
              )
              assertEquals(actions.head.get("commitInfo").get("txnId"), crc.get("txnId"))
            } else assertFalse(Files.exists(nextFile))
            val after =
              InstalledAppendBoundaryProbe.manifest(root, trace.directory.resolve("after"))
            assertTrue(before.forall { case (name, digest) => after.get(name).contains(digest) })
            trace.record(
              "empty-native-outcome",
              s"marker=$transactionMarker beforeVersion=${beforeTarget.version} afterVersion=${afterTarget.version} noRetry=true cleanup=true"
            )
          } else if (fault.nonEmpty) {
            assertEquals(beforeSource, observe(sourcePath))
            val after =
              InstalledAppendBoundaryProbe.manifest(root, trace.directory.resolve("after"))
            assertTrue(before.forall { case (name, digest) => after.get(name).contains(digest) })
            assertFalse(Files.exists(targetPath.resolve("_delta_log/00000000000000000002.json")))
            if (fault == "foreign-next-file") {
              assertArrayEquals(
                Files.readAllBytes(targetPath.resolve("_delta_log/00000000000000000000.json")),
                Files.readAllBytes(nextFile)
              )
              assertFalse(Files.exists(targetPath.resolve("_delta_log/00000000000000000001.crc")))
              trace.record(
                "foreign-file-result",
                "syntheticFileRetained=true ownNativeCommit=false noRetry=true"
              )
            } else if (beforePublicationFault) {
              assertEquals(
                before.filter(_._1.contains("/_delta_log/")),
                after.filter(_._1.contains("/_delta_log/"))
              )
              assertTrue(
                after.keySet
                  .diff(before.keySet)
                  .forall(name =>
                    name.startsWith("publication-target/") && !name.contains("/_delta_log/")
                  )
              )
              assertFalse(Files.exists(nextFile))
            } else {
              assertTrue(Files.exists(nextFile), "Committed publication remains retained")
              val original = commitActions(trace.directory.resolve("original-published.json"))
              assertEquals(appId, original.find(_.has("txn")).get.get("txn").get("appId").asText())
              assertEquals(
                2L,
                original
                  .filter(_.has("add"))
                  .map(n =>
                    mapper.readTree(n.get("add").get("stats").asText()).get("numRecords").asLong()
                  )
                  .sum
              )
              assertFalse(Files.exists(targetPath.resolve("_delta_log/00000000000000000001.crc")))
            }
            trace.record(
              "denied-drift",
              s"fault=$fault injected=true committed=${!beforePublicationFault} noRetry=true cleanup=true"
            )
          } else {
            // All following native observations read independently published bytes, never the governed cache.
            val afterTarget = observe(targetPath)
            val afterSource = observe(sourcePath)
            assertEquals(nextVersion, afterTarget.version)
            assertEquals(beforeSource, afterSource)
            assertEquals(beforeTarget.metadata, afterTarget.metadata)
            assertEquals(beforeTarget.protocol, afterTarget.protocol)
            assertEquals(beforeTarget.schema, afterTarget.schema)
            assertEquals(
              Vector(Row(1L, "MASKED", "keep"), Row(3L, "MASKED", "keep")) ++ baselineRows,
              rows(spark, targetPath, afterTarget)
            )
            assertEquals(sourceRows, rows(spark, sourcePath, afterSource))
            val published =
              commitActions(targetPath.resolve(f"_delta_log/$nextVersion%020d.json"))
            assertEquals(1, published.count(_.has("txn")))
            assertTrue(published.exists(_.has("add")))
            assertEquals(0, published.count(_.has("remove")))
            val txn = published.find(_.has("txn")).get.get("txn")
            assertEquals(appId, txn.get("appId").asText())
            assertEquals(1L, txn.get("version").asLong())
            val commitInfo = published.find(_.has("commitInfo")).get.get("commitInfo")
            assertEquals("WRITE", commitInfo.get("operation").asText())
            assertEquals("Append", commitInfo.get("operationParameters").get("mode").asText())
            val nativeTxnId = commitInfo.get("txnId").asText()
            val committedReads =
              reads.filter(r => r.catalog == "a" && r.nativeVersion == nextVersion)
            assertTrue(committedReads.nonEmpty)
            assertTrue(committedReads.head.commitOwner)
            assertTrue(committedReads.head.stack.contains("updateAfterCommit"))
            assertTrue(committedReads.head.stack.contains("stateReconstruction"))
            assertEquals(Some(nativeTxnId), committedReads.head.transactionId)
            assertEquals(afterTarget.headJsonSha256, committedReads.head.headJsonSha256)
            assertTrue(
              reads.exists(r =>
                r.catalog == "a" && r.nativeVersion == seedCommits && r.stack.contains(
                  "BoundAppendTransaction.requireLive"
                )
              )
            )
            assertTrue(
              reads
                .filter(_.catalog == "a")
                .forall(r => r.nativeId == beforeTarget.nativeId && r.schemaJson == schema.json)
            )
            assertTrue(
              reads
                .filter(_.catalog == "a")
                .forall(r => r.properties - versionKey == targetProperties - versionKey)
            )
            assertTrue(reads.filter(_.catalog == "b").forall(_.properties == sourceProperties))
            assertEquals(
              if (refresh) nextVersion.toString else seedCommits.toString,
              targetFixture.properties.get(versionKey)
            )
            assertEquals(sourceProperties, sourceFixture.properties.asScala.toMap)
            assertEquals(
              targetProperties - versionKey,
              targetFixture.properties.asScala.toMap - versionKey
            )
            assertTrue(
              committedReads.forall(
                _.receiptVersion == (if (refresh) nextVersion.toString else seedCommits.toString)
              )
            )
            val crc = targetPath.resolve(f"_delta_log/$nextVersion%020d.crc")
            assertTrue(Files.isRegularFile(crc), "Native checksum hook must complete")
            assertEquals(
              nativeTxnId,
              mapper.readTree(Files.readAllBytes(crc)).get("txnId").asText()
            )
            val adds = published.filter(_.has("add")).map(_.get("add"))
            assertEquals(
              2L,
              adds
                .map(add => mapper.readTree(add.get("stats").asText()).get("numRecords").asLong())
                .sum
            )
            adds.foreach { add =>
              assertTrue(add.get("dataChange").asBoolean())
              assertEquals(
                add.get("size").asLong(),
                Files.size(targetPath.resolve(add.get("path").asText()))
              )
            }
            if (seedCommits == 9) {
              val checkpoint =
                targetPath.resolve("_delta_log/00000000000000000010.checkpoint.parquet")
              assertTrue(
                Files.isRegularFile(checkpoint),
                "Native checkpoint hook must produce real bytes"
              )
              val pointer = mapper.readTree(
                Files.readAllBytes(targetPath.resolve("_delta_log/_last_checkpoint"))
              )
              assertEquals(nextVersion, pointer.get("version").asLong())
              val reader = ParquetReader
                .builder[Group](new GroupReadSupport, new HadoopPath(checkpoint.toUri))
                .withConf(spark.sessionState.newHadoopConf())
                .build()
              var count = 0L
              try { while (reader.read() != null) count += 1 }
              finally reader.close()
              assertEquals(pointer.get("size").asLong(), count)
              assertTrue(count > 0)
              trace.record(
                "checkpoint-produced",
                s"version=$nextVersion rows=$count bytes=${Files.size(checkpoint)}"
              )
            }
            if (warmOrder.nonEmpty) {
              assertRebuilt()
              val committedManifest =
                InstalledAppendBoundaryProbe.manifest(root, trace.directory.resolve("first-return"))
              asApp {
                val duplicate = spark.table("b.db.t")
                if (frontend == "insertInto") duplicate.write.mode("append").insertInto("a.db.t")
                else duplicate.write.mode("append").saveAsTable("a.db.t")
              }
              clean()
              assertRebuilt()
              assertEquals(
                committedManifest,
                InstalledAppendBoundaryProbe
                  .manifest(root, trace.directory.resolve("duplicate-return"))
              )
              target.denied.add("a:*")
              try
                asApp {
                  assertThrows(
                    classOf[SecurityException],
                    () => {
                      val denied = spark.table("b.db.t")
                      if (frontend == "insertInto") denied.write.mode("append").insertInto("a.db.t")
                      else denied.write.mode("append").saveAsTable("a.db.t")
                    }
                  )
                }
              finally target.denied.remove("a:*")
              clean()
              assertEquals(
                committedManifest,
                InstalledAppendBoundaryProbe
                  .manifest(root, trace.directory.resolve("denied-duplicate"))
              )
              trace.record(
                "warm-duplicate",
                s"dependentFirst=${warmOrder.get} populated=true appendRebuilt=true duplicateRebuilt=true deniedDuplicate=true unchangedStorage=true"
              )
            }
            val after =
              InstalledAppendBoundaryProbe.manifest(root, trace.directory.resolve("after"))
            assertTrue(before.forall { case (name, digest) => after.get(name).contains(digest) })
            assertEquals(
              before.filter(_._1.startsWith("publication-source/")),
              after.filter(_._1.startsWith("publication-source/"))
            )
            trace.record(
              "independent-result",
              s"beforeVersion=$seedCommits afterVersion=$nextVersion beforeRows=${baselineRows.size} afterRows=${baselineRows.size + 2} adds=${adds.size} removes=0 matchingSetTransaction=true nativeTxnId=$nativeTxnId firstPostcommitTxnIdMatches=true receiptVersion=${targetFixture.properties.get(versionKey)} reads=${reads.size} cleanup=true noRetry=true"
            )
          }
          clean()
        },
        Some(extensions => {
          new GravitinoAuthorizationSparkSessionExtensions().apply(extensions)
          install(extensions)
        }),
        startupOptions =
          Map("spark.databricks.delta.write.txnVersion.autoReset.enabled" -> "false") ++
            warmOrder.map(_ =>
              "spark.gravitino.deltaRanger.rawCacheMaintenance.config" -> rawConfig.toString
            ),
        beforeExtension = spark => seed(spark, seedCommits)
      )
    finally {
      NativeSelectFixture.catalogPolicies = previousPolicies
      NativeSelectFixture.policy = previousPolicy
      target.engine.close()
      source.engine.close()
      trace.close()
    }
  }

  private def installRefresh(
      fixture: NativeBoundCatalogFixture,
      path: Path,
      catalog: String,
      refresh: Boolean,
      reads: ArrayBuffer[ReadObservation],
      trace: InstalledAppendBoundaryProbe.Trace,
      mutate: NativeObservation => Unit = _ => ()
  ): Unit = {
    val converter = new SparkTypeConverter
    fixture.onRead = ordinal => {
      val native = observe(path)
      if (refresh) {
        fixture.properties.put(versionKey, native.version.toString)
        fixture.properties.put(nativeIdKey, native.nativeId)
        fixture.columns = native.schema.fields.map(field =>
          Column.of(
            field.name,
            converter.toGravitinoType(field.dataType),
            null,
            field.nullable,
            false,
            Column.DEFAULT_VALUE_NOT_SET
          )
        )
      }
      mutate(native)
      val stack = Thread.currentThread().getStackTrace.map(_.toString).mkString("\n")
      val transactionId = OptimisticTransaction.getActive.map(_.txnId.toString)
      val read = ReadObservation(
        catalog,
        native.version,
        fixture.properties.get(versionKey),
        native.headJsonSha256,
        native.nativeId,
        native.schema.json,
        fixture.properties.asScala.toMap,
        DeltaRangerAppendCommit.current.nonEmpty,
        transactionId,
        stack
      )
      reads += read
      trace.record(
        "metadata-read",
        s"catalog=$catalog ordinal=$ordinal nativeVersion=${read.nativeVersion} receiptVersion=${read.receiptVersion} headJsonSha256=${read.headJsonSha256} headJsonLength=${native.headJsonLength} nativeId=${read.nativeId} schema=${read.schemaJson} properties=${read.properties.toSeq.sorted} commitOwner=${read.commitOwner} nativeTransactionId=${read.transactionId} stack=\n$stack"
      )
    }
  }

  private def corrupt(
      spark: SparkSession,
      path: Path,
      fault: String,
      trace: InstalledAppendBoundaryProbe.Trace
  ): Unit = {
    val original = Files.readAllBytes(path)
    val actions = parseActions(original)
    val info = actions.head.get("commitInfo").asInstanceOf[ObjectNode]
    val add = actions.find(_.has("add")).get.get("add").asInstanceOf[ObjectNode]
    val txn = actions.find(_.has("txn")).get.get("txn").asInstanceOf[ObjectNode]
    val changed = fault match {
      case "read-version" => info.put("readVersion", 9L); actions
      case "operation"    => info.put("operation", "DELETE"); actions
      case "operation-parameters" =>
        info.putObject("operationParameters").put("mode", "Overwrite"); actions
      case "uuid"              => info.put("txnId", UUID.randomUUID().toString); actions
      case "app-version"       => txn.put("version", 2L); actions
      case "changed-add"       => add.put("size", add.get("size").asLong() + 1); actions
      case "missing-action"    => actions.dropRight(1)
      case "extra-action"      => actions :+ actions.last
      case "reordered-actions" => actions.head +: actions.tail.reverse
      case "mixed-action"      => actions :+ mapper.readTree("{\"remove\":{\"path\":\"foreign\"}}")
      case "tags"              => info.putObject("tags").put("foreign", "value"); actions
      case "unknown-envelope"  => actions :+ mapper.readTree("{\"foreign\":{}}")
      case "multiple-envelope" =>
        actions.last.asInstanceOf[ObjectNode].putObject("foreign"); actions
      case "same-stat-content" => info.put("timestamp", info.get("timestamp").asLong() + 1); actions
      case "duplicate-key"     => actions
      case "same-content-status" => actions
    }
    val text = changed.map(_.toString).mkString("", "\n", "\n")
    val value =
      if (fault == "duplicate-key")
        text.replace("\"commitInfo\":{", "\"commitInfo\":{\"txnId\":\"foreign\",")
      else text
    val bytes = value.getBytes(StandardCharsets.UTF_8)
    val hadoop = new HadoopPath(path.toUri)
    val fs = hadoop.getFileSystem(spark.sessionState.newHadoopConf())
    val status = fs.getFileStatus(hadoop)
    val output = fs.create(hadoop, true)
    try output.write(bytes)
    finally output.close()
    fs.setTimes(
      hadoop,
      status.getModificationTime + (if (fault == "same-content-status") 10000L else 0L),
      status.getAccessTime
    )
    val observed = fs.getFileStatus(hadoop)
    if (fault == "same-content-status") {
      assertArrayEquals(original, bytes)
      assertEquals(status.getLen, observed.getLen)
      assertNotEquals(status.getModificationTime, observed.getModificationTime)
    }
    if (fault == "same-stat-content") {
      assertEquals(status.getLen, observed.getLen)
      assertEquals(status.getModificationTime, observed.getModificationTime)
      assertFalse(java.util.Arrays.equals(original, bytes))
    }
    Files.write(trace.directory.resolve("corrupted-published.json"), bytes)
    trace.record(
      "corrupted-publication",
      s"fault=$fault beforeSha=${sha256(original)} afterSha=${sha256(bytes)} sameLength=${status.getLen == observed.getLen} sameMtime=${status.getModificationTime == observed.getModificationTime}"
    )
  }

  private def observe(path: Path): NativeObservation = {
    val stream = Files.list(path.resolve("_delta_log"))
    val json =
      try
        stream
          .iterator()
          .asScala
          .filter(_.getFileName.toString.matches("[0-9]{20}\\.json"))
          .toVector
          .sortBy(_.getFileName.toString)
      finally stream.close()
    assertTrue(json.nonEmpty)
    assertEquals(
      (0L until json.size.toLong).toVector,
      json.map(_.getFileName.toString.stripSuffix(".json").toLong)
    )
    val bytes = json.map(Files.readAllBytes(_))
    NativeObservation(
      json.last.getFileName.toString.stripSuffix(".json").toLong,
      bytes.flatMap(parseActions),
      sha256(bytes.last),
      bytes.last.length
    )
  }

  private def commitActions(path: Path): Vector[JsonNode] = parseActions(Files.readAllBytes(path))

  private def parseActions(bytes: Array[Byte]): Vector[JsonNode] =
    new String(bytes, StandardCharsets.UTF_8)
      .split("\n")
      .filter(_.nonEmpty)
      .map(mapper.readTree)
      .toVector

  private def sha256(bytes: Array[Byte]): String =
    MessageDigest.getInstance("SHA-256").digest(bytes).map(b => f"${b & 0xff}%02x").mkString

  private def rows(spark: SparkSession, path: Path, native: NativeObservation): Vector[Row] = {
    assertFalse(native.actions.exists(_.has("remove")))
    native.actions
      .filter(_.has("add"))
      .flatMap { action =>
        val reader = ParquetReader
          .builder[Group](
            new GroupReadSupport,
            new HadoopPath(path.resolve(action.get("add").get("path").asText()).toUri)
          )
          .withConf(spark.sessionState.newHadoopConf())
          .build()
        val result = ArrayBuffer.empty[Row]
        try {
          var group = reader.read()
          while (group != null) {
            val secret =
              if (group.getFieldRepetitionCount("secret") == 0) null
              else group.getString("secret", 0)
            result += Row(group.getLong("id", 0), secret, group.getString("region", 0))
            group = reader.read()
          }
        } finally reader.close()
        result
      }
      .sortBy(_.getLong(0))
  }

  private def seed(spark: SparkSession, commits: Int): Unit = {
    val root = Paths.get(NativeSelectFixture.locations("a")).getParent
    Seq("a" -> ("publication-target", Vector(sentinel)), "b" -> ("publication-source", sourceRows))
      .foreach { case (catalog, (name, values)) =>
        val path = root.resolve(name).toString
        spark.createDataFrame(values.asJava, schema).write.format("delta").save(path)
        if (catalog == "a") (0 until commits).foreach { _ =>
          spark
            .createDataFrame(Vector(sentinel).asJava, schema)
            .write
            .format("delta")
            .mode("append")
            .save(path)
        }
        NativeSelectFixture.locations = NativeSelectFixture.locations.updated(catalog, path)
      }
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
      classOf[OptimisticTransaction],
      classOf[RequiredPrivilegesCheck],
      classOf[DeltaRangerSparkExtension],
      classOf[DeltaRangerRegistrationBindings],
      classOf[DeltaRangerAppendCommit],
      DeltaRangerAppendCoordinator.getClass,
      classOf[TestDeltaRangerCommitPublication],
      DeltaRangerAppendCommit.getClass
    ).foreach { clazz =>
      val input = clazz.getResourceAsStream("/" + clazz.getName.replace('.', '/') + ".class")
      assertNotNull(input)
      val bytes =
        try input.readAllBytes()
        finally input.close()
      val digest =
        MessageDigest.getInstance("SHA-256").digest(bytes).map(b => f"${b & 0xff}%02x").mkString
      trace.record(
        "loaded-class",
        s"class=${clazz.getName} sha256=$digest origin=${clazz.getProtectionDomain.getCodeSource.getLocation}"
      )
    }
    trace.record(
      "runtime",
      s"spark=${spark.version} deltaOrigin=$deltaOrigin requestedSpark=${requestedSpark.getOrElse("absent")} requestedDelta=${requestedDelta.getOrElse("absent")} javaHome=${System.getProperty("java.home")} java=17 scala=${scala.util.Properties.versionNumberString}"
    )
  }

  private def clean(): Unit = {
    assertNull(DeltaRangerNativeOperation.current())
    assertTrue(OptimisticTransaction.getActive.isEmpty)
    assertTrue(DeltaRangerAppendCommit.current.isEmpty)
  }

}

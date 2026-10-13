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

import java.nio.file.{Files, Path, Paths}
import scala.collection.JavaConverters._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.delta.{DeltaLog, OptimisticTransaction}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class TestDeltaRangerInstalledAppendBoundaries {
  import InstalledAppendBoundaryProbe._
  private val txnApp = "spark.databricks.delta.write.txnAppId"
  private val txnVersion = "spark.databricks.delta.write.txnVersion"
  private val reset = "spark.databricks.delta.write.txnVersion.autoReset.enabled"

  @Test
  def installedCommandRemainsClosedWithValidTargetUpdate(): Unit =
    withCase("command") { (spark, policy, trace) =>
      val target = bound(spark)
      val receipt = DeltaRangerAppendTargetReceipt.capture(spark, target)
      trace.record(
        "target-capture",
        s"table=${trace.id(target.table())} descriptor=${trace.id(target.descriptor())} receipt=${trace.id(receipt)} unattached=true"
      )
      val error = rejected(trace, "command-analysis") {
        spark.sql("INSERT INTO a.db.t VALUES (9L, 'new', 'keep')").collect()
      }
      assertCause(error, "Delta append inventory rejected: resolution rules")
      assertTrue(
        trace.events.exists(_.contains("phase=resolution")),
        "Missing command analysis trace evidence"
      )
      assertTrue(trace.events.exists(_.contains("failure command-analysis")))
      assertTrue(trace.observedAppends.nonEmpty)
      trace.observedAppends.foreach(a => assertFalse(DeltaRangerDecisionReceipt.present(a)))
      assertTrue(policy.requests.asScala.exists(_.getAccessType == "update"))
    }

  @Test
  def resolutionObserverPreservesInstalledSelectAndRejectsNoAdditionalShape(): Unit = {
    var baselineRows = Set.empty[Seq[Any]]
    var baselineRequests = Vector.empty[String]
    var baselineManifest = Map.empty[String, Vector[String]]
    Seq(false, true).foreach { enabled =>
      withCase("select-observer-" + enabled, observed = enabled) { (spark, policy, trace) =>
        val data = spark.sql("SELECT id, secret, region FROM a.db.t")
        val rows = data.collect().map(_.toSeq).toSet
        assertEquals(Set(Seq(1L, "a-one-masked", "keep"), Seq(3L, null, "keep")), rows)
        val requests = inventory(policy)
        val rules = ruleManifest(spark)
        trace.record("installed-rule-manifest", rules.toSeq.sortBy(_._1).mkString("\n"))
        trace.record("healthy-rows", rows.toString)
        if (!enabled) { baselineRows = rows; baselineRequests = requests; baselineManifest = rules }
        else {
          assertEquals(baselineRows, rows)
          assertEquals(baselineRequests, requests)
          val observerClass = classOf[ResolutionObserver].getName
          assertEquals(1, rules("resolution").count(_ == observerClass))
          assertEquals(
            baselineManifest,
            rules.updated("resolution", rules("resolution").filterNot(_ == observerClass))
          )
          val logical = data.queryExecution.analyzed
          assertSame(logical, new ResolutionObserver(spark, trace).apply(logical))
          val location = NativeSelectFixture.locations("a")
          assertCause(
            rejected(trace, "raw-path-select") {
              spark.sql(s"SELECT * FROM delta.`$location`").collect()
            },
            "Raw Delta paths are not admitted"
          )
          val unreceipted = logical.clone()
          val tag = TreeNodeTag[DeltaRangerDecisionReceipt.Reference](
            "gravitino.delta.ranger.decision-receipt"
          )
          unreceipted.foreach(_.unsetTagValue(tag))
          assertCause(
            rejected(trace, "unreceipted-source") { DeltaRangerReadRule.check(unreceipted, spark) },
            "missing receipt"
          )
        }
      }
    }
  }

  @Test
  def frontendsExposeCommandInputsBeforeCurrentAdmissionRejection(): Unit = {
    Seq(
      "values",
      "insert-select",
      "insertInto",
      "saveAsTable",
      "writeTo",
      "raw-path",
      "create",
      "replace"
    ).foreach { frontend =>
      withCase("frontend-" + frontend) { (spark, _, trace) =>
        val target = bound(spark)
        val receipt = DeltaRangerAppendTargetReceipt.capture(spark, target)
        trace.record(
          "target-capture",
          s"table=${trace.id(target.table())} descriptor=${trace.id(target.descriptor())} receipt=${trace.id(receipt)} unattached=true"
        )
        val input =
          if (Set("insertInto", "saveAsTable", "writeTo")(frontend))
            Some(spark.sql("SELECT id, secret, region FROM b.db.t"))
          else None
        val retainedSource = input.map(data => source(spark, data, trace))
        val error = rejected(trace, "frontend-" + frontend) {
          frontend match {
            case "values" => spark.sql("INSERT INTO a.db.t VALUES (9L, 'new', 'keep')").collect()
            case "insert-select" =>
              spark.sql("INSERT INTO a.db.t SELECT id, secret, region FROM b.db.t").collect()
            case "insertInto"  => input.get.write.mode("append").insertInto("a.db.t")
            case "saveAsTable" => input.get.write.mode("append").saveAsTable("a.db.t")
            case "writeTo"     => input.get.writeTo("a.db.t").append()
            case "raw-path" =>
              spark
                .sql(
                  s"INSERT INTO delta.`${NativeSelectFixture.locations("a")}` VALUES (9L, 'new', 'keep')"
                )
                .collect()
            case "create" =>
              spark
                .sql(
                  "CREATE TABLE a.db.created USING delta AS SELECT 9L id, 'new' secret, 'keep' region"
                )
                .collect()
            case "replace" =>
              spark
                .sql(
                  "REPLACE TABLE a.db.t USING delta AS SELECT 9L id, 'new' secret, 'keep' region"
                )
                .collect()
          }
        }
        val targetFailure = attempt(trace, "frontend-retained-target") { receipt.validate(spark) }
        val sourceFailures = retainedSource.flatMap { original =>
          attempt(trace, "frontend-retained-source") { original.validateAction(spark) }
        }
        trace.record(
          "frontend-authority",
          s"targetValid=${targetFailure.isEmpty} sourceReceipt=${retainedSource.map(trace.id)} sourceValid=${retainedSource.map(_ => sourceFailures.isEmpty)} actualCommandTargetIsCaptured=${trace.observedAppends.map(a => a.table match { case r: DataSourceV2Relation => r.table eq target.table(); case _ => false })}"
        )
        assertTrue(targetFailure.isEmpty, targetFailure.toString)
        assertTrue(sourceFailures.isEmpty, sourceFailures.toString)
        val expected = frontend match {
          case "insertInto" | "saveAsTable" => "Delta append inventory rejected: resolution rules"
          case "create"                     => "Unqualified Delta Ranger route: CreateTableAsSelect"
          case "replace" => "Unqualified Delta Ranger route: ReplaceTableAsSelect"
          case _         => "Delta append inventory rejected: resolution rules"
        }
        assertCause(error, expected)
        if (!Set("create", "replace")(frontend)) assertTrue(trace.observedAppends.nonEmpty)
        assertTrue(trace.observedAppends.forall(_.analyzedQuery.isEmpty))
        trace.record(
          "frontend-outcome",
          s"frontend=$frontend observedAppendCount=${trace.observedAppends.size} analyzedInputs=${trace.observedAppends.count(_.analyzedQuery.nonEmpty)} physicalV1=NOT_REACHED commit=NOT_REACHED"
        )
      }
    }
  }

  @Test
  def targetUpdateAndOriginalSourceObligationsCoexistWithoutCommandAuthority(): Unit = {
    Seq("separate", "self-source-first", "self-target-first", "empty", "join", "shared-location")
      .foreach { mode =>
        withCase(
          "paired-" + mode,
          setup = (_, _) => {
            if (mode == "shared-location")
              NativeSelectFixture.locations += "b" -> NativeSelectFixture.locations("a")
          }
        ) { (spark, policy, trace) =>
          val catalog = if (mode.startsWith("self")) "a" else "b"
          // A separate executed SELECT control cannot precompute the driver's source query execution.
          val control =
            spark.sql(s"SELECT id, secret, region FROM $catalog.db.t").collect().toVector
          assertEquals(if (catalog == "a") 2 else 3, control.size)
          assertTrue(control.forall(r => r.isNullAt(1) || r.getString(1).endsWith("-masked")))
          val early = if (mode == "self-target-first") Some(bound(spark)) else None
          val earlyReceipt = early.map(DeltaRangerAppendTargetReceipt.capture(spark, _))
          val sql = mode match {
            case "empty" => "SELECT id, secret, region FROM b.db.t WHERE 1 = 0"
            case "join" =>
              "SELECT x.id, x.secret, y.region FROM b.db.t x JOIN b.db.t y ON x.id = y.id"
            case _ => s"SELECT id, secret, region FROM $catalog.db.t"
          }
          val data = spark.sql(sql)
          val sourceReceipt = source(spark, data, trace)
          assertEquals(if (mode == "join") 2 else 1, sourceReceipt.obligations.size)
          assertTrue(sourceReceipt.obligations.forall(_.columns.nonEmpty))
          assertTrue(sourceReceipt.obligations.exists(_.decisions.masks.nonEmpty))
          val target = early.getOrElse(bound(spark))
          policy.events.clear()
          val targetReceipt =
            earlyReceipt.getOrElse(DeltaRangerAppendTargetReceipt.capture(spark, target))
          assertFalse(policy.events.asScala.exists(e => e == "mask" || e == "filter"))
          trace.record(
            "paired-identities",
            s"target=${trace.id(target.table())} descriptor=${trace.id(target.descriptor())} targetReceipt=${trace.id(targetReceipt)} sourceReceipt=${trace.id(sourceReceipt)} sameLog=${sourceReceipt.obligations.exists(_.binding.log() eq target.log())}"
          )
          val targetFailure = attempt(trace, "paired-target") { targetReceipt.validate(spark) }
          val sourceFailure = attempt(trace, "paired-source") {
            sourceReceipt.validateAction(spark)
          }
          assertTrue(targetFailure.isEmpty, targetFailure.toString)
          assertTrue(sourceFailure.isEmpty, sourceFailure.toString)
          trace.record(
            "paired-outcome",
            s"targetValid=${targetFailure.isEmpty} sourceValid=${sourceFailure.isEmpty}"
          )
          if (mode == "separate") {
            policy.onRequest = request => {
              if (
                request.getAccessType == "select" && request.getResource.getValue("catalog") == "b"
              ) policy.denied.add("b:secret")
            }
            targetReceipt.validate(spark)
            assertCause(
              rejected(trace, "source-select-denied") { sourceReceipt.validateAction(spark) },
              "denied"
            )
            assertTrue(
              policy.requests.asScala.exists(r =>
                r.getAccessType == "select" && r.getResource.getValue("column") == "secret"
              )
            )
          }
          assertCause(
            rejected(trace, "paired-command") {
              spark.sql("INSERT INTO a.db.t VALUES (9L, 'new', 'keep')").collect()
            },
            "Delta append inventory rejected: resolution rules"
          )
        }
      }
  }

  @Test
  def nativeIdempotencyRecordsInstalledResetReachability(): Unit = {
    Seq("duplicate-reset", "duplicate-retain", "first", "explicit-options").foreach { mode =>
      withCase(
        "reset-" + mode,
        setup = (spark, setupTrace) => {
          spark
            .sql("SELECT 4L id, 'prior' secret, 'keep' region")
            .write
            .format("delta")
            .mode("append")
            .option("txnAppId", "probe-app")
            .option("txnVersion", "1")
            .save(NativeSelectFixture.locations("a"))
        }
      ) { (spark, _, trace) =>
        spark.conf.set(
          txnApp,
          if (mode == "first") "first-app"
          else if (mode == "explicit-options") "session-app"
          else "probe-app"
        )
        spark.conf.set(txnVersion, if (mode == "explicit-options") "99" else "1")
        spark.conf.set(reset, (mode != "duplicate-retain").toString)
        val data = spark.sql("SELECT id, secret, region FROM b.db.t")
        val sourceReceipt = source(spark, data, trace)
        val target = bound(spark)
        val targetReceipt = DeltaRangerAppendTargetReceipt.capture(spark, target)
        val options =
          if (mode == "explicit-options") Map("txnAppId" -> "probe-app", "txnVersion" -> "1")
          else Map.empty[String, String]
        val failure = attempt(trace, "reset-driver") {
          inspectNativePhases(
            spark,
            target,
            targetReceipt,
            Some(sourceReceipt),
            data,
            options,
            trace,
            invokeHelper = false
          )
        }
        // Snapshot transaction-state reconstruction is the measured first blocker.
        assertTrue(trace.skip.isEmpty)
        assertTrue(failure.isDefined)
        assertEquals(Some("hasBeenExecuted"), trace.firstFailure.map(_._1))
        assertCause(failure.get, "Unqualified Delta Ranger route: Aggregate")
        assertEquals(trace.beforeSkip, trace.afterSkip)
        assertEquals(trace.beforeSkip, spark.sessionState.conf.getAllConfs)
        assertFalse(trace.completed.contains("helper"))
        targetReceipt.validate(spark)
        sourceReceipt.validateAction(spark)
        trace.record(
          "reset-outcome",
          s"mode=$mode skip=NOT_RETURNED reset=NOT_REACHED helper=NOT_REACHED oldReceiptsStillValid=true"
        )
      }
    }
  }

  @Test
  def selfSourceRetainsNativeTransactionGeneratorAndCurrentPreparationRejects(): Unit = {
    Seq("self", "different-log", "unrelated-target").foreach { mode =>
      withCase("transaction-source-" + mode) { (spark, _, trace) =>
        val catalog = if (mode == "self") "a" else "b"
        spark.sql(s"SELECT id, secret, region FROM $catalog.db.t").collect()
        val data = spark.sql(s"SELECT id, secret, region FROM $catalog.db.t")
        val sourceReceipt = source(spark, data, trace)
        val target = bound(spark, if (mode == "unrelated-target") "q" else "a")
        val targetReceipt = DeltaRangerAppendTargetReceipt.capture(spark, target)
        val failure = attempt(trace, "preparation-driver") {
          inspectNativePhases(
            spark,
            target,
            targetReceipt,
            Some(sourceReceipt),
            data,
            Map.empty,
            trace,
            prepareSource = true,
            invokeHelper = false
          )
        }
        trace.record(
          "preparation-outcome",
          s"mode=$mode failure=$failure generatorMatches=${trace.generatorMatches}"
        )
        assertEquals(Vector(mode == "self"), trace.generatorMatches)
        if (mode == "self") {
          assertTrue(failure.isDefined)
          assertEquals(Some("source-optimized"), trace.firstFailure.map(_._1))
          assertCause(failure.get, "Native transactional scan preparation is not qualified")
        } else {
          assertTrue(failure.isEmpty, failure.toString)
          assertTrue(trace.completed.contains("source-optimized"))
        }
      }
    }
  }

  @Test
  def nativeHelperKeepsCompanionChecksAndNeverCommits(): Unit = {
    Seq("separate", "self", "empty", "constraint", "literal").foreach { mode =>
      withCase(
        "helper-" + mode,
        durableReadback = true,
        setup = (spark, setupTrace) => {
          if (mode == "constraint")
            spark.sql(
              s"ALTER TABLE delta.`${NativeSelectFixture.locations("a")}` ADD CONSTRAINT positive CHECK (id > 0)"
            )
        }
      ) { (spark, _, trace) =>
        if (mode == "literal") {
          val target = bound(spark)
          val targetReceipt = DeltaRangerAppendTargetReceipt.capture(spark, target)
          trace.record("input-kind", "literal driver-level construction; no source authority")
          assertCause(
            rejected(trace, "literal-construction") {
              spark.sql("SELECT -1L id, 'literal' secret, 'keep' region")
            },
            "Unqualified Delta Ranger route: OneRowRelation"
          )
          targetReceipt.validate(spark)
          trace.record(
            "helper-outcome",
            "mode=literal lastCompleted=target-capture firstFailure=literal-construction transaction=NOT_REACHED idempotency=NOT_REACHED helper=NOT_REACHED commit=NOT_REACHED"
          )
        } else {
          val catalog = if (mode == "self") "a" else "b"
          val sql =
            s"SELECT id, secret, region FROM $catalog.db.t" + (if (mode == "empty") " WHERE 1 = 0"
                                                               else "")
          val data = spark.sql(sql)
          val sourceReceipt = source(spark, data, trace)
          trace.record("input-kind", "named-source")
          val target = bound(spark)
          val targetReceipt = DeltaRangerAppendTargetReceipt.capture(spark, target)
          val failure = attempt(trace, "helper-driver") {
            inspectNativePhases(
              spark,
              target,
              targetReceipt,
              Some(sourceReceipt),
              data,
              Map.empty,
              trace
            )
          }
          assertTrue(
            failure.isDefined,
            "Unexpected native helper success requires separate qualification"
          )
          assertEquals(Some("helper"), trace.firstFailure.map(_._1))
          assertCause(failure.get, "Derived or reused Delta plans are not qualified")
          assertFalse(trace.completed.contains("helper"))
          assertEquals(Some(false), trace.skip)
          assertEquals(trace.beforeSkip, trace.afterSkip)
          targetReceipt.validate(spark)
          sourceReceipt.validateAction(spark)
          trace.record(
            "helper-outcome",
            s"mode=$mode firstFailure=helper setTransactionGeneration=NOT_REACHED resetAtHelper=NOT_REACHED commit=NOT_REACHED"
          )
        }
      }
    }
  }

  @Test
  def checksumBearingAndAbsentHistoriesKeepDistinctInstalledOutcomes(): Unit = {
    Seq(true, false).foreach { checksum =>
      var staged = Map.empty[Path, Array[Byte]]
      withCase(
        "history-checksum-" + checksum,
        setup = (spark, setupTrace) => {
          val location = NativeSelectFixture.locations("a")
          spark
            .sql("SELECT 4L id, 'tail' secret, 'keep' region")
            .write
            .format("delta")
            .mode("append")
            .save(location)
          assertEquals(1L, DeltaLog.forTable(spark, location).update().version)
          val paths = Files.list(Paths.get(location, "_delta_log"))
          try
            staged = paths
              .iterator()
              .asScala
              .filter(_.getFileName.toString.contains("00000000000000000001."))
              .map(p => p -> Files.readAllBytes(p))
              .toMap
          finally paths.close()
          assertTrue(staged.keys.exists(_.getFileName.toString == "00000000000000000001.crc"))
          val original = setupTrace.directory.resolve("original-version-1")
          Files.createDirectory(original)
          staged.foreach { case (path, bytes) =>
            Files.write(original.resolve(path.getFileName), bytes)
          }
          staged.keys.foreach(Files.delete)
          DeltaLog.clearCache()
        },
        unchanged = false
      ) { (spark, _, trace) =>
        val target = bound(spark)
        val receipt = DeltaRangerAppendTargetReceipt.capture(spark, target)
        staged.foreach { case (path, bytes) =>
          val checksumFile =
            path.getFileName.toString == "00000000000000000001.crc" || path.getFileName.toString == ".00000000000000000001.crc.crc"
          if (checksum || !checksumFile) Files.write(path, bytes)
        }
        val published = manifest(
          Paths.get(NativeSelectFixture.locations("a")),
          trace.directory.resolve("published")
        )
        val currentFailure = attempt(trace, "validate-current") { target.validateCurrent(spark) }
        val receiptFailure = attempt(trace, "validate-receipt") { receipt.validate(spark) }
        if (checksum) {
          assertTrue(currentFailure.isEmpty, currentFailure.toString)
          assertTrue(receiptFailure.isEmpty, receiptFailure.toString)
          assertEquals(1L, target.log().unsafeVolatileSnapshot.version)
        } else {
          assertTrue(currentFailure.isDefined)
          assertTrue(receiptFailure.isDefined)
          assertCause(currentFailure.get, "Native Delta registration binding is unavailable")
          assertCause(receiptFailure.get, "Native Delta registration binding is unavailable")
          assertEquals(0L, target.log().unsafeVolatileSnapshot.version)
          assertTrue(trace.events.exists(_.contains("DeserializeToObject")))
        }
        trace.record(
          "history-outcome",
          s"checksum=$checksum currentFailure=$currentFailure receiptFailure=$receiptFailure lastNativeVersion=${target.log().unsafeVolatileSnapshot.version}; outer validation may redact inner native cause"
        )
        assertEquals(
          published,
          manifest(
            Paths.get(NativeSelectFixture.locations("a")),
            trace.directory.resolve("validated")
          )
        )
      }
    }
  }

  private def withCase(
      name: String,
      observed: Boolean = true,
      setup: (SparkSession, Trace) => Unit = (_, _) => (),
      durableReadback: Boolean = false,
      unchanged: Boolean = true
  )(body: (SparkSession, TestDeltaRangerNativeSelect.Fixture, Trace) => Unit): Unit = {
    val trace = fresh(name)
    val policy = new TestDeltaRangerNativeSelect.Fixture
    val install = NativeSelectFixture.extension(policy)
    val extension: SparkSessionExtensions => Unit = extensions => {
      if (observed) observer(trace)(extensions)
      install(extensions)
    }
    var location = ""
    var durableRows = Set.empty[Seq[Any]]
    var nativeConf: SparkConf = null
    try {
      NativeSelectFixture.withSpark(
        spark => {
          trace.activate(spark)
          location = NativeSelectFixture.locations("a")
          nativeConf = spark.sparkContext.getConf
          val before = manifest(Paths.get(location).getParent, trace.directory.resolve("before"))
          val publication = policy.engine.snapshotStamp()
          trace.record(
            "publication-before",
            s"stamp=${trace.id(publication)} policy=${policy.engine.versions().policy()} role=${policy.engine.versions().role()}"
          )
          try body(spark, policy, trace)
          finally {
            trace.requests(policy, "policy-final")
            policy.engine.requireSnapshot(publication)
            trace.record(
              "publication-after",
              s"samePublication=true policy=${policy.engine.versions().policy()} role=${policy.engine.versions().role()}"
            )
            trace.observe(spark, "case-unwound", None)
            val after = manifest(Paths.get(location).getParent, trace.directory.resolve("after"))
            trace.record(
              "storage-delta",
              s"added=${after.keySet -- before.keySet} removed=${before.keySet -- after.keySet} changed=${before.keySet.intersect(after.keySet).filter(k => before(k) != after(k))}"
            )
            if (unchanged) assertEquals(before, after, "Owned target storage changed")
            assertNull(DeltaRangerNativeOperation.current())
            assertTrue(OptimisticTransaction.getActive.isEmpty)
          }
        },
        Some(extension),
        beforeExtension = spark => {
          setup(spark, trace)
          if (durableReadback)
            durableRows = spark.read
              .format("delta")
              .load(NativeSelectFixture.locations("a"))
              .collect()
              .map(_.toSeq)
              .toSet
        }
      )
      if (durableReadback) {
        DeltaLog.clearCache()
        val native = SparkSession
          .builder()
          .config(nativeConf)
          .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
          .getOrCreate()
        try {
          val rows = native.read.format("delta").load(location).collect().map(_.toSeq).toSet
          trace.record("separate-native-readback", s"rows=$rows; installed attempt already closed")
          assertEquals(durableRows, rows)
        } finally {
          native.stop(); SparkSession.clearActiveSession(); SparkSession.clearDefaultSession()
        }
      }
    } finally { policy.engine.close(); trace.close() }
  }
  private def source(
      spark: SparkSession,
      data: DataFrame,
      trace: Trace
  ): DeltaRangerDecisionReceipt = {
    val analyzed = data.queryExecution.analyzed
    trace.observe(spark, "source-analyzed", Some(analyzed))
    val receipt = DeltaRangerDecisionReceipt.consumeOwned(analyzed, spark, optimized = false)
    trace.source(receipt)
    receipt
  }
  private def bound(
      spark: SparkSession,
      catalog: String = "a"
  ): DeltaRangerRegistrationBindings.BoundAppendTarget = {
    val wrapper = NativeSelectFixture.wrapper(spark, catalog)
    val table = wrapper.loadTable(Identifier.of(Array("db"), "t")).asInstanceOf[DeltaTableV2]
    wrapper.registrationBindings().resolveAppendTarget(table)
  }
  private def rejected(trace: Trace, phase: String)(body: => Any): Throwable = {
    val error = assertThrows(classOf[Exception], () => { body; () })
    trace.failed(phase, error)
    error
  }
  private def attempt(trace: Trace, phase: String)(body: => Any): Option[Throwable] =
    try { body; trace.record("returned", phase); None }
    catch { case error: Exception => trace.failed(phase, error); Some(error) }
  private def causes(error: Throwable): Seq[Throwable] =
    Iterator.iterate(error)(_.getCause).takeWhile(_ != null).toVector
  private def assertCause(error: Throwable, message: String): Unit =
    assertAnyCause(error, Seq(message))
  private def assertAnyCause(error: Throwable, messages: Seq[String]): Unit =
    assertTrue(
      causes(error).exists(e =>
        e.isInstanceOf[SecurityException] && messages.exists(m =>
          Option(e.getMessage).exists(_.contains(m))
        )
      ),
      causes(error).mkString("\n")
    )
  private def inventory(policy: TestDeltaRangerNativeSelect.Fixture): Vector[String] =
    policy.requests.asScala
      .map(r => s"${r.getAccessType}:${r.getResource.getAsMap}:${r.getUser}:${r.getUserGroups}")
      .toVector
      .sorted
  private def ruleManifest(spark: SparkSession): Map[String, Vector[String]] = Map(
    "extensions" -> spark.conf.get("spark.sql.extensions").split(",").toVector,
    "resolution" -> spark.sessionState.analyzer.extendedResolutionRules
      .map(_.getClass.getName)
      .toVector,
    "optimizer" -> spark.sessionState.optimizer.batches
      .flatMap(_.rules.map(_.getClass.getName))
      .toVector,
    "strategy" -> spark.sessionState.planner.strategies.map(_.getClass.getName).toVector,
    "columnar" -> spark.sessionState.columnarRules.map(_.getClass.getName).toVector
  )
}

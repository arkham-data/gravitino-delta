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

package org.apache.gravitino.spark.connector.delta

import java.util.{ArrayList, List => JList, Map => JMap, Collections}
import java.nio.charset.StandardCharsets
import java.nio.file.{FileVisitResult, Files, Path, Paths, SimpleFileVisitor, StandardCopyOption, StandardOpenOption}
import java.nio.file.attribute.{BasicFileAttributes, PosixFilePermissions}
import java.util.{UUID, TreeMap}
import java.io.{StringWriter, PrintWriter}
import java.security.MessageDigest
import java.util.HexFormat

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.write.V1Write
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.delta.{ColumnWithDefaultExprUtils, DeltaLog, DeltaOperations, DeltaOptions, OptimisticTransaction}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.{DeltaCommand, WriteIntoDelta}
import org.apache.spark.sql.delta.hooks.AutoCompact
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExecV1, DataSourceV2Relation}
import org.apache.spark.sql.sources.InsertableRelation

/** Installs a test-only, inert-without-scope observer ahead of Delta's extension. */
final class NativeAppendProbeExtension extends (SparkSessionExtensions => Unit) {
  /** Registers the bounded logical and physical observers. */
  override def apply(extensions: SparkSessionExtensions): Unit = {
    TestDeltaNativeAdapter.appendProbeFactory =
      (spark: SparkSession, target: String, mode: String, path: Path) =>
        NativeAppendProbe.begin(spark, target, mode, path)
    extensions.injectCheckRule { spark => plan => NativeAppendProbe.observeLogical(spark, plan) }
    extensions.injectColumnar { spark => new ColumnarRule {
      override def postColumnarTransitions: Rule[SparkPlan] = new Rule[SparkPlan] {
        override def apply(plan: SparkPlan): SparkPlan = NativeAppendProbe.decorate(spark, plan)
      }
    }}
  }
}

/** Scoped observation of the real native V1 append route. */
object NativeAppendProbe {
  private val active = new ThreadLocal[Capture]()

  private final class Report extends TestDeltaNativeAdapter.AppendProbeReport {
    val eventValues = new ArrayList[String]()
    val planValues = new ArrayList[String]()
    var entries = 0
    var helpers = 0
    var commits = 0
    var skips = 0
    var txnActiveAfter = false
    var bound = false
    val optionValues = new TreeMap[String, String]()
    override def options(): JMap[String, String] = Collections.unmodifiableMap(optionValues)
    val actionValues = new ArrayList[String]()
    var metadata = ""
    var protocol = ""
    override def actions(): JList[String] = actionValues
    override def pendingMetadata(): String = metadata
    override def pendingProtocol(): String = protocol
    override def inputBound(): Boolean = bound
    override def events(): JList[String] = eventValues
    override def plans(): JList[String] = planValues
    override def entryCalls(): Int = entries
    override def helperCalls(): Int = helpers
    override def commitCalls(): Int = commits
    override def idempotentSkips(): Int = skips
    override def activeTransactionAfter(): Boolean = txnActiveAfter
  }

  private val configKeys = Seq(
    "spark.databricks.delta.write.txnAppId",
    "spark.databricks.delta.write.txnVersion",
    "spark.databricks.delta.write.txnVersion.autoReset.enabled",
    "spark.databricks.delta.schema.autoMerge.enabled",
    "spark.databricks.delta.autoCompact.enabled",
    "spark.sql.sources.partitionOverwriteMode",
    "spark.sql.parquet.compression.codec",
    "spark.sql.files.maxRecordsPerFile",
    "spark.sql.storeAssignmentPolicy",
    "spark.sql.ansi.enabled")

  private def configuration(spark: SparkSession): Map[String, Option[String]] =
    configKeys.map(key => key -> spark.conf.getOption(key)).toMap +
      ("nullAsDefault" -> Some(spark.sessionState.conf.useNullsForMissingDefaultColumnValues.toString))

  private def verifyNativeTransition(before: Map[String, Option[String]],
      after: Map[String, Option[String]]): Unit = {
    val version = "spark.databricks.delta.write.txnVersion"
    val reset = "spark.databricks.delta.write.txnVersion.autoReset.enabled"
    require((before - version) == (after - version), "native append changed unrelated SQLConf")
    require(before(version) == after(version) ||
      (before(reset).contains("true") && before(version).nonEmpty && after(version).isEmpty),
      "unexpected native transaction-version transition")
  }

  private case class Bound(command: AppendData, input: LogicalPlan,
      config: Map[String, Option[String]])

  private final class Capture(val spark: SparkSession, val target: String, val mode: String,
      val targetPath: Path) extends TestDeltaNativeAdapter.AppendProbeCapture {
    val observations = new Report
    var logical: Option[Bound] = None
    val initialManifest = manifest(targetPath)
    val log = DeltaLog.forTable(spark, targetPath.toString)
    val baseline = log.update()
    val baselineId = baseline.metadata.id
    val baselineConfiguration = baseline.metadata.configuration
    val baselineMetadata = baseline.metadata.json
    val baselineProtocol = baseline.protocol.json
    override def report(): TestDeltaNativeAdapter.AppendProbeReport = observations
    override def failure(error: Throwable): Unit = {
      val text = new StringWriter()
      val writer = new PrintWriter(text)
      error.printStackTrace(writer)
      writer.flush()
      observations.eventValues.add("recorded failure\n" + text.toString)
    }
    override def verified(assertion: String): Unit =
      observations.eventValues.add("verified: " + assertion)
    override def close(): Unit = {
      require(active.get() eq this, "append probe scope mismatch")
      try {
        val after = log.update()
        require(after.metadata.id == baselineId, "append changed native table identity")
        require(after.metadata.configuration == baselineConfiguration,
          "append changed table configuration/registration marker")
        if (mode == "DENY_ENTRY" || mode == "DENY_PRECOMMIT") {
          require(after.version == baseline.version && after.metadata == baseline.metadata &&
            after.protocol == baseline.protocol, "denial published Delta state")
        }
        require(!OptimisticTransaction.getActive.nonEmpty, "append leaked active transaction")
        verified("own native ID, configuration, location and registration marker retained; transaction clear")
      } finally {
        active.remove()
        require(active.get() == null, "append probe scope was not cleared")
        verified("probe scope cleared")
        preserve(this)
      }
    }
  }

  /** Binds one owned target before analysis, including analysis-failure evidence. */
  def begin(spark: SparkSession, targetTable: String, mode: String, targetPath: Path)
      : TestDeltaNativeAdapter.AppendProbeCapture = {
    require(active.get() == null, "nested append probe")
    require(Set("NATIVE", "SPLIT", "DENY_ENTRY", "DENY_PRECOMMIT").contains(mode),
      s"unsupported append probe mode $mode")
    val path = targetPath.toRealPath()
    require(path.startsWith(TestDeltaNativeAdapter.directory.toRealPath()) &&
      !Files.isSymbolicLink(targetPath), "probe target must be owned JUnit storage")
    val capture = new Capture(spark, targetTable, mode, path)
    active.set(capture)
    capture
  }

  private def owner(spark: SparkSession): Capture = {
    val capture = active.get()
    capture
  }

  private[delta] def observeLogical(spark: SparkSession, plan: LogicalPlan): Unit = {
    val capture = owner(spark)
    if (capture != null) {
      if (!(capture.spark eq spark)) {
        require(!plan.exists(_.isInstanceOf[AppendData]), "append probe used by another session")
        capture.observations.planValues.add(s"native nested non-append session=${System.identityHashCode(spark)} plan=${plan.treeString}")
        return
      }
      plan.foreach {
        case append: AppendData if matchesLogical(append, capture) =>
          val input = append.analyzedQuery.getOrElse(
            throw new IllegalStateException("resolved append has no analyzed fallback input"))
          val candidate = Bound(append, input, configuration(spark))
          capture.logical.foreach { previous =>
            require((previous.command.table.asInstanceOf[DataSourceV2Relation].table eq
              append.table.asInstanceOf[DataSourceV2Relation].table) &&
              previous.input.fastEquals(input) && previous.command.writeOptions == append.writeOptions &&
              previous.config == candidate.config, "ambiguous append observation")
          }
          capture.logical = Some(candidate)
          capture.observations.planValues.add(
            s"logical query=${append.query.treeString} analyzed=${append.analyzedQuery.map(_.treeString)} " +
              s"options=${append.writeOptions} byName=${append.isByName} " +
              s"nullAsDefault=Some(${spark.sessionState.conf.useNullsForMissingDefaultColumnValues}) " +
              s"config=${candidate.config}")
        case append: AppendData =>
          capture.observations.planValues.add(
            s"observed AppendData target=${append.table.name} targetClass=${append.table.getClass.getName}")
        case _ =>
      }
    }
  }

  private[delta] def decorate(spark: SparkSession, plan: SparkPlan): SparkPlan = {
    val capture = owner(spark)
    if (capture == null) return plan
    if (!(capture.spark eq spark)) {
      require(!plan.exists(_.isInstanceOf[AppendDataExecV1]), "append probe used by another session")
      return plan
    }
    capture.observations.planValues.add(s"postColumnar root=${plan.getClass.getName}")
    plan.transformUp {
      case exec: AppendDataExecV1 if matches(exec.table, capture) &&
          !exec.write.isInstanceOf[ObservedWrite] =>
        val table = exec.table.asInstanceOf[DeltaTableV2]
        require(Paths.get(table.deltaLog.dataPath.toUri).toRealPath() == capture.targetPath,
          "physical target differs from owned baseline")
        val bound = capture.logical.getOrElse(
          throw new IllegalStateException("missing resolved AppendData before physical append"))
        require(bound.command.table.asInstanceOf[DataSourceV2Relation].table eq exec.table,
          "resolved and physical Delta targets differ")
        require(bound.input.fastEquals(exec.plan), "physical fallback is not bound analyzedQuery")
        capture.observations.planValues.add(
          s"bound descriptor=${table.catalogTable.get.identifier} object=${System.identityHashCode(table)} " +
            s"fallbackIdentity=${bound.input eq exec.plan} exprIds=${exec.plan.output.map(_.exprId)}")
        capture.observations.planValues.add(
          s"physical=${exec.getClass.getName} plan=${exec.plan.treeString} " +
            s"writer=${exec.write.getClass.getName}")
        exec.copy(exec.table, exec.plan, exec.refreshCache,
          new ObservedWrite(exec.write, table, capture, bound))
      case exec: AppendDataExecV1 =>
        capture.observations.planValues.add(
          s"observed AppendDataExecV1 target=${exec.table.name()} writer=${exec.write.getClass.getName}")
        exec
    }
  }

  private def matchesLogical(append: AppendData, capture: Capture): Boolean =
    append.table match {
      case relation: DataSourceV2Relation => matches(relation.table, capture)
      case _ => false
    }

  private def matches(table: Table, capture: Capture): Boolean =
    table match {
      case delta: DeltaTableV2 =>
        delta.catalogTable.exists { metadata =>
          val id = metadata.identifier
          id.catalog.contains("canonical") && id.database.contains("db") &&
            id.table == capture.target
        }
      case _ => false
    }

  private final class ObservedWrite(original: V1Write, table: DeltaTableV2, capture: Capture, bound: Bound)
      extends V1Write {
    override def toInsertableRelation: InsertableRelation = {
      val native = original.toInsertableRelation
      new InsertableRelation {
        override def insert(data: Dataset[Row], overwrite: Boolean): Unit = {
          capture.observations.entries += 1
          capture.observations.eventValues.add(
            s"insert analyzed=${data.queryExecution.analyzed.treeString} " +
              s"schema=${data.schema.json} overwrite=$overwrite")
          require(active.get() eq capture, "insertion outlived its probe scope")
          require(data.sparkSession eq capture.spark, "insertion changed Spark session")
          require(!overwrite, "append probe received overwrite")
          val received = data.queryExecution.analyzed
          capture.observations.eventValues.add(s"receivedIdentity=${received eq bound.input} receivedFastEquals=${received.fastEquals(bound.input)}")
          require(received.fastEquals(bound.input) && received.schema == bound.input.schema &&
            received.output.map(_.exprId) == bound.input.output.map(_.exprId),
            "received DataFrame differs from bound analyzedQuery/schema/ExprIds")
          require(configuration(data.sparkSession) == bound.config,
            "external SQLConf drift before append entry")
          capture.observations.bound = true
          bound.command.writeOptions.foreach { case (key, value) =>
            capture.observations.optionValues.put(key, value)
          }
          capture.observations.eventValues.add("entry configuration=" + configuration(data.sparkSession))
          try {
            if (capture.mode == "NATIVE") {
              native.insert(data, overwrite)
            } else {
              appendSplit(data.sparkSession, table, data, bound.command.writeOptions,
                capture.mode == "DENY_ENTRY", capture.mode == "DENY_PRECOMMIT",
                event => capture.observations.eventValues.add(event), capture.observations)
              data.sparkSession.sharedState.cacheManager.recacheByPlan(
                data.sparkSession, table.toLogicalRelation)
            }
          } catch {
            case error: Throwable => capture.failure(error); throw error
          } finally {
            capture.observations.eventValues.add("exit configuration=" + configuration(data.sparkSession))
            verifyNativeTransition(bound.config, configuration(data.sparkSession))
            capture.observations.txnActiveAfter = OptimisticTransaction.getActive.nonEmpty
          }
        }
      }
    }
  }

  private object SplitCoordinator extends DeltaCommand {
    def appendSplit(
        spark: SparkSession,
        table: DeltaTableV2,
        data: DataFrame,
        options: Map[String, String],
        denyEntry: Boolean,
        denyPrecommit: Boolean,
        record: String => Unit,
        report: Report): Unit = {
      record("split entry")
      if (denyEntry) throw new SecurityException("NATIVE_APPEND_DENY_ENTRY")
      val effectiveOptions =
        if (spark.sessionState.conf.useNullsForMissingDefaultColumnValues) {
          options + (ColumnWithDefaultExprUtils.USE_NULL_AS_DEFAULT_DELTA_OPTION -> "true")
        } else options
      val lowered = effectiveOptions.map { case (key, value) => key.toLowerCase -> value }
      require(!lowered.contains("replacewhere") && lowered.get("datachange").forall(_.toBoolean),
        "unqualified append mutation options")
      val autoCompact = AutoCompact.getAutoCompactType(spark.sessionState.conf,
        table.deltaLog.unsafeVolatileSnapshot.metadata)
      require(autoCompact.isEmpty, "unqualified auto-compaction")
      record(s"effective options=$effectiveOptions autoCompact=$autoCompact")
      val deltaOptions = new DeltaOptions(effectiveOptions, spark.sessionState.conf)
      val native = WriteIntoDelta(table.deltaLog, SaveMode.Append, deltaOptions, Nil,
        table.deltaLog.unsafeVolatileSnapshot.metadata.configuration, data, table.catalogTable)
      table.deltaLog.withNewTransaction(table.catalogTable) { txn =>
        require(txn.readVersion >= 0, "split append requires an existing table snapshot")
        record(s"transaction readVersion=${txn.readVersion} metadata=${txn.metadata.json} " +
          s"protocol=${txn.protocol.json}")
        record("before hasBeenExecuted configuration=" + configuration(spark))
        val beforeSkip = configuration(spark)
        val executed = try hasBeenExecuted(txn, spark, Some(native.options))
          finally {
            record("after hasBeenExecuted configuration=" + configuration(spark))
            verifyNativeTransition(beforeSkip, configuration(spark))
          }
        if (executed) {
          report.skips += 1
          record("native idempotent skip")
        } else {
          report.helpers += 1
          record("before helper configuration=" + configuration(spark))
          val beforeHelper = configuration(spark)
          val tagged = try native.writeAndReturnCommitData(txn, spark)
            finally {
              record("after helper configuration=" + configuration(spark))
              verifyNativeTransition(beforeHelper, configuration(spark))
            }
          tagged.actions.foreach(action => report.actionValues.add(action.json))
          report.metadata = txn.metadata.json
          report.protocol = txn.protocol.json
          record(s"helper actions=${tagged.actions.map(_.json)} " +
            s"tags=${tagged.stringTags} pendingMetadata=${txn.metadata.json} " +
            s"protocol=${txn.protocol.json}")
          if (denyPrecommit) throw new SecurityException("NATIVE_APPEND_DENY_PRECOMMIT")
          val operation = DeltaOperations.Write(SaveMode.Append, Some(Nil),
            native.options.replaceWhere, native.options.userMetadata)
          report.commits += 1
          txn.commitIfNeeded(tagged.actions, operation, tags = tagged.stringTags)
          record("native commit returned")
        }
      }
    }
  }

  private def appendSplit(
      spark: SparkSession,
      table: DeltaTableV2,
      data: DataFrame,
      options: Map[String, String],
      denyEntry: Boolean,
      denyPrecommit: Boolean,
      record: String => Unit,
      report: Report): Unit =
    SplitCoordinator.appendSplit(spark, table, data, options, denyEntry, denyPrecommit, record, report)

  private def preserve(capture: Capture): Unit = {
    val setting = sys.env.get("GRAVITINO_DELTA_APPEND_EVIDENCE_DIR")
    if (setting.isEmpty) return
    val evidenceRoot = Paths.get(setting.get)
    require(evidenceRoot.isAbsolute && Files.isDirectory(evidenceRoot) &&
      !Files.isSymbolicLink(evidenceRoot), "append evidence root must be an existing absolute directory")
    val source = capture.targetPath.toRealPath()
    val owned = TestDeltaNativeAdapter.directory.toRealPath()
    require(source.startsWith(owned), s"append target is outside JUnit storage: $source")
    val destination = Files.createDirectory(
      evidenceRoot.resolve(capture.target + "-" + UUID.randomUUID().toString),
      PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")))
    Files.write(destination.resolve("plans.txt"),
      capture.observations.planValues.toArray.mkString("\n").getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE_NEW)
    Files.write(destination.resolve("events.txt"),
      capture.observations.eventValues.toArray.mkString("\n").getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE_NEW)
    Files.write(destination.resolve("manifest-before.txt"),
      capture.initialManifest.getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE_NEW)
    Files.write(destination.resolve("manifest-after.txt"),
      manifest(source).getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE_NEW)
    val result = s"mode=${capture.mode} entries=${capture.observations.entries} " +
      s"helpers=${capture.observations.helpers} commits=${capture.observations.commits} " +
      s"skips=${capture.observations.skips} inputBound=${capture.observations.bound} " +
      s"transactionActiveAfter=${capture.observations.txnActiveAfter} scopeCleared=${active.get() == null}\n" +
      s"baselineMetadata=${capture.baselineMetadata}\nbaselineProtocol=${capture.baselineProtocol}\n" +
      s"pendingMetadata=${capture.observations.metadata}\npendingProtocol=${capture.observations.protocol}\n" +
      s"helperActions=${capture.observations.actionValues}\n"
    Files.write(destination.resolve("result.txt"), result.getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE_NEW)
    val beforeKeys = capture.initialManifest.linesIterator.map(_.takeWhile(_ != ' ')).toSet
    val classifications = manifest(source).linesIterator.map { line =>
      val path = line.takeWhile(_ != ' ')
      val kind = if (path.endsWith(".json")) "delta-json"
        else if (path.contains(".checkpoint.")) "checkpoint"
        else if (path.endsWith(".parquet")) "data-parquet"
        else if (path.endsWith(".crc")) "checksum"
        else "other"
      s"$path $kind ${if (beforeKeys(path)) "preexisting" else "new"}"
    }.mkString("\n")
    Files.write(destination.resolve("artifact-classifications.txt"),
      classifications.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE_NEW)
    val runtimeClasses = Seq(classOf[SparkSession], classOf[DeltaTableV2],
      classOf[AppendDataExecV1], classOf[WriteIntoDelta], classOf[scala.Option[_]])
    val runtime = runtimeClasses.map { runtimeClass =>
      s"${runtimeClass.getName} ${runtimeClass.getProtectionDomain.getCodeSource.getLocation}"
    }.mkString("\n")
    Files.write(destination.resolve("runtime-origins.txt"), runtime.getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE_NEW)
    Files.walkFileTree(source, new SimpleFileVisitor[Path] {
      override def preVisitDirectory(path: Path, attributes: BasicFileAttributes): FileVisitResult = {
        require(!Files.isSymbolicLink(path), s"symlink in owned table: $path")
        Files.createDirectories(destination.resolve("table").resolve(source.relativize(path)))
        FileVisitResult.CONTINUE
      }
      override def visitFile(path: Path, attributes: BasicFileAttributes): FileVisitResult = {
        require(attributes.isRegularFile && !Files.isSymbolicLink(path),
          s"unexpected file in owned table: $path")
        Files.copy(path, destination.resolve("table").resolve(source.relativize(path)),
          StandardCopyOption.COPY_ATTRIBUTES)
        FileVisitResult.CONTINUE
      }
    })
  }

  private def manifest(root: Path): String = {
    val source = root.toRealPath()
    val entries = new TreeMap[String, String]()
    Files.walkFileTree(source, new SimpleFileVisitor[Path] {
      override def visitFile(path: Path, attributes: BasicFileAttributes): FileVisitResult = {
        require(attributes.isRegularFile && !Files.isSymbolicLink(path),
          s"unexpected file in owned table: $path")
        val bytes = Files.readAllBytes(path)
        val sha = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes))
        entries.put(source.relativize(path).toString, s"${bytes.length}:$sha")
        FileVisitResult.CONTINUE
      }
    })
    val lines = new StringBuilder
    val iterator = entries.entrySet().iterator()
    while (iterator.hasNext) {
      val entry = iterator.next()
      lines.append(entry.getKey).append(' ').append(entry.getValue).append('\n')
    }
    lines.toString
  }
}

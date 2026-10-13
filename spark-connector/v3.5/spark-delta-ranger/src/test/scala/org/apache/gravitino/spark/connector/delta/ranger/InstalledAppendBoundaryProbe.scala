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
import java.nio.file.attribute.PosixFilePermissions
import java.io.{PrintWriter, StringWriter}
import java.security.MessageDigest
import java.time.Instant
import java.util.{HexFormat, IdentityHashMap, UUID}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal
import org.apache.ranger.plugin.service.RangerBasePlugin
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.{ColumnWithDefaultExprUtils, DeltaOptions, OptimisticTransaction}
import org.apache.spark.sql.delta.commands.{DeltaCommand, WriteIntoDelta}
import org.apache.spark.sql.delta.hooks.AutoCompact
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.delta.stats.{DeltaScanGenerator, PrepareDeltaScan}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/** Test-only observations carry no execution authority. */
private[ranger] object InstalledAppendBoundaryProbe {
  final class Trace(val directory: Path) extends AutoCloseable {
    private val entries = ArrayBuffer.empty[String]
    private val identities = new IdentityHashMap[AnyRef, java.lang.Integer]()
    private var owner: Option[SparkSession] = None
    private var closed = false
    private var sequence = 0
    val observedAppends = ArrayBuffer.empty[AppendData]
    val completed = ArrayBuffer.empty[String]
    private val started = ArrayBuffer.empty[String]
    var firstFailure: Option[(String, Throwable)] = None
    var skip: Option[Boolean] = None
    var beforeSkip = Map.empty[String, String]
    var afterSkip = Map.empty[String, String]
    var generatorMatches = Vector.empty[Boolean]

    def events: Vector[String] = synchronized { entries.toVector }
    def activate(spark: SparkSession): Unit = synchronized {
      require(!closed && owner.isEmpty)
      owner = Some(spark)
      record("activate", s"session=${id(spark)}")
      val subject = DeltaRangerQuerySubject.current(spark)
      record("subject", s"user=${subject.user()} groups=${subject.groups()}")
      Seq(
        classOf[SparkSession],
        classOf[WriteIntoDelta],
        classOf[RangerBasePlugin],
        classOf[scala.Option[_]]
      ).foreach { clazz =>
        record(
          "runtime-origin",
          s"${clazz.getName} ${clazz.getProtectionDomain.getCodeSource.getLocation}"
        )
      }
    }
    def id(value: AnyRef): String = synchronized {
      if (value == null) "none"
      else {
        if (!identities.containsKey(value)) identities.put(value, identities.size() + 1)
        s"${value.getClass.getName}@${identities.get(value)}"
      }
    }
    def record(phase: String, value: String): Unit = synchronized {
      require(!closed, "trace already closed")
      sequence += 1
      entries += s"event=$sequence time=${Instant.now()} thread=${Thread.currentThread().getName} phase=$phase $value"
    }
    def observe(spark: SparkSession, phase: String, plan: Option[LogicalPlan]): Unit =
      synchronized {
        if (closed || owner.isEmpty) return
        try {
          val txn = OptimisticTransaction.getActive
          record(
            phase,
            s"session=${id(spark)} owner=${owner.exists(_ eq spark)} native=${id(DeltaRangerNativeOperation.current())} activeTxn=${id(txn.orNull)}"
          )
          record(phase + "-conf", spark.sessionState.conf.getAllConfs.toSeq.sorted.mkString("\n"))
          txn.foreach(t =>
            record(
              phase + "-txn",
              s"txn=${id(t)} log=${id(t.deltaLog)} readVersion=${t.readVersion} metadata=${t.metadata.json} protocol=${t.protocol.json}"
            )
          )
          plan.foreach { root =>
            record(phase + "-plan", describe(root))
            root.foreach {
              case append: AppendData =>
                observedAppends += append
                val table = append.table match {
                  case relation: DataSourceV2Relation => id(relation.table)
                  case other                          => id(other)
                }
                record(
                  phase + "-append",
                  s"command=${id(append)} target=$table byName=${append.isByName} options=${append.writeOptions} query=${describe(append.query)} analyzedQuery=${append.analyzedQuery.map(describe).getOrElse("ABSENT")} physicalV1=UNOBSERVED_BY_RESOLUTION"
                )
              case _ => ()
            }
          }
        } catch {
          case NonFatal(error) => throw new AssertionError("Trace instrumentation failed", error)
        }
      }
    def failed(phase: String, error: Throwable): Unit = synchronized {
      val stack = new StringWriter
      error.printStackTrace(new PrintWriter(stack))
      if (firstFailure.isEmpty) firstFailure = Some(phase -> error)
      record("failure " + phase, stack.toString)
    }
    def phase[T](spark: SparkSession, name: String)(body: => T): T = {
      observe(spark, "before-" + name, None)
      started += name
      try {
        val result = body
        completed += name
        record("returned", name)
        result
      } catch {
        case error: Throwable => failed(name, error); throw error
      } finally observe(spark, "after-" + name, None)
    }
    def requests(policy: TestDeltaRangerNativeSelect.Fixture, phase: String): Unit = {
      policy.requests.asScala.zipWithIndex.foreach { case (request, index) =>
        record(
          phase,
          s"request=$index type=${request.getAccessType} resource=${request.getResource.getAsMap} user=${request.getUser} groups=${request.getUserGroups} time=${request.getAccessTime}"
        )
      }
      record(phase + "-events", policy.events.asScala.mkString("\n"))
    }
    def source(receipt: DeltaRangerDecisionReceipt): Unit = {
      receipt.obligations.zipWithIndex.foreach { case (obligation, occurrence) =>
        record(
          "source-obligation",
          s"receipt=${id(receipt)} occurrence=$occurrence relation=${id(obligation.scan.relation)} descriptor=${id(obligation.binding.descriptor())} index=${id(obligation.binding.index())} log=${id(obligation.binding.log())} catalog=${obligation.scan.catalog} columns=${obligation.columns.toSeq.sorted} filter=${obligation.decisions.filter} masks=${obligation.decisions.masks.toSeq.sortBy(_._1).map { case (column, decision) => s"$column:${decision.kind()}:${decision.expression()}" }}"
        )
      }
    }
    def account(phases: Seq[String]): Unit = phases.foreach { phase =>
      val state =
        if (completed.contains(phase)) "RETURNED"
        else if (started.contains(phase)) "FAILED"
        else "NOT_REACHED"
      record("phase-accounting", s"name=$phase state=$state")
    }
    override def close(): Unit = synchronized {
      if (!closed) {
        owner = None
        write(directory.resolve("events.txt"), entries.mkString("\n"))
        closed = true
      }
    }
    private def describe(plan: LogicalPlan): String = {
      val shape =
        s"id=${id(plan)} resolved=${plan.resolved} receiptPresent=${DeltaRangerDecisionReceipt.present(plan)} receiptNodes=${plan.collect { case node if DeltaRangerDecisionReceipt.present(node) => id(node) }} tree=${plan.treeString}"
      if (!plan.resolved) shape + " schema/output=UNAVAILABLE_UNRESOLVED"
      else
        shape + s" schema=${plan.schema.json} output=${plan.output.map(a => s"${a.name}:${a.dataType}:${a.nullable}:${a.exprId}")}"
    }
  }

  final class ResolutionObserver(spark: SparkSession, trace: Trace) extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      trace.observe(spark, "resolution", Some(plan))
      plan
    }
  }
  def observer(trace: Trace): SparkSessionExtensions => Unit = extensions =>
    extensions.injectResolutionRule(spark => new ResolutionObserver(spark, trace))

  final class GeneratorObservation(spark: SparkSession) extends PrepareDeltaScan(spark) {
    def inspect(index: TahoeLogFileIndex): DeltaScanGenerator = getDeltaScanGenerator(index)
  }

  def inspectNativePhases(
      spark: SparkSession,
      target: DeltaRangerRegistrationBindings.BoundAppendTarget,
      targetReceipt: DeltaRangerAppendTargetReceipt,
      sourceReceipt: Option[DeltaRangerDecisionReceipt],
      data: DataFrame,
      options: Map[String, String],
      trace: Trace,
      prepareSource: Boolean = false,
      invokeHelper: Boolean = true
  ): Unit = NativeDriver.inspect(
    spark,
    target,
    targetReceipt,
    sourceReceipt,
    data,
    options,
    trace,
    prepareSource,
    invokeHelper
  )

  private object NativeDriver extends DeltaCommand {
    def inspect(
        spark: SparkSession,
        target: DeltaRangerRegistrationBindings.BoundAppendTarget,
        targetReceipt: DeltaRangerAppendTargetReceipt,
        sourceReceipt: Option[DeltaRangerDecisionReceipt],
        data: DataFrame,
        options: Map[String, String],
        trace: Trace,
        prepareSource: Boolean,
        invokeHelper: Boolean
    ): Unit = {
      def validateBoth(prefix: String): Unit = {
        val errors = ArrayBuffer.empty[Throwable]
        try trace.phase(spark, prefix + "-target") { targetReceipt.validate(spark) }
        catch { case e: Exception => errors += e }
        sourceReceipt.foreach { receipt =>
          try trace.phase(spark, prefix + "-source") { receipt.validateAction(spark) }
          catch { case e: Exception => errors += e }
        }
        if (errors.nonEmpty) throw errors.head
      }
      val phases = Seq(
        "entry-target",
        "entry-source",
        "transaction",
        "hasBeenExecuted",
        "post-idempotency-target",
        "post-idempotency-source",
        "generator",
        "source-optimized",
        "helper"
      )
      try {
        validateBoth("entry")
        val table = target.table()
        val effective =
          if (spark.sessionState.conf.useNullsForMissingDefaultColumnValues)
            options + (ColumnWithDefaultExprUtils.USE_NULL_AS_DEFAULT_DELTA_OPTION -> "true")
          else options
        val autoCompact = AutoCompact.getAutoCompactType(
          spark.sessionState.conf,
          table.deltaLog.unsafeVolatileSnapshot.metadata
        )
        require(autoCompact.isEmpty, "unqualified auto-compaction")
        trace.record(
          "effective-options",
          s"supplied=$options effective=$effective autoCompact=$autoCompact sourceReceipt=${sourceReceipt.map(trace.id)} targetReceipt=${trace.id(targetReceipt)} target=${trace.id(table)} descriptor=${trace.id(target.descriptor())}"
        )
        val native = WriteIntoDelta(
          table.deltaLog,
          SaveMode.Append,
          new DeltaOptions(effective, spark.sessionState.conf),
          Nil,
          table.deltaLog.unsafeVolatileSnapshot.metadata.configuration,
          data,
          table.catalogTable
        )
        trace.phase(spark, "transaction") {
          table.deltaLog.withNewTransaction(table.catalogTable) { txn =>
            trace.observe(spark, "transaction-active", None)
            validateBoth("transaction-entry")
            trace.beforeSkip = spark.sessionState.conf.getAllConfs
            val skipped = trace.phase(spark, "hasBeenExecuted") {
              try hasBeenExecuted(txn, spark, Some(native.options))
              finally trace.afterSkip = spark.sessionState.conf.getAllConfs
            }
            trace.skip = Some(skipped)
            trace.record(
              "idempotency-result",
              s"skipped=$skipped removed=${trace.beforeSkip.keySet -- trace.afterSkip.keySet} added=${trace.afterSkip.keySet -- trace.beforeSkip.keySet} changed=${trace.afterSkip.filter { case (k, v) => trace.beforeSkip.get(k).exists(_ != v) }}"
            )
            validateBoth("post-idempotency")
            if (!skipped) {
              if (prepareSource) {
                trace.phase(spark, "generator") {
                  sourceReceipt.get.obligations.foreach { obligation =>
                    val generator =
                      new GeneratorObservation(spark).inspect(obligation.binding.index())
                    trace.generatorMatches :+= (generator eq txn)
                    trace.record(
                      "native-generator",
                      s"index=${trace.id(obligation.binding.index())} sourceLog=${trace.id(obligation.binding.log())} targetLog=${trace.id(table.deltaLog)} generator=${trace.id(generator)} active=${trace.id(txn)} identical=${generator eq txn}"
                    )
                  }
                }
                trace.phase(spark, "source-optimized") {
                  trace.observe(
                    spark,
                    "source-optimized-return",
                    Some(data.queryExecution.optimizedPlan)
                  )
                }
              }
              if (invokeHelper) {
                val tagged = trace.phase(spark, "helper") {
                  native.writeAndReturnCommitData(txn, spark)
                }
                trace.record(
                  "helper-actions",
                  s"actions=${tagged.actions.map(_.json)} tags=${tagged.stringTags} pendingMetadata=${txn.metadata.json} protocol=${txn.protocol.json}"
                )
                validateBoth("post-helper")
              }
            }
          }
        }
      } finally {
        trace.account(
          phases ++ Seq(
            "transaction-entry-target",
            "transaction-entry-source",
            "post-helper-target",
            "post-helper-source"
          )
        )
        trace.record("not-reached", "commit;recache;postcommit;cloned-session")
        trace.observe(spark, "driver-unwound", None)
      }
    }
  }

  def fresh(name: String): Trace = {
    val root = sys.env
      .get("GRAVITINO_DELTA_INSTALLED_APPEND_EVIDENCE_DIR")
      .map(Paths.get(_))
      .getOrElse(Files.createTempDirectory("installed-append-evidence"))
    require(root.isAbsolute && Files.isDirectory(root) && !Files.isSymbolicLink(root))
    Files.setPosixFilePermissions(root, PosixFilePermissions.fromString("rwx------"))
    new Trace(
      Files.createDirectory(
        root.resolve(name + "-" + UUID.randomUUID()),
        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"))
      )
    )
  }
  def write(path: Path, text: String): Unit = {
    Files.write(path, text.getBytes(StandardCharsets.UTF_8))
    Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-------"))
  }
  def manifest(root: Path, archive: Path): Map[String, String] = {
    Files.createDirectory(
      archive,
      PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"))
    )
    val files = Files.walk(root)
    val result =
      try
        files
          .iterator()
          .asScala
          .filter(Files.isRegularFile(_))
          .map { path =>
            require(!Files.isSymbolicLink(path))
            val name = root.relativize(path).toString
            val bytes = Files.readAllBytes(path)
            val output = archive.resolve(name)
            Files.createDirectories(output.getParent)
            var directory = output.getParent
            while (directory.startsWith(archive)) {
              Files.setPosixFilePermissions(directory, PosixFilePermissions.fromString("rwx------"))
              directory = directory.getParent
            }
            Files.write(output, bytes)
            Files.setPosixFilePermissions(output, PosixFilePermissions.fromString("rw-------"))
            name -> s"${bytes.length}:${HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes))}"
          }
          .toMap
      finally files.close()
    write(
      archive.resolveSibling(archive.getFileName.toString + "-manifest.txt"),
      result.toSeq.sorted.mkString("\n")
    )
    result
  }
}

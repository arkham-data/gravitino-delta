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

import java.time.Instant
import java.util.function.Predicate
import org.apache.spark.sql.execution.LogicalRDD
import java.nio.charset.StandardCharsets
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import org.apache.hadoop.fs.Path
import org.apache.kyuubi.plugin.spark.authz.rule.RuleEliminateMarker
import scala.collection.JavaConverters._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  And,
  Attribute,
  AttributeSet,
  ExprId,
  Expression
}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.delta.OptimisticTransaction
import org.apache.spark.sql.delta.commands.MergeIntoCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation

/** Original SQL demands and driver-local identity for one exact native successor. */
private[ranger] final class DeltaRangerMergeReceipt private (
    val spark: SparkSession,
    original: DeltaMergeInto,
    catalogs: DeltaRangerReadRule.Scan => DeltaRangerCatalogSpark35,
    authoredCapture: Option[DeltaRangerMergeEvolutionIntent.Authored]
) {
  import DeltaRangerMergeCommand.denied
  if (original.notMatchedBySourceClauses.nonEmpty)
    denied("unsupported merge profile")
  val source = DeltaRangerCdfRead
    .pending(original.source, spark)
    .getOrElse(denied("one fresh named bounded CDF source required"))
  private val targets = original.target.collect { case relation: LogicalRelation => relation }
  if (
    targets.size != 1 || original.target.exists {
      case _: LogicalRelation | _: SubqueryAlias => false
      case _                                     => true
    }
  ) denied("full named target required")
  val targetRelation = targets.head
  private val descriptor = targetRelation.catalogTable.getOrElse(denied("raw target"))
  private val id = descriptor.identifier
  if (id.catalog.isEmpty || id.database.isEmpty) denied("canonical target required")
  private val scan = DeltaRangerReadRule.Scan(
    targetRelation,
    id.catalog.get,
    id.database.get,
    id.table,
    Option(descriptor.owner).filter(_.nonEmpty)
  )
  private val targetCatalog = catalogs(scan)
  val target = targetCatalog.registrationBindings().resolve(targetRelation)
  val transactionIdentity: Option[(String, Long)] = {
    val app = spark.conf.getOption("spark.databricks.delta.write.txnAppId")
    val version = spark.conf.getOption("spark.databricks.delta.write.txnVersion")
    if (app.isDefined != version.isDefined || app.exists(_.isEmpty))
      denied("incomplete transaction identity")
    if (
      app.nonEmpty && spark.conf.get(
        "spark.databricks.delta.write.txnVersion.autoReset.enabled",
        "true"
      ) != "false"
    )
      denied("transaction version auto reset must be false")
    app.map(a => a -> version.get.toLong)
  }
  val schemaIntent = DeltaRangerMergeSchema.derive(
    spark,
    target.index().snapshotAtAnalysis.metadata,
    target.index().snapshotAtAnalysis.protocol,
    original.finalSchema,
    original.withSchemaEvolution
  )
  val recoveryCandidate = DeltaRangerMergeJournal
    .peek(targetCatalog.mergeJournalDirectory(), transactionIdentity)
    .filter(_.evolution.nonEmpty)
  private val requiresAlter = schemaIntent.evolves || recoveryCandidate.nonEmpty
  if (requiresAlter && authoredCapture.isEmpty) denied("missing authored evolution capture")
  private val sourceAnchor = source.binding.anchor()
  if (
    (target.observation().nativeId() == sourceAnchor.observation().nativeId() ||
      target.observation().location() == sourceAnchor.observation().location()) &&
    target.descriptor().identifier != sourceAnchor.descriptor().identifier
  )
    denied("different named aliases for one dataset")
  private val subject = DeltaRangerQuerySubject.current(spark)
  private val settings = spark.sessionState.conf.getAllConfs
  private val engine = target.engine()
  private val stamp = engine.snapshotStamp()
  private val sourceEngine = sourceAnchor.engine()
  private val sourceStamp = sourceEngine.snapshotStamp()
  private val expressions: Seq[Expression] = Seq(original.condition) ++
    original.matchedClauses.flatMap(c => c.condition.toSeq ++ c.resolvedActions.map(_.expr)) ++
    original.notMatchedClauses.flatMap(c => c.condition.toSeq ++ c.resolvedActions.map(_.expr))
  expressions.foreach(DeltaRangerPlanAdmission.expression)
  private val refs = AttributeSet(expressions.flatMap(_.references))
  private val selected = targetRelation.output.filter(refs.contains).map(_.name).toSet
  private val writes = original.matchedClauses.flatMap {
    case update: DeltaMergeIntoMatchedUpdateClause =>
      update.resolvedActions.map { a =>
        if (a.targetColNameParts.size != 1) denied("nested assignment")
        a.targetColNameParts.head
      }
    case _: DeltaMergeIntoMatchedDeleteClause =>
      schemaIntent.expectedMetadata.schema.fieldNames.toSeq
    case _ => denied("unknown matched clause")
  }.toSet ++ (if (original.notMatchedClauses.nonEmpty)
                schemaIntent.expectedMetadata.schema.fieldNames.toSet
              else Set.empty[String])
  if (writes.isEmpty || original.matchedClauses.isEmpty)
    denied("unsupported empty/insert-only merge")
  private val requiredWrites =
    writes ++ recoveryCandidate.toSeq.flatMap(_.evolution.get.requiredUpdates)
  private val keys = selected.toVector.map(name =>
    new DeltaRangerResourceKey(
      scan.catalog,
      scan.database,
      scan.table,
      name,
      "select",
      scan.owner.orNull
    )
  ) ++ requiredWrites.toVector.map(name =>
    new DeltaRangerResourceKey(
      scan.catalog,
      scan.database,
      scan.table,
      name,
      "update",
      scan.owner.orNull
    )
  )
  private val alterKey = new DeltaRangerResourceKey(
    scan.catalog,
    scan.database,
    scan.table,
    null,
    "alter",
    scan.owner.orNull
  )
  private val decisions =
    DeltaRangerMaskRules.capture(scan, selected, engine, subject, Instant.now())
  // Reuse the read parser and type checks, but retain only its policy expressions. The native
  // target itself must remain complete for hidden rows and unassigned raw copy-forward fields.
  private val governed = DeltaRangerMaskRules.rewrite(scan, selected, decisions, spark)
  private val masks = governed
    .collectFirst { case Project(output, _) =>
      targetRelation.output
        .zip(output)
        .collect { case (attribute, alias: Alias) =>
          attribute.exprId -> alias.child
        }
        .toMap
    }
    .getOrElse(Map.empty)
  private val filter = governed.collectFirst { case Filter(condition, _) => condition }
  if (AttributeSet(original.source.output).intersect(AttributeSet(original.target.output)).nonEmpty)
    denied("ambiguous source/target attribute occurrence")
  private var retainedTransaction: OptimisticTransaction = null
  private var metadataStaged = false
  private var baseState: Predicate[LogicalRDD] = null
  // Diagnostic evidence is immutable and never used to authorize execution or publication.
  private final case class Outcome(status: String, version: Option[Long], digest: Option[String])
  private var outcome = Outcome("not-committed", None, None)
  private var commitFailure: Throwable = null
  private var sourceAttempt: DeltaRangerMergeSource.Attempt = null
  private var sourceAttemptNumber = 0
  private var sourceClosed = false
  private var commitOwner: DeltaRangerMergeCommit = null
  private var publicationAdvanced = false
  private var recached = false
  private var completedRows: Vector[Vector[Long]] = null
  private var completed: DeltaRangerMergeResult = null
  private var physical: DeltaRangerMergeCommand.Exec = null
  private var running = false
  private var closed = false
  private var command: DeltaRangerMergeCommand = null
  private var successor: MergeIntoCommand = null
  private var rewritten: DeltaMergeInto = null
  private var sourceReceipt: DeltaRangerCdfReadReceipt = null
  validate()

  private def requireContext(published: Boolean = false): Unit = {
    if (closed) denied("expired command receipt")
    val actual = spark.sessionState.conf.getAllConfs
    val after = settings.updated(
      "spark.databricks.delta.lastCommitVersionInSession",
      (target.nativeVersion() + 1).toString
    )
    if ((SparkSession.active ne spark) || (actual != settings && !(published && actual == after)))
      denied("session/configuration changed")
    val current = DeltaRangerQuerySubject.current(spark)
    if (current.user() != subject.user() || current.groups() != subject.groups())
      denied("subject changed")
    engine.requireSnapshot(stamp); sourceEngine.requireSnapshot(sourceStamp)
    val now = Instant.now()
    keys.foreach(engine.authorize(subject, _, now))
    if (requiresAlter) engine.authorize(subject, alterKey, now)
    val currentDecisions = DeltaRangerMaskRules.capture(scan, selected, engine, subject, now)
    if (currentDecisions.filter != decisions.filter || currentDecisions.masks != decisions.masks)
      denied("target policies changed")
    if (sourceReceipt != null) sourceReceipt.requireMergePolicies(this)
    engine.requireSnapshot(stamp); sourceEngine.requireSnapshot(sourceStamp)
  }
  def validate(): Unit = {
    requireContext()
    if (sourceClosed) denied("source phase expired")
    if (schemaIntent.evolves) {
      // Reuse Spark's actual cache inventory, including dependent plans and older raw snapshots.
      val manager = spark.sharedState.cacheManager
      val field = manager.getClass.getDeclaredField("cachedData")
      field.setAccessible(true)
      val entries = field.get(manager).asInstanceOf[Seq[org.apache.spark.sql.execution.CachedData]]
      if (
        entries.exists(_.plan.exists {
          case relation: LogicalRelation =>
            relation.relation match {
              case files: org.apache.spark.sql.execution.datasources.HadoopFsRelation =>
                files.location match {
                  case index: org.apache.spark.sql.delta.files.TahoeLogFileIndex =>
                    index.path == target.index().path
                  case _ => false
                }
              case _ => false
            }
          case node => node.sameResult(targetRelation)
        })
      ) denied("schema evolution has target cache history")
    }
    target.validateCurrent(spark); source.binding.validateCurrent(spark)
    if (target.log().unsafeVolatileSnapshot.version != target.nativeVersion())
      denied("target head changed")
    if (sourceReceipt != null) sourceReceipt.validateAction(spark)
  }
  def openSourceAttempt(candidate: DeltaRangerMergeSource.Attempt, number: Int): Unit = {
    if (
      !running || sourceClosed || !metadataStaged || recoveryCandidate.nonEmpty || number != sourceAttemptNumber + 1 ||
      (sourceAttempt != null && !sourceAttempt.isClosed)
    ) denied("foreign source attempt")
    sourceAttempt = candidate
    sourceAttemptNumber = number
    try {
      validate()
      checkTransaction(retainedTransaction)
      sourceReceipt.openMergeExecution(this)
    } catch {
      case failure: Throwable => candidate.close(); throw failure
    }
  }
  def requireSourceAttempt(candidate: DeltaRangerMergeSource.Attempt): Unit = {
    if (!running || closed || sourceClosed || (candidate ne sourceAttempt))
      denied("foreign source attempt")
    checkTransaction(retainedTransaction)
  }
  def requireSourceExecution(candidate: DeltaRangerCdfReadReceipt): Unit = {
    requireSource(candidate)
    if (sourceAttempt == null) denied("missing source attempt")
    sourceAttempt.requireExecution()
  }
  def closeSourceAttempt(candidate: DeltaRangerMergeSource.Attempt): Unit = {
    if (candidate ne sourceAttempt) denied("foreign source attempt cleanup")
    sourceReceipt.closeMergeExecution(this)
  }
  def closeSource(): Unit = {
    validate()
    checkTransaction(retainedTransaction)
    if (sourceClosed) denied("source already closed")
    sourceClosed = true
  }
  def beginCommit(candidate: DeltaRangerMergeCommit, transaction: OptimisticTransaction): Unit = {
    if (!sourceClosed || commitOwner != null || !running) denied("invalid commit transition")
    requireRetainedTransaction(transaction)
    commitOwner = candidate
  }
  def requireCommitContext(
      candidate: DeltaRangerMergeCommit,
      transaction: OptimisticTransaction,
      published: Boolean
  ): Boolean = {
    if (!sourceClosed || (candidate ne commitOwner) || !running) denied("foreign commit owner")
    requireContext(published)
    checkTransaction(transaction, published)
    val advanced = target.requireMergePublication(
      published,
      target.nativeVersion() + 1,
      schemaIntent.expectedMetadata
    )
    if (publicationAdvanced && !advanced) denied("registration publication regressed")
    publicationAdvanced ||= advanced
    if (sourceAnchor.descriptor().identifier == target.descriptor().identifier) {
      sourceAnchor.requireMergePublication(
        published,
        target.nativeVersion() + 1,
        schemaIntent.expectedMetadata
      )
    } else {
      sourceAnchor.requireRetained()
      source.binding.recheckPublication()
    }
    // Metadata readback can complete a newer policy download. Check again at the boundary.
    requireContext(published)
    advanced
  }
  def recache(candidate: DeltaRangerMergeCommit, transaction: OptimisticTransaction): Unit = {
    if (recached || !requireCommitContext(candidate, transaction, published = true))
      denied("unreconciled recache")
    spark.sharedState.cacheManager.recacheByPlan(spark, original.target)
    if (!requireCommitContext(candidate, transaction, published = true))
      denied("recache state changed")
    recached = true
  }
  def completeNoop(candidate: DeltaRangerMergeCommit, transaction: OptimisticTransaction): Unit = {
    if (
      recached || transactionIdentity.nonEmpty || requireCommitContext(
        candidate,
        transaction,
        published = false
      )
    )
      denied("invalid native no-op completion")
    target.recheckPublication()
    if (target.log().unsafeVolatileSnapshot.version != target.nativeVersion())
      denied("no-op head changed")
    recached = true
  }

  def invokedCommit(candidate: DeltaRangerMergeCommit): Unit = {
    if ((candidate ne commitOwner) || !sourceClosed || !running) denied("foreign commit evidence")
    outcome = Outcome("unknown", Some(target.nativeVersion() + 1), None)
  }
  def observedCommit(candidate: DeltaRangerMergeCommit, version: Long, digest: String): Unit = {
    if (
      (candidate ne commitOwner) || !sourceClosed || !running || version != target
        .nativeVersion() + 1
    )
      denied("foreign commit evidence")
    val observed = Outcome("committed", Some(version), Some(digest))
    if (outcome.status == "committed" && outcome != observed) denied("commit evidence changed")
    outcome = observed
  }
  def skippedCommit(candidate: DeltaRangerMergeCommit): Unit = {
    if ((candidate ne commitOwner) || outcome.status != "unknown")
      denied("foreign skipped evidence")
    outcome = Outcome("not-committed", None, None)
  }
  def failedCommit(candidate: DeltaRangerMergeCommit, failure: Throwable): Unit = {
    if (candidate ne commitOwner) denied("foreign commit failure")
    if (commitFailure == null) commitFailure = failure
  }
  def preserveFailure(failure: Throwable): Throwable = {
    if (commitFailure == null) failure
    else {
      if (commitFailure ne failure) commitFailure.addSuppressed(failure)
      commitFailure
    }
  }
  def retainBaseState(
      candidate: DeltaRangerMergeCommand,
      transaction: OptimisticTransaction,
      operation: DeltaRangerNativeOperation
  ): Unit = {
    requireCommand(candidate)
    requireRetainedTransaction(transaction)
    if (metadataStaged || baseState != null) denied("invalid base-state capture")
    baseState = operation.retainTransactionState()
  }
  def containsBaseState(
      candidate: DeltaRangerMergeCommit,
      transaction: OptimisticTransaction,
      cache: LogicalRDD
  ): Boolean = {
    if (
      (candidate ne commitOwner) || (transaction ne retainedTransaction) ||
      !sourceClosed || !running || closed || baseState == null
    ) denied("foreign base-state consumer")
    baseState.test(cache)
  }

  def endCommit(candidate: DeltaRangerMergeCommit): Unit = {
    if (candidate ne commitOwner) denied("foreign commit cleanup")
    commitOwner = null
  }
  def requireRetainedTransaction(transaction: OptimisticTransaction): Unit = {
    requireContext()
    target.requireRetained(); sourceAnchor.requireRetained()
    target.recheckPublication(); source.binding.recheckPublication()
    checkTransaction(transaction)
  }
  private def govern(expression: Expression, sourceAttributes: Map[ExprId, Attribute]): Expression =
    expression.transformUp {
      // transformUp does not revisit the replacement: the mask's raw attribute stays raw.
      case attribute: Attribute if masks.contains(attribute.exprId) => masks(attribute.exprId)
      case attribute: Attribute if sourceAttributes.contains(attribute.exprId) =>
        sourceAttributes(attribute.exprId)
    }
  def rewrite(): DeltaMergeInto = {
    if (rewritten != null) denied("repeated rewrite")
    source.observe(original.source)
    sourceReceipt =
      if (recoveryCandidate.nonEmpty)
        DeltaRangerCdfReadReceipt.recovering(source, original.source, spark, this)
      else DeltaRangerCdfReadReceipt.issue(source, original.source, spark)
    sourceReceipt.bindMerge(this)
    if (original.source.output.size != sourceReceipt.carrier.output.size)
      denied("governed source arity changed")
    val sourceAttributes = original.source.output
      .zip(sourceReceipt.carrier.output)
      .map { case (before, after) =>
        if (before.name != after.name || before.dataType != after.dataType)
          denied("governed source output changed")
        before.exprId -> after
      }
      .toMap
    val userCondition = govern(original.condition, sourceAttributes)
    rewritten = original.copy(
      source = sourceReceipt.carrier,
      condition = filter.map(And(_, userCondition)).getOrElse(userCondition),
      matchedClauses = original.matchedClauses.map(c =>
        govern(c, sourceAttributes).asInstanceOf[DeltaMergeIntoMatchedClause]
      ),
      notMatchedClauses = original.notMatchedClauses.map(c =>
        govern(c, sourceAttributes).asInstanceOf[DeltaMergeIntoNotMatchedClause]
      )
    )
    rewritten
  }
  private def evolutionIntent: DeltaRangerMergeEvolutionIntent = {
    import DeltaRangerMergeEvolutionIntent._
    def observation(value: DeltaRangerRegistrationBindings.BoundRegistrationScan): Observation = {
      val o = value.observation()
      Observation(
        Vector(o.metalake(), o.catalog(), o.database(), o.table()),
        o.receipt().toProperties.asScala.toMap,
        o.location(),
        o.nativeId(),
        Option(value.descriptor().owner).filter(_.nonEmpty),
        value.index().snapshotAtAnalysis.metadata,
        value.index().snapshotAtAnalysis.protocol,
        value.nativeVersion()
      )
    }
    val policy = RuleEliminateMarker(governed) match {
      case Project(output, child) => Project(output.filter(a => selected(a.name)), child)
      case other                  => Project(other.output.filter(a => selected(a.name)), other)
    }
    capture(
      Capture(
        authoredCapture.getOrElse(denied("missing authored evolution capture")),
        original,
        rewritten,
        source.logical,
        targetRelation,
        sourceReceipt.mergePolicy,
        policy,
        observation(target),
        observation(sourceAnchor),
        schemaIntent.expectedMetadata,
        source.request.start,
        source.request.end,
        sourceReceipt.mergeColumns,
        selected,
        requiredWrites,
        Context(transactionIdentity, subject.user(), subject.groups().asScala.toSet, settings)
      )
    )
  }

  lazy val journal: DeltaRangerMergeJournal = createJournal()
  private def createJournal(): DeltaRangerMergeJournal = {
    if (recoveryCandidate.nonEmpty)
      return DeltaRangerMergeJournal.recovering(
        targetCatalog.mergeJournalDirectory(),
        recoveryCandidate.get,
        evolutionIntent
      )
    if (schemaIntent.evolves)
      return DeltaRangerMergeJournal.evolving(
        targetCatalog.mergeJournalDirectory(),
        evolutionIntent
      )

    if (sourceReceipt == null || rewritten == null) denied("unresolved journal intent")
    import DeltaRangerMergeJournal.{vector, hash}
    def binding(value: DeltaRangerRegistrationBindings.BoundRegistrationScan): String = {
      val observation = value.observation()
      vector(
        Vector(
          observation.metalake(),
          observation.catalog(),
          observation.database(),
          observation.table(),
          vector(
            observation
              .receipt()
              .toProperties
              .asScala
              .toVector
              .filterNot(_._1 == "delta.registration.receipt.nativeVersion")
              .sortBy(_._1)
              .map { case (key, v) => vector(Seq(key, v)) }
          ),
          value.nativeSchemaJson(),
          value.nativeProtocolJson(),
          vector(
            value.index().snapshotAtAnalysis.metadata.configuration.toVector.sortBy(_._1).map {
              case (k, v) => vector(Seq(k, v))
            }
          )
        )
      )
    }
    val legacy = vector(
      Vector(
        binding(sourceAnchor),
        binding(target),
        source.request.start.toString,
        source.request.end.toString,
        DeltaRangerMergeJournal.plan(original.source),
        vector(original.expressions.map(_.sql)),
        sourceReceipt.mergeIntent,
        vector(rewritten.expressions.map(_.sql)),
        subject.user(),
        vector(subject.groups().asScala.toVector.sorted),
        vector(
          settings.toVector
            .filterNot(_._1 == "spark.databricks.delta.lastCommitVersionInSession")
            .sortBy(_._1)
            .map { case (k, v) => vector(Seq(k, v)) }
        )
      )
    )
    // Preserve the complete legacy encoding for unpartitioned targets, including partitioned
    // CDF sources. Partitioned targets use a distinct intent with both ordered native layouts;
    // full schema/type identity remains in each existing binding above.
    val fingerprint = hash(
      if (target.nativePartitionStamp().isEmpty) legacy
      else
        vector(
          Vector(
            "partitioned-merge-v1",
            legacy,
            sourceAnchor.nativePartitionStamp(),
            target.nativePartitionStamp()
          )
        )
    )
    new DeltaRangerMergeJournal(
      targetCatalog.mergeJournalDirectory(),
      transactionIdentity,
      fingerprint
    )
  }

  def recoverPublication(): Unit = {
    validate()
    if (!running || transactionIdentity.isEmpty)
      denied("recovery requires fresh transaction identity")
    val entry = journal.existing.getOrElse(denied("missing durable MERGE intent"))
    if (
      entry.state == DeltaRangerMergeJournal.Unknown || entry.skipped ||
      target.nativeVersion() < entry.version || target.observation().nativeVersion() != target
        .nativeVersion()
    )
      denied("unknown publication or unreconciled current registration")
    val bytes = recoveryBytes(entry.version)
    if (entry.evolution.nonEmpty) {
      val mapper = new ObjectMapper()
        .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
        .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
      ((entry.version + 1) to target.nativeVersion()).foreach { version =>
        val suffix =
          new String(recoveryBytes(version), StandardCharsets.UTF_8).split("\n", -1).toVector
        if (suffix.last.nonEmpty || suffix.init.exists(_.trim.isEmpty))
          denied("incomplete recovery suffix")
        suffix.init.foreach { line =>
          val action = mapper.readTree(line)
          if (
            action == null || !action.isObject || action.size() != 1 ||
            action.has("metaData") || action.has("protocol")
          ) denied("recovery suffix changes metadata or protocol")
        }
      }
    }
    val digest = journal.verifiedDigest(bytes)
    outcome = Outcome("committed", Some(entry.version), Some(digest))
    journal.committed(bytes)
    validate()
    closeSource()
    spark.sharedState.cacheManager.recacheByPlan(spark, original.target)
    requireContext()
    target.recheckPublication(); source.binding.recheckPublication()
    requireContext()
    journal.reconciled()
    recached = true
  }

  private def recoveryBytes(version: Long): Array[Byte] = {
    val path = new Path(target.log().logPath, f"$version%020d.json")
    val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
    val before = fs.getFileStatus(path)
    val input = fs.open(path)
    val bytes =
      try input.readAllBytes()
      finally input.close()
    val after = fs.getFileStatus(path)
    if (
      !before.isFile || before.getLen != after.getLen ||
      before.getModificationTime != after.getModificationTime || bytes.length.toLong != after.getLen
    )
      denied("recovery publication changed")
    bytes
  }

  def cdf: DeltaRangerCdfReadReceipt = sourceReceipt
  def requireSource(candidate: DeltaRangerCdfReadReceipt): Unit = {
    if ((candidate ne sourceReceipt) || closed || sourceClosed)
      denied("expired MERGE CDF source")
  }
  def materializedColumns = {
    val governedRefs = AttributeSet(rewritten.expressions.flatMap(_.references))
    sourceReceipt.carrier.output.filter(governedRefs.contains)
  }
  def plan(candidate: DeltaRangerMergeCommand): DeltaRangerMergeCommand.Exec = {
    requireCommand(candidate)
    validate()
    if (physical != null) denied("command planned twice")
    physical = DeltaRangerMergeCommand.Exec()(this)
    physical
  }
  def requirePhysical(candidate: DeltaRangerMergeCommand.Exec): Unit = {
    if ((candidate ne physical) || physical == null) denied("copied physical command")
  }
  def output = command.output
  def execute(
      candidate: DeltaRangerMergeCommand.Exec
  ): Seq[org.apache.spark.sql.catalyst.InternalRow] = {
    requirePhysical(candidate)
    val rows = command.run(spark)
    completedRows = rows.map(row => Vector.tabulate(4)(row.getLong)).toVector
    rows.map(row =>
      org.apache.spark.sql.catalyst
        .InternalRow(row.getLong(0), row.getLong(1), row.getLong(2), row.getLong(3))
    )
  }
  def result(candidate: CommandResult): LogicalPlan = {
    if (
      completedRows == null || !closed || !recached ||
      (candidate.commandLogicalPlan ne command) || (candidate.commandPhysicalPlan ne physical) ||
      candidate.output != command.output || candidate.rows
        .exists(row => row.numFields != 4 || row.anyNull) ||
      candidate.rows.map(row => Vector.tabulate(4)(row.getLong)).toVector != completedRows
    )
      denied("foreign completed command result")
    if (completed == null) completed = new DeltaRangerMergeResult(this, candidate)
    completed.carrier
  }
  def requireResult(candidate: DeltaRangerMergeResult): Unit = {
    if (
      (candidate ne completed) || !closed || !recached || completedRows == null ||
      (SparkSession.active ne spark)
    ) denied("foreign completed result owner")
    val current = DeltaRangerQuerySubject.current(spark)
    if (current.user() != subject.user() || current.groups() != subject.groups())
      denied("result subject changed")
  }
  def begin(candidate: DeltaRangerMergeCommand): Unit = {
    requireCommand(candidate); validate()
    if (running) denied("repeated command execution")
    running = true
  }
  def end(): Unit = { running = false; closed = true }

  def bind(native: MergeIntoCommand): DeltaRangerMergeCommand = {
    if (
      command != null || rewritten == null || (native.source ne rewritten.source) ||
      (native.target ne rewritten.target) || native.schemaEvolutionEnabled != original.withSchemaEvolution ||
      native.migratedSchema != original.finalSchema ||
      native.trackHighWaterMarks.nonEmpty || !native.catalogTable.exists(_ eq descriptor)
    )
      denied("foreign preprocessing successor")
    successor = native
    command = DeltaRangerMergeCommand(native)(this)
    command
  }
  def requireCommand(candidate: DeltaRangerMergeCommand): Unit = {
    if (command == null || (candidate ne command)) denied("copied command")
  }
  def beginTransaction(
      candidate: DeltaRangerMergeCommand,
      transaction: OptimisticTransaction
  ): Unit = {
    requireCommand(candidate)
    if (sourceAttempt != null && !sourceAttempt.isClosed) denied("previous attempt remains open")
    retainedTransaction = transaction
    metadataStaged = false
    baseState = null
    validate()
    checkTransaction(transaction)
  }
  def stageMetadata(candidate: DeltaRangerMergeCommand, transaction: OptimisticTransaction)(
      body: => Unit
  ): Unit = {
    requireCommand(candidate)
    requireRetainedTransaction(transaction)
    if (metadataStaged || recoveryCandidate.nonEmpty) denied("invalid metadata staging phase")
    body
    schemaIntent.requirePending(transaction.metadata, transaction.protocol)
    metadataStaged = true
    requireRetainedTransaction(transaction)
  }
  def requireTransaction(transaction: OptimisticTransaction): Unit = {
    validate()
    checkTransaction(transaction)
  }
  private def checkTransaction(
      transaction: OptimisticTransaction,
      published: Boolean = false
  ): Unit = {
    if (
      !running || (transaction ne retainedTransaction) || !(OptimisticTransaction.getActive.exists(
        _ eq transaction
      ) ||
        (published && OptimisticTransaction.getActive.isEmpty)) ||
      (transaction.deltaLog ne target.log()) || !transaction.catalogTable.exists(_ eq descriptor) ||
      transaction.readVersion != target.nativeVersion() ||
      transaction.metadata != (if (metadataStaged) schemaIntent.expectedMetadata
                               else
                                 schemaIntent.baseMetadata) || transaction.protocol != schemaIntent.protocol ||
      transaction.snapshot.metadata != target.index().snapshotAtAnalysis.metadata ||
      transaction.snapshot.metadata.schemaString != target.nativeSchemaJson() ||
      transaction.snapshot.protocol.json != target.nativeProtocolJson()
    ) denied("foreign transaction")
  }
}
private[ranger] object DeltaRangerMergeReceipt {
  def issue(
      spark: SparkSession,
      capture: DeltaMergeInto,
      catalogs: DeltaRangerReadRule.Scan => DeltaRangerCatalogSpark35,
      authored: Option[DeltaRangerMergeEvolutionIntent.Authored] = None
  ): DeltaRangerMergeReceipt =
    new DeltaRangerMergeReceipt(spark, capture, catalogs, authored)
}

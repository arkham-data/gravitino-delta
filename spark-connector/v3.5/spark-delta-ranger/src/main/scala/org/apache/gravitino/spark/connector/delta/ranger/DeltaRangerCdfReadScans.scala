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

import java.util.{Collections, IdentityHashMap, List => JList}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{
  LogicalPlan,
  Union,
  Project,
  Filter,
  SubqueryAlias
}
import org.apache.spark.sql.delta.files.{
  TahoeFileIndexWithSnapshotDescriptor,
  CdcAddFileIndex,
  TahoeChangeFileIndex,
  TahoeRemoveFileIndex
}
import org.apache.spark.sql.execution.{LogicalRDD, SparkPlan}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerRegistrationBindings.{
  BoundCdfRead,
  BoundRegistrationScan
}

/** Driver-local ownership of the native branches emitted by one bounded reconstruction. */
private[ranger] final class DeltaRangerCdfReadScans private[ranger] (
    val interval: DeltaRangerCdfHistory.ValidatedInterval,
    spark: SparkSession
) extends DeltaRangerNativeOperation.CdfReadOwner {
  import DeltaRangerCdfRead.denied
  private val settings = spark.sessionState.conf.getAllConfs
  private val thread = Thread.currentThread()
  private val stockEmptyClass =
    spark.sparkContext.emptyRDD[org.apache.spark.sql.catalyst.InternalRow].getClass
  private val indices =
    new IdentityHashMap[TahoeFileIndexWithSnapshotDescriptor, java.lang.Boolean]()
  private val relations = new IdentityHashMap[LogicalRelation, java.lang.Boolean]()
  private var scanning: TahoeFileIndexWithSnapshotDescriptor = null
  private var empty: LogicalRDD = null
  private var emptyBinding: Vector[Any] = null
  private val relationBindings = new IdentityHashMap[LogicalRelation, Vector[Any]]()
  private var running = false
  private var consumed = false
  private val wrappers =
    new IdentityHashMap[TahoeFileIndexWithSnapshotDescriptor, DeltaRangerCdfReadScans.OwnedIndex]()
  private var physicalBinding: Vector[Any] = null
  private var physicalShapes = Set.empty[Vector[Class[_]]]
  private var prepared = false
  private var executed: org.apache.spark.rdd.RDD[org.apache.spark.sql.catalyst.InternalRow] = null
  private var rddGraph: Vector[AnyRef] = null
  private val requiredFiles = scala.collection.mutable.Map.empty[String, (Long, Long)]
  private var matching: TahoeFileIndexWithSnapshotDescriptor = null
  private var nativeTimes: Map[Long, java.sql.Timestamp] = null
  private val selected = new IdentityHashMap[TahoeFileIndexWithSnapshotDescriptor, Vector[
    org.apache.spark.sql.delta.actions.AddFile
  ]]()
  private var helper: DeltaRangerCdfReadScans.Match = null

  def timestamps(values: Map[Long, java.sql.Timestamp]): Unit = {
    requireLive()
    if (nativeTimes != null) denied("repeated native timestamp capture")
    nativeTimes = values.map { case (version, time) =>
      version -> new java.sql.Timestamp(time.getTime)
    }
  }

  // These are proof rows for native-selected actions, never an alternate event selector.
  private def manifest(
      index: TahoeFileIndexWithSnapshotDescriptor
  ): Vector[org.apache.spark.sql.delta.actions.AddFile] = {
    import org.apache.spark.sql.delta.actions.{Action, AddFile}
    def partitions(
        action: Action,
        original: Map[String, String],
        change: Option[String]
    ): Map[String, String] = {
      val version = interval.versionOf(action, this)
      val time = Option(nativeTimes)
        .flatMap(_.get(version))
        .getOrElse(denied("missing native commit timestamp"))
      val values = original + ("_commit_version" -> version.toString) +
        ("_commit_timestamp" -> new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS Z")
          .format(time))
      change.map(value => values + ("_change_type" -> value)).getOrElse(values)
    }
    index match {
      case adds: CdcAddFileIndex =>
        adds.addFiles.map { action =>
          action.copy(partitionValues = partitions(action, action.partitionValues, Some("insert")))
        }.toVector
      case changes: TahoeChangeFileIndex =>
        changes.filesByVersion.flatMap { spec =>
          spec.actions.map { action =>
            if (
              interval.versionOf(action, this) != spec.version || nativeTimes.get(
                spec.version
              ) != Option(spec.timestamp)
            ) denied("foreign CDC version")
            AddFile(
              action.path,
              partitions(action, action.partitionValues, None),
              action.size,
              0L,
              dataChange = false,
              tags = action.tags
            )
          }
        }.toVector
      case removes: TahoeRemoveFileIndex =>
        removes.filesByVersion.flatMap { spec =>
          spec.actions.map { action =>
            if (
              interval.versionOf(action, this) != spec.version || nativeTimes.get(
                spec.version
              ) != Option(spec.timestamp)
            ) denied("foreign remove version")
            if (!action.extendedFileMetadata.contains(true) || action.size.isEmpty)
              denied("missing remove metadata")
            AddFile(
              action.path,
              partitions(action, action.partitionValues, Some("delete")),
              action.size.get,
              0L,
              action.dataChange,
              tags = action.tags,
              deletionVector = action.deletionVector,
              baseRowId = action.baseRowId
            )
          }
        }.toVector
      case _ => denied("unsupported native branch")
    }
  }
  override def session(): SparkSession = spark
  override def bindings(): JList[BoundRegistrationScan] =
    Collections.singletonList(interval.binding.anchor())
  override def ownsHistoryFile(path: org.apache.hadoop.fs.Path): Boolean = false
  override def requireLive(): Unit = {
    if (
      !running || Thread
        .currentThread() != thread || SparkSession.active != spark || settings != spark.sessionState.conf.getAllConfs
    )
      denied("inactive CDF construction owner")
    interval.binding.anchor().requireRetained()
  }
  def requireInterval(candidate: DeltaRangerCdfHistory.ValidatedInterval): Unit = {
    requireLive()
    if (candidate ne interval) denied("foreign interval")
  }
  def run[T](body: => T): T = {
    if (running || consumed) denied("reused native reconstruction")
    interval.binding.validateCurrent(spark)
    running = true
    consumed = true
    val operation = DeltaRangerNativeOperation.cdfRead(this)
    try { val result = body; operation.succeed(); result }
    finally { operation.close(); running = false }
  }
  def issued(index: TahoeFileIndexWithSnapshotDescriptor): Unit = {
    requireLive()
    if (scanning != null || matching != null) denied("foreign scan issuance phase")
    if (index.deltaLog ne interval.readSnapshot.deltaLog) denied("foreign native index log")
    if (
      index.version != interval.readSnapshot.version || index.metadata != interval.readSnapshot.metadata || index.protocol != interval.readSnapshot.protocol
    )
      denied("foreign native index profile")
    if (
      index.getClass != classOf[CdcAddFileIndex] && index.getClass != classOf[
        TahoeChangeFileIndex
      ] && index.getClass != classOf[TahoeRemoveFileIndex]
    )
      denied("unsupported native branch")
    if (indices.put(index, true) != null) denied("reissued index")
    selected.put(index, manifest(index))
    scanning = index
    val conf = index.deltaLog.newDeltaHadoopConf()
    index.inputFiles.foreach { name =>
      val path = new org.apache.hadoop.fs.Path(name)
      val status = path.getFileSystem(conf).getFileStatus(path)
      if (!status.isFile) denied("missing historical file")
      requiredFiles.put(name, (status.getLen, status.getModificationTime))
    }
  }
  def emitted(
      index: TahoeFileIndexWithSnapshotDescriptor,
      frame: org.apache.spark.sql.DataFrame
  ): Unit = {
    requireLive()
    if ((scanning ne index) || !relations.containsKey(frame.queryExecution.analyzed))
      denied("native scan returned another relation")
    scanning = null
  }
  def returned(plan: LogicalPlan): Unit = {
    requireLive()
    if (scanning != null || !analysis(plan) || relations.size != indices.size)
      denied("incomplete native branch return")
  }
  def analysis(plan: LogicalPlan): Boolean = {
    requireLive()
    if (helper != null) return helper.analysis(plan)
    plan match {
      case union: Union
          if union.getClass == classOf[Union] && !union.byName && !union.allowMissingCol =>
        union.children.forall(analysis)
      case relation: LogicalRelation if !relation.isStreaming && relation.catalogTable.isEmpty =>
        relation.relation match {
          case fs: HadoopFsRelation =>
            fs.location match {
              case index: TahoeFileIndexWithSnapshotDescriptor if indices.containsKey(index) =>
                if (relations.containsKey(relation)) {
                  relationBindings.get(relation) == DeltaRangerNativeProvenance.binding(relation)
                } else if (
                  (scanning eq index) && !relations.keySet().toArray.exists {
                    case previous: LogicalRelation =>
                      previous.relation.asInstanceOf[HadoopFsRelation].location eq index
                    case _ => false
                  }
                ) {
                  val expected =
                    org.apache.spark.sql.delta.commands.cdc.CDCReader.cdcReadSchema(index.schema)
                  if (
                    fs.dataSchema != expected || fs.partitionSchema != index.partitionSchema ||
                    fs.options != index.deltaLog.options || fs.bucketSpec.nonEmpty ||
                    fs.fileFormat.getClass != classOf[
                      org.apache.spark.sql.delta.DeltaParquetFileFormat
                    ]
                  ) false
                  else {
                    relations.put(relation, true)
                    relationBindings.put(relation, DeltaRangerNativeProvenance.binding(relation))
                    true
                  }
                } else false
              case _ => false
            }
          case _ => false
        }
      case rdd: LogicalRDD if rdd.getClass == classOf[LogicalRDD] =>
        val expected = org.apache.spark.sql.delta.commands.cdc.CDCReader
          .cdcReadSchema(interval.readSnapshot.schema)
        if (
          matching != null || scanning != null || rdd.stats.sizeInBytes != 0 || !rdd.stats.rowCount
            .contains(BigInt(0)) ||
          rdd.isStreaming || rdd.schema != expected || rdd.rdd.getClass != stockEmptyClass || rdd.rdd.partitions.nonEmpty || rdd.rdd.dependencies.nonEmpty
        )
          false
        else if (empty == null) {
          empty = rdd
          emptyBinding = DeltaRangerNativeProvenance.binding(rdd)
          true
        } else (empty eq rdd) && emptyBinding == DeltaRangerNativeProvenance.binding(rdd)
      case _ => false
    }
  }
  def prepare(plan: LogicalPlan): LogicalPlan = {
    if (prepared) denied("CDF logical recipe already prepared")
    prepared = true
    val seen = new IdentityHashMap[LogicalPlan, java.lang.Boolean]()
    def copy(node: LogicalPlan): LogicalPlan = node match {
      case relation: LogicalRelation =>
        if (seen.put(relation, true) != null) denied("repeated native CDF branch")
        if (
          !relations.containsKey(relation) || relationBindings.get(
            relation
          ) != DeltaRangerNativeProvenance.binding(relation)
        )
          denied("copied native CDF relation")
        relation.relation match {
          case fs: HadoopFsRelation =>
            fs.location match {
              case index: TahoeFileIndexWithSnapshotDescriptor if indices.containsKey(index) =>
                var wrapper = wrappers.get(index)
                if (wrapper == null) {
                  wrapper = new DeltaRangerCdfReadScans.OwnedIndex(this, index, spark)
                  wrappers.put(index, wrapper)
                }
                relation.copy(relation = fs.copy(location = wrapper)(spark))
              case _ => denied("unissued CDF index")
            }
          case _ => denied("foreign CDF relation")
        }
      case union: Union         => union.copy(children = union.children.map(copy))
      case project: Project     => project.copy(child = copy(project.child))
      case filter: Filter       => filter.copy(child = copy(filter.child))
      case alias: SubqueryAlias => copy(alias.child)
      case rdd: LogicalRDD
          if empty != null && (empty eq rdd) && emptyBinding == DeltaRangerNativeProvenance.binding(
            rdd
          ) =>
        if (seen.put(rdd, true) != null) denied("repeated empty CDF branch")
        rdd
      case _ => denied("foreign CDF native subtree")
    }
    val result = copy(plan)
    if (seen.size != relations.size + 1 || !seen.containsKey(empty)) denied("incomplete CDF recipe")
    result
  }

  def matchingFiles(
      index: TahoeFileIndexWithSnapshotDescriptor,
      delegate: DeltaRangerCdfReadScans.OwnedIndex,
      partitions: Seq[org.apache.spark.sql.catalyst.expressions.Expression]
  )(
      body: => Seq[org.apache.spark.sql.delta.actions.AddFile]
  ): Seq[org.apache.spark.sql.delta.actions.AddFile] = {
    if (
      running || matching != null || !indices
        .containsKey(index) || (wrappers.get(index) ne delegate)
    )
      denied("foreign native file matching")
    interval.binding.validateCurrent(spark)
    validateFiles()
    partitions.foreach(DeltaRangerPlanAdmission.expression)
    matching = index
    running = true
    helper = new DeltaRangerCdfReadScans.Match(this, index, selected.get(index), partitions, spark)
    val operation = DeltaRangerNativeOperation.cdfRead(this)
    try {
      val result = body
      helper.requireReturned(result)
      operation.succeed()
      result
    } finally { operation.close(); running = false; matching = null; helper = null }
  }

  def helperCarrier(plan: LogicalPlan): Option[DeltaRangerCdfReadScans.HelperCarrier] = {
    requireLive()
    if (helper == null) None else helper.normalize(plan)
  }
  def requireHelper(candidate: DeltaRangerCdfReadScans.Match): Unit = {
    requireLive()
    if ((helper ne candidate) || matching == null) denied("inactive CDF matching helper")
  }

  def seal(plan: SparkPlan): Unit = {
    if (physicalBinding != null) denied("CDF physical owner already sealed")
    physicalBinding = DeltaRangerPhysicalProvenance.binding(plan)
    physicalShapes += shape(plan)
    val transitions = org.apache.spark.sql.execution
      .ApplyColumnarRulesAndInsertTransitions(Nil, outputsColumnar = false)(plan.clone())
    if (DeltaRangerPhysicalProvenance.binding(transitions) != physicalBinding)
      denied("CDF columnar recipe changed")
    physicalShapes += shape(transitions)
  }
  def tail(plan: SparkPlan): Unit = {
    if (physicalBinding == null || DeltaRangerPhysicalProvenance.binding(plan) != physicalBinding)
      denied("CDF physical tail changed")
    physicalShapes += shape(plan)
  }
  private def shape(plan: SparkPlan): Vector[Class[_]] =
    DeltaRangerPhysicalProvenance.nodes(plan).map(_.getClass)
  def validateFiles(): Unit = {
    val conf = interval.readSnapshot.deltaLog.newDeltaHadoopConf()
    requiredFiles.foreach { case (name, expected) =>
      val path = new org.apache.hadoop.fs.Path(name)
      val status = path.getFileSystem(conf).getFileStatus(path)
      if (!status.isFile || (status.getLen, status.getModificationTime) != expected)
        denied("historical data file changed")
    }
  }
  private def graph(root: org.apache.spark.rdd.RDD[_]): Vector[AnyRef] = {
    val seen = new IdentityHashMap[org.apache.spark.rdd.RDD[_], java.lang.Boolean]()
    val active = new IdentityHashMap[org.apache.spark.rdd.RDD[_], java.lang.Boolean]()
    val result = Vector.newBuilder[AnyRef]
    def visit(rdd: org.apache.spark.rdd.RDD[_]): Unit = {
      if (seen.size >= 10000 || active.containsKey(rdd)) denied("cyclic or oversized CDF RDD graph")
      result += rdd
      if (seen.put(rdd, true) == null) {
        active.put(rdd, true)
        rdd.partitions.foreach(result += _)
        rdd.dependencies.foreach { dependency => result += dependency; visit(dependency.rdd) }
        active.remove(rdd)
      }
    }
    visit(root)
    result.result()
  }
  def execute(
      plan: SparkPlan
  ): org.apache.spark.rdd.RDD[org.apache.spark.sql.catalyst.InternalRow] = {
    verify(plan)
    if (executed == null) {
      executed = plan.execute()
      rddGraph = graph(executed)
    }
    verify(plan)
    executed
  }
  def verify(plan: SparkPlan): Unit = {
    if (
      physicalBinding == null || physicalBinding != DeltaRangerPhysicalProvenance.binding(
        plan
      ) || !physicalShapes(shape(plan))
    )
      denied("CDF physical child changed")
    if (executed != null) {
      val current = graph(executed)
      if (
        current.size != rddGraph.size || !current.zip(rddGraph).forall { case (now, before) =>
          now eq before
        }
      )
        denied("CDF RDD dependencies changed")
    }
  }
}

private[ranger] object DeltaRangerCdfReadScans {

  /** One exact native file-matching call and its encoded, policy-internal file rows. */
  final class Match(
      owner: DeltaRangerCdfReadScans,
      index: TahoeFileIndexWithSnapshotDescriptor,
      files: Vector[org.apache.spark.sql.delta.actions.AddFile],
      partitions: Seq[org.apache.spark.sql.catalyst.expressions.Expression],
      spark: SparkSession
  ) {
    import DeltaRangerCdfRead.denied
    import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
    import org.apache.spark.sql.catalyst.expressions.{And, Literal}
    private val encoder = org.apache.spark.sql.Encoders
      .product[org.apache.spark.sql.delta.actions.AddFile]
      .asInstanceOf[org.apache.spark.sql.catalyst.encoders.ExpressionEncoder[
        org.apache.spark.sql.delta.actions.AddFile
      ]]
    private val serialize = encoder.createSerializer()
    private val expectedRows = files.map(file => serialize(file).copy())
    private var local: LocalRelation = null
    private var original: LogicalPlan = null
    private var analyzedFilter: Filter = null
    private var carrier: HelperCarrier = null
    private var physical: Vector[Any] = null
    private var returnedFiles: Vector[String] = null
    private var root: HelperExec = null
    private var started = false
    private var shapes = Set.empty[Vector[Class[_]]]
    private def shape(plan: SparkPlan): Vector[Class[_]] =
      DeltaRangerPhysicalProvenance.nodes(plan).map(_.getClass)
    def replace(previous: HelperExec, child: SparkPlan): HelperExec = {
      verify(previous)
      if (child eq previous.child) return previous
      if (started) denied("matching helper transitions closed")
      verifyChild(child)
      root = HelperExec(child)(this)
      root
    }
    def begin(candidate: HelperExec): Unit = { verify(candidate); started = true }
    def collected(rows: Array[org.apache.spark.sql.catalyst.InternalRow]): Unit = {
      owner.requireHelper(this)
      if (returnedFiles != null) denied("matching helper collected twice")
      val deserialize = encoder.resolveAndBind().createDeserializer()
      returnedFiles = rows.toVector.map(row => deserialize(row).json)
    }
    def requireReturned(result: Seq[org.apache.spark.sql.delta.actions.AddFile]): Unit = {
      owner.requireHelper(this)
      if (
        returnedFiles == null || result.map(_.json).groupBy(identity).map { case (key, rows) =>
          key -> rows.size
        } !=
          returnedFiles.groupBy(identity).map { case (key, rows) => key -> rows.size }
      )
        denied("native matching returned another filtered manifest")
    }

    private def checkRows(candidate: LocalRelation): Boolean = {
      val schema = encoder.schema
      !candidate.isStreaming && candidate.output.map(a =>
        (a.name, a.dataType, a.nullable, a.metadata)
      ) ==
        schema.fields.map(f => (f.name, f.dataType, f.nullable, f.metadata)).toSeq &&
        candidate.data.toVector == expectedRows
    }
    def analysis(plan: LogicalPlan): Boolean = {
      owner.requireHelper(this)
      plan match {
        case candidate: LocalRelation
            if candidate.getClass == classOf[LocalRelation] && checkRows(candidate) =>
          if (local == null) local = candidate
          candidate eq local
        case filter: Filter if local != null && (filter.child eq local) && filter.resolved =>
          if (!checkRows(local)) denied("matching helper rows changed")
          val rewritten = org.apache.spark.sql.delta.DeltaLog.rewritePartitionFilters(
            index.partitionSchema,
            spark.sessionState.conf.resolver,
            partitions
          )
          val condition = rewritten.reduceLeftOption(And).getOrElse(Literal.TrueLiteral)
          val expected =
            org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer.execute(Filter(condition, local))
          val valid =
            DeltaRangerNativeProvenance.binding(expected) == DeltaRangerNativeProvenance.binding(
              filter
            )
          if (valid && analyzedFilter == null) analyzedFilter = filter
          valid && (analyzedFilter eq filter)
        case _ => false
      }
    }
    def normalize(plan: LogicalPlan): Option[HelperCarrier] = {
      if (!plan.isInstanceOf[Filter] || !analysis(plan)) return None
      if (carrier == null) {
        original = plan
        carrier = HelperCarrier(plan.output.toVector)(this)
      } else if (original ne plan) denied("replaced matching helper query")
      Some(carrier)
    }
    def check(candidate: HelperCarrier): Unit = {
      owner.requireHelper(this)
      if ((candidate ne carrier) || original == null || !analysis(original))
        denied("foreign matching helper carrier")
    }
    def plan(candidate: HelperCarrier): SparkPlan = {
      check(candidate)
      if (physical != null) denied("matching helper planned twice")
      val child = DeltaRangerPhysicalPlanning.lower(spark, original)
      physical = DeltaRangerPhysicalProvenance.binding(child)
      shapes += shape(child)
      shapes += shape(
        org.apache.spark.sql.execution
          .ApplyColumnarRulesAndInsertTransitions(Nil, outputsColumnar = false)(child.clone())
      )
      shapes += shape(DeltaRangerPhysicalPlanning.tail(spark, original, child))
      root = HelperExec(child)(this)
      root
    }
    def verify(candidate: HelperExec): Unit = {
      if (candidate ne root) denied("copied matching helper physical carrier")
      verifyChild(candidate.child)
    }
    private def verifyChild(child: SparkPlan): Unit = {
      owner.requireHelper(this)
      if (
        physical == null || !shapes(shape(child)) || !analysis(
          original
        ) || physical != DeltaRangerPhysicalProvenance
          .binding(child)
      )
        denied("matching helper physical child changed")
    }
  }

  final case class HelperCarrier(
      override val output: Seq[org.apache.spark.sql.catalyst.expressions.Attribute]
  )(@transient private val authority: Match)
      extends org.apache.spark.sql.catalyst.plans.logical.LeafNode {
    def ticket: Match = {
      if (authority == null || isCanonicalizedPlan)
        DeltaRangerCdfRead.denied("inert matching helper")
      authority.check(this)
      authority
    }
    override protected def otherCopyArgs: Seq[AnyRef] = Seq(authority)
    override def clone(): LogicalPlan = this
  }

  final case class HelperExec(child: SparkPlan)(@transient private val authority: Match)
      extends org.apache.spark.sql.execution.UnaryExecNode {
    def verify(): Unit = {
      if (authority == null || isCanonicalizedPlan)
        DeltaRangerCdfRead.denied("inert matching helper physical carrier")
      authority.verify(this)
    }
    override def clone(): SparkPlan = this
    override def output = child.output
    override def outputPartitioning = child.outputPartitioning
    override def outputOrdering = child.outputOrdering
    override protected def otherCopyArgs: Seq[AnyRef] = Seq(authority)
    override def executeCollect(): Array[org.apache.spark.sql.catalyst.InternalRow] = {
      verify()
      authority.begin(this)
      val rows = super.executeCollect()
      authority.collected(rows)
      rows
    }
    override protected def doExecute()
        : org.apache.spark.rdd.RDD[org.apache.spark.sql.catalyst.InternalRow] = {
      verify(); authority.begin(this); child.execute()
    }
    override protected def withNewChildInternal(child: SparkPlan): HelperExec =
      authority.replace(this, child)
  }

  def normalize(plan: LogicalPlan, spark: SparkSession): Option[LogicalPlan] = {
    val operation = DeltaRangerNativeOperation.current()
    if (operation == null) None
    else
      operation.cdfReadOwner() match {
        case owner: DeltaRangerCdfReadScans if owner.session() eq spark => owner.helperCarrier(plan)
        case _                                                          => None
      }
  }

  final class OwnedIndex(
      owner: DeltaRangerCdfReadScans,
      original: TahoeFileIndexWithSnapshotDescriptor,
      spark: SparkSession
  ) extends TahoeFileIndexWithSnapshotDescriptor(
        spark,
        original.deltaLog,
        original.path,
        owner.interval.readSnapshot
      ) {
    override def partitionSchema = original.partitionSchema
    override def inputFiles: Array[String] = original.inputFiles
    override def sizeInBytes: Long = original.sizeInBytes
    override def refresh(): Unit = ()
    override def matchingFiles(
        partitions: Seq[org.apache.spark.sql.catalyst.expressions.Expression],
        data: Seq[org.apache.spark.sql.catalyst.expressions.Expression]
    ): Seq[org.apache.spark.sql.delta.actions.AddFile] =
      owner.matchingFiles(original, this, partitions) { original.matchingFiles(partitions, data) }
  }

  def history(
      binding: BoundCdfRead,
      version: Long,
      spark: SparkSession
  ): org.apache.spark.sql.delta.Snapshot = {
    val settings = spark.sessionState.conf.getAllConfs
    val thread = Thread.currentThread()
    val log = binding.anchor().log()
    if (version < 0L || version > log.unsafeVolatileSnapshot.version)
      DeltaRangerCdfRead.denied("invalid historical snapshot request")
    val files = log.logPath
      .getFileSystem(log.newDeltaHadoopConf())
      .listStatus(log.logPath)
      .filter { status =>
        val name = status.getPath.getName
        status.isFile && name.matches(
          "[0-9]{20}(\\.json|\\.checkpoint(\\.[0-9]{10}\\.[0-9]{10})?\\.parquet)"
        ) &&
        name.take(20).toLong <= version
      }
      .map(_.getPath)
      .toSet
    val owner = new DeltaRangerNativeOperation.CdfReadOwner {
      override def session(): SparkSession = spark
      override def bindings(): JList[BoundRegistrationScan] =
        Collections.singletonList(binding.anchor())
      override def ownsHistoryFile(path: org.apache.hadoop.fs.Path): Boolean = {
        requireLive()
        files(path)
      }
      override def requireLive(): Unit = {
        if (
          Thread
            .currentThread() != thread || SparkSession.active != spark || spark.sessionState.conf.getAllConfs != settings
        )
          DeltaRangerCdfRead.denied("history capture context changed")
        binding.anchor().requireRetained()
      }
    }
    val operation = DeltaRangerNativeOperation.cdfRead(owner)
    try {
      val result = log.getSnapshotAt(version)
      if (result.version != version || (result.deltaLog ne log))
        DeltaRangerCdfRead.denied("historical getter returned another snapshot")
      operation.succeed()
      result
    } finally operation.close()
  }

  def analysis(plan: LogicalPlan, spark: SparkSession): Boolean = {
    val operation = DeltaRangerNativeOperation.current()
    if (operation == null || operation.cdfReadOwner() == null) false
    else
      operation.cdfReadOwner() match {
        case owner: DeltaRangerCdfReadScans if owner.session() eq spark => owner.analysis(plan)
        case _                                                          => false
      }
  }
}

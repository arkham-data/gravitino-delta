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

import org.apache.spark.sql.delta.OptimisticTransaction

import java.util.{IdentityHashMap, UUID}
import java.time.Instant

import scala.collection.JavaConverters._

import scala.collection.mutable

import org.apache.kyuubi.plugin.spark.authz.{OperationType, PrivilegeObjectType, PrivilegesBuilder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.catalyst.plans.logical.{
  LogicalPlan,
  SetTableProperties,
  AppendData,
  Project,
  Filter,
  Join,
  SubqueryAlias,
  LocalRelation
}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.kyuubi.plugin.spark.authz.rule.RuleEliminateMarker
import org.apache.kyuubi.plugin.spark.authz.rule.datamasking.RuleApplyDataMaskingStage1
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.delta.stats.{
  DeltaScan,
  DeltaScanGenerator,
  PrepareDeltaScan,
  PreparedDeltaFileIndex
}
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

private[ranger] class DeltaRangerReadRule(
    spark: SparkSession,
    catalogs: DeltaRangerReadRule.Scan => DeltaRangerCatalogSpark35
) extends Rule[LogicalPlan] {
  import DeltaRangerReadRule._

  override def apply(plan: LogicalPlan): LogicalPlan = {
    DeltaRangerNativeOperation.requireNoRawSelection()
    if (DeltaRangerMergeWrite.analysis(plan, spark)) return plan
    if (DeltaRangerMergeNative.analysis(plan, spark)) return plan
    if (DeltaRangerMergeSource.analysis(plan, spark)) return plan
    if (DeltaRangerCdfReadScans.analysis(plan, spark)) return plan
    DeltaRangerCdfRead.resolve(plan, spark, catalogs).foreach(result => return result)
    DeltaRangerMergeCommand.resolve(plan, spark, catalogs).foreach(result => return result)
    if (DeltaRangerCdfRead.pending(plan, spark).nonEmpty) return plan
    plan match {
      case helper: DeltaRangerCdfReadScans.HelperCarrier => helper.ticket; return helper
      case cdf: DeltaRangerCdfRead.Carrier               => cdf.receipt; return cdf
      case cdf: DeltaRangerCdfPropertyCommand.Carrier    => cdf.owner.check(cdf, spark); return cdf
      case cdf: SetTableProperties if cdf.resolved =>
        return DeltaRangerCdfPropertyCommand.resolve(cdf, spark, catalogs)
      case append: AppendData if append.resolved =>
        DeltaRangerAppendV1Input.inspect(append, spark)
        val table = append.table match {
          case relation: DataSourceV2Relation =>
            relation.table match {
              case native: DeltaTableV2 => native
              case _ =>
                throw new SecurityException("Delta append input rejected: unsupported target")
            }
          case _ => throw new SecurityException("Delta append input rejected: unsupported target")
        }
        val id = table.catalogTable
          .getOrElse(throw new SecurityException("Delta append input rejected: unnamed target"))
          .identifier
        return DeltaRangerAppendV1Input.resolve(
          append,
          spark,
          catalogs(Scan(null, id.catalog.get, id.database.get, id.table, None))
        )
      case append: AppendData =>
        DeltaRangerAppendV1Input.inspect(append, spark)
        return plan
      case _ =>
    }
    if (DeltaRangerAppendHelper.analysis(plan, spark)) return plan
    if (!plan.resolved || nativeAdmission(plan, spark) != DeltaRangerNativeProvenance.UserPlan)
      return plan
    // Our pre-CBO ordering also places this resolution rule before Delta's native V1 fallback.
    // Defer only native intermediate relations; the final check never admits an unmarked V2 plan.
    if (
      plan.exists {
        case relation: DataSourceV2Relation => relation.table.isInstanceOf[DeltaTableV2]
        case _                              => false
      }
    ) return plan
    if (
      !spark.conf
        .get("spark.databricks.delta.catalog.update.enabled", "false")
        .equalsIgnoreCase("false")
    ) {
      throw new SecurityException("Native Delta catalog updates must remain disabled")
    }
    val subject = DeltaRangerQuerySubject.current(spark)
    if (DeltaRangerDecisionReceipt.present(plan)) {
      DeltaRangerDecisionReceipt.check(plan, spark, optimized = false)
      return plan
    }
    if (plan.exists(DeltaRangerDecisionReceipt.present))
      throw new SecurityException("Derived or reused Delta plans are not qualified")
    DeltaRangerPlanAdmission.query(plan)
    val scans = extract(plan, spark)
    val bound = scans.map { case (scan, columns) =>
      val binding = catalogs(scan).registrationBindings().resolve(scan.relation)
      if (binding.index().metadata.configuration.get("delta.enableRowTracking").contains("true"))
        throw new SecurityException(
          "Delta decision receipt rejected: row tracking is not qualified"
        )
      (scan, columns, binding, binding.engine())
    }
    val configuration = spark.sessionState.conf.clone()
    val stamps = bound.map(_._4).distinct.map(e => e -> e.snapshotStamp()).toMap
    val instant = Instant.now()
    // Complete original authorization for every table before asking for any rewrite.
    bound
      .flatMap { case (scan, columns, binding, engine) =>
        DeltaRangerResourceKey
          .select(scan.catalog, scan.database, scan.table, columns.asJava, scan.owner.orNull)
          .asScala
          .map(key => engine -> key)
      }
      .distinct
      .foreach { case (engine, key) => engine.authorize(subject, key, instant) }
    val replacements = new IdentityHashMap[LogicalRelation, LogicalPlan]()
    val obligations = bound.map { case (scan, columns, binding, engine) =>
      new DeltaRangerDecisionReceipt.Obligation(
        scan,
        columns,
        binding,
        DeltaRangerMaskRules.capture(scan, columns, engine, subject, instant)
      )
    }
    if (configuration.getAllConfs != spark.sessionState.conf.getAllConfs)
      throw new SecurityException("Delta decision receipt rejected: configuration changed")
    val rewritten = SQLConf.withExistingConf(configuration) {
      obligations.foreach { obligation =>
        val scan = obligation.scan
        val columns = obligation.columns
        replacements.put(
          scan.relation,
          DeltaRangerMaskRules.rewrite(scan, columns, obligation.decisions, spark)
        )
      }
      val stage0 = plan.transformUp { case relation: LogicalRelation => replacements.get(relation) }
      RuleEliminateMarker(RuleApplyDataMaskingStage1(spark)(stage0))
    }
    DeltaRangerDecisionReceipt.attach(
      rewritten,
      spark,
      subject,
      obligations,
      stamps,
      instant,
      configuration
    )
    rewritten
  }
}

private[ranger] object DeltaRangerReadRule {
  private def nativeAdmission(
      plan: LogicalPlan,
      spark: SparkSession
  ): DeltaRangerNativeProvenance.Admission = {
    val admission = DeltaRangerNativeProvenance.classify(plan)
    if (admission != DeltaRangerNativeProvenance.UserPlan) {
      try DeltaRangerDecisionReceipt.qualifyNative(plan, spark)
      catch {
        case failure: SecurityException =>
          admission match {
            case sealedNative: DeltaRangerNativeProvenance.SealedNative =>
              sealedNative.witness.invalidate()
            case _ => Option(DeltaRangerNativeOperation.current()).foreach(_.invalidate())
          }
          throw new DeltaRangerNativeOperation.OptimizerRejected(
            failure.getMessage.contains("consumer")
          )
      }
    }
    admission
  }

  def check(plan: LogicalPlan, spark: SparkSession): Unit = {
    if (DeltaRangerMergeWrite.analysis(plan, spark)) return
    if (DeltaRangerMergeNative.analysis(plan, spark)) return
    if (DeltaRangerMergeSource.analysis(plan, spark)) return
    if (DeltaRangerCdfReadScans.analysis(plan, spark)) return
    plan match {
      case merge: DeltaRangerMergeCommand => merge.receipt; return
      case _                              =>
    }
    DeltaRangerCdfRead.pending(plan, spark).foreach { source =>
      source.observe(plan)
      return
    }
    plan match {
      case helper: DeltaRangerCdfReadScans.HelperCarrier => helper.ticket; return
      case cdf: DeltaRangerCdfRead.Carrier               => cdf.receipt; return
      case cdf: DeltaRangerCdfPropertyCommand.Carrier    => cdf.owner.check(cdf, spark); return
      case _                                             =>
    }
    if (DeltaRangerAppendHelper.analysis(plan, spark)) return
    if (plan.isInstanceOf[AppendData]) {
      DeltaRangerAppendV1Input.analyzed(plan.asInstanceOf[AppendData], spark)
      return
    }
    if (nativeAdmission(plan, spark) == DeltaRangerNativeProvenance.UserPlan)
      DeltaRangerDecisionReceipt.check(plan, spark, optimized = false)
  }

  def prepare(plan: LogicalPlan, spark: SparkSession): LogicalPlan = {
    DeltaRangerMergeWrite.forbidConstructionAction()
    plan match {
      case result: DeltaRangerMergeResult.Carrier => result.owner; return result
      case writer: DeltaRangerMergeWrite.Carrier  => writer.owner; return writer
      case metric: DeltaRangerMergeNative.Carrier => metric.owner; return metric
      case source: DeltaRangerMergeSource.Carrier => source.owner; return source
      case _                                      =>
    }
    plan match {
      case helper: DeltaRangerCdfReadScans.HelperCarrier => helper.ticket; return helper
      case cdf: DeltaRangerCdfRead.Carrier               => cdf.receipt; return cdf
      case cdf: DeltaRangerCdfPropertyCommand.Carrier    => cdf.owner.check(cdf, spark); return cdf
      case merge: DeltaRangerMergeCommand                => merge.receipt; return merge
      case raw: DeltaRangerRawMaintenance.Carrier        => return raw.owner.prepare(raw.child)
      case _                                             =>
    }
    if (plan.isInstanceOf[AppendData])
      return DeltaRangerAppendV1Input.prepare(plan.asInstanceOf[AppendData], spark)
    nativeAdmission(plan, spark) match {
      case native: DeltaRangerNativeProvenance.SealedNative =>
        return DeltaRangerPhysicalPlanning.sealNative(plan, spark, native.witness)
      case DeltaRangerNativeProvenance.AnalysisOnlyNative =>
        throw new SecurityException("Pending native Delta analysis cannot execute")
      case DeltaRangerNativeProvenance.UserPlan =>
    }
    DeltaRangerNativeProvenance.continueAtPreCbo(plan, spark).foreach { witness =>
      return DeltaRangerPhysicalPlanning.sealNative(plan, spark, witness)
    }
    if (spark.sessionState.conf.adaptiveExecutionEnabled)
      throw new SecurityException("Unsupported user AQE for Delta Ranger collection")
    val owner = DeltaRangerAppendHelper
      .source(plan, spark)
      .orElse(DeltaRangerAppendCoordinator.source(plan, spark))
      .getOrElse(DeltaRangerNativeScans.consume(plan, spark))
    owner.run { operation =>
      val prepared = new PrepareDeltaScan(spark) {
        override protected def prepareDeltaScanWithoutFileSkipping(
            candidate: LogicalPlan
        ): LogicalPlan = {
          val native = super.prepareDeltaScanWithoutFileSkipping(candidate)
          def replace(node: LogicalPlan): LogicalPlan = {
            val result = node match {
              case relation: LogicalRelation =>
                relation.relation match {
                  case fs: HadoopFsRelation =>
                    fs.location match {
                      case index: TahoeLogFileIndex =>
                        val owned = DeltaRangerLogIndex.create(owner, index, operation)
                        relation.copy(relation = fs.copy(location = owned)(spark))
                      case _ => throw new SecurityException("Unqualified no-skipping native index")
                    }
                  case _ => throw new SecurityException("Unqualified no-skipping native relation")
                }
              case project: Project => project.copy(child = replace(project.child))
              case filter: Filter   => filter.copy(child = replace(filter.child))
              case join: Join => join.copy(left = replace(join.left), right = replace(join.right))
              case alias: SubqueryAlias => alias.copy(child = replace(alias.child))
              case local: LocalRelation if local.data.isEmpty => local
              case _ => throw new SecurityException("Unqualified no-skipping native transition")
            }
            result.copyTagsFrom(node)
            result
          }
          // Tahoe equality intentionally ignores authority. Generic transformUp would discard
          // an equal native index replacement; direct finite copies preserve its exact identity.
          replace(native)
        }

        override protected def getPreparedIndex(
            scan: DeltaScan,
            index: TahoeLogFileIndex
        ): PreparedDeltaFileIndex =
          DeltaRangerPreparedIndex.create(owner, index, scan, operation)

        override protected def getDeltaScanGenerator(
            index: TahoeLogFileIndex
        ): DeltaScanGenerator = {
          val generator = super.getDeltaScanGenerator(index)
          generator match {
            case snapshot: Snapshot => DeltaRangerPlanAdmission.registerSnapshot(index, snapshot)
            case transaction: OptimisticTransaction =>
              owner.requireGenerator(transaction)
              DeltaRangerPlanAdmission.registerSnapshot(index, transaction.snapshot)
            case _ =>
              throw new SecurityException("Native transactional scan preparation is not qualified")
          }
          generator
        }
      }.apply(plan)
      owner.finish(prepared, operation)
    }
  }

  case class Scan(
      relation: LogicalRelation,
      catalog: String,
      database: String,
      table: String,
      owner: Option[String]
  )

  def extract(plan: LogicalPlan, spark: SparkSession): Seq[(Scan, Set[String])] = {
    val (original, targets, operation) = PrivilegesBuilder.build(plan, spark)
    requireQuery(targets.isEmpty && operation == OperationType.QUERY)
    val tokens = mutable.Map.empty[String, Scan]
    // Only this extraction copy receives opaque identities. Execution retains the original
    // descriptors, expressions and ExprIds, including distinct occurrences of self joins.
    val extraction = plan.transformUp { case relation: LogicalRelation =>
      val scan = canonical(relation)
      val token = "delta_scan_" + UUID.randomUUID().toString.replace("-", "")
      tokens.put(token, scan)
      val descriptor = relation.catalogTable.get
      relation.copy(catalogTable =
        Some(descriptor.copy(identifier = descriptor.identifier.copy(table = token)))
      )
    }
    val (inputs, outputs, kind) = PrivilegesBuilder.build(extraction, spark)
    requireQuery(outputs.isEmpty && kind == OperationType.QUERY && tokens.nonEmpty)
    requireQuery(inputs.size == tokens.size && original.size == inputs.size)
    val seen = mutable.Set.empty[String]
    val result = inputs.toSeq.map { privilege =>
      requireQuery(privilege.privilegeObjectType == PrivilegeObjectType.TABLE_OR_VIEW)
      val scan = tokens.getOrElse(
        privilege.objectName,
        throw new SecurityException("Unknown Delta extraction resource")
      )
      requireQuery(seen.add(privilege.objectName))
      requireQuery(privilege.dbname == scan.database && privilege.owner == scan.owner)
      requireQuery(privilege.catalog.isEmpty || privilege.catalog.contains(scan.catalog))
      val columns = privilege.columns.toSet
      requireQuery(columns.nonEmpty && columns.subsetOf(scan.relation.output.map(_.name).toSet))
      // Root output is implicit demand, including passthrough relations, filters and joins.
      // Preserve expression demand for projected producers and predicate/join references.
      val usedIds = plan.output.map(_.exprId).toSet ++ plan
        .collect {
          case node if !node.isInstanceOf[LogicalRelation] =>
            node.expressions.flatMap(_.references.map(_.exprId))
        }
        .flatten
        .toSet
      val usedColumns = scan.relation.output.filter(a => usedIds(a.exprId)).map(_.name).toSet
      requireQuery(columns == usedColumns)
      scan -> columns
    }
    result
  }

  private def canonical(relation: LogicalRelation): Scan = {
    requireQuery(!relation.isStreaming)
    relation.relation match {
      case fs: HadoopFsRelation =>
        fs.location match {
          case index: TahoeLogFileIndex =>
            requireQuery(!index.isTimeTravelQuery && index.partitionFilters.isEmpty)
          case _ => throw new SecurityException("Only registered native Delta scans are admitted")
        }
      case _ => throw new SecurityException("Only registered native Delta scans are admitted")
    }
    val descriptor = relation.catalogTable.getOrElse(
      throw new SecurityException("Raw Delta paths are not admitted")
    )
    val id = descriptor.identifier
    requireQuery(
      descriptor.provider.contains("delta") && id.catalog.isDefined && id.database.isDefined
    )
    val catalog = DeltaRangerResourceKey.literal(id.catalog.get)
    val database = DeltaRangerResourceKey.literal(id.database.get)
    val table = DeltaRangerResourceKey.literal(id.table)
    val names = relation.output.map(a => DeltaRangerResourceKey.literal(a.name))
    requireQuery(names.distinct.size == names.size)
    Scan(relation, catalog, database, table, Option(descriptor.owner).filter(_.nonEmpty))
  }

  private def requireQuery(condition: Boolean): Unit = {
    if (!condition) throw new SecurityException("Unsupported or ambiguous Delta SELECT extraction")
  }
}

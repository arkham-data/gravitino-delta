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

import java.nio.file.Paths
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, DeserializeToObject, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.delta.{DeltaOptions, OptimisticTransaction, Snapshot}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.DeltaCommand
import org.apache.spark.sql.execution.LogicalRDD
import org.junit.jupiter.api.Assertions._

/** Observation-only inventory of an installed native transaction's cold state read. */
private[ranger] object NativeIdempotencyProbe {
  import InstalledAppendBoundaryProbe.{Trace, fresh, manifest}

  val TxnApp = "spark.databricks.delta.write.txnAppId"
  val TxnVersion = "spark.databricks.delta.write.txnVersion"
  val Reset = "spark.databricks.delta.write.txnVersion.autoReset.enabled"
  val ExpectedAliases: Set[String] = Set(
    "sizeInBytes",
    "numOfSetTransactions",
    "numOfFiles",
    "numOfRemoves",
    "numDeletedRecordsOpt",
    "numDeletionVectorsOpt",
    "numOfMetadata",
    "numOfProtocol",
    "setTransactions",
    "domainMetadata",
    "metadata",
    "protocol",
    "fileSizeHistogram",
    "deletedRecordCountsHistogramOpt"
  )

  final case class Case(
      spark: SparkSession,
      policy: TestDeltaRangerNativeSelect.Fixture,
      trace: Trace
  ) {
    var transaction: Option[OptimisticTransaction] = None
    val completions = new ConcurrentLinkedQueue[QueryExecution]()
    val completion = new CountDownLatch(1)
    val listenerFailures = new ConcurrentLinkedQueue[Exception]()
    var operation: Option[DeltaRangerNativeOperation] = None
    var expectedPublication: Option[DeltaRangerPolicyEngine.SnapshotStamp] = None
    var observe: LogicalPlan => Unit = (_: LogicalPlan) => ()
    var uncachedStorage: Map[String, String] = Map.empty
    val aggregates: ArrayBuffer[Aggregate] = ArrayBuffer.empty
    val bindings: ArrayBuffer[DeserializeToObject] = ArrayBuffer.empty
  }

  def withCase(name: String, preseed: Boolean = true)(body: Case => Unit): Unit =
    withFixture(name, preseed, (_, _) => (), unchanged = true)(body)

  def withHistoryCase(name: String, setup: (SparkSession, Trace) => Unit)(
      body: Case => Unit
  ): Unit =
    withFixture(name, preseed = false, setup, unchanged = false)(body)

  def computed(snapshot: Snapshot): Boolean =
    snapshot.getClass
      .getMethod("_computedStateTriggered")
      .invoke(snapshot)
      .asInstanceOf[java.lang.Boolean]
      .booleanValue()

  def otherCopyArgs(rdd: LogicalRDD): Vector[Any] =
    rdd.getClass.getMethod("otherCopyArgs").invoke(rdd).asInstanceOf[Seq[Any]].toVector

  def inventory(spark: SparkSession, trace: Trace, snapshot: Snapshot): Unit = {
    trace.record(
      "cold-inventory",
      s"snapshot=${trace.id(snapshot)} computed=${computed(snapshot)} version=${snapshot.version} nativeId=${snapshot.metadata.id} metadata=${snapshot.metadata.json} protocol=${snapshot.protocol.json} session=${trace.id(spark)} activeTxn=${trace.id(OptimisticTransaction.getActive.orNull)} operation=${trace.id(DeltaRangerNativeOperation.current())}"
    )
    trace.record(
      "cold-inventory-conf",
      spark.sessionState.conf.getAllConfs.toSeq.sorted.mkString("\n")
    )
    val subject = DeltaRangerQuerySubject.current(spark)
    trace.record("cold-inventory-subject", s"user=${subject.user()} groups=${subject.groups()}")
  }

  def inspectCold(
      c: Case,
      target: DeltaRangerRegistrationBindings.BoundAppendTarget,
      targetReceipt: DeltaRangerAppendTargetReceipt,
      sourceReceipt: DeltaRangerDecisionReceipt,
      options: Map[String, String]
  ): Boolean =
    InventoryDriver.inspect(c, target, targetReceipt, sourceReceipt, options, uncache = false)

  def inspectUncached(
      c: Case,
      target: DeltaRangerRegistrationBindings.BoundAppendTarget,
      targetReceipt: DeltaRangerAppendTargetReceipt,
      sourceReceipt: DeltaRangerDecisionReceipt
  ): Boolean =
    InventoryDriver.inspect(c, target, targetReceipt, sourceReceipt, Map.empty, uncache = true)

  def bound(c: Case): DeltaRangerRegistrationBindings.BoundAppendTarget = {
    val wrapper = NativeSelectFixture.wrapper(c.spark, "a")
    val table = wrapper.loadTable(Identifier.of(Array("db"), "t")).asInstanceOf[DeltaTableV2]
    val target = wrapper.registrationBindings().resolveAppendTarget(table)
    c.trace.record(
      "bound-target",
      s"target=${c.trace.id(target)} table=${c.trace.id(table)} descriptor=${c.trace.id(target.descriptor())} log=${c.trace.id(target.log())}"
    )
    target
  }

  def source(c: Case): DeltaRangerDecisionReceipt = {
    val analyzed = c.spark.sql("SELECT id, secret, region FROM b.db.t").queryExecution.analyzed
    val receipt = DeltaRangerDecisionReceipt.consumeOwned(analyzed, c.spark, optimized = false)
    c.trace.source(receipt)
    assertTrue(receipt.obligations.nonEmpty)
    assertTrue(receipt.obligations.exists(_.decisions.masks.nonEmpty))
    receipt
  }

  def assertSecurity(error: Throwable, message: String): Unit = {
    val causes = Iterator.iterate(error)(_.getCause).takeWhile(_ != null).toVector
    assertTrue(
      causes.exists(e =>
        e.isInstanceOf[SecurityException] && Option(e.getMessage).exists(_.contains(message))
      ),
      causes.mkString("\n")
    )
  }

  private object InventoryDriver extends DeltaCommand {
    def inspect(
        c: Case,
        target: DeltaRangerRegistrationBindings.BoundAppendTarget,
        targetReceipt: DeltaRangerAppendTargetReceipt,
        sourceReceipt: DeltaRangerDecisionReceipt,
        options: Map[String, String],
        uncache: Boolean
    ): Boolean = {
      val spark = c.spark
      val trace = c.trace
      trace.record(
        "original-receipts",
        s"targetReceipt=${trace.id(targetReceipt)} sourceReceipt=${trace.id(sourceReceipt)} options=$options"
      )
      try {
        trace.phase(spark, "entry-target") { targetReceipt.validate(spark) }
        trace.phase(spark, "entry-source") { sourceReceipt.validateAction(spark) }
        trace.phase(spark, "transaction") {
          target.log().withNewTransaction(target.table().catalogTable) { txn =>
            trace.phase(spark, "transaction-entry-target") { targetReceipt.validate(spark) }
            trace.phase(spark, "transaction-entry-source") { sourceReceipt.validateAction(spark) }
            assertTrue(OptimisticTransaction.getActive.contains(txn))
            assertSame(target.log(), txn.deltaLog)
            assertFalse(
              computed(txn.snapshot),
              "Selected transaction snapshot was already computed"
            )
            c.transaction = Some(txn)
            if (uncache) {
              trace.record(
                "uncache-before",
                s"snapshot=${trace.id(txn.snapshot)} computed=${computed(txn.snapshot)}"
              )
              txn.snapshot.uncache()
              trace.record(
                "uncache-after",
                s"snapshot=${trace.id(txn.snapshot)} computed=${computed(txn.snapshot)}"
              )
              c.uncachedStorage = manifest(
                Paths.get(NativeSelectFixture.locations("a")),
                trace.directory.resolve("uncached-baseline")
              )
            }
            inventory(spark, trace, txn.snapshot)
            assertFalse(computed(txn.snapshot), "Inventory warmed transaction state")
            trace.beforeSkip = spark.sessionState.conf.getAllConfs
            val result = trace.phase(spark, "hasBeenExecuted") {
              try
                hasBeenExecuted(
                  txn,
                  spark,
                  Some(new DeltaOptions(options, spark.sessionState.conf))
                )
              finally {
                trace.afterSkip = spark.sessionState.conf.getAllConfs
                trace.record(
                  "native-return-or-failure",
                  s"snapshot=${trace.id(txn.snapshot)} computed=${computed(txn.snapshot)}"
                )
              }
            }
            trace.skip = Some(result)
            result
          }
        }
      } finally {
        trace.account(
          Seq(
            "entry-target",
            "entry-source",
            "transaction",
            "transaction-entry-target",
            "transaction-entry-source",
            "hasBeenExecuted"
          )
        )
        trace.record("not-reached", "source-preparation;helper;commit;recache;postcommit")
        trace.observe(spark, "driver-unwound", None)
      }
    }
  }

  private def withFixture(
      name: String,
      preseed: Boolean,
      setup: (SparkSession, Trace) => Unit,
      unchanged: Boolean
  )(body: Case => Unit): Unit = {
    val trace = fresh(name)
    val policy = new TestDeltaRangerNativeSelect.Fixture
    val install = NativeSelectFixture.extension(policy)
    var current: Option[Case] = None
    var observedCase: Option[Case] = None
    val extension: SparkSessionExtensions => Unit = extensions => {
      extensions.injectResolutionRule(spark =>
        new Rule[LogicalPlan] {
          override def apply(plan: LogicalPlan): LogicalPlan = {
            current.foreach { c =>
              trace.record(
                "resolution-observed",
                s"root=${trace.id(plan)} operation=${trace.id(DeltaRangerNativeOperation.current())} activeTxn=${trace.id(OptimisticTransaction.getActive.orNull)} tree=${plan.treeString}"
              )
              val operation = DeltaRangerNativeOperation.current()
              if (operation != null && operation.transactionState()) {
                val txn = OptimisticTransaction.getActive.get
                if (c.transaction.isEmpty) {
                  assertFalse(
                    computed(txn.snapshot),
                    "Native execution entered with computed state"
                  )
                  c.transaction = Some(txn)
                  c.operation = Some(operation)
                  inventory(c.spark, trace, txn.snapshot)
                }
                c.observe(plan)
              }
              c.transaction.filter(t => OptimisticTransaction.getActive.exists(_ eq t)).foreach { txn =>
                plan.foreach {
                  case aggregate: Aggregate =>
                    if (operation != null && operation.transactionState())
                      assertFalse(
                        computed(txn.snapshot),
                        "State computed before Aggregate extraction"
                      )
                    c.aggregates += aggregate
                    trace.record(
                      "native-aggregate",
                      s"txn=${trace.id(txn)} snapshot=${trace.id(txn.snapshot)} computed=${computed(txn.snapshot)} root=${trace.id(plan)} aggregate=${trace.id(aggregate)} resolved=${aggregate.resolved} grouping=${aggregate.groupingExpressions} output=${aggregate.output} tree=${aggregate.treeString}"
                    )
                    aggregate.aggregateExpressions.zipWithIndex.foreach {
                      case (expression, ordinal) =>
                        trace.record(
                          "native-aggregate-expression",
                          s"ordinal=$ordinal class=${expression.getClass.getName} alias=${expression.name} type=${expression.dataType.json} nullable=${expression.nullable} tree=${expression.treeString} json=${expression.toJSON}"
                        )
                    }
                    aggregate.child
                      .collect { case rdd: LogicalRDD => rdd }
                      .foreach(rdd => recordRDD(c, rdd))
                  case binding: DeserializeToObject =>
                    c.bindings += binding
                    trace.record(
                      "native-encoder",
                      s"txn=${trace.id(txn)} snapshot=${trace.id(txn.snapshot)} binding=${trace.id(binding)} tree=${binding.treeString} expression=${binding.deserializer.toJSON}"
                    )
                  case _ => ()
                }
              }
            }
            plan
          }
        }
      )
      install(extensions)
    }
    try
      NativeSelectFixture.withSpark(
        spark => {
          val c = Case(spark, policy, trace)
          current = Some(c)
          observedCase = Some(c)
          spark.listenerManager.register(new QueryExecutionListener {
            override def onSuccess(name: String, qe: QueryExecution, duration: Long): Unit = {
              try {
                if (
                  qe.analyzed.exists {
                    case aggregate: Aggregate =>
                      aggregate.aggregateExpressions.map(_.name).toSet == ExpectedAliases
                    case _ => false
                  }
                ) {
                  c.completions.add(qe)
                  trace.record(
                    "native-physical-completion",
                    s"name=$name logical=${qe.analyzed.treeString} physical=${qe.executedPlan.treeString}"
                  )
                  c.completion.countDown()
                }
              } catch {
                case error: Exception => c.listenerFailures.add(error); c.completion.countDown()
              }
            }
            override def onFailure(name: String, qe: QueryExecution, error: Exception): Unit = ()
          })
          trace.activate(spark)
          trace.record(
            "jvm-runtime",
            s"javaVersion=${System.getProperty("java.version")} javaHome=${System.getProperty("java.home")} vm=${System.getProperty("java.vm.name")} spark=${spark.version} scala=${scala.util.Properties.versionNumberString}"
          )
          val installedRules = ruleManifest(spark)
          trace.record(
            "installed-rule-manifest",
            ruleManifest(spark).toSeq.sortBy(_._1).mkString("\n")
          )
          val root = Paths.get(NativeSelectFixture.locations("a")).getParent
          val before = manifest(root, trace.directory.resolve("before"))
          val publication = policy.engine.snapshotStamp()
          try body(c)
          finally {
            assertEquals(installedRules, ruleManifest(spark), "Installed rule manifest changed")
            trace.requests(policy, "policy-final")
            policy.engine.requireSnapshot(c.expectedPublication.getOrElse(publication))
            trace.record(
              "publication-after",
              s"samePublication=${c.expectedPublication.isEmpty} deliberatePublication=${c.expectedPublication.nonEmpty}"
            )
            trace.observe(spark, "case-unwound", None)
            val after = manifest(root, trace.directory.resolve("after"))
            trace.record(
              "storage-delta",
              s"added=${after.keySet -- before.keySet} removed=${before.keySet -- after.keySet} changed=${before.keySet.intersect(after.keySet).filter(k => before(k) != after(k))}"
            )
            if (unchanged)
              assertEquals(before, after, "Storage changed during cold native inventory")
            assertNull(DeltaRangerNativeOperation.current())
            assertTrue(OptimisticTransaction.getActive.isEmpty)
            trace.record("scope-cleanup", "current=null activeTxn=None")
            current = None
          }
        },
        Some(extension),
        beforeExtension = spark => {
          if (preseed)
            spark
              .sql("SELECT 4L id, 'prior' secret, 'keep' region")
              .write
              .format("delta")
              .mode("append")
              .option("txnAppId", "probe-app")
              .option("txnVersion", "1")
              .save(NativeSelectFixture.locations("a"))
          setup(spark, trace)
        }
      )
    finally {
      try
        observedCase.foreach(c =>
          assertTrue(c.listenerFailures.isEmpty, c.listenerFailures.toString)
        )
      finally { policy.engine.close(); trace.close() }
    }
  }

  private def recordRDD(c: Case, rdd: LogicalRDD): Unit = {
    val trace = c.trace
    val args = otherCopyArgs(rdd)
    trace.record(
      "native-logical-rdd",
      s"logical=${trace.id(rdd)} backing=${trace.id(rdd.rdd)} partitioningClass=${rdd.outputPartitioning.getClass.getName} partitioning=${rdd.outputPartitioning} partitions=${rdd.outputPartitioning.numPartitions} ordering=${rdd.outputOrdering} streaming=${rdd.isStreaming} productArity=${rdd.productArity} otherCopyArgs=${args.zipWithIndex.map { case (a, i) => s"$i:${trace.id(a.asInstanceOf[AnyRef])}:$a" }}"
    )
    args.zipWithIndex.foreach { case (arg, index) =>
      val detail = arg match {
        case session: SparkSession =>
          s"sameSession=${session eq c.spark} identity=${trace.id(session)}"
        case option: Option[_] =>
          s"defined=${option.isDefined} payload=${option.map(p => trace.id(p.asInstanceOf[AnyRef]))} value=$option"
        case other => s"unknown=${trace.id(other.asInstanceOf[AnyRef])} value=$other"
      }
      trace.record("native-rdd-copy-arg", s"index=$index $detail")
    }
    rdd.output.zipWithIndex.foreach { case (attribute, index) =>
      trace.record(
        "native-rdd-output",
        s"ordinal=$index class=${attribute.getClass.getName} name=${attribute.name} exprId=${attribute.exprId.id} jvmId=${attribute.exprId.jvmId} qualifier=${attribute.qualifier.toVector} type=${attribute.dataType.json} nullable=${attribute.nullable} metadata=${attribute.metadata.json}"
      )
      assertEquals(classOf[AttributeReference], attribute.getClass)
    }
  }

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

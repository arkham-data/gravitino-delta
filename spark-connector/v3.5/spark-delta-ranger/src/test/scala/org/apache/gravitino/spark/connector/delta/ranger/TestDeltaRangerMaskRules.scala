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

import java.security.PrivilegedExceptionAction

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class TestDeltaRangerMaskRules {
  @Test
  def quotedColumnsAndSelfJoinAliasesKeepOriginalIdentity(): Unit = {
    NativeSelectFixture.withSpark { spark =>
      val f = new TestDeltaRangerNativeSelect.Fixture
      try {
        val rule = new DeltaRangerReadRule(spark, _ => f.engine)
        val plan = spark
          .sql(
            "SELECT x.secret AS first, y.secret AS second FROM a.db.t x JOIN a.db.t y ON x.id = y.id WHERE y.region = 'keep'"
          )
          .queryExecution
          .analyzed
        assertEquals(
          Set(Row("a-one-masked", "a-one-masked"), Row(null, null)),
          TestDeltaRangerNativeSelect.rows(spark, rule(plan)).toSet
        )
        val scans = DeltaRangerReadRule.extract(plan, spark)
        assertEquals(2, scans.size)
        assertTrue(
          scans.head._1.relation.output
            .map(_.exprId)
            .toSet
            .intersect(scans.last._1.relation.output.map(_.exprId).toSet)
            .isEmpty
        )
        f.maskColumn = "odd` name"; f.customCatalog = "q"
        assertEquals(
          Set(Row("q-one-masked")),
          rows(spark, rule, "SELECT x.`odd`` name` AS `quoted alias` FROM q.db.t x WHERE x.id = 1")
        )
        f.maskType = "MASK_NULL"
        assertEquals(
          Set(Row(null)),
          rows(spark, rule, "SELECT `odd`` name` FROM q.db.t WHERE id = 1")
        )
      } finally f.engine.close()
    }
  }

  @Test
  def installedExtensionExecutesFreshNamedScansAndInternalNativeReconstruction(): Unit = {
    val f = new TestDeltaRangerNativeSelect.Fixture
    val nativeUdfPlans = scala.collection.mutable.ArrayBuffer.empty[LogicalPlan]
    val nativeBindings = scala.collection.mutable.Set.empty[String]
    try {
      NativeSelectFixture.withSpark(
        spark => {
          val rules = spark.sessionState.optimizer.preCBORules.map(_.getClass.getName)
          val companion = rules.indexWhere(_.startsWith(classOf[DeltaRangerSparkExtension].getName))
          val native =
            rules.indexOf(classOf[org.apache.spark.sql.delta.stats.PrepareDeltaScan].getName)
          assertTrue(companion >= 0 && native > companion, rules.mkString(","))
          assertEquals(
            Set(Row("a-one-masked"), Row(null)),
            spark.sql("SELECT secret FROM a.db.t").collect().toSet
          )
          Seq(("a", "b"), ("b", "a")).foreach { case (left, right) =>
            assertEquals(
              Set(Row("a-one-masked", null), Row(null, null)),
              spark
                .sql(
                  s"SELECT a.secret, b.secret FROM $left.db.t $left JOIN $right.db.t $right ON a.id = b.id"
                )
                .collect()
                .toSet
            )
          }
          f.denied.add("a:region"); f.events.clear()
          assertRejected(() =>
            spark.sql("SELECT secret FROM a.db.t WHERE region = 'keep'").collect()
          )
          assertFalse(f.events.contains("mask")); assertFalse(f.events.contains("filter"))
          f.denied.clear()
          spark.udf.register("user_mask", (value: String) => value)
          assertRejected(() => spark.sql("SELECT user_mask(secret) FROM a.db.t").collect())
          val optimized = spark.sql("SELECT secret FROM a.db.t").queryExecution.optimizedPlan
          assertEquals(
            optimized,
            new org.apache.spark.sql.delta.stats.PrepareDeltaScan(spark).apply(optimized)
          )
          assertThrows(
            classOf[SecurityException],
            () =>
              spark.sql(s"SELECT * FROM delta.`${NativeSelectFixture.locations("a")}`").collect()
          )
          assertRejected(() =>
            spark
              .sql(s"SELECT * FROM json.`${NativeSelectFixture.locations("a")}/_delta_log`")
              .collect()
          )
          assertRejected(() =>
            spark.sql(s"SELECT * FROM parquet.`${NativeSelectFixture.locations("a")}`").collect()
          )
          assertThrows(
            classOf[SecurityException],
            () => spark.sql("DELETE FROM a.db.t WHERE id = 1").collect()
          )
          assertRejected(() =>
            spark
              .sql(
                s"SELECT a.secret FROM a.db.t a JOIN parquet.`${NativeSelectFixture.locations("b")}` b ON a.id = b.id"
              )
              .collect()
          )
          val nextSession = spark.newSession()
          assertEquals(
            Set(Row("a-one-masked"), Row(null)),
            nextSession.sql("SELECT secret FROM a.db.t").collect().toSet
          )
          val indices = spark
            .sql("SELECT a.secret, b.secret FROM a.db.t a JOIN b.db.t b ON a.id = b.id")
            .queryExecution
            .analyzed
            .collect { case relation: org.apache.spark.sql.execution.datasources.LogicalRelation =>
              relation.relation
                .asInstanceOf[org.apache.spark.sql.execution.datasources.HadoopFsRelation]
                .location
                .asInstanceOf[org.apache.spark.sql.delta.files.TahoeLogFileIndex]
            }
          assertEquals(
            Set(
              "org.apache.spark.sql.delta.actions.SingleAction",
              "org.apache.spark.sql.delta.actions.AddFile",
              "org.apache.spark.sql.Row"
            ),
            nativeBindings.toSet
          )
          assertTrue(nativeUdfPlans.nonEmpty)
          val qualified = nativeUdfPlans.toVector
          DeltaRangerPlanAdmission.withNativeScans(indices) {
            indices.foreach(index =>
              DeltaRangerPlanAdmission.registerSnapshot(index, index.getSnapshot)
            )
            qualified.foreach { plan =>
              assertTrue(DeltaRangerPlanAdmission.internalMetadata(plan))
              val differentFunction = plan.transformAllExpressions {
                case udf: org.apache.spark.sql.catalyst.expressions.ScalaUDF =>
                  val replacement: AnyRef =
                    if (udf.children.size == 1) ((x: String) => x)
                    else
                      (
                          (
                              include: Boolean,
                              size: java.lang.Long,
                              logical: java.lang.Long,
                              rows: java.lang.Long
                          ) => include
                      )
                  udf.copy(function = replacement)
              }
              assertFalse(
                DeltaRangerPlanAdmission.internalMetadata(differentFunction),
                "Same-signature user UDF cannot enter native metadata admission"
              )
              val wrongArguments = plan.transformAllExpressions {
                case udf: org.apache.spark.sql.catalyst.expressions.ScalaUDF =>
                  val position = if (udf.children.size == 1) 0 else 1
                  val replacement = org.apache.spark.sql.catalyst.expressions.Literal
                    .create(null, udf.children(position).dataType)
                  udf.copy(children = udf.children.updated(position, replacement))
              }
              assertFalse(
                DeltaRangerPlanAdmission.internalMetadata(wrongArguments),
                "Native function with unrelated arguments must reject"
              )
            }
          }
          qualified.foreach(plan => assertFalse(DeltaRangerPlanAdmission.internalMetadata(plan)))
          f.maskColumn = "odd` name"; f.customCatalog = "q"
          assertEquals(
            Set(Row("q-one-masked")),
            spark.sql("SELECT `odd`` name` FROM q.db.t WHERE id = 1").collect().toSet
          )
        },
        Some(extensions => {
          extensions.injectResolutionRule(_ =>
            new org.apache.spark.sql.catalyst.rules.Rule[LogicalPlan] {
              override def apply(plan: LogicalPlan): LogicalPlan = {
                if (
                  DeltaRangerPlanAdmission.internalMetadata(plan) &&
                  plan.exists(
                    _.expressions.exists(
                      _.exists(_.isInstanceOf[org.apache.spark.sql.catalyst.expressions.ScalaUDF])
                    )
                  )
                ) {
                  nativeUdfPlans += plan
                }
                plan match {
                  case binding: org.apache.spark.sql.catalyst.plans.logical.DeserializeToObject
                      if binding.resolved =>
                    binding.deserializer.dataType match {
                      case target: org.apache.spark.sql.types.ObjectType =>
                        nativeBindings += target.cls.getName
                      case _ =>
                    }
                  case _ =>
                }
                plan
              }
            }
          )
          new DeltaRangerSparkExtension((_, _) => f.engine)(extensions)
        })
      )
    } finally f.engine.close()
  }

  @Test
  def nativeRowsKeepCatalogFiltersMasksAndJoinIdentityInBothOrders(): Unit = {
    NativeSelectFixture.withSpark { spark =>
      val f = new TestDeltaRangerNativeSelect.Fixture
      try {
        val rule = new DeltaRangerReadRule(spark, _ => f.engine)
        Seq(("a", "b"), ("b", "a")).foreach { case (left, right) =>
          val plan = spark
            .sql(
              s"SELECT a.secret, b.secret FROM $left.db.t $left JOIN $right.db.t $right ON a.id = b.id"
            )
            .queryExecution
            .analyzed
          val rewritten = rule(plan)
          assertEquals(
            Set(Row("a-one-masked", null), Row(null, null)),
            TestDeltaRangerNativeSelect.rows(spark, rewritten).toSet
          )
          val decisions = f.events.size()
          assertEquals(rewritten, rule(rewritten))
          assertEquals(decisions, f.events.size())
        }
        assertEquals(
          Set(Row("a-one-masked")),
          rows(spark, rule, "SELECT secret FROM a.db.t WHERE secret = 'a-one-masked'")
        )
        assertEquals(
          Set(Row(1L, "a-one-masked", "keep"), Row(3L, null, "keep")),
          rows(spark, rule, "SELECT * FROM a.db.t")
        )
      } finally f.engine.close()
    }
  }

  @Test
  def originalDeniedProjectionPredicateAndJoinColumnsRejectBeforeMasks(): Unit = {
    NativeSelectFixture.withSpark { spark =>
      val f = new TestDeltaRangerNativeSelect.Fixture
      try {
        val rule = new DeltaRangerReadRule(spark, _ => f.engine)
        Seq(
          ("a:secret", "SELECT secret FROM a.db.t"),
          ("a:region", "SELECT secret FROM a.db.t WHERE region = 'keep'"),
          ("b:id", "SELECT a.secret FROM a.db.t a JOIN b.db.t b ON a.id = b.id"),
          ("a:region", "SELECT * FROM a.db.t")
        ).foreach { case (denied, query) =>
          f.denied.clear(); f.denied.add(denied); f.events.clear()
          assertThrows(classOf[SecurityException], () => rows(spark, rule, query))
          assertFalse(f.events.contains("mask")); assertFalse(f.events.contains("filter"))
        }
      } finally f.engine.close()
    }
  }

  @Test
  def unsupportedPathsShapesMasksAndNewSubjectReject(): Unit = {
    NativeSelectFixture.withSpark { spark =>
      val f = new TestDeltaRangerNativeSelect.Fixture
      try {
        val rule = new DeltaRangerReadRule(spark, _ => f.engine)
        Seq(
          s"SELECT * FROM delta.`${NativeSelectFixture.locations("a")}`",
          "SELECT count(*) FROM a.db.t",
          "SELECT 1 FROM a.db.t",
          "SELECT upper(secret) FROM a.db.t GROUP BY secret"
        ).foreach { query =>
          assertThrows(classOf[SecurityException], () => rows(spark, rule, query))
        }
        f.maskType = "MASK_HASH"
        assertThrows(
          classOf[SecurityException],
          () => rows(spark, rule, "SELECT secret FROM a.db.t")
        )
        f.maskType = "CUSTOM"; f.mask = "upper(region)"
        assertThrows(
          classOf[SecurityException],
          () => rows(spark, rule, "SELECT secret FROM a.db.t")
        )
        f.mask = "upper("
        assertThrows(
          classOf[SecurityException],
          () => rows(spark, rule, "SELECT secret FROM a.db.t")
        )
        f.mask = "upper({col})"
        val rewritten = rule(spark.sql("SELECT secret FROM a.db.t").queryExecution.analyzed)
        val denied = UserGroupInformation.createUserForTesting("denied-user", Array.empty[String])
        denied.doAs(new PrivilegedExceptionAction[Unit] {
          override def run(): Unit = {
            assertThrows(classOf[SecurityException], () => rule(rewritten))
            assertThrows(
              classOf[SecurityException],
              () => rows(spark, rule, "SELECT secret FROM a.db.t")
            )
          }
        })
      } finally f.engine.close()
    }
  }

  private def assertRejected(action: () => Any): Unit = {
    val failure = assertThrows(classOf[Exception], () => { action(); () })
    var cause: Throwable = failure
    while (cause != null && !cause.isInstanceOf[SecurityException]) cause = cause.getCause
    assertNotNull(cause, "Spark must expose the admission rejection in its exception chain")
  }

  private def rows(spark: SparkSession, rule: DeltaRangerReadRule, sql: String): Set[Row] =
    TestDeltaRangerNativeSelect.rows(spark, rule(spark.sql(sql).queryExecution.analyzed)).toSet
}

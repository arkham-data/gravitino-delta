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
import java.nio.file.{Files, Paths}
import java.time.Instant
import java.util.Collections
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions.{col, lit}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class TestDeltaRangerReadOperators {

  @Test
  def generatedGroupingCannotBorrowAggregateResultIdentity(): Unit =
    DeltaRangerReadParityFixture.withOperators { (spark, _) =>
      import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference}
      import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
      import org.apache.spark.sql.execution.aggregate.HashAggregateExec
      val frame = spark.sql(
        "SELECT count(*) AS n FROM (SELECT DISTINCT email, concat(email, '!') AS second FROM a.db.t)"
      )
      val logical = frame.queryExecution.optimizedPlan
      val resultId = logical
        .flatMap(_.expressions.flatMap(_.collect { case aggregate: AggregateExpression =>
          aggregate.resultId
        }))
        .head
      val candidate = DeltaRangerPhysicalPlanning.lower(spark, logical)
      val prepared = DeltaRangerNativeScans.prepared(logical)
      val witness = DeltaRangerPhysicalProvenance.user(candidate, logical, prepared, spark)
      val generatedId = DeltaRangerPhysicalProvenance
        .nodes(candidate)
        .collect { case aggregate: HashAggregateExec =>
          aggregate.groupingExpressions.collect { case alias: Alias => alias.exprId }
        }
        .flatten
        .head
      val changed = candidate.transformUp { case node =>
        node.transformExpressionsUp {
          case alias: Alias if alias.exprId == generatedId =>
            Alias(alias.child, alias.name)(exprId = resultId)
          case attribute: AttributeReference if attribute.exprId == generatedId =>
            attribute.withExprId(resultId)
        }
      }
      assertThrows(classOf[SecurityException], () => witness.verify(changed))
    }

  @Test
  def columnlessSameIndexSelfJoinKeepsDistinctOccurrences(): Unit =
    DeltaRangerReadParityFixture.withOperators { (spark, policy) =>
      policy.filter = null
      val base = spark.table("a.db.t")
      val joined = base.join(base, lit(true), "inner")
      val frame = joined.selectExpr("count(1) AS n")
      assertEquals(
        Vector(("a.db.t", Set.empty[String]), ("a.db.t", Set.empty[String])),
        DeltaRangerReadParityFixture.demands(frame)
      )
      val prepared = DeltaRangerNativeScans.prepared(frame.queryExecution.optimizedPlan)
      assertEquals(2, prepared.occurrences.size)
      assertEquals(2, prepared.occurrences.map(_.obligation).distinct.size)
      assertEquals(Seq(Row(25L)), frame.collect().toSeq)
      assertEquals(25L, joined.count())
      record("columnless-selfjoin", frame)
      sys.env.get("READ_ACTION_EVIDENCE").foreach { directory =>
        Files.write(
          Paths.get(
            directory,
            "columnless-occurrences-" + spark.conf.get("spark.sql.adaptive.enabled") + ".txt"
          ),
          prepared.occurrences
            .map(o =>
              "original=" + o.obligation.scan.relation.output
                .map(_.exprId) + "; prepared=" + o.relation.output.map(_.exprId)
            )
            .mkString("\n")
            .getBytes(StandardCharsets.UTF_8)
        )
      }
      policy.denied.add("a:*")
      assertThrows(classOf[SecurityException], () => frame.collect())
    }

  @Test
  def hiddenPredicatesAndNestedCountsRetainOriginalDemands(): Unit =
    DeltaRangerReadParityFixture.withOperators { (spark, policy) =>
      policy.denied.add("a:secret")
      val hidden =
        spark.sql("SELECT count(*) FROM (SELECT 1 AS one FROM a.db.t WHERE region = 'east')")
      assertEquals(Vector(("a.db.t", Set("region"))), DeltaRangerReadParityFixture.demands(hidden))
      assertEquals(Seq(Row(4L)), hidden.collect().toSeq)
      assertEquals(4L, spark.table("a.db.t").filter("region = 'east'").select(lit(1)).count())
      assertEquals(
        4L,
        spark
          .sql("SELECT count(*) FROM (SELECT 1 AS one FROM a.db.t a JOIN b.db.t b ON a.id = b.id)")
          .collect()
          .head
          .getLong(0)
      )
      val nested = spark.sql("SELECT count(*) FROM (SELECT count(email) AS n FROM a.db.t)")
      assertEquals(Vector(("a.db.t", Set("email"))), DeltaRangerReadParityFixture.demands(nested))
      assertEquals(Seq(Row(1L)), nested.collect().toSeq)
      policy.denied.add("a:region")
      assertThrows(classOf[SecurityException], () => hidden.collect())
      policy.denied.add("a:email")
      assertThrows(
        classOf[SecurityException],
        () => spark.sql("SELECT count(*) FROM (SELECT count(email) AS n FROM a.db.t)").collect()
      )
    }

  @Test
  def sortContinuationRequiresTheCompleteOriginalRecipe(): Unit =
    DeltaRangerReadParityFixture.withOperators { (spark, policy) =>
      import org.apache.spark.sql.catalyst.expressions.{Literal, Descending, NullsLast}
      import org.apache.spark.sql.catalyst.plans.logical.{Filter, Sort}
      val frame = spark.table("a.db.t").orderBy("email", "id").select("id")
      val issued = frame.queryExecution.normalized
      val detached = issued.collectFirst { case sort: Sort => sort }.get
      assertThrows(
        classOf[SecurityException],
        () => DeltaRangerDecisionReceipt.consumeOwned(detached, spark, optimized = false)
      )
      assertThrows(
        classOf[SecurityException],
        () =>
          DeltaRangerOrdinaryRead.stage(
            detached,
            spark,
            scan => NativeSelectFixture.wrapper(spark, scan.catalog)
          )
      )
      val prefix = DeltaRangerDecisionReceipt.nativePrefix(issued.clone(), spark)
      val receipt = DeltaRangerDecisionReceipt.consumeOwned(prefix, spark, optimized = true)
      assertThrows(
        classOf[SecurityException],
        () => DeltaRangerDecisionReceipt.consumeOwned(prefix.children.head, spark, optimized = true)
      )
      Seq[LogicalPlan](
        prefix.transformUp { case sort: Sort =>
          sort.copy(order = sort.order.map(_.copy(direction = Descending)))
        },
        prefix.transformUp { case sort: Sort =>
          sort.copy(order = sort.order.map(_.copy(nullOrdering = NullsLast)))
        },
        prefix.transformUp { case filter: Filter => filter.copy(condition = Literal.TrueLiteral) }
      ).foreach(changed =>
        assertThrows(
          classOf[SecurityException],
          () => DeltaRangerDecisionReceipt.consumeOwned(changed, spark, optimized = true)
        )
      )
      policy.denied.add("a:email")
      assertThrows(classOf[SecurityException], () => receipt.validateAction(spark))
    }

  @Test
  def eliminatedDerivationsRejectChangedCountOutputAndRows(): Unit =
    DeltaRangerReadParityFixture.withOperators { (spark, policy) =>
      import org.apache.spark.sql.catalyst.InternalRow
      import org.apache.spark.sql.catalyst.expressions.{Alias, Literal}
      import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LocalRelation}
      val frame = spark.sql("SELECT count(id) AS n FROM a.db.t WHERE false")
      val logical = frame.queryExecution.optimizedPlan
      val prepared = DeltaRangerNativeScans.prepared(logical)
      assertEquals(Seq(Row(0L)), frame.collect().toSeq)
      val changedCount = logical.transformUp { case aggregate: Aggregate =>
        aggregate.copy(aggregateExpressions =
          Seq(Alias(Literal(7L), "n")(exprId = aggregate.output.head.exprId))
        )
      }
      assertThrows(classOf[SecurityException], () => prepared.verifyExecutable(changedCount, spark))
      val changedRows = logical.transformUp { case local: LocalRelation =>
        local.copy(data = Seq(InternalRow.empty))
      }
      assertThrows(classOf[SecurityException], () => prepared.verifyExecutable(changedRows, spark))
      val changedOutput = logical.transformAllExpressions { case alias: Alias =>
        Alias(alias.child, "changed")(exprId = alias.exprId)
      }
      assertThrows(
        classOf[SecurityException],
        () => prepared.verifyExecutable(changedOutput, spark)
      )
      policy.denied.add("a:id")
      assertThrows(classOf[SecurityException], () => frame.collect())
    }

  @Test
  def nonnullableSourceStillCountsNullMaskedValues(): Unit = {
    val policy = new TestDeltaRangerNativeSelect.Fixture
    policy.customCatalog = "b"; policy.filter = "region = 'east'"; policy.maskColumn = "unused"
    try
      NativeSelectFixture.withSpark(
        spark => {
          val location = NativeSelectFixture.locations("b")
          val snapshot =
            DeltaLog.forTable(spark, location).unsafeVolatileSnapshot
          assertFalse(snapshot.schema("email").nullable)
          assertEquals(4L, spark.sql("SELECT count(email) FROM b.db.t").collect().head.getLong(0))
          policy.maskColumn = "email"; policy.maskType = "MASK_NULL"
          assertEquals(0L, spark.sql("SELECT count(email) FROM b.db.t").collect().head.getLong(0))
          assertEquals(
            Seq(Row(0L)),
            spark
              .table("b.db.t")
              .selectExpr("email AS renamed")
              .selectExpr("renamed AS e")
              .selectExpr("count(e)")
              .collect()
              .toSeq
          )
        },
        Some(NativeSelectFixture.extension(policy)),
        beforeExtension = spark => {
          val location = NativeSelectFixture.locations("b") + "-nonnull"
          NativeSelectFixture.locations = NativeSelectFixture.locations.updated("b", location)
          spark
            .sql(
              s"CREATE TABLE delta.`$location` (id BIGINT, region STRING, email STRING NOT NULL, secret STRING) USING delta"
            )
            .collect()
          spark
            .sql(
              s"INSERT INTO delta.`$location` VALUES (1L,'east','z','s1'), (2L,'east','a','s2'), (3L,'east','c','s3'), (4L,'west','hidden','s4'), (5L,'east','b','s5')"
            )
            .collect()
        }
      )
    finally policy.engine.close()
  }

  @Test
  def actualRangerWildcardDecisionsRemainAuthoritative(): Unit = {
    val policy = new DeltaRangerCdfPolicyFixture
    policy.withSpark(DeltaRangerReadParityFixture.seedOperators) { spark =>
      policy.publish(granted = Seq("id"), filter = Some("region = 'east'"))
      assertEquals(4L, spark.table("a.db.t").select("id").count())
      assertThrows(classOf[SecurityException], () => spark.table("a.db.t").select("*").count())
      assertThrows(classOf[SecurityException], () => spark.table("a.db.t").count())
      policy.publish(denied = Set("email"), filter = Some("region = 'east'"))
      val key = DeltaRangerResourceKey
        .select("a", "db", "t", Collections.emptySet[String](), null)
        .iterator()
        .next()
      val wildcardAllowed =
        try {
          policy.engine.authorize(
            DeltaRangerQuerySubject.current(spark),
            key,
            Instant.now()
          )
          true
        } catch { case _: SecurityException => false }
      if (wildcardAllowed) assertEquals(4L, spark.table("a.db.t").count())
      else assertThrows(classOf[SecurityException], () => spark.table("a.db.t").count())
      assertThrows(
        classOf[SecurityException],
        () => spark.sql("SELECT count(email) FROM a.db.t").collect()
      )
      sys.env.get("READ_ACTION_EVIDENCE").foreach { directory =>
        Files.write(
          Paths.get(directory, "ranger-wildcard-decisions.txt"),
          ("Ranger 2.8.0\nid-only grant: id count allowed; explicit star and column=* denied\nwildcard grant plus email deny: column=* allowed=" + wildcardAllowed + "; email denied\n")
            .getBytes(StandardCharsets.UTF_8)
        )
      }
    }
  }

  @Test
  def ordinaryOperatorsCannotBecomeAppendSources(): Unit =
    DeltaRangerReadParityFixture.withOperators { (spark, _) =>
      import scala.collection.JavaConverters._
      val log = Paths.get(NativeSelectFixture.locations("a"), "_delta_log")
      def files(): Set[String] = {
        val stream = Files.list(log)
        try stream.iterator().asScala.map(_.getFileName.toString).toSet
        finally stream.close()
      }
      val before = files()
      Seq[DataFrame => DataFrame](
        _.limit(1),
        _.distinct(),
        _.orderBy("id")
      ).foreach { operator =>
        val failure = assertThrows(
          classOf[SecurityException],
          () => operator(spark.table("b.db.t")).write.mode("append").insertInto("a.db.t")
        )
        assertTrue(
          failure.getMessage.contains("Unqualified Delta Ranger route"),
          failure.getMessage
        )
        assertEquals(before, files())
      }
    }

  @Test
  def unfilteredColumnlessCountsUseOwnedScans(): Unit = DeltaRangerReadParityFixture.withOperators {
    (spark, _) =>
      val frame = spark.sql("SELECT count(*) AS n FROM b.db.t")
      assertEquals(Seq(Row(5L)), frame.collect().toSeq)
      record("unfiltered-count", frame)
      assertTrue(
        frame.queryExecution.optimizedPlan.exists(
          _.isInstanceOf[LogicalRelation]
        )
      )
  }

  @Test
  def generatedGroupingPreservesExactSemanticsAndWiring(): Unit =
    DeltaRangerReadParityFixture.withOperators { (spark, _) =>
      import org.apache.spark.sql.catalyst.expressions.{
        Alias,
        AttributeReference,
        Literal,
        NamedExpression
      }
      import org.apache.spark.sql.execution.aggregate.HashAggregateExec
      val frame = spark.sql("SELECT DISTINCT email, concat(email, '!') AS second FROM a.db.t")
      val logical = frame.queryExecution.optimizedPlan
      val prepared = DeltaRangerNativeScans.prepared(logical)
      val candidate = DeltaRangerPhysicalPlanning.lower(spark, logical)
      val witness = DeltaRangerPhysicalProvenance.user(candidate, logical, prepared, spark)
      witness.verify(DeltaRangerPhysicalPlanning.lower(spark, logical))
      val ids = DeltaRangerPhysicalProvenance
        .nodes(candidate)
        .collect { case aggregate: HashAggregateExec =>
          aggregate.groupingExpressions.collect { case alias: Alias =>
            alias.exprId
          }
        }
        .flatten
      assertEquals(2, ids.distinct.size)
      def changed(
          rewrite: PartialFunction[
            Expression,
            Expression
          ]
      ) =
        candidate.transformUp { case node => node.transformExpressionsUp(rewrite) }
      val semantic = changed {
        case literal: Literal if literal.value != null && literal.value.toString == "M!" =>
          Literal("changed")
      }
      assertThrows(classOf[SecurityException], () => witness.verify(semantic))
      val broken = changed {
        case attr: AttributeReference if attr.exprId == ids.head =>
          attr.withExprId(NamedExpression.newExprId)
      }
      assertThrows(classOf[SecurityException], () => witness.verify(broken))
      val swapped = changed {
        case attr: AttributeReference if ids.contains(attr.exprId) =>
          attr.withExprId(if (attr.exprId == ids.head) ids.last else ids.head)
      }
      assertThrows(classOf[SecurityException], () => witness.verify(swapped))
      val original = changed {
        case attr: AttributeReference if attr.name == "region" =>
          attr.withExprId(NamedExpression.newExprId)
      }
      assertThrows(classOf[SecurityException], () => witness.verify(original))
    }

  @Test
  def predicateAndJoinCountsKeepOnlyAuthoredDemand(): Unit =
    DeltaRangerReadParityFixture.withOperators { (spark, policy) =>
      Seq("email", "secret").foreach(c => {
        policy.denied.add("a:" + c); policy.denied.add("b:" + c)
      })
      assertEquals(
        4L,
        spark.sql("SELECT count(*) FROM a.db.t WHERE region = 'east'").collect().head.getLong(0)
      )
      assertEquals(
        4L,
        spark
          .sql("SELECT count(*) FROM a.db.t a JOIN b.db.t b ON a.id = b.id")
          .collect()
          .head
          .getLong(0)
      )
      policy.denied.add("b:id")
      assertThrows(
        classOf[SecurityException],
        () =>
          spark
            .sql("SELECT count(*) FROM a.db.t a JOIN b.db.t b ON a.id = b.id WHERE false")
            .collect()
      )
    }

  @Test
  def countUsesGovernedValues(): Unit = DeltaRangerReadParityFixture.withOperators {
    (spark, policy) =>
      val rowCount = spark.table("a.db.t").count()
      assertEquals(4L, rowCount)
      assertEquals(4, spark.table("a.db.t").select(lit(1)).collect().length)
      assertEquals(4L, spark.table("a.db.t").select(lit(1)).count())
      policy.maskColumn = "unused"
      val unmaskedEmailCount =
        spark.sql("SELECT count(email) FROM a.db.t").collect().head.getLong(0)
      assertEquals(3L, unmaskedEmailCount)
      policy.maskColumn = "email"
      val constantMaskedEmailCount =
        spark.sql("SELECT count(email) FROM a.db.t").collect().head.getLong(0)
      assertEquals(4L, constantMaskedEmailCount)
      assertEquals(
        Seq(Row(4L, 4L, 4L)),
        spark
          .sql("SELECT count(*) AS n, count(1) AS ones, count(email) AS emails FROM a.db.t")
          .collect()
          .toSeq
      )
      policy.maskType = "MASK_NULL"
      val nullMaskedEmailCount =
        spark.sql("SELECT count(email) FROM a.db.t").collect().head.getLong(0)
      assertEquals(0L, nullMaskedEmailCount)
      assertEquals(1L, spark.table("a.db.t").select("email").distinct().count())
  }

  @Test
  def distinctAndOrderingUseMasks(): Unit = DeltaRangerReadParityFixture.withOperators {
    (spark, policy) =>
      val constantDistinctRows = spark.sql("SELECT DISTINCT email FROM a.db.t").collect().toSeq
      assertEquals(Seq(Row("M")), constantDistinctRows)
      assertEquals(
        constantDistinctRows,
        spark.table("a.db.t").select("email").distinct().collect().toSeq
      )
      val maskedEmailThenIdOrder = spark
        .table("a.db.t")
        .orderBy(col("email").asc_nulls_first, col("id"))
        .select("id")
        .collect()
        .map(_.getLong(0))
        .toSeq
      assertEquals(Seq(1L, 2L, 3L, 5L), maskedEmailThenIdOrder)
      val topN = spark.sql("SELECT id FROM a.db.t ORDER BY email DESC NULLS LAST, id ASC LIMIT 2")
      val maskedOrderedLimitTwo = topN
        .collect()
        .map(_.getLong(0))
        .toSeq
      assertEquals(Seq(1L, 2L), maskedOrderedLimitTwo)
      record("masked-topn", topN)
      val unordered = spark.table("a.db.t").select("id").limit(2).collect().map(_.getLong(0)).toSeq
      assertEquals(2, unordered.size)
      assertTrue(unordered.forall(Set(1L, 2L, 3L, 5L)))
      Seq(col("email").asc_nulls_last, col("email").desc_nulls_first).foreach { order =>
        assertEquals(
          Seq(1L, 2L, 3L, 5L),
          spark
            .table("a.db.t")
            .orderBy(order, col("id"))
            .select("id")
            .collect()
            .map(_.getLong(0))
            .toSeq
        )
      }
      policy.maskType = "MASK_NULL"
      val nullDistinctRows = spark.sql("SELECT DISTINCT email FROM a.db.t").collect().toSeq
      assertEquals(Seq(Row(null)), nullDistinctRows)
      assertEquals(
        Seq(1L, 2L, 3L, 5L),
        spark
          .table("a.db.t")
          .orderBy(col("email").desc_nulls_last, col("id"))
          .select("id")
          .collect()
          .map(_.getLong(0))
          .toSeq
      )
      val composed = spark.table("a.db.t").select("id").limit(3).limit(1).collect()
      assertEquals(1, composed.length)
      assertTrue(Set(1L, 2L, 3L, 5L)(composed.head.getLong(0)))
  }

  @Test
  def emptyAndPrunedCountsRetainAuthority(): Unit = DeltaRangerReadParityFixture.withOperators {
    (spark, policy) =>
      val emptyCount = spark.sql("SELECT count(*) FROM a.db.t WHERE false")
      sys.env.get("READ_ACTION_EVIDENCE").foreach { directory =>
        Files.write(
          Paths.get(directory, "empty-count-prefix.txt"),
          DeltaRangerDecisionReceipt
            .nativePrefix(emptyCount.queryExecution.normalized.clone(), spark)
            .treeString
            .getBytes(StandardCharsets.UTF_8)
        )
      }
      val countAfterFalsePredicate = emptyCount.collect().toSeq
      record("empty-count", emptyCount)
      assertEquals(Seq(Row(0L)), countAfterFalsePredicate)
      assertEquals(
        Seq(Row(0L)),
        spark.sql("SELECT count(*) FROM a.db.t WHERE id > 999").collect().toSeq
      )
      assertEquals(0, spark.table("a.db.t").select("id").limit(0).collect().length)
      assertEquals(0L, spark.table("a.db.t").select("id").limit(0).count())
      policy.denied.add("a:id")
      assertThrows(
        classOf[SecurityException],
        () => spark.table("a.db.t").select("id").filter("false").count()
      )
      assertThrows(
        classOf[SecurityException],
        () => spark.table("a.db.t").select("id").limit(0).collect()
      )
  }

  @Test
  def countRetainsOriginalDemand(): Unit = DeltaRangerReadParityFixture.withOperators {
    (spark, policy) =>
      Seq("email", "region", "secret", "*").foreach(c => policy.denied.add("a:" + c))
      assertEquals(4L, spark.table("a.db.t").select("id").count())
      assertEquals(4L, spark.table("a.db.t").select("id").distinct().count())
      assertThrows(
        classOf[SecurityException],
        () => spark.table("a.db.t").distinct().select("id").collect()
      )
      assertThrows(
        classOf[SecurityException],
        () => spark.sql("SELECT id FROM (SELECT DISTINCT * FROM a.db.t)").collect()
      )
      assertThrows(classOf[SecurityException], () => spark.table("a.db.t").select("*").count())
      assertThrows(classOf[SecurityException], () => spark.table("a.db.t").count())
      assertThrows(classOf[SecurityException], () => spark.table("a.db.t").select(lit(1)).collect())
      assertThrows(
        classOf[SecurityException],
        () => spark.table("a.db.t").filter("region = 'east'").select("id").count()
      )
      assertThrows(
        classOf[SecurityException],
        () => spark.sql("SELECT count(email) FROM a.db.t").collect()
      )
  }

  @Test
  def unsupportedAggregateAndDedupNeighborsReject(): Unit =
    DeltaRangerReadParityFixture.withOperators { (spark, _) =>
      Seq(
        "SELECT count(*) FROM a.db.t GROUP BY region",
        "SELECT count(DISTINCT email) FROM a.db.t",
        "SELECT sum(id) FROM a.db.t",
        "SELECT count(*) OVER () FROM a.db.t",
        "SELECT count(email) FILTER (WHERE id > 1) FROM a.db.t",
        "SELECT count(upper(email)) FROM a.db.t",
        "SELECT count(id + 1) FROM a.db.t",
        "SELECT id FROM a.db.t ORDER BY upper(email)",
        "SELECT id FROM a.db.t LIMIT 2 OFFSET 1"
      ).foreach(sql => assertThrows(classOf[SecurityException], () => spark.sql(sql).collect()))
      assertThrows(
        classOf[SecurityException],
        () => spark.table("a.db.t").sortWithinPartitions("id").collect()
      )
      assertThrows(
        classOf[SecurityException],
        () => spark.table("a.db.t").dropDuplicates("email").collect()
      )
      assertThrows(
        classOf[SecurityException],
        () => spark.table("a.db.t").select().distinct().collect()
      )
    }
  private def record(name: String, frame: DataFrame): Unit =
    sys.env.get("READ_ACTION_EVIDENCE").foreach { directory =>
      val qe = frame.queryExecution
      Files.write(
        Paths.get(
          directory,
          name + "-" + frame.sparkSession.conf.get("spark.sql.adaptive.enabled") + ".txt"
        ),
        ("ANALYZED\n" + qe.analyzed.treeString + "\nNORMALIZED\n" + qe.normalized.treeString + "\nOPTIMIZED\n" + qe.optimizedPlan.treeString + "\nPLANNED\n" + qe.sparkPlan.treeString + "\nEXECUTED\n" + qe.executedPlan.treeString)
          .getBytes(StandardCharsets.UTF_8)
      )
    }
}

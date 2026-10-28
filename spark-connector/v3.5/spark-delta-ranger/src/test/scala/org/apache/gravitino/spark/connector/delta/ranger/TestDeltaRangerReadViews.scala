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

import java.nio.file.{Files, Paths}
import java.security.{MessageDigest, PrivilegedExceptionAction}
import org.apache.hadoop.security.UserGroupInformation
import scala.collection.JavaConverters._
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.analysis.{DeduplicateRelations, EliminateSubqueryAliases}
import org.apache.spark.sql.catalyst.expressions.{Cast, EqualTo, Literal}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, JoinHint}
import org.apache.spark.sql.functions.col
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class TestDeltaRangerReadViews {
  @Test
  def nativeIntegerPredicateWaitsForTimezoneBinding(): Unit =
    DeltaRangerReadParityFixture.withOperators { (spark, _) =>
      val base = spark.table("a.db.t").select("id")
      val filtered = base.filter("id = 1")
      assertEquals(Seq(Row(1L)), filtered.collect().toSeq)
      val altered = filtered.queryExecution.analyzed.transformAllExpressions { case cast: Cast =>
        cast.withTimeZone("Pacific/Honolulu")
      }
      assertThrows(classOf[SecurityException], () => DeltaRangerOrdinaryRead.check(altered, spark))
      val illtyped = Filter(
        EqualTo(base.queryExecution.analyzed.output.head, Literal(true)),
        base.queryExecution.analyzed
      )
      assertFalse(illtyped.resolved)
      assertThrows(classOf[SecurityException], () => DeltaRangerOrdinaryRead.check(illtyped, spark))
    }

  @Test
  def nativeViewIntegerPredicateWaitsForTimezoneBinding(): Unit =
    DeltaRangerReadParityFixture.withOperators { (spark, _) =>
      spark.table("a.db.t").select("id").createTempView("numeric_view")
      assertEquals(Seq(Row(1L)), spark.table("numeric_view").filter("id = 1").collect().toSeq)
    }

  @Test
  def localViewPreservesOriginalDemands(): Unit =
    DeltaRangerReadParityFixture.withOperators { (spark, policy) =>
      Seq("email", "region", "secret", "*").foreach(c => policy.denied.add("a:" + c))
      val resources = scala.collection.mutable.ArrayBuffer.empty[String]
      policy.onRequest =
        request => resources.synchronized { resources += request.getResource.getAsString; () }
      spark.table("a.db.t").createTempView("local_pending")
      assertTrue(resources.isEmpty, resources.toString)
      val stored = spark.sessionState.catalog.getTempView("local_pending").get
      assertTrue(stored.desc.viewText.isEmpty)
      assertTrue(stored.isTempViewStoringAnalyzedPlan)
      assertEquals(
        Vector(("a.db.t", Set("id"))),
        DeltaRangerReadParityFixture.demands(spark.table("local_pending").select("id"))
      )
      assertEquals(4L, spark.table("local_pending").select("id").count())
      assertThrows(
        classOf[SecurityException],
        () => spark.table("local_pending").select("secret").collect()
      )
      spark.table("a.db.t").select("id", "secret").createTempView("explicit_secret")
      assertThrows(
        classOf[SecurityException],
        () => spark.table("explicit_secret").select("id").collect()
      )
      assertFalse(resources.exists(_.contains("local_pending")), resources.toString)
      assertTrue(spark.catalog.dropTempView("local_pending"))
    }

  @Test
  def viewReplacementAndDropKeepBoundDefinitions(): Unit =
    DeltaRangerReadParityFixture.withOperators { (spark, policy) =>
      spark.table("a.db.t").select("id").filter("id = 1L").createTempView("retained_view")
      val retained = spark.table("retained_view")
      spark.table("a.db.t").select("id").filter("id = 2L").createOrReplaceTempView("retained_view")
      val replacement = spark.table("retained_view")
      val normalized = replacement.queryExecution.normalized
      val noAliases = EliminateSubqueryAliases(normalized)
      sys.env.get("READ_ACTION_EVIDENCE").foreach { directory =>
        Files.write(
          Paths.get(directory, "view-alias-transition.txt"),
          ("BEFORE receipt=" + DeltaRangerDecisionReceipt
            .present(normalized) + "\n" + normalized.treeString +
            "AFTER receipt=" + DeltaRangerDecisionReceipt
              .present(noAliases) + "\n" + noAliases.treeString).getBytes("UTF-8")
        )
      }
      assertEquals(Seq(Row(2L)), replacement.collect().toSeq)
      assertEquals(Seq(Row(1L)), retained.collect().toSeq)
      assertTrue(spark.catalog.dropTempView("retained_view"))
      assertThrows(
        classOf[AnalysisException],
        () => spark.table("retained_view")
      )
      assertEquals(Seq(Row(1L)), retained.collect().toSeq)
      policy.denied.add("a:id")
      assertThrows(classOf[SecurityException], () => retained.collect())
    }
  @Test
  def actionedAndRenamedViewSourcesKeepOriginalMasks(): Unit =
    DeltaRangerReadParityFixture.withOperators { (spark, policy) =>
      policy.mask = "concat({col}, '!')"
      val source = spark.table("a.db.t").select("id", "email")
      val expected = source.orderBy("id").collect().toSeq
      source.queryExecution.executedPlan
      source.collect()
      source.toDF("key", "mail").createTempView("renamed_view")
      assertEquals(expected, spark.table("renamed_view").alias("v").orderBy("key").collect().toSeq)
      policy.mask = "'NEW'"
      assertEquals(
        Seq(Row("NEW")),
        spark.table("renamed_view").select("mail").distinct().collect().toSeq
      )
      assertThrows(classOf[SecurityException], () => source.collect())
    }

  @Test
  def viewNeighborsAndChangedContextsReject(): Unit =
    DeltaRangerReadParityFixture.withOperators { (spark, _) =>
      val source = spark.table("a.db.t").select("id")
      source.createTempView("bound_view")
      val frame = spark.table("bound_view")
      assertEquals(4, frame.collect().length)
      assertThrows(classOf[SecurityException], () => source.createGlobalTempView("global_view"))
      assertThrows(
        classOf[SecurityException],
        () => source.createOrReplaceGlobalTempView("global_view")
      )
      assertThrows(
        classOf[SecurityException],
        () => spark.sql("CREATE TEMP VIEW text_view AS SELECT id FROM a.db.t")
      )
      assertThrows(
        classOf[SecurityException],
        () => spark.sql("CREATE VIEW permanent_view AS SELECT id FROM a.db.t")
      )
      assertTrue(spark.sessionState.catalog.getTempView("text_view").isEmpty)
      assertThrows(classOf[SecurityException], () => frame.cache())
      assertThrows(classOf[SecurityException], () => frame.localCheckpoint())
      assertThrows(
        classOf[SecurityException],
        () => DeltaRangerOrdinaryRead.normalize(frame.queryExecution.analyzed, spark.newSession())
      )
      UserGroupInformation
        .createUserForTesting("other-view", Array("other-group"))
        .doAs(new PrivilegedExceptionAction[Unit] {
          override def run(): Unit = {
            assertThrows(
              classOf[SecurityException],
              () => DeltaRangerOrdinaryRead.normalize(frame.queryExecution.analyzed, spark)
            )
            assertThrows(classOf[SecurityException], () => frame.collect())
          }
        })
      val zone = spark.conf.getAll.get("spark.sql.session.timeZone")
      spark.conf.set("spark.sql.session.timeZone", "Pacific/Honolulu")
      try assertThrows(classOf[SecurityException], () => frame.collect())
      finally
        zone match {
          case Some(value) => spark.conf.set("spark.sql.session.timeZone", value)
          case None        => spark.conf.unset("spark.sql.session.timeZone")
        }
      val registration = NativeSelectFixture.bindingFixture(spark, "a")
      val token = registration.properties.get("delta.registration.receipt.token")
      registration.properties.put("delta.registration.receipt.token", "changed")
      try assertThrows(classOf[SecurityException], () => frame.collect())
      finally registration.properties.put("delta.registration.receipt.token", token)
      assertEquals(4, frame.collect().length)
    }

  @Test
  def localViewSelfJoinRetainsBothOriginalOccurrences(): Unit =
    DeltaRangerReadParityFixture.withOperators { (spark, policy) =>
      policy.mask = "concat({col}, '!')"
      spark.table("a.db.t").select("id", "email").createTempView("self_view")
      val left = spark.table("self_view").alias("left")
      val right = spark.table("self_view").alias("right")
      val original = Join(
        left.queryExecution.analyzed,
        right.queryExecution.analyzed,
        Inner,
        None,
        JoinHint.NONE
      )
      val remapped = DeduplicateRelations(original)
      sys.env.get("READ_ACTION_EVIDENCE").foreach { directory =>
        Files.write(
          Paths.get(directory, "view-selfjoin-native-remap.json"),
          ("{\"before\":" + original.toJSON + ",\"after\":" + remapped.toJSON + "}")
            .getBytes("UTF-8")
        )
      }
      val joined = left
        .join(right, col("left.id") === col("right.id"))
        .select(col("left.id"), col("left.email"), col("right.email"))
        .orderBy(col("left.id"))
      assertEquals(
        Vector(("a.db.t", Set("id", "email")), ("a.db.t", Set("id", "email"))),
        DeltaRangerReadParityFixture.demands(joined)
      )
      assertEquals(
        Seq(Row(1L, "z!", "z!"), Row(2L, "a!", "a!"), Row(3L, null, null), Row(5L, "b!", "b!")),
        joined.collect().toSeq
      )
      policy.denied.add("a:email")
      assertThrows(classOf[SecurityException], () => joined.collect())
    }

  @Test
  def localViewCannotWidenAppendSourceGrammar(): Unit = {
    val policy = new TestDeltaRangerNativeSelect.Fixture
    try
      NativeSelectFixture.withSpark(
        spark => {
          val base = spark.table("b.db.t")
          base.write.mode("append").insertInto("a.db.t")
          spark.table("b.db.t").createTempView("append_view")
          val target = Paths.get(NativeSelectFixture.locations("a"))
          def manifest(): Map[String, String] = {
            val stream = Files.walk(target)
            try
              stream
                .iterator()
                .asScala
                .filter(Files.isRegularFile(_))
                .map { path =>
                  val hash = MessageDigest
                    .getInstance("SHA-256")
                    .digest(Files.readAllBytes(path))
                    .map(value => f"${value & 0xff}%02x")
                    .mkString
                  target.relativize(path).toString -> hash
                }
                .toMap
            finally stream.close()
          }
          val before = manifest()
          assertTrue(before.keys.exists(_.startsWith("_delta_log/")))
          assertTrue(before.keys.exists(_.endsWith(".parquet")))
          assertThrows(
            classOf[SecurityException],
            () => spark.table("append_view").write.mode("append").insertInto("a.db.t")
          )
          assertEquals(before, manifest())
        },
        Some(NativeSelectFixture.extension(policy)),
        startupOptions = Map("spark.databricks.delta.write.txnVersion.autoReset.enabled" -> "false")
      )
    finally policy.engine.close()
  }
}

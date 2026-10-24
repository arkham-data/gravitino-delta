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

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{
  Add,
  AttributeReference,
  Cast,
  Literal,
  NamedExpression,
  Subtract
}
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.delta.DeltaLog
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory

/** Installed SELECT policy rewrites must finish native cast completion before receipt issuance. */
class TestDeltaRangerArithmeticMaskBinding {
  private val typedMask = Map("id" -> "{col} + CAST(100 AS BIGINT)")

  @Test
  def projectedBigintMaskReturnsEachMaskedValueOnce(): Unit = {
    new TestDeltaRangerInstalledMerge().withPolicies { (spark, plugin) =>
      new TestDeltaRangerMergePolicies().publish(plugin, masks = typedMask)
      Seq(classOf[SparkSession], classOf[DeltaLog], classOf[Cast], plugin.getClass).foreach { cls =>
        LoggerFactory
          .getLogger(getClass)
          .warn(
            "ARITHMETIC_ORIGIN " + cls.getName + " " + cls.getProtectionDomain.getCodeSource.getLocation
          )
      }
      Seq("UTC", "America/Mexico_City").foreach { zone =>
        spark.conf.set("spark.sql.session.timeZone", zone)
        assertEquals(
          Set(Row(101L), Row(102L), Row(103L)),
          spark.sql("SELECT id FROM b.db.t").collect().toSet
        )
      }
    }
  }

  @Test
  def aliasesAndPredicatesKeepMasksAndOriginalDemands(): Unit = {
    new TestDeltaRangerInstalledMerge().withPolicies { (spark, plugin) =>
      val policies = new TestDeltaRangerMergePolicies()
      policies.publish(
        plugin,
        masks = typedMask,
        filter = Some("region = 'keep'"),
        deniedSelect = Seq("secret")
      )
      val alias = spark.sql("SELECT id AS secret FROM b.db.t")
      val receipt = DeltaRangerDecisionReceipt.consumeOwned(
        alias.queryExecution.analyzed,
        spark,
        optimized = false
      )
      assertEquals(Set("id"), receipt.obligations.head.columns)
      assertEquals(Set(Row(101L), Row(103L)), alias.collect().toSet)
      assertEquals(
        Set(Row(103L)),
        spark.sql("SELECT x.id AS masked FROM b.db.t x WHERE x.id = 103L").collect().toSet
      )
      assertEquals(
        Set(Row(102L), Row(104L)),
        spark.sql("SELECT id + 1L AS shifted FROM b.db.t").collect().toSet
      )
      Seq(
        "SELECT secret FROM b.db.t WHERE false",
        "SELECT id FROM b.db.t WHERE secret = 'hidden' AND false"
      ).foreach(sql => assertThrows(classOf[SecurityException], () => spark.sql(sql).collect()))
      policies.publish(plugin, masks = Map("id" -> "{col} + 100"))
      val rejected =
        assertThrows(classOf[SecurityException], () => spark.sql("SELECT id FROM b.db.t"))
      assertTrue(rejected.getMessage.contains("Unsupported Ranger mask"))
    }
  }

  @Test
  def modifiedArithmeticMasksCannotBorrowOriginalReceipt(): Unit = {
    new TestDeltaRangerInstalledMerge().withPolicies { (spark, plugin) =>
      val policies = new TestDeltaRangerMergePolicies()
      policies.publish(plugin, masks = typedMask)
      val approved = spark.sql("SELECT id FROM b.db.t").queryExecution.analyzed
      DeltaRangerReadRule.check(approved.clone(), spark)
      val changedMasks = Seq(
        approved.transformAllExpressions { case add: Add =>
          Subtract(add.left, add.right, add.evalMode)
        },
        approved.transformAllExpressions {
          case literal: Literal if literal.value == 100 => Literal(101)
        },
        approved.transformAllExpressions { case cast: Cast =>
          val zone = if (cast.timeZoneId.contains("Pacific/Auckland")) "UTC" else "Pacific/Auckland"
          cast.copy(timeZoneId = Some(zone))
        },
        approved.transformAllExpressions { case attribute: AttributeReference =>
          attribute.withQualifier(Seq("forged"))
        },
        approved.transformAllExpressions { case attribute: AttributeReference =>
          attribute.withExprId(NamedExpression.newExprId)
        },
        approved.transformUp {
          case project: Project if project.projectList.exists(_.exists(_.isInstanceOf[Add])) =>
            project.child
        }
      )
      changedMasks.foreach { changed =>
        changed.copyTagsFrom(approved)
        assertThrows(classOf[SecurityException], () => DeltaRangerReadRule.check(changed, spark))
      }
      val originalZone = spark.conf.getAll.get("spark.sql.session.timeZone")
      spark.conf.set(
        "spark.sql.session.timeZone",
        if (spark.conf.get("spark.sql.session.timeZone") == "UTC") "America/Mexico_City" else "UTC"
      )
      try {
        val rejected =
          assertThrows(classOf[SecurityException], () => DeltaRangerReadRule.check(approved, spark))
        assertTrue(rejected.getMessage.contains("configuration changed"))
      } finally
        originalZone match {
          case Some(zone) => spark.conf.set("spark.sql.session.timeZone", zone)
          case None       => spark.conf.unset("spark.sql.session.timeZone")
        }
      DeltaRangerReadRule.check(approved, spark)
      policies.publish(plugin, masks = Map("id" -> "{col} + CAST(200 AS BIGINT)"))
      assertThrows(classOf[SecurityException], () => DeltaRangerReadRule.check(approved, spark))
    }
  }
}

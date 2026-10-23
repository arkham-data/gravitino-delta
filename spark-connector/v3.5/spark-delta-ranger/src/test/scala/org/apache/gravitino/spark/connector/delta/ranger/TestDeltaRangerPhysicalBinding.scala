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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.execution.{
  FilterExec,
  InputAdapter,
  LocalLimitExec,
  LocalTableScanExec,
  SparkPlan,
  UnionExec,
  WholeStageCodegenExec
}
import org.apache.spark.sql.types.{Decimal, DecimalType, IntegerType}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Test, Timeout}

class TestDeltaRangerPhysicalBinding {
  @Test
  @Timeout(10)
  def deepIndependentSnapshotsPreserveSharedAndCopiedTrees(): Unit = {
    val column = AttributeReference("id", IntegerType)()
    def branch(): SparkPlan = nested(LocalTableScanExec(Seq(column), Seq.empty))
    val shared = branch()
    val expected = DeltaRangerPhysicalProvenance.binding(UnionExec(Seq(shared, shared)))
    (0 until 10).foreach { _ =>
      assertEquals(
        expected,
        DeltaRangerPhysicalProvenance.binding(UnionExec(Seq(branch(), branch())))
      )
    }
    val wrapped = WholeStageCodegenExec(InputAdapter(shared))(7)
    assertEquals(
      DeltaRangerPhysicalProvenance.binding(shared),
      DeltaRangerPhysicalProvenance.binding(wrapped)
    )
  }

  @Test
  def deepConstructorChildOrderOutputAndIdentityChangesRemainDistinct(): Unit = {
    val column = AttributeReference("id", IntegerType)()
    val row = InternalRow(1)
    val leaf = LocalTableScanExec(Seq(column), Seq(row))
    def bound(child: SparkPlan): Vector[Any] = DeltaRangerPhysicalProvenance.binding(nested(child))
    assertNotEquals(bound(LocalLimitExec(1, leaf)), bound(LocalLimitExec(2, leaf)))
    val other = LocalLimitExec(2, leaf)
    assertNotEquals(bound(UnionExec(Seq(leaf, other))), bound(UnionExec(Seq(other, leaf))))
    assertNotEquals(bound(leaf), bound(leaf.copy(output = Seq(column.withName("changed")))))
    assertNotEquals(bound(leaf), bound(leaf.copy(rows = Seq(InternalRow(1)))))
  }

  @Test
  def repeatedSnapshotsObserveDeepMutableConstructorValues(): Unit = {
    val column = AttributeReference("id", IntegerType)()
    val decimal = Decimal(1L)
    val literal = Literal.create(decimal, DecimalType(10, 0))
    val plan = nested(
      FilterExec(
        EqualTo(literal, Literal.create(Decimal(1L), DecimalType(10, 0))),
        LocalTableScanExec(Seq(column), Seq.empty)
      )
    )
    val frozen = DeltaRangerPhysicalProvenance.binding(plan)
    decimal.set(2L)
    assertNotEquals(frozen, DeltaRangerPhysicalProvenance.binding(plan))
  }

  private def nested(leaf: SparkPlan): SparkPlan =
    (0 until 64).foldLeft(leaf)((child, index) => LocalLimitExec(index + 1, child))
}

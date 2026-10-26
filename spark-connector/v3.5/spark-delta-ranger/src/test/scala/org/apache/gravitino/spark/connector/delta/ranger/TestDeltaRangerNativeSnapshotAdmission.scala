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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects.{
  LambdaVariable,
  MapObjects,
  NewInstance,
  StaticInvoke
}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.SnapshotState
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.types.{LongType, ObjectType, StringType}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class TestDeltaRangerNativeSnapshotAdmission {
  @Test
  def nativeStateLineageCanBeCapturedBeforeComputedState(): Unit = {
    val f = new TestDeltaRangerNativeSelect.Fixture
    try {
      NativeSelectFixture.withSpark(
        spark => {
          NativeSelectFixture.withNativeOperation(spark) { index =>
            val snapshot = index.getSnapshot
            val triggered = snapshot.getClass.getMethod("_computedStateTriggered")
            assertEquals(false, triggered.invoke(snapshot))
            DeltaRangerPlanAdmission.registerSnapshot(index, snapshot)
            val first = snapshot.stateDF.queryExecution.analyzed.asInstanceOf[LogicalRDD]
            val second = snapshot.stateDF.queryExecution.analyzed.asInstanceOf[LogicalRDD]
            assertTrue(first.rdd eq second.rdd)
            assertEquals(first.output, second.output)
            assertTrue(DeltaRangerPlanAdmission.internalMetadata(first))
            assertEquals(false, triggered.invoke(snapshot))
          }
        },
        Some(extensions => NativeSelectFixture.extension(f)(extensions))
      )
    } finally f.engine.close()
  }

  @Test
  def installedUnfilteredProjectionAcceptsOnlyExactScopedSnapshotState(): Unit = {
    val f = new TestDeltaRangerNativeSelect.Fixture
    var nativeAggregate: Option[Aggregate] = None
    var nativeBinding: Option[DeserializeToObject] = None
    try {
      NativeSelectFixture.withSpark(
        spark => {
          assertEquals(
            Set(Row(1L, "keep"), Row(2L, "drop"), Row(3L, "keep")),
            spark.sql("SELECT id, region FROM b.db.t").collect().toSet
          )
          assertNativeSpoofsReject(spark, nativeAggregate.get, nativeBinding.get)
          Seq(
            "SELECT count(id + 1) FROM b.db.t",
            "SELECT region, count(*) FROM b.db.t GROUP BY region",
            "SELECT id FROM b.db.t LIMIT 1 OFFSET 1"
          )
            .foreach(sql =>
              assertThrows(classOf[SecurityException], () => spark.sql(sql).collect())
            )
        },
        Some(extensions => {
          extensions.injectResolutionRule(_ =>
            new Rule[LogicalPlan] {
              override def apply(plan: LogicalPlan): LogicalPlan = {
                if (plan.resolved) plan match {
                  case aggregate: Aggregate if aggregate.child.isInstanceOf[LogicalRDD] =>
                    nativeAggregate = Some(aggregate)
                  case binding: DeserializeToObject
                      if binding.deserializer.dataType == ObjectType(classOf[SnapshotState]) =>
                    nativeBinding = Some(binding)
                  case _ =>
                }
                plan
              }
            }
          )
          NativeSelectFixture.extension(f)(extensions)
        })
      )
    } finally f.engine.close()
  }

  private def assertNativeSpoofsReject(
      spark: SparkSession,
      aggregate: Aggregate,
      binding: DeserializeToObject
  ): Unit = {
    def rejected(plan: LogicalPlan): Unit =
      assertFalse(DeltaRangerPlanAdmission.internalMetadata(plan), plan.nodeName)
    assertTrue(DeltaRangerPlanAdmission.internalMetadata(aggregate.clone()))
    rejected(binding)
    val rdd = aggregate.child.asInstanceOf[LogicalRDD]
    val local = binding.child.asInstanceOf[LocalRelation]
    NativeSelectFixture.withNativeOperation(spark) { index =>
      rejected(binding)
      DeltaRangerPlanAdmission.registerSnapshot(index, index.getSnapshot)
      assertTrue(DeltaRangerPlanAdmission.internalMetadata(aggregate))
      assertTrue(DeltaRangerPlanAdmission.internalMetadata(binding))
      val pruned = rdd.output.filter(attribute =>
        Set("txn", "add", "remove", "metaData", "protocol", "domainMetadata").contains(
          attribute.name
        )
      )
      val optimized = aggregate.copy(
        child = Project(pruned, rdd),
        aggregateExpressions = aggregate.aggregateExpressions.map {
          case alias: Alias
              if Set("fileSizeHistogram", "deletedRecordCountsHistogramOpt").contains(alias.name) =>
            alias
              .withNewChildren(Seq(Literal.create(null, alias.child.dataType)))
              .asInstanceOf[NamedExpression]
          case other => other
        }
      )
      assertTrue(DeltaRangerPlanAdmission.internalMetadata(optimized))
      Seq(
        optimized.copy(child = Project(pruned.reverse, rdd)),
        optimized.copy(child = Project(pruned.dropRight(1), rdd)),
        optimized.copy(child = Project(pruned :+ rdd.output.last, rdd)),
        optimized.copy(child =
          Project(pruned.updated(0, Alias(pruned.head, pruned.head.name)()), rdd)
        ),
        optimized.copy(child = Project(pruned.updated(0, pruned.head.newInstance()), rdd)),
        optimized.copy(child =
          Project(pruned, LogicalRDD(rdd.output, spark.sparkContext.emptyRDD[InternalRow])(spark))
        ),
        optimized.copy(aggregateExpressions = optimized.aggregateExpressions.map {
          case alias: Alias if alias.name == "fileSizeHistogram" =>
            alias.withNewChildren(Seq(Literal.create(null, LongType))).asInstanceOf[NamedExpression]
          case other => other
        }),
        optimized.copy(aggregateExpressions = optimized.aggregateExpressions.map {
          case alias: Alias if alias.name == "fileSizeHistogram" =>
            alias
              .withNewChildren(Seq(Literal.create(new GenericInternalRow(3), alias.child.dataType)))
              .asInstanceOf[NamedExpression]
          case other => other
        })
      ).foreach(rejected)
      val first = GlobalLimit(Literal(1), LocalLimit(Literal(1), aggregate))
      assertTrue(DeltaRangerPlanAdmission.internalMetadata(first))
      Seq(
        aggregate.copy(groupingExpressions = Seq(rdd.output.head)),
        aggregate.copy(aggregateExpressions = aggregate.aggregateExpressions.dropRight(1)),
        aggregate.copy(aggregateExpressions = aggregate.aggregateExpressions :+ rdd.output.head),
        aggregate.copy(aggregateExpressions =
          aggregate.aggregateExpressions
            .updated(0, Alias(Literal(0L), aggregate.aggregateExpressions.head.name)())
        ),
        aggregate.copy(child =
          LogicalRDD(rdd.output, spark.sparkContext.emptyRDD[InternalRow])(spark)
        ),
        aggregate.copy(child = LogicalRDD(rdd.output.map(_.newInstance()), rdd.rdd)(spark)),
        aggregate.copy(child =
          LogicalRDD(
            rdd.output.updated(0, rdd.output.head.withNullability(!rdd.output.head.nullable)),
            rdd.rdd
          )(spark)
        ),
        aggregate.copy(child =
          LogicalRDD(rdd.output.updated(0, rdd.output.head.withQualifier(Seq("foreign"))), rdd.rdd)(
            spark
          )
        ),
        aggregate.copy(child = LogicalRDD(rdd.output.dropRight(1), rdd.rdd)(spark)),
        GlobalLimit(Literal(2), LocalLimit(Literal(1), aggregate)),
        GlobalLimit(Literal(1), LocalLimit(Literal(2), aggregate)),
        GlobalLimit(Literal(1L), LocalLimit(Literal(1), aggregate)),
        LocalLimit(Literal(1), GlobalLimit(Literal(1), aggregate)),
        GlobalLimit(Literal(1), aggregate),
        LocalLimit(Literal(1), aggregate),
        Project(aggregate.output, aggregate),
        binding.copy(child = local.copy(data = Seq(InternalRow.empty))),
        binding.copy(child = local.copy(isStreaming = true)),
        binding.copy(child = aggregate),
        binding.copy(child = local.copy(output = local.output.map(_.newInstance()))),
        binding.copy(child = local.copy(output = local.output.dropRight(1))),
        binding.copy(child =
          local.copy(output =
            local.output.updated(0, local.output.head.withNullability(!local.output.head.nullable))
          )
        ),
        binding.copy(deserializer = Literal.create(null, ObjectType(classOf[SnapshotState]))),
        binding.copy(deserializer = Literal.create(null, ObjectType(classOf[String]))),
        binding.copy(deserializer =
          ScalaUDF(() => null, ObjectType(classOf[SnapshotState]), Seq.empty)
        ),
        binding.copy(deserializer =
          ScalarSubquery(
            LocalRelation(Seq(AttributeReference("object", ObjectType(classOf[SnapshotState]))()))
          )
        )
      ).foreach(rejected)
      val constructorChanged = binding.deserializer.transformUp {
        case n: NewInstance if n.cls == classOf[SnapshotState] =>
          n.copy(arguments = n.arguments.updated(0, Literal(9L)))
      }
      rejected(binding.copy(deserializer = constructorChanged))
      val staticChanged = binding.deserializer.transformUp {
        case a: AttributeReference if a.name == "sizeInBytes" =>
          StaticInvoke(classOf[java.lang.Long], LongType, "sum", Seq(a, Literal(1L)))
      }
      rejected(binding.copy(deserializer = staticChanged))
      val variables = binding.deserializer
        .collect { case variable: LambdaVariable => variable }
        .groupBy(_.id)
        .values
        .map(_.head)
        .toSeq
      assertTrue(variables.size > 2)
      val renamed = binding.deserializer.transformDown { case variable: LambdaVariable =>
        variable.copy(id = variable.id + 100000L)
      }
      assertTrue(DeltaRangerPlanAdmission.internalMetadata(binding.copy(deserializer = renamed)))
      for (
        changed <- Seq[Expression](
          binding.deserializer.transformDown { case variable: LambdaVariable =>
            variable.copy(id = variables.head.id)
          },
          binding.deserializer.transformDown {
            case variable: LambdaVariable if variable.id == variables.head.id =>
              variable.copy(id = 0L)
          },
          binding.deserializer.transformDown {
            case variable: LambdaVariable if variable.id == variables.head.id =>
              variable.copy(id = -1L)
          },
          binding.deserializer.transformDown {
            case variable: LambdaVariable if variable.id == variables.head.id =>
              variable.copy(name = "foreign")
          },
          binding.deserializer.transformDown {
            case variable: LambdaVariable if variable.id == variables.head.id =>
              variable.copy(nullable = !variable.nullable)
          },
          binding.deserializer.transformDown {
            case variable: LambdaVariable if variable.id == variables.head.id =>
              variable
                .copy(dataType = if (variable.dataType == StringType) LongType else StringType)
          },
          binding.deserializer.transformDown { case map: MapObjects =>
            map.copy(loopVar = map.loopVar.copy(id = map.loopVar.id + 200000L))
          },
          binding.deserializer.transformDown { case map: MapObjects =>
            map.copy(lambdaFunction = map.lambdaFunction.transformDown {
              case variable: LambdaVariable if variable.id == map.loopVar.id =>
                variable.copy(id = variable.id + 300000L)
            })
          },
          binding.deserializer
            .transformDown { case map: MapObjects => map.copy(customCollectionCls = None) }
        )
      ) rejected(binding.copy(deserializer = changed))
      val changedOrdinal = binding.deserializer.transformUp {
        case attribute: AttributeReference if attribute.name == "sizeInBytes" =>
          local.output.find(_.name == "numOfFiles").get
      }
      rejected(binding.copy(deserializer = changedOrdinal))
      val stringMaps = binding.deserializer.collect {
        case map: MapObjects if map.loopVar.dataType == StringType && map.loopVar.nullable => map
      }
      assertTrue(stringMaps.size >= 2)
      val left = stringMaps.head.loopVar
      val right = stringMaps(1).loopVar
      val swappedUses = binding.deserializer.transformDown { case map: MapObjects =>
        map.copy(lambdaFunction = map.lambdaFunction.transformDown {
          case variable: LambdaVariable if variable.id == left.id  => variable.copy(id = right.id)
          case variable: LambdaVariable if variable.id == right.id => variable.copy(id = left.id)
        })
      }
      rejected(binding.copy(deserializer = swappedUses))
      val firstMap = binding.deserializer.collectFirst { case map: MapObjects => map }.get
      val capture = binding.deserializer.transformDown {
        case map: MapObjects if map.loopVar.id != firstMap.loopVar.id =>
          map.copy(lambdaFunction = map.lambdaFunction.transformDown {
            case variable: LambdaVariable if variable.id == map.loopVar.id => firstMap.loopVar
          })
      }
      rejected(binding.copy(deserializer = capture))
    }
    assertTrue(DeltaRangerPlanAdmission.internalMetadata(aggregate.clone()))
    rejected(binding)
    NativeSelectFixture.withNativeOperation(spark, "a") { foreign =>
      DeltaRangerPlanAdmission.registerSnapshot(foreign, foreign.getSnapshot)
      rejected(aggregate); rejected(binding)
    }
  }

  private def snapshotIndex(spark: SparkSession, catalog: String): TahoeLogFileIndex =
    spark
      .sql(s"SELECT id, region FROM $catalog.db.t")
      .queryExecution
      .analyzed
      .collectFirst { case relation: LogicalRelation =>
        relation.relation.asInstanceOf[HadoopFsRelation].location.asInstanceOf[TahoeLogFileIndex]
      }
      .get
}

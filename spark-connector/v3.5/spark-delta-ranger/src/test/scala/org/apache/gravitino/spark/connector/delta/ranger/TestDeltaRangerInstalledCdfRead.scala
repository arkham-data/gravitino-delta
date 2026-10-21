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

import org.apache.spark.sql.Row
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** Installed named CDF reads must preserve native events under current Ranger policy. */
class TestDeltaRangerInstalledCdfRead {
  @Test
  def namedBoundedReadUsesOwnedNativeBranches(): Unit = {
    val policy = new TestDeltaRangerNativeSelect.Fixture
    policy.filter = null
    policy.maskColumn = "unused"
    val fixture = new DeltaRangerCdfReadFixture
    try
      NativeSelectFixture.withSpark(
        spark => {
          val frame = fixture.read(spark)
          val rows = frame.collect().toVector
          val carrier = frame.queryExecution.optimizedPlan.asInstanceOf[DeltaRangerCdfRead.Carrier]
          assertSame(carrier, carrier.clone())
          assertThrows(classOf[SecurityException], () => carrier.copy()(carrier.receipt).receipt)
          val producer = frame.queryExecution.executedPlan.asInstanceOf[DeltaRangerCdfProducer]
          val indexes = DeltaRangerPhysicalProvenance.nodes(producer.child).collect {
            case scan: org.apache.spark.sql.execution.FileSourceScanExec =>
              scan.relation.location.asInstanceOf[DeltaRangerCdfReadScans.OwnedIndex]
          }
          def retained[T](value: AnyRef, kind: Class[T]): T = {
            val field =
              value.getClass.getDeclaredFields.find(f => kind.isAssignableFrom(f.getType)).get
            field.setAccessible(true)
            kind.cast(field.get(value))
          }
          val owners = indexes.map(retained(_, classOf[DeltaRangerCdfReadScans]))
          assertFalse(owners.isEmpty)
          assertTrue(owners.forall(_ eq owners.head))
          assertEquals(
            Set("CdcAddFileIndex", "TahoeChangeFileIndex", "TahoeRemoveFileIndex"),
            indexes
              .map(
                retained(
                  _,
                  classOf[org.apache.spark.sql.delta.files.TahoeFileIndexWithSnapshotDescriptor]
                ).getClass.getSimpleName
              )
              .toSet
          )
          assertThrows(
            classOf[SecurityException],
            () => producer.copy()(producer.receipt).executeCollect()
          )
          assertSame(producer, producer.withNewChildren(Seq(producer.child)))
          assertThrows(
            classOf[SecurityException],
            () =>
              producer.receipt.replaceProducer(producer.copy()(producer.receipt), producer.child)
          )
          val differentChild =
            org.apache.spark.sql.execution.ProjectExec(producer.child.output, producer.child)
          assertThrows(
            classOf[SecurityException],
            () => producer.receipt.replaceProducer(producer, differentChild)
          )
          assertThrows(
            classOf[SecurityException],
            () => producer.withNewChildren(Seq(differentChild))
          )
          val owner = owners.head
          val rdd = retained(owner, classOf[org.apache.spark.rdd.RDD[_]])
          val dependencyField =
            classOf[org.apache.spark.rdd.RDD[_]].getDeclaredField("dependencies_")
          dependencyField.setAccessible(true)
          val dependencies = rdd.dependencies
          assertFalse(dependencies.isEmpty)
          try {
            dependencyField.set(rdd, Seq.empty)
            assertThrows(
              classOf[SecurityException],
              () => producer.receipt.validateCollection(producer.child)
            )
          } finally dependencyField.set(rdd, dependencies)
          assertThrows(classOf[SecurityException], () => owner.requireLive())
          assertThrows(classOf[SecurityException], () => owner.run(()))
          val originalIndex = retained(
            indexes.head,
            classOf[org.apache.spark.sql.delta.files.TahoeFileIndexWithSnapshotDescriptor]
          )
          assertThrows(classOf[SecurityException], () => owner.issued(originalIndex))
          val foreignDelegate = new DeltaRangerCdfReadScans.OwnedIndex(owner, originalIndex, spark)
          assertThrows(
            classOf[SecurityException],
            () => foreignDelegate.matchingFiles(Seq.empty, Seq.empty)
          )

          assertThrows(
            classOf[SecurityException],
            () => owner.matchingFiles(originalIndex, indexes.head, Seq.empty)(Seq.empty)
          )
          val selectedField = owner.getClass.getDeclaredFields.find(_.getName == "selected").get
          selectedField.setAccessible(true)
          val selected = selectedField
            .get(owner)
            .asInstanceOf[java.util.IdentityHashMap[
              org.apache.spark.sql.delta.files.TahoeFileIndexWithSnapshotDescriptor,
              Vector[org.apache.spark.sql.delta.actions.AddFile]
            ]]
          var helper: DeltaRangerCdfReadScans.HelperCarrier = null
          owner.matchingFiles(originalIndex, indexes.head, Seq.empty) {
            import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Filter, Union}
            import org.apache.spark.sql.catalyst.expressions.Literal
            import org.apache.spark.sql.delta.actions.AddFile
            val files = selected.get(originalIndex)
            assertFalse(files.isEmpty)
            val encoded = spark.createDataset(files)(org.apache.spark.sql.Encoders.product[AddFile])
            val local = encoded.queryExecution.analyzed.asInstanceOf[LocalRelation]
            assertFalse(owner.analysis(local.copy()))
            assertFalse(owner.analysis(local.copy(data = Seq.empty)))
            assertFalse(
              owner.analysis(
                local.copy(output = local.output.head.withName("foreign") +: local.output.tail)
              )
            )
            assertFalse(owner.analysis(Filter(Literal.FalseLiteral, local)))
            assertFalse(owner.analysis(Union(Seq(local, local))))
            val filtered = encoded.filter(org.apache.spark.sql.functions.lit(true))
            helper =
              filtered.queryExecution.normalized.asInstanceOf[DeltaRangerCdfReadScans.HelperCarrier]
            assertSame(helper, helper.clone())
            assertThrows(classOf[SecurityException], () => helper.copy()(helper.ticket).ticket)
            val physical =
              filtered.queryExecution.executedPlan.asInstanceOf[DeltaRangerCdfReadScans.HelperExec]
            assertThrows(
              classOf[SecurityException],
              () => physical.copy()(helper.ticket).executeCollect()
            )
            filtered.collect().toVector
          }
          assertThrows(classOf[SecurityException], () => helper.ticket)
          assertThrows(
            classOf[SecurityException],
            () =>
              owner.matchingFiles(originalIndex, indexes.head, Seq.empty) {
                val nativeFiles = originalIndex.matchingFiles(Seq.empty, Seq.empty)
                nativeFiles.head.copy(path = "foreign.parquet") +: nativeFiles.tail
              }
          )
          val noPartitions = Seq(org.apache.spark.sql.catalyst.expressions.Literal.FalseLiteral)
          assertTrue(
            owner
              .matchingFiles(originalIndex, indexes.head, noPartitions) {
                originalIndex.matchingFiles(noPartitions, Seq.empty)
              }
              .isEmpty
          )

          assertThrows(
            classOf[SecurityException],
            () => owner.matchingFiles(indexes.head, indexes.head, Seq.empty)(Seq.empty)
          )
          val nativeField =
            carrier.receipt.getClass.getDeclaredFields.find(_.getName == "native").get
          nativeField.setAccessible(true)
          val native = nativeField
            .get(carrier.receipt)
            .asInstanceOf[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan]
          val empty = native.collectFirst { case rdd: org.apache.spark.sql.execution.LogicalRDD =>
            rdd
          }.get
          assertThrows(classOf[SecurityException], () => owner.analysis(empty))
          assertThrows(
            classOf[SecurityException],
            () => owner.prepare(empty.copy()(spark, Some(empty.stats), None))
          )
          assertThrows(
            classOf[SecurityException],
            () =>
              owner.prepare(org.apache.spark.sql.catalyst.plans.logical.Union(Seq(native, native)))
          )
          assertEquals(fixture.multiset(fixture.expected), fixture.multiset(rows))
          assertEquals(
            Set("insert", "delete", "update_preimage", "update_postimage"),
            rows.map(_.getString(3)).toSet
          )
          val columns = NativeSelectFixture.bindingFixture(spark, "a").columns.map(_.name()).toSet
          assertEquals(Set("id", "secret", "region"), columns)
          assertEquals(fixture.multiset(rows), fixture.multiset(frame.collect().toVector))
          val otherSession = spark.newSession()
          try {
            org.apache.spark.sql.SparkSession.setActiveSession(otherSession)
            assertThrows(classOf[SecurityException], () => producer.executeCollect())
          } finally org.apache.spark.sql.SparkSession.setActiveSession(spark)
          policy.denied.add("a:secret")
          assertThrows(classOf[Exception], () => frame.collect())
          policy.denied.clear()

        },
        Some(NativeSelectFixture.extension(policy)),
        beforeExtension = fixture.prepare
      )
    finally policy.engine.close()
  }

  @Test
  def deniedAndFilteredReadCannotEscape(): Unit = {
    val policy = new TestDeltaRangerNativeSelect.Fixture
    policy.filter = "region = 'US'"
    policy.maskColumn = "id"
    policy.mask = "CAST(0 AS BIGINT)"
    val fixture = new DeltaRangerCdfReadFixture
    try
      NativeSelectFixture.withSpark(
        spark => {
          policy.denied.add("a:secret")
          assertThrows(
            classOf[Exception],
            () => { fixture.read(spark).select("secret").collect(); () }
          )
          policy.denied.clear()
          val expected = fixture.expected
            .filter(_.getString(2) == "US")
            .map(row => Row.fromSeq(0L +: row.toSeq.tail))
          assertEquals(
            fixture.multiset(expected),
            fixture.multiset(fixture.read(spark).collect().toVector)
          )
          assertEquals(
            fixture.multiset(expected),
            fixture.multiset(fixture.read(spark).filter("id = 0").collect().toVector)
          )
          policy.denied.add("a:secret")
          val metadata =
            expected.map(row => Row(row.getString(3), row.getLong(4), row.getTimestamp(5)))
          assertEquals(
            fixture.multiset(metadata),
            fixture.multiset(
              fixture
                .read(spark)
                .select("_change_type", "_commit_version", "_commit_timestamp")
                .collect()
                .toVector
            )
          )
        },
        Some(NativeSelectFixture.extension(policy)),
        beforeExtension = fixture.prepare
      )
    finally policy.engine.close()
  }
}

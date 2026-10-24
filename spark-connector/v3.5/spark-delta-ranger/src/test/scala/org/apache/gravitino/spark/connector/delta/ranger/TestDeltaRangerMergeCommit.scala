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
import scala.collection.JavaConverters._
import org.apache.spark.sql.Row
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** Native constraint execution is retained under the owned MERGE writer. */
class TestDeltaRangerMergeCommit {
  import TestDeltaRangerMergeSource._

  @Test
  def nativeConstraintAcceptsValidRows(): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies(
      (spark, _) => {
        assertEquals(Seq(Row(3L, 1L, 1L, 1L)), spark.sql(sql).collect().toSeq)
        assertEquals(2L, version("b"))
      },
      prepareTarget = spark => {
        spark
          .sql(
            s"ALTER TABLE delta.`${NativeSelectFixture.locations("b")}` ADD CONSTRAINT positive_id CHECK (id > 0)"
          )
          .collect()
      }
    )
  @Test
  def nativeConstraintFailureDoesNotPublish(): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies(
      (spark, _) => {
        val failure = assertThrows(
          classOf[Exception],
          () => spark.sql(sql.replace("secret = s.secret", "id = -1, secret = s.secret")).collect()
        )
        assertTrue(
          causes(failure).exists(t =>
            t.getClass.getName.contains("DeltaInvariantViolationException")
          ),
          failure.toString
        )
        assertEquals(1L, version("b"))
      },
      prepareTarget = spark =>
        spark
          .sql(
            s"ALTER TABLE delta.`${NativeSelectFixture.locations("b")}` ADD CONSTRAINT positive_id CHECK (id > 0)"
          )
          .collect()
    )

  @Test
  def zeroVisibleSourceSucceeds(): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies { (spark, plugin) =>
      new TestDeltaRangerMergePolicies().publish(plugin, sourceFilter = Some("region = 'absent'"))
      val before = spark.table("b.db.t").collect().toSeq
      assertEquals(Seq(Row(0L, 0L, 0L, 0L)), spark.sql(sql).collect().toSeq)
      assertEquals(before, spark.table("b.db.t").collect().toSeq)
      assertEquals(0L, version("b"))
    }

  @Test
  def nativeNoMatchingRowsSucceeds(): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies { (spark, _) =>
      val query = sql
        .substring(0, sql.indexOf("WHEN NOT MATCHED"))
        .replace("ON t.id = s.id", "ON t.id = s.id + 1000")
      val before = spark.table("b.db.t").collect().toSeq
      assertEquals(Seq(Row(0L, 0L, 0L, 0L)), spark.sql(query).collect().toSeq)
      assertEquals(before, spark.table("b.db.t").collect().toSeq)
      assertEquals(0L, version("b"))
    }

  @Test
  def missingNativeHistoryFailsWithoutPublication(): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies { (spark, _) =>
      val root = Paths.get(NativeSelectFixture.locations("a"), "_change_data")
      val files = Files.walk(root)
      try
        files
          .iterator()
          .asScala
          .filter(_.getFileName.toString.endsWith(".parquet"))
          .foreach(Files.delete)
      finally files.close()
      assertThrows(classOf[Exception], () => spark.sql(sql).collect())
      assertEquals(0L, version("b"))
    }

  @Test
  def fakeScalarOwnerCannotMintTransactionAuthority(): Unit = {
    val fake = new DeltaRangerNativeOperation.TransactionOwner {
      override def session(): org.apache.spark.sql.SparkSession = null
      override def snapshot(): org.apache.spark.sql.delta.Snapshot = null
      override def requireLive(): Unit = ()
    }
    assertThrows(
      classOf[SecurityException],
      () => DeltaRangerNativeOperation.transactionState(fake)
    )
  }

  @Test
  def mergeIntentRejectsForeignActionsAndCopiedAttempt(): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies(
      (spark, _) => {
        import org.apache.spark.sql.delta.actions.{
          Action,
          AddFile,
          RemoveFile,
          AddCDCFile,
          SetTransaction
        }
        import org.apache.spark.sql.delta.DeltaOperations
        spark.conf.set("spark.databricks.delta.write.txnAppId", "intent-consumer")
        spark.conf.set("spark.databricks.delta.write.txnVersion", "1")
        spark.conf.set("spark.databricks.delta.write.txnVersion.autoReset.enabled", "false")
        val registration = NativeSelectFixture.bindingFixture(spark, "b")
        val refresh = registration.onRead
        var inspected = false
        var retired: DeltaRangerMergeCommit = null
        registration.onRead = n => {
          refresh(n)
          DeltaRangerMergeCommit.current.foreach { owner =>
            if (!inspected) {
              inspected = true; retired = owner
              val actions = field[Seq[Action]](owner, "actions")
              assertTrue(actions.exists(_.isInstanceOf[AddFile]))
              assertTrue(actions.exists(_.isInstanceOf[RemoveFile]))
              assertTrue(actions.exists(_.isInstanceOf[AddCDCFile]))
              assertTrue(actions.exists(_.isInstanceOf[SetTransaction]))
              val intent = field[AnyRef](owner, "intent")
              val operation = field[DeltaOperations.Merge](owner, "nativeOperation")
              val method =
                intent.getClass.getDeclaredMethods.find(_.getName == "requireArguments").get
              method.setAccessible(true)
              method.invoke(intent, actions, operation, Map.empty[String, String])
              actions.indices.foreach { index =>
                val changed: Action = actions(index) match {
                  case a: AddFile        => a.copy(path = "../foreign.parquet")
                  case a: RemoveFile     => a.copy(path = "../foreign.parquet")
                  case a: AddCDCFile     => a.copy(path = "../foreign.parquet")
                  case a: SetTransaction => a.copy(appId = "foreign-consumer")
                  case other =>
                    throw new AssertionError(
                      "Unexpected native MERGE action: " + other.getClass.getName
                    )
                }
                val failure = assertThrows(
                  classOf[java.lang.reflect.InvocationTargetException],
                  () => {
                    method.invoke(
                      intent,
                      actions.updated(index, changed),
                      operation,
                      Map.empty[String, String]
                    ); ()
                  }
                )
                assertTrue(failure.getCause.isInstanceOf[SecurityException])
              }
              val foreign = DeltaOperations.Merge(None, Nil, Nil, Nil)
              val failure = assertThrows(
                classOf[java.lang.reflect.InvocationTargetException],
                () => {
                  method.invoke(intent, actions, foreign, Map.empty[String, String]); ()
                }
              )
              assertTrue(failure.getCause.isInstanceOf[SecurityException])
              val permit = field[DeltaRangerMergeCommand.PublicationPermit](owner, "permit")
              assertThrows(classOf[SecurityException], () => new DeltaRangerMergeCommit(permit))
            }
          }
        }
        assertEquals(Seq(Row(3L, 1L, 1L, 1L)), spark.sql(sql).collect().toSeq)
        assertTrue(inspected)
        assertThrows(classOf[SecurityException], () => retired.run())
        val rows = spark.table("b.db.t").collect().toSeq.groupBy(row => row).map {
          case (row, copies) => row -> copies.size
        }
        assertEquals(
          Map(
            Row(1L, "changed", "keep") -> 1,
            Row(3L, null, "keep") -> 1,
            Row(4L, "new", "keep") -> 1
          ),
          rows
        )
        val events = spark
          .sql("SELECT id, _change_type FROM table_changes('b.db.t', 2, 2)")
          .collect()
          .toSeq
          .groupBy(row => row)
          .map { case (row, copies) => row -> copies.size }
        assertEquals(
          Map(
            Row(1L, "update_preimage") -> 1,
            Row(1L, "update_postimage") -> 1,
            Row(2L, "delete") -> 1,
            Row(4L, "insert") -> 1
          ),
          events
        )
      },
      prepareTarget = spark =>
        spark
          .sql(
            s"ALTER TABLE delta.`${NativeSelectFixture.locations("b")}` SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
          )
          .collect()
    )

  @Test
  def postHelperDriftPreventsPublication(): Unit = {
    Seq("subject", "configuration", "native-id", "schema", "source-policy").foreach { change =>
      new TestDeltaRangerInstalledMerge().withPolicies { (spark, plugin) =>
        val registration = NativeSelectFixture.bindingFixture(spark, "b")
        val refresh = registration.onRead
        val before = parquetFiles("b")
        val login = org.apache.hadoop.security.UserGroupInformation.getLoginUser
        var writing = false; var changed = false
        registration.onRead = n => {
          refresh(n)
          val active =
            field[ThreadLocal[DeltaRangerMergeWrite]](DeltaRangerMergeWrite, "active").get()
          writing ||= active != null
          if (writing && active == null && !changed && parquetFiles("b").diff(before).nonEmpty) {
            changed = true
            change match {
              case "subject" =>
                org.apache.hadoop.security.UserGroupInformation.setLoginUser(
                  org.apache.hadoop.security.UserGroupInformation
                    .createRemoteUser("changed-subject")
                )
              case "configuration" => spark.conf.set("spark.sql.shuffle.partitions", "3")
              case "native-id" =>
                registration.properties.put(
                  "delta.registration.receipt.nativeId",
                  java.util.UUID.randomUUID().toString
                )
              case "schema" => registration.columns = Array.empty
              case "source-policy" =>
                new TestDeltaRangerMergePolicies().publish(plugin, sourceDeniedSelect = Seq("id"))
            }
          }
        }
        try {
          assertThrows(classOf[Exception], () => { spark.sql(sql).collect(); () }, change)
          assertTrue(changed, change)
          assertEquals(0L, version("b"), change)
          assertTrue(
            parquetFiles("b").diff(before).nonEmpty,
            "orphan native files remain separate from committed state"
          )
        } finally org.apache.hadoop.security.UserGroupInformation.setLoginUser(login)
      }
    }
  }

  private def parquetFiles(name: String): Set[String] = {
    val paths = Files.walk(Paths.get(NativeSelectFixture.locations(name)))
    try
      paths
        .iterator()
        .asScala
        .filter(_.getFileName.toString.endsWith(".parquet"))
        .map(_.toString)
        .toSet
    finally paths.close()
  }

  private def causes(error: Throwable): Seq[Throwable] =
    if (error == null) Nil else error +: causes(error.getCause)

}

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

import java.lang.reflect.InvocationTargetException
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.spark.sql.delta.{DeltaLog, OptimisticTransaction}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** Adversarial checks against the real captured transaction and its original native publication. */
class TestDeltaRangerCdfPublication {
  @Test
  def nativePublicationRejectsForeignIdentityMetadataAndActions(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      spark.conf.set("spark.sql.catalog.a", classOf[CdfAlterForwardingCatalog].getName)
      val fixture = NativeSelectFixture.bindingFixture(spark, "a")
      val location = NativeSelectFixture.locations("a")
      val table =
        fixture.wrapper.loadTable(Identifier.of(Array("db"), "t")).asInstanceOf[DeltaTableV2]
      val target = fixture.wrapper.registrationBindings().resolveCdfPropertyMutation(table)
      val foreignTransaction =
        target.captureTransaction(() => target.log().startTransaction(Some(target.descriptor())))
      val version = DeltaLog.forTable(spark, location).unsafeVolatileSnapshot.version + 1
      val path = Paths.get(location).resolve("_delta_log").resolve(f"$version%020d.json")
      val mapper = new ObjectMapper
      var exercised = false
      fixture.onRead = _ =>
        DeltaRangerCdfPropertyMutation.current.foreach { owner =>
          if (!exercised && Files.exists(path)) {
            exercised = true
            // Reach the content verifier directly so a frozen digest cannot mask missing checks.
            val transactionField =
              owner.getClass.getDeclaredFields.find(_.getType == classOf[OptimisticTransaction]).get
            transactionField.setAccessible(true)
            val transaction = transactionField.get(owner).asInstanceOf[OptimisticTransaction]
            val verify =
              owner.getClass.getDeclaredMethods.find(_.getName == "verifyPublication").get
            verify.setAccessible(true)
            val original = Files.readAllBytes(path)
            verify.invoke(owner, original, transaction.metadata, transaction.protocol)
            def reject(change: Vector[JsonNode] => Vector[JsonNode]): Unit = {
              val actions = new String(original, StandardCharsets.UTF_8).trim
                .split("\n")
                .toVector
                .map(mapper.readTree)
              val changed = (change(actions).map(_.toString).mkString("\n") + "\n")
                .getBytes(StandardCharsets.UTF_8)
              assertThrows(
                classOf[InvocationTargetException],
                () => {
                  verify.invoke(owner, changed, transaction.metadata, transaction.protocol); ()
                }
              )
            }
            reject { actions =>
              actions.head
                .get("commitInfo")
                .asInstanceOf[ObjectNode]
                .put("txnId", foreignTransaction.txnId)
              actions
            }
            reject { actions =>
              actions.head
                .get("commitInfo")
                .asInstanceOf[ObjectNode]
                .put("readVersion", version + 1)
              actions
            }
            reject { actions =>
              val overflow = new java.math.BigInteger(transaction.readVersion.toString)
                .add(java.math.BigInteger.ONE.shiftLeft(64))
              actions.head.get("commitInfo").asInstanceOf[ObjectNode].put("readVersion", overflow)
              actions
            }
            reject { actions =>
              actions.head.get("commitInfo").asInstanceOf[ObjectNode].put("operation", "WRITE")
              actions
            }
            reject { actions =>
              actions.head
                .get("commitInfo")
                .get("operationParameters")
                .asInstanceOf[ObjectNode]
                .put("properties", "{}")
              actions
            }
            reject { actions =>
              actions
                .find(_.has("metaData"))
                .get
                .get("metaData")
                .asInstanceOf[ObjectNode]
                .put("description", "unrelated")
              actions
            }
            reject(actions => actions :+ mapper.readTree("{\"add\":{\"path\":\"foreign\"}}"))
            reject(actions =>
              actions :+ mapper.readTree("{\"txn\":{\"appId\":\"foreign\",\"version\":1}}")
            )
            reject(actions => actions :+ actions.find(_.has("metaData")).get)
            reject { actions =>
              actions.head.get("commitInfo").asInstanceOf[ObjectNode].put("unexpected", "foreign")
              actions
            }
          }
        }
      spark
        .sql("ALTER TABLE a.db.t SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true')")
        .collect()
      assertTrue(exercised)
      assertNull(DeltaRangerNativeOperation.current())
    }
  @Test
  def alteredPublishedBytesInvalidateTheLiveOwner(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      spark.conf.set("spark.sql.catalog.a", classOf[CdfAlterForwardingCatalog].getName)
      CdfAlterForwardingCatalog.alters.set(0)
      val fixture = NativeSelectFixture.bindingFixture(spark, "a")
      val location = NativeSelectFixture.locations("a")
      val version = DeltaLog.forTable(spark, location).unsafeVolatileSnapshot.version + 1
      val path = Paths.get(location).resolve("_delta_log").resolve(f"$version%020d.json")
      var changed = false
      var owner: DeltaRangerCdfPropertyMutation = null
      fixture.onRead = _ =>
        DeltaRangerCdfPropertyMutation.current.foreach { current =>
          owner = current
          if (!changed && Files.exists(path)) {
            changed = true
            val mapper = new ObjectMapper
            val original = Files.readAllBytes(path)
            val actions = new String(original, StandardCharsets.UTF_8).trim
              .split("\n")
              .toVector
              .map(mapper.readTree)
            val info = actions.head.get("commitInfo").asInstanceOf[ObjectNode]
            info.put("timestamp", info.get("timestamp").longValue() + 1)
            val altered =
              (actions.map(_.toString).mkString("\n") + "\n").getBytes(StandardCharsets.UTF_8)
            assertEquals(original.length, altered.length)
            val hadoop = new HadoopPath(path.toUri)
            val fs = hadoop.getFileSystem(spark.sessionState.newHadoopConf())
            val status = fs.getFileStatus(hadoop)
            val output = fs.create(hadoop, true)
            try output.write(altered)
            finally output.close()
            fs.setTimes(hadoop, status.getModificationTime, status.getAccessTime)
          }
        }
      assertThrows(
        classOf[Exception],
        () => {
          spark
            .sql("ALTER TABLE a.db.t SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true')")
            .collect(); ()
        }
      )
      assertTrue(changed)
      assertEquals(1, CdfAlterForwardingCatalog.alters.get())
      assertNotNull(owner)
      assertThrows(classOf[SecurityException], () => owner.requireLive())
      assertNull(DeltaRangerNativeOperation.current())
      assertTrue(OptimisticTransaction.getActive.isEmpty)
      assertFalse(Files.exists(path.resolveSibling(f"${version + 1}%020d.json")))
    }

  @Test
  def foreignNextVersionAndNestedMutationAreNotAdopted(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      spark.conf.set("spark.sql.catalog.a", classOf[CdfAlterForwardingCatalog].getName)
      CdfAlterForwardingCatalog.alters.set(0)
      val fixture = NativeSelectFixture.bindingFixture(spark, "a")
      val table =
        fixture.wrapper.loadTable(Identifier.of(Array("db"), "t")).asInstanceOf[DeltaTableV2]
      val target = fixture.wrapper.registrationBindings().resolveCdfPropertyMutation(table)
      var nestedRejected = false
      fixture.onRead = _ =>
        DeltaRangerCdfPropertyMutation.current.foreach { owner =>
          if (!nestedRejected) {
            nestedRejected = true
            val nested = new DeltaRangerCdfPropertyMutation(
              target,
              spark,
              Map("delta.enableChangeDataFeed" -> "true"),
              () => owner.requireLive(),
              () => ()
            )
            assertThrows(classOf[SecurityException], () => nested.run())
          }
        }
      spark
        .sql("ALTER TABLE a.db.t SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true')")
        .collect()
      assertTrue(nestedRejected)
      fixture.onRead = _ => ()
      val location = NativeSelectFixture.locations("a")
      val version = DeltaLog.forTable(spark, location).unsafeVolatileSnapshot.version + 1
      val log = Paths.get(location).resolve("_delta_log")
      val next = log.resolve(f"$version%020d.json")
      val original = Files.readAllBytes(log.resolve(f"${version - 1}%020d.json"))
      var injected = false
      policy.onRequest = _ => if (!injected) { injected = true; Files.write(next, original); () }
      val calls = CdfAlterForwardingCatalog.alters.get()
      assertThrows(
        classOf[Exception],
        () => {
          spark
            .sql("ALTER TABLE a.db.t SET TBLPROPERTIES ('delta.enableChangeDataFeed'='false')")
            .collect(); ()
        }
      )
      assertTrue(injected)
      assertEquals(calls, CdfAlterForwardingCatalog.alters.get())
      assertArrayEquals(original, Files.readAllBytes(next))
      assertFalse(Files.exists(log.resolve(f"${version + 1}%020d.json")))
      assertNull(DeltaRangerNativeOperation.current())
      assertTrue(OptimisticTransaction.getActive.isEmpty)
    }

}

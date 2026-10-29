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

import com.fasterxml.jackson.databind.ObjectMapper
import java.nio.file.Files
import scala.collection.JavaConverters._
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

/** Fail-closed request and payload classification for the native Python append bridge. */
class TestDeltaRangerLiveAppendAqe {
  private val json = new ObjectMapper()

  /** Only the two existing native writer frontends are admitted. */
  @Test
  def boundedFrontends(): Unit = {
    Seq("insertInto", "saveAsTable").foreach(DeltaRangerLiveAppendAqe.validateFrontend)
    Seq("writeTo", "sql", "insertInto ", "", null).foreach(api =>
      assertThrows(
        classOf[IllegalArgumentException],
        () => DeltaRangerLiveAppendAqe.validateFrontend(api)
      )
    )
  }

  /** Failed and metadata callbacks cannot stand in for an owned payload query. */
  @Test
  def payloadOwnership(): Unit = {
    assert(DeltaRangerLiveAppendAqe.callbackKind(owned = true, failed = false) == "write-payload")
    assert(
      DeltaRangerLiveAppendAqe.callbackKind(owned = false, failed = false) == "metadata-or-readback"
    )
    assert(DeltaRangerLiveAppendAqe.callbackKind(owned = true, failed = true) == "failed-unplanned")
  }

  /** Fresh native adaptive caches have no maintenance owner before the first append. */
  @Test
  def nativeAdaptiveWarmupBeforeOwnership(): Unit = {
    for (order <- Seq("target-first", "project-first")) {
      val source = new TestDeltaRangerNativeSelect.Fixture
      source.customCatalog = "b"
      try
        RawNativeObservationFixture.withEnabled(
          storage = Map(
            "spark.sql.adaptive.enabled" -> "true",
            "spark.databricks.delta.write.txnVersion.autoReset.enabled" -> "false"
          )
        ) { (spark, _, policy) =>
          NativeSelectFixture.catalogPolicies = Map("a" -> policy, "b" -> source)
          val output = Files.createTempDirectory("append-aqe-native-warm-")
          val observer = new DeltaRangerLiveAppend.WarmObserver(order)
          val (native, frames) = DeltaRangerLiveAppend.warmCaches(
            spark,
            NativeSelectFixture.locations("a"),
            order,
            output,
            observer
          )
          val before = json.readTree(output.resolve("warm-before.json").toFile)
          assert(before.path("aqe").asBoolean())
          assert(before.path("entries").elements().asScala.forall(_.path("owner").isNull))
          assert(before.path("entries").elements().asScala.forall(_.path("loaded").asBoolean()))
          assert(spark.sessionState.conf.adaptiveExecutionEnabled)
          assert(native.sessionState.conf.adaptiveExecutionEnabled)
          RawNativeObservationFixture.active(native) {
            assert(frames.head.collect().length == 3)
            assert(frames(1).collect().length == 3)
          }
          spark.conf.set("spark.databricks.delta.write.txnAppId", "native-warm-" + order)
          spark.conf.set("spark.databricks.delta.write.txnVersion", "1")
          spark
            .sql("SELECT l.id, l.secret, l.region FROM b.db.t l JOIN b.db.t r ON l.id = r.id")
            .write
            .mode("append")
            .insertInto("a.db.t")
          DeltaRangerLiveAppendAqe.lateWarmReadback(
            native,
            NativeSelectFixture.locations("a"),
            output.resolve("late.json")
          )
          val late = json.readTree(output.resolve("late.json").toFile)
          assert(late.path("freshDataFrames").asBoolean())
          for (key <- Seq("targetRows", "repeatedTargetRows", "dependentRows"))
            assert(late.path(key).size() == 5)
          observer.record(spark, frames, output.resolve("after.json"), "success")
          val after = json.readTree(output.resolve("after.json").toFile)
          assert(
            after
              .path("entries")
              .elements()
              .asScala
              .forall(entry =>
                !entry.path("owner").isNull && entry.path("ownerKey") == entry.path("key")
              )
          )
        }
      finally source.engine.close()
    }
  }
}

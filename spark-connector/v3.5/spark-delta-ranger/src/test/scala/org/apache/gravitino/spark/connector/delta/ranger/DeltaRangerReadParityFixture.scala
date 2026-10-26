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
import java.util.{LinkedHashMap, UUID}
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{DataFrame, SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.ranger.plugin.service.RangerBasePlugin
import org.junit.jupiter.api.Assertions._

/** Installed parity fixtures preserve both existing ordinary execution profiles. */
private[ranger] object DeltaRangerReadParityFixture {
  def withSpark(test: (SparkSession, TestDeltaRangerNativeSelect.Fixture) => Unit): Unit =
    Seq(false, true).foreach { adaptive =>
      val policy = new TestDeltaRangerNativeSelect.Fixture
      try
        NativeSelectFixture.withSpark(
          spark => { recordProviders(spark); test(spark, policy) },
          Some(NativeSelectFixture.extension(policy)),
          startupOptions = Map("spark.sql.adaptive.enabled" -> adaptive.toString)
        )
      finally policy.engine.close()
    }

  def demands(frame: DataFrame): Vector[(String, Set[String])] = {
    val receipt = DeltaRangerDecisionReceipt.consumeOwned(
      frame.queryExecution.normalized,
      frame.sparkSession,
      optimized = false
    )
    receipt.obligations.map(o =>
      s"${o.scan.catalog}.${o.scan.database}.${o.scan.table}" -> o.columns
    )
  }

  // Construct a separate stock session sharing only SharedState, so its real Range cache is
  // unrelated to the governed routes. The constructor is public in bytecode but Scala-scoped.
  def withUnrelatedCache(spark: SparkSession)(test: => Unit): Unit = {
    val constructor = classOf[SparkSession].getConstructors.find(_.getParameterCount == 5).get
    val plain = constructor
      .newInstance(
        spark.sparkContext,
        Some(spark.sharedState),
        None,
        new SparkSessionExtensions,
        Map.empty[String, String]
      )
      .asInstanceOf[SparkSession]
    val unrelated = plain.range(3).cache()
    val manager = spark.sharedState.cacheManager
    try {
      assertEquals(3L, unrelated.count())
      val entry = manager.lookupCachedData(unrelated.queryExecution.normalized).get
      test
      assertSame(entry, manager.lookupCachedData(unrelated.queryExecution.normalized).get)
      assertEquals(3L, unrelated.count())
    } finally unrelated.unpersist()
  }

  private def recordProviders(spark: SparkSession): Unit = {
    assertEquals("17", System.getProperty("java.specification.version"))
    assertEquals("2.12.21", scala.util.Properties.versionNumberString)
    sys.env.get("READ_PARITY_SPARK_PIN").foreach(pin => assertEquals(pin, spark.version))
    val providers = new LinkedHashMap[String, String]()
    providers.put("spark", spark.version)
    providers.put("java", System.getProperty("java.version"))
    providers.put("scala", scala.util.Properties.versionNumberString)
    providers.put("adaptive", spark.sessionState.conf.adaptiveExecutionEnabled.toString)
    Seq(
      classOf[SparkSession],
      classOf[DeltaLog],
      classOf[RangerBasePlugin],
      classOf[DeltaRangerReadRule]
    ).foreach { cls =>
      providers.put(cls.getName, cls.getProtectionDomain.getCodeSource.getLocation.toString)
    }
    sys.env.get("READ_PARITY_DELTA_PIN").foreach { pin =>
      assertTrue(providers.get(classOf[DeltaLog].getName).contains("-" + pin + ".jar"))
    }
    assertTrue(providers.get(classOf[RangerBasePlugin].getName).contains("-2.8.0.jar"))
    sys.env.get("READ_ACTION_EVIDENCE").foreach { directory =>
      Files.write(
        Paths.get(directory, "providers-" + UUID.randomUUID() + ".json"),
        new ObjectMapper().writeValueAsBytes(providers)
      )
    }
  }
}

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

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.delta.actions.{Format, Metadata, Protocol}
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

/** Expectations are literal schemas; no transaction or authorization is issued by this fixture. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestDeltaRangerAppendSchema {
  private var spark: SparkSession = _
  private val original = StructType(
    Seq(
      StructField("id", IntegerType, nullable = true),
      StructField("value", StringType, nullable = true)
    )
  )
  private val ordinary = Protocol(1, 2)
  private val cdf = Protocol(1, 7, None, Some(Set("changeDataFeed", "appendOnly", "invariants")))
  private def base: Metadata = Metadata(
    id = "retained-native-id",
    name = "retained-name",
    description = "retained-description",
    format = Format("parquet", Map("retained" -> "format")),
    schemaString = original.json,
    partitionColumns = Seq("id"),
    configuration = Map("delta.appendOnly" -> "false", "registration-marker" -> "exact"),
    createdTime = Some(1234L)
  )
  private def wider: StructType = original.add(StructField("extra", StringType, nullable = false))
  private val enabled = Map("mergeSchema" -> "true")

  @BeforeAll
  def start(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("append-schema-intent")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val origins = Seq(classOf[SparkSession], classOf[Protocol], classOf[StructType]).map(c =>
      c.getName + "=" + c.getProtectionDomain.getCodeSource.getLocation
    )
    LoggerFactory
      .getLogger(getClass)
      .warn("Schema intent loaded runtime: " + origins.mkString("; "))
  }

  @AfterAll
  def stop(): Unit = if (spark != null) {
    spark.stop(); SparkSession.clearActiveSession(); SparkSession.clearDefaultSession()
  }

  @Test
  def identicalSchemasKeepOrdinaryCdfAndPreviouslyQualifiedMetadata(): Unit = {
    for (protocol <- Seq(ordinary, cdf); options <- Seq(Map.empty[String, String], enabled)) {
      val metadata =
        base.copy(configuration = base.configuration + ("delta.enableChangeDataFeed" -> "true"))
      val intent = DeltaRangerAppendSchema.derive(spark, metadata, protocol, original, options)
      assertFalse(intent.evolves)
      assertEquals(metadata, intent.baseMetadata)
      assertEquals(metadata, intent.expectedMetadata)
      intent.requirePending(metadata, protocol)
    }
    val special = original.add(
      StructField(
        "mapped",
        StringType,
        true,
        new MetadataBuilder().putLong("delta.columnMapping.id", 1L).build()
      )
    )
    val mapped = base.copy(
      schemaString = special.json,
      configuration = Map("delta.columnMapping.mode" -> "name")
    )
    assertFalse(
      DeltaRangerAppendSchema.derive(spark, mapped, Protocol(2, 5), special, enabled).evolves
    )
  }

  @Test
  def additionsUseNativeNullabilityAndChangeOnlySchemaString(): Unit = {
    for (protocol <- Seq(ordinary, cdf)) {
      val metadata =
        if (protocol == cdf)
          base.copy(configuration = base.configuration + ("delta.enableChangeDataFeed" -> "true"))
        else base
      val intent = DeltaRangerAppendSchema.derive(spark, metadata, protocol, wider, enabled)
      val expected =
        metadata.copy(schemaString = original.add("extra", StringType, nullable = true).json)
      assertTrue(intent.evolves)
      assertEquals(metadata, intent.baseMetadata)
      assertEquals(expected, intent.expectedMetadata)
      assertEquals(protocol, intent.protocol)
      intent.requirePending(expected, protocol)
    }
  }

  @Test
  def concreteScalarAdditionsStayConcreteAndNullable(): Unit = {
    Seq(
      BooleanType,
      ByteType,
      ShortType,
      IntegerType,
      LongType,
      FloatType,
      DoubleType,
      StringType,
      BinaryType,
      DateType,
      TimestampType,
      DecimalType(10, 2)
    ).foreach(dataType => {
      val intent = DeltaRangerAppendSchema
        .derive(spark, base, ordinary, original.add("extra", dataType, nullable = false), enabled)
      assertEquals(
        base.copy(schemaString = original.add("extra", dataType, nullable = true).json),
        intent.expectedMetadata
      )
    })
    Seq(TimestampNTZType, CharType(5), VarcharType(5), CalendarIntervalType).foreach(dataType =>
      rejects {
        DeltaRangerAppendSchema.derive(
          spark,
          base,
          ordinary,
          original.add("extra", dataType, nullable = false),
          enabled
        )
      }
    )
  }

  @Test
  def evolutionPreservesOrdinaryExistingAndAddedFieldMetadata(): Unit = {
    val existingComment = new MetadataBuilder().putString("comment", "retained old comment").build()
    val sourceComment = new MetadataBuilder().putString("comment", "source comment").build()
    val addedComment = new MetadataBuilder()
      .putString("comment", "added comment")
      .putString("lineage", "ordinary metadata")
      .build()
    val old = StructType(original.fields.updated(0, original(0).copy(metadata = existingComment)))
    val source = StructType(original.fields.updated(0, original(0).copy(metadata = sourceComment)))
      .add(StructField("extra", StringType, false, addedComment))
    val targetMetadata = base.copy(schemaString = old.json)
    val intent = DeltaRangerAppendSchema.derive(spark, targetMetadata, ordinary, source, enabled)
    assertEquals(
      targetMetadata.copy(schemaString =
        old.add(StructField("extra", StringType, true, addedComment)).json
      ),
      intent.expectedMetadata
    )
  }

  @Test
  def explicitMergeOptionOverridesSessionAndOverwriteIsNotEvolutionPermission(): Unit = {
    val key = "spark.databricks.delta.schema.autoMerge.enabled"
    spark.conf.set(key, "false")
    try {
      rejects { DeltaRangerAppendSchema.derive(spark, base, ordinary, wider, Map.empty) }
      rejects {
        DeltaRangerAppendSchema.derive(
          spark,
          base,
          ordinary,
          wider,
          Map("overwriteSchema" -> "true")
        )
      }
      assertTrue(DeltaRangerAppendSchema.derive(spark, base, ordinary, wider, enabled).evolves)
      spark.conf.set(key, "true")
      assertTrue(DeltaRangerAppendSchema.derive(spark, base, ordinary, wider, Map.empty).evolves)
      rejects {
        DeltaRangerAppendSchema.derive(spark, base, ordinary, wider, Map("mergeSchema" -> "false"))
      }
    } finally spark.conf.unset(key)
  }

  @Test
  def oldPrefixNamesOrderTypesAndDirectionalNullabilityStayExact(): Unit = {
    val strict = base.copy(schemaString =
      StructType(original.fields.updated(0, original(0).copy(nullable = false))).json
    )
    val bad = Seq(
      StructType(wider.fields.reverse),
      StructType(wider.fields.updated(0, wider(0).copy(name = "ID"))),
      StructType(wider.fields.updated(0, wider(0).copy(name = "renamed"))),
      StructType(wider.fields.updated(0, wider(0).copy(dataType = LongType))),
      StructType(Seq(original(0))),
      StructType(Seq(original(0), StructField("extra", StringType)))
    )
    bad.foreach(source =>
      rejects { DeltaRangerAppendSchema.derive(spark, base, ordinary, source, enabled) }
    )
    rejects { DeltaRangerAppendSchema.derive(spark, strict, ordinary, wider, enabled) }
    val safe = StructType(original.fields.updated(0, original(0).copy(nullable = false)))
    assertFalse(DeltaRangerAppendSchema.derive(spark, base, ordinary, safe, enabled).evolves)
  }

  @Test
  def nullCollidingAndInvalidOptionsAreRefusedEvenWithoutEvolution(): Unit = {
    Seq(
      null,
      Map(null.asInstanceOf[String] -> "true"),
      Map("mergeSchema" -> null.asInstanceOf[String]),
      Map("mergeSchema" -> "true", "MERGESCHEMA" -> "false"),
      Map("mergeSchema" -> "yes"),
      Map("unknown" -> "true")
    ).foreach(options =>
      rejects {
        DeltaRangerAppendSchema.derive(spark, base, ordinary, original, options)
      }
    )
    rejects { DeltaRangerAppendSchema.derive(null, base, ordinary, original, enabled) }
    rejects { DeltaRangerAppendSchema.derive(spark, null, ordinary, original, enabled) }
    rejects { DeltaRangerAppendSchema.derive(spark, base, null, original, enabled) }
    rejects { DeltaRangerAppendSchema.derive(spark, base, ordinary, null, enabled) }
    Seq(original.add("ID", StringType), wider.add("EXTRA", StringType)).foreach(source =>
      assertThrows(
        classOf[AnalysisException],
        () => {
          DeltaRangerAppendSchema.derive(spark, base, ordinary, source, enabled); ()
        }
      )
    )
  }

  @Test
  def evolutionRefusesNullNestedAndSpecialFieldMetadataWithoutWriterCleanup(): Unit = {
    val special = new MetadataBuilder().putString("delta.generationExpression", "id + 1").build()
    val sources = Seq(
      original.add("extra", NullType),
      original.add("extra", StructType(Seq(StructField("child", StringType)))),
      original.add("extra", ArrayType(StringType)),
      original.add("extra", MapType(StringType, StringType)),
      original.add(StructField("extra", StringType, true, special)),
      StructType(wider.fields.updated(0, wider(0).copy(metadata = special)))
    )
    sources.foreach(source =>
      rejects { DeltaRangerAppendSchema.derive(spark, base, ordinary, source, enabled) }
    )
    val nested = StructType(
      Seq(StructField("nested", StructType(Seq(StructField("child", StringType)))))
    )
    val nestedBase = base.copy(schemaString = nested.json)
    val nestedChange = StructType(
      Seq(StructField("nested", nested(0).dataType.asInstanceOf[StructType].add("new", StringType)))
    )
    rejects { DeltaRangerAppendSchema.derive(spark, nestedBase, ordinary, nestedChange, enabled) }
  }

  @Test
  def evolutionRefusesUnqualifiedProfilesAndMetadata(): Unit = {
    Seq(
      Protocol(2, 5),
      Protocol(1, 4),
      Protocol(1, 7, None, Some(Set("typeWidening"))),
      cdf.copy(minReaderVersion = 3, readerFeatures = Some(Set.empty[String]))
    ).foreach(protocol =>
      rejects {
        DeltaRangerAppendSchema.derive(spark, base, protocol, wider, enabled)
      }
    )
    Seq(
      "delta.columnMapping.mode" -> "name",
      "delta.enableRowTracking" -> "true",
      "delta.enableTypeWidening" -> "true",
      "delta.enableInCommitTimestamps" -> "true",
      "delta.coordinatedCommits.commitCoordinator-preview" -> "test",
      "delta.minWriterVersion" -> "7",
      "delta.feature.unknown" -> "supported"
    ).foreach(property =>
      rejects {
        DeltaRangerAppendSchema.derive(
          spark,
          base.copy(configuration = base.configuration + property),
          ordinary,
          wider,
          enabled
        )
      }
    )
    Seq(
      "delta.identity.start",
      "delta.columnMapping.id",
      "parquet.field.id",
      "EXISTS_DEFAULT",
      "__CHAR_VARCHAR_TYPE_STRING",
      "__file_source_generated_metadata_col"
    ).foreach(key => {
      val special = new MetadataBuilder().putString(key, "excluded").build()
      rejects {
        DeltaRangerAppendSchema.derive(
          spark,
          base,
          ordinary,
          original.add(StructField("extra", StringType, true, special)),
          enabled
        )
      }
    })
    val metadata = new MetadataBuilder().putString("CURRENT_DEFAULT", "1").build()
    val existingSpecial = base.copy(schemaString =
      StructType(original.fields.updated(0, original(0).copy(metadata = metadata))).json
    )
    rejects { DeltaRangerAppendSchema.derive(spark, existingSpecial, ordinary, wider, enabled) }
  }

  @Test
  def pendingComparisonIsExactForEveryMetadataAndProtocolField(): Unit = {
    val intent = DeltaRangerAppendSchema.derive(spark, base, ordinary, wider, enabled)
    val expected = intent.expectedMetadata
    Seq(
      base,
      expected.copy(id = "different"),
      expected.copy(name = "different"),
      expected.copy(description = "different"),
      expected.copy(format = Format("different")),
      expected.copy(partitionColumns = Seq.empty),
      expected.copy(configuration = expected.configuration + ("unrelated" -> "change")),
      expected.copy(createdTime = Some(99L)),
      expected.copy(schemaString = wider.json),
      null
    ).foreach(metadata =>
      rejects {
        intent.requirePending(metadata, ordinary)
      }
    )
    Seq(Protocol(1, 3), cdf, null).foreach(protocol =>
      rejects { intent.requirePending(expected, protocol) }
    )
    intent.requirePending(expected, ordinary)
  }

  @Test
  def retainedConfigurationCannotChangeTheFrozenExpectation(): Unit = {
    val retained = new AppendWriterOptionsFixture.RetainedMap(base.configuration)
    val metadata = base.copy(configuration = retained)
    val intent = DeltaRangerAppendSchema.derive(spark, metadata, ordinary, wider, enabled)
    retained.current = retained.current + ("unrelated" -> "change")
    assertEquals(base.configuration, intent.baseMetadata.configuration)
    assertEquals(base.configuration, intent.expectedMetadata.configuration)
    rejects {
      intent.requirePending(intent.expectedMetadata.copy(configuration = retained), ordinary)
    }
  }

  private def rejects(body: => Unit): Unit = {
    assertThrows(classOf[IllegalArgumentException], () => body)
    ()
  }
}

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

import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** Pure read compatibility checks grant no execution or writer authority. */
class TestDeltaRangerCdfSchema {
  private val original = StructType(
    Seq(StructField("id", LongType, false), StructField("value", StringType, true))
  )
  private def check(previous: StructType, next: StructType): Unit =
    DeltaRangerCdfSchema.requireCompatible(previous, next)
  private def refused(previous: StructType, next: StructType): Unit = {
    val failure = assertThrows(classOf[SecurityException], () => check(previous, next))
    assertTrue(failure.getMessage.contains("CDF schema"))
  }

  @Test
  def matchingSchemasRetainExistingAdmission(): Unit = {
    val unusual = StructType(
      Seq(
        StructField(
          "odd name",
          TimestampNTZType,
          false,
          new MetadataBuilder().putLong("delta.columnMapping.id", 1).build()
        )
      )
    )
    check(original, original)
    check(unusual, unusual)
    check(StructType(Nil), StructType(Nil))
  }

  @Test
  def nullableConcreteScalarAdditionsPreserveExactPrefix(): Unit = {
    val types = Seq(
      BooleanType,
      ByteType,
      ShortType,
      IntegerType,
      LongType,
      FloatType,
      DoubleType,
      DecimalType(19, 4),
      StringType,
      BinaryType,
      DateType,
      TimestampType
    )
    types.foreach(t => check(original, original.add("extra", t, true)))
    check(
      original,
      StructType(original.fields ++ types.zipWithIndex.map { case (t, i) =>
        StructField("extra" + i, t, true)
      })
    )
  }

  @Test
  def unchangedNestedFieldsAndOrdinaryMetadataSurviveAddition(): Unit = {
    val comment =
      new MetadataBuilder().putString("comment", "retained").putString("app.label", "label").build()
    val nested = original.add(
      StructField(
        "nested",
        ArrayType(
          StructType(Seq(StructField("leaf", MapType(StringType, LongType), false, comment)))
        ),
        true,
        comment
      )
    )
    check(nested, nested.add(StructField("added", StringType, true, comment)))
  }

  @Test
  def oldFieldTypeDropRenameReorderNullabilityAndMetadataChangesFail(): Unit = {
    val changes = Seq(
      StructType(original.fields.take(1)),
      StructType(original.fields.reverse),
      StructType(original.fields.updated(0, original.fields(0).copy(name = "renamed"))),
      StructType(original.fields.updated(0, original.fields(0).copy(dataType = IntegerType))),
      StructType(original.fields.updated(0, original.fields(0).copy(nullable = true))),
      StructType(original.fields.updated(1, original.fields(1).copy(nullable = false))),
      StructType(
        original.fields.updated(
          1,
          original
            .fields(1)
            .copy(metadata = new MetadataBuilder().putString("comment", "changed").build())
        )
      )
    )
    changes.foreach(refused(original, _))
    changes.foreach(next => refused(original, next.add("extra", StringType, true)))
  }

  @Test
  def nestedAndNonconcreteOrNonnullableAdditionsFail(): Unit = {
    Seq(
      StructType(Seq(StructField("leaf", StringType))),
      ArrayType(StringType),
      MapType(StringType, LongType),
      NullType,
      TimestampNTZType,
      DayTimeIntervalType(),
      YearMonthIntervalType()
    ).foreach(t => refused(original, original.add("extra", t, true)))
    refused(original, original.add("extra", LongType, false))
    val before = StructType(
      Seq(StructField("nested", StructType(Seq(StructField("leaf", StringType)))))
    )
    val after = StructType(
      Seq(
        StructField(
          "nested",
          StructType(Seq(StructField("leaf", StringType), StructField("extra", LongType)))
        )
      )
    )
    refused(before, after)
  }

  @Test
  def duplicateInvalidAndVirtualNamesFailOnEvolution(): Unit = {
    Seq(
      "ID",
      "value",
      "",
      "bad name",
      "bad,name",
      "bad;name",
      "bad{name",
      "bad}name",
      "bad(name",
      "bad)name",
      "bad\nname",
      "bad\tname",
      "bad=name",
      "_change_type",
      "_COMMIT_VERSION",
      "_commit_timestamp"
    ).foreach(name => refused(original, original.add(name, StringType, true)))
    val duplicate = original.add("ID", StringType, true)
    refused(duplicate, duplicate.add("extra", StringType, true))
    val nestedDuplicate = original.add(
      "nested",
      StructType(Seq(StructField("a", StringType), StructField("A", StringType)))
    )
    refused(nestedDuplicate, nestedDuplicate.add("extra", StringType, true))
  }

  @Test
  def engineMetadataAdditionsFailWithoutStrippingOrdinaryMetadata(): Unit = {
    Seq(
      "delta.columnMapping.id",
      "DELTA.IDENTITY.start",
      "parquet.field.id",
      "delta.generationExpression",
      "CURRENT_DEFAULT",
      "EXISTS_DEFAULT",
      "__CHAR_VARCHAR_TYPE_STRING",
      "__file_source_generated_metadata_col"
    ).foreach { key =>
      val field = StructField(
        "extra",
        StringType,
        true,
        new MetadataBuilder().putString(key, "engine-value").build()
      )
      refused(original, original.add(field))
    }
  }

  @Test
  def changedThenRestoredHistoryCannotHideNonadditiveTransition(): Unit = {
    val added = original.add("extra", StringType, true)
    check(original, added)
    refused(added, original)
    check(original, original)
  }

  @Test
  def missingSchemasFailClosed(): Unit = {
    refused(null, original)
    refused(original, null)
    refused(null, null)
  }
}

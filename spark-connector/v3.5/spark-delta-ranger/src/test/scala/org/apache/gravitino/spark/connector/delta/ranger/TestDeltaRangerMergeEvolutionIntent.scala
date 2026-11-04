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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.nio.file.attribute.PosixFilePermissions
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import scala.collection.JavaConverters._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** Detached evidence must compare the original command, never grant a replay permit. */
class TestDeltaRangerMergeEvolutionIntent {
  import DeltaRangerMergeEvolutionIntent._
  private val mapper = new ObjectMapper()

  @Test
  def roundtripDetachesMetadataAndAllowsOnlyItsOwnExpectedCurrentSchema(): Unit = {
    val original = fixture()
    val retained = capture(original)
    val decoded = decode(retained.encoded)
    assertEquals(original.target.metadata, decoded.baseMetadata)
    assertEquals(original.expectedMetadata, decoded.expectedMetadata)
    assertEquals(Protocol(1, 2), decoded.protocol)
    assertEquals(Set("id", "value", "extra"), decoded.requiredUpdates)
    assertTrue(decoded.requiresAlter)
    assertTrue(decoded.sameRegistration)
    assertEquals(4L, decoded.baseVersion)
    assertEquals(Some("consumer" -> 7L), decoded.transactionIdentity)
    decoded.requireCurrent(capture(fixture(current = true)))
    decoded.requireCurrent(capture(fixture(current = true, head = 9L)))
    val leaked = decoded.expectedMetadata.schema
    leaked.fields(0) = StructField("changed", LongType)
    assertEquals("id", decoded.expectedMetadata.schema.fields(0).name)
    assertThrows(classOf[SecurityException], () => decoded.requireCurrent(capture(fixture())))
    assertThrows(
      classOf[SecurityException],
      () => decoded.requireCurrent(capture(fixture(current = true, drift = true)))
    )
  }

  @Test
  def authoredResolvedGovernedPoliciesRegistrationContextAndSourceStayExact(): Unit = {
    val retained = capture(fixture())
    val fresh = fixture(current = true)
    val variants = Seq(
      fresh.copy(start = 1L),
      fresh.copy(end = 5L),
      fresh.copy(context = fresh.context.copy(user = "other")),
      fresh.copy(context = fresh.context.copy(groups = Set("other"))),
      fresh.copy(context = fresh.context.copy(settings = Map("spark.sql.ansi.enabled" -> "true"))),
      fresh.copy(context = fresh.context.copy(transaction = Some("consumer" -> 8L))),
      fresh.copy(target = fresh.target.copy(owner = Some("other"))),
      fresh.copy(source = fresh.source.copy(receipt = Map("registration" -> "different"))),
      fresh.copy(sourceSelected = Set("id")),
      fresh.copy(targetUpdated = Set("id", "value")),
      fresh.copy(resolved = fresh.resolved.copy(condition = Literal.FalseLiteral)),
      fresh.copy(governed = fresh.governed.copy(condition = Literal.FalseLiteral)),
      fresh.copy(sourcePolicy = Filter(Literal.FalseLiteral, fresh.sourcePolicy)),
      fresh.copy(targetPolicy = Filter(Literal.FalseLiteral, fresh.targetPolicy)),
      fixture(current = true, literal = "changed"),
      fixture(current = true, star = false),
      fixture(current = true, projection = "renamed")
    )
    variants.zipWithIndex.foreach { case (candidate, index) =>
      assertThrows(
        classOf[SecurityException],
        () => retained.requireCurrent(capture(candidate)),
        index.toString
      )
    }
    val lastCommit =
      fresh.context.settings.updated("spark.databricks.delta.lastCommitVersionInSession", "9")
    retained.requireCurrent(
      capture(fresh.copy(context = fresh.context.copy(settings = lastCommit)))
    )
  }

  @Test
  def distinctSourcesAndReferencedTypesCannotBorrowSameRegistrationNormalization(): Unit = {
    val original = fixture()
    val distinct = original.source.copy(
      name = Vector("m", "source", "db", "t"),
      location = "file:/source",
      nativeId = "source-id",
      metadata = original.source.metadata.copy(id = "source-id")
    )
    val retained = capture(original.copy(source = distinct))
    val fresh = fixture(current = true)
    retained.requireCurrent(capture(fresh.copy(source = distinct)))
    assertThrows(
      classOf[SecurityException],
      () =>
        retained.requireCurrent(
          capture(
            fresh.copy(source =
              distinct.copy(metadata = original.expectedMetadata.copy(id = "source-id"))
            )
          )
        )
    )
    val bad = original.expectedMetadata.copy(schemaString =
      StructType(
        Seq(
          StructField("id", LongType),
          StructField("value", StringType),
          StructField("extra", LongType)
        )
      ).json
    )
    assertThrows(classOf[SecurityException], () => capture(original.copy(expectedMetadata = bad)))
    assertThrows(
      classOf[SecurityException],
      () =>
        capture(
          original.copy(expectedMetadata =
            original.expectedMetadata.copy(configuration = Map("changed" -> "true"))
          )
        )
    )
    assertThrows(
      classOf[SecurityException],
      () =>
        retained.requireCurrent(
          capture(fresh.copy(source = distinct.copy(protocol = Protocol(1, 3))))
        )
    )
  }

  @Test
  def finiteExpressionsKeepOccurrencesLiteralsAndOperatorSemantics(): Unit = {
    val original = fixture()
    assertEquals(
      capture(original).encoded,
      capture(fixture()).encoded,
      "fresh ExprIds are not intent"
    )
    val unknown = original.resolved.copy(condition = StartsWith(Literal("abc"), Literal("a")))
    assertThrows(classOf[SecurityException], () => capture(original.copy(resolved = unknown)))
    val join = Join(
      original.sourcePolicy,
      original.sourcePolicy,
      org.apache.spark.sql.catalyst.plans.Inner,
      None,
      JoinHint.NONE
    )
    assertThrows(classOf[SecurityException], () => capture(original.copy(sourcePolicy = join)))
  }

  @Test
  def duplicateSourceNamesRetainChildOutputOccurrenceAndEffectiveEvolutionFlag(): Unit = {
    val original = fixture()
    val child = Project(
      Seq(Alias(Literal("first"), "duplicate")(), Alias(Literal("second"), "duplicate")()),
      original.sourceLeaf
    )
    val first = Project(Seq(child.output(0)), child)
    val second = Project(Seq(child.output(1)), child)
    assertNotEquals(
      capture(original.copy(sourcePolicy = first)).encoded,
      capture(original.copy(sourcePolicy = second)).encoded,
      "same-named projected fields are distinct source occurrences"
    )
    val retained = capture(original)
    val fresh = fixture(current = true)
    assertThrows(
      classOf[SecurityException],
      () =>
        retained.requireCurrent(
          capture(fresh.copy(resolved = fresh.resolved.copy(withSchemaEvolution = false)))
        )
    )
  }

  @Test
  def strictEnvelopeRejectsMalformedFieldsAndSchemaEvenWithRehashedCommand(): Unit = {
    val retained = capture(fixture())
    assertThrows(
      classOf[SecurityException],
      () => retained.requireCurrent(decode(capture(fixture(current = true)).encoded))
    )
    val nested = mapper.readTree(retained.encoded).asInstanceOf[ObjectNode]
    nested
      .get("resolved")
      .asInstanceOf[ArrayNode]
      .set(2, mapper.readTree("[\"literal\",\"boolean\",\"false\"]"))
    assertThrows(
      classOf[SecurityException],
      () => decode(nested.toString).requireCurrent(capture(fixture(current = true)))
    )
    val edits: Seq[ObjectNode => Unit] = Seq(
      n => { n.put("other", true); () },
      n => { n.remove("context"); () },
      n => { n.put("requiresAlter", false); () },
      n => { n.put("sameRegistration", false); () },
      n => { n.put("start", "0"); () },
      n => { n.put("end", -1L); () },
      n => { n.putArray("targetUpdated").add("id").add("value"); () },
      n => { n.get("context").asInstanceOf[ObjectNode].put("groups", "group"); () },
      n => { n.get("target").asInstanceOf[ObjectNode].put("metadata", "not metadata"); () },
      n => {
        n.get("source").get("protocol").asInstanceOf[ObjectNode].put("minWriterVersion", 3); ()
      },
      n => {
        n.get("expectedMetadata")
          .get("configuration")
          .asInstanceOf[ObjectNode]
          .put("added", "true"); ()
      },
      n => { n.putArray("resolved").add("unknown-v1"); () }
    )
    edits.zipWithIndex.foreach { case (edit, index) =>
      val node = mapper.readTree(retained.encoded).asInstanceOf[ObjectNode]
      edit(node)
      assertThrows(classOf[SecurityException], () => { decode(node.toString); () }, index.toString)
    }
    assertThrows(classOf[SecurityException], () => decode(retained.encoded + " {}"))
    assertThrows(
      classOf[SecurityException],
      () =>
        decode(
          retained.encoded
            .replace("\"requiresAlter\":true", "\"requiresAlter\":true,\"requiresAlter\":true")
        )
    )
  }

  @Test
  def recoveryRechecksPeekedEvidenceAndNeverAcknowledgesUnverifiedEvolvedBytes(): Unit =
    withDirectory { directory =>
      val retained = capture(fixture())
      val journal = DeltaRangerMergeJournal.evolving(directory, retained)
      journal.prepare(proof(retained), 5L)
      val peeked = DeltaRangerMergeJournal.peek(directory, retained.transactionIdentity).get
      val reopened =
        DeltaRangerMergeJournal.recovering(directory, peeked, capture(fixture(current = true)))
      assertEquals(DeltaRangerMergeJournal.Prepared, reopened.existing.get.state)
      assertThrows(
        classOf[SecurityException],
        () => reopened.committed("{}\n".getBytes(StandardCharsets.UTF_8))
      )
      assertThrows(classOf[SecurityException], () => reopened.reconciled())
      assertThrows(classOf[SecurityException], () => reopened.skipped())
      assertEquals(
        DeltaRangerMergeJournal.Prepared,
        DeltaRangerMergeJournal.peek(directory, retained.transactionIdentity).get.state
      )
      journal.unknown()
      assertThrows(
        classOf[SecurityException],
        () =>
          DeltaRangerMergeJournal.recovering(directory, peeked, capture(fixture(current = true)))
      )
      assertThrows(
        classOf[SecurityException],
        () =>
          DeltaRangerMergeJournal.recovering(
            directory,
            DeltaRangerMergeJournal.peek(directory, retained.transactionIdentity).get,
            capture(fixture(current = true))
          )
      )
    }

  @Test
  def journalRejectsWrongTypesCardinalityBoundsAndPrivateFilePermissions(): Unit = withDirectory {
    directory =>
      val retained = capture(fixture())
      val journal = DeltaRangerMergeJournal.evolving(directory, retained)
      journal.prepare(proof(retained), 5L)
      val files = Files.list(directory.resolve("merge"))
      val path =
        try files.iterator().asScala.toVector.head
        finally files.close()
      val original = Files.readAllBytes(path)
      val edits: Seq[ObjectNode => Unit] = Seq(
        n => { n.put("unexpected", "value"); () },
        n => { n.remove("proof"); () },
        n => { n.put("format", 4294967298L); () },
        n => { n.put("version", "5"); () },
        n => { n.put("version", 6L); () },
        n => { n.put("state", "COMMITTED"); () },
        n => { n.put("digest", "0" * 64); () },
        n => { n.put("command", "0" * 64); () },
        n => { n.put("proof", "{}"); () },
        n => { n.put("evolution", "invalid"); () }
      )
      edits.zipWithIndex.foreach { case (edit, index) =>
        val node = mapper.readTree(original).asInstanceOf[ObjectNode]
        edit(node); Files.write(path, node.toString.getBytes(StandardCharsets.UTF_8))
        assertThrows(
          classOf[SecurityException],
          () => { DeltaRangerMergeJournal.peek(directory, retained.transactionIdentity); () },
          index.toString
        )
      }
      Files.write(path, Array.fill[Byte](16 * 1024 * 1024 + 1)(32.toByte))
      assertThrows(
        classOf[SecurityException],
        () => DeltaRangerMergeJournal.peek(directory, retained.transactionIdentity)
      )
      Files.write(path, original)
      Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-r--r--"))
      assertThrows(
        classOf[SecurityException],
        () => DeltaRangerMergeJournal.peek(directory, retained.transactionIdentity)
      )
      Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-------"))
      assertEquals(
        DeltaRangerMergeJournal.Prepared,
        DeltaRangerMergeJournal.peek(directory, retained.transactionIdentity).get.state
      )
  }

  @Test
  def policyMasksAndSelectedFieldTypesMetadataStayExactAcrossUnreferencedLeafGrowth(): Unit = {
    def selected(input: Capture, replacement: Option[Expression] = None): Capture = {
      val leaf = input.sourceLeaf
      val policy = Project(
        Seq(leaf.output(0), Alias(replacement.getOrElse(leaf.output(1)), "value")()),
        Filter(IsNotNull(leaf.output(1)), leaf)
      )
      input.copy(sourcePolicy = policy)
    }
    val retained = capture(selected(fixture()))
    retained.requireCurrent(capture(selected(fixture(current = true))))
    assertThrows(
      classOf[SecurityException],
      () =>
        retained.requireCurrent(capture(selected(fixture(current = true), Some(Literal("mask")))))
    )
    val fresh = fixture(current = true)
    Seq(
      fresh.sourceLeaf.output(1).withDataType(LongType),
      AttributeReference(
        "value",
        StringType,
        nullable = true,
        new MetadataBuilder().putString("comment", "changed").build()
      )(fresh.sourceLeaf.output(1).exprId)
    ).foreach { changed =>
      val leaf = LocalRelation(fresh.sourceLeaf.output.updated(1, changed))
      assertThrows(
        classOf[SecurityException],
        () => retained.requireCurrent(capture(selected(fresh.copy(sourceLeaf = leaf))))
      )
    }
  }

  @Test
  def targetPolicyDemandProjectionIgnoresOnlyUnusedLeafGrowth(): Unit = {
    def selected(input: Capture, mask: Boolean = false): Capture = {
      val leaf = input.targetLeaf
      input.copy(targetPolicy =
        Project(
          Seq(leaf.output(0), Alias(if (mask) Literal("masked") else leaf.output(1), "value")()),
          Filter(IsNotNull(leaf.output(1)), leaf)
        )
      )
    }
    val retained = capture(selected(fixture()))
    retained.requireCurrent(capture(selected(fixture(current = true))))
    assertThrows(
      classOf[SecurityException],
      () => retained.requireCurrent(capture(selected(fixture(current = true), mask = true)))
    )
    val fresh = selected(fixture(current = true))
    assertThrows(
      classOf[SecurityException],
      () =>
        retained.requireCurrent(
          capture(fresh.copy(targetPolicy = Filter(Literal.FalseLiteral, fresh.targetPolicy)))
        )
    )
  }

  @Test
  def nestedNativeSchemaJsonRejectsDuplicateFieldsAndTrailingValues(): Unit = {
    val retained = capture(fixture())
    Seq(
      retained.baseMetadata.schemaString
        .replace("\"type\":\"struct\"", "\"type\":\"struct\",\"type\":\"struct\""),
      retained.baseMetadata.schemaString + " {}"
    ).foreach { schema =>
      val node = mapper.readTree(retained.encoded).asInstanceOf[ObjectNode]
      node.get("target").get("metadata").asInstanceOf[ObjectNode].put("schemaString", schema)
      node.get("source").get("metadata").asInstanceOf[ObjectNode].put("schemaString", schema)
      assertThrows(classOf[SecurityException], () => decode(node.toString))
    }
  }

  @Test
  def authoredNativeParserRetainsSupportedUnresolvedScalarFunctionsAndRejectsOthers(): Unit = {
    def withFunction(input: Capture, value: String): Capture = {
      val parsed = CatalystSqlParser
        .parsePlan(
          s"MERGE INTO destination t USING source s ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = $value WHEN NOT MATCHED THEN INSERT *"
        )
        .asInstanceOf[MergeIntoTable]
      val command =
        parsed.copy(targetTable = input.resolved.target, sourceTable = input.resolved.source)
      input.copy(authored = authored(command, input.sourceLeaf, input.targetLeaf))
    }
    Seq(
      "upper(s.value)",
      "lower(s.value)",
      "concat(s.value, 'suffix')",
      "coalesce(s.value, 'fallback')"
    ).foreach { expression =>
      val retained = capture(withFunction(fixture(), expression))
      retained.requireCurrent(capture(withFunction(fixture(current = true), expression)))
      assertThrows(
        classOf[SecurityException],
        () =>
          retained.requireCurrent(
            capture(withFunction(fixture(current = true), "concat(s.value, 'different')"))
          )
      )
    }
    val retained = capture(withFunction(fixture(), "upper(s.value)"))
    assertThrows(
      classOf[SecurityException],
      () =>
        retained.requireCurrent(
          capture(withFunction(fixture(current = true), "upper(DISTINCT s.value)"))
        )
    )
    val alias = capture(withFunction(fixture(), "nvl(s.value, 'fallback')"))
    alias.requireCurrent(capture(withFunction(fixture(current = true), "nvl(s.value, 'fallback')")))
    val input = withFunction(fixture(), "regexp_replace(s.value, 'x', 'y')")
    val unsupported = DeltaMergeAction(
      Seq("value"),
      RegExpReplace(input.resolved.source.output(1), Literal("x"), Literal("y")),
      targetColNameResolved = true
    )
    assertThrows(
      classOf[SecurityException],
      () =>
        capture(
          input.copy(resolved =
            input.resolved.copy(matchedClauses =
              Seq(DeltaMergeIntoMatchedUpdateClause(None, Seq(unsupported)))
            )
          )
        )
    )

  }

  @Test
  def nativeProofBindingCrossChecksTransitionTransactionAndMetadata(): Unit = {
    val retained = capture(fixture())
    val valid = proof(retained)
    retained.requireProofBinding(valid, 5L)
    assertThrows(classOf[SecurityException], () => retained.requireProofBinding(valid, 6L))
    Seq("readVersion", "expectedMetadata", "actions", "nativeId").foreach { field =>
      val node = mapper.readTree(valid).asInstanceOf[ObjectNode]
      field match {
        case "readVersion" => node.put(field, 3L)
        case "expectedMetadata" =>
          node.set(field, mapper.readTree(retained.baseMetadata.json).get("metaData"))
        case "actions" =>
          node
            .putArray(field)
            .add(mapper.readTree("{\"txn\":{\"appId\":\"different\",\"version\":7}}"))
        case "nativeId" => node.put(field, "")
      }
      assertThrows(
        classOf[SecurityException],
        () => retained.requireProofBinding(node.toString, 5L),
        field
      )
    }
  }

  @Test
  def strictFormatTwoAndLegacyBytesPeekWithoutClaiming(): Unit = withDirectory { directory =>
    assertEquals(None, DeltaRangerMergeJournal.peek(directory, Some("consumer" -> 7L)))
    assertFalse(Files.exists(directory.resolve("merge")))
    val intent = capture(fixture())
    val journal = DeltaRangerMergeJournal.evolving(directory, intent)
    assertEquals(None, DeltaRangerMergeJournal.peek(directory, None))
    journal.prepare(proof(intent), 5L)
    val before = entries(directory)
    val entry = DeltaRangerMergeJournal.peek(directory, intent.transactionIdentity).get
    assertEquals(intent.encoded, entry.evolution.get.encoded)
    assertEquals(DeltaRangerMergeJournal.Prepared, entry.state)
    assertEquals(before, entries(directory))
    assertThrows(
      classOf[SecurityException],
      () => new DeltaRangerMergeJournal(directory, intent.transactionIdentity, entry.command)
    )
    val file = Files.list(directory.resolve("merge"))
    val path =
      try file.iterator().asScala.toVector.head
      finally file.close()
    val bytes = Files.readAllBytes(path)
    val text = new String(bytes, StandardCharsets.UTF_8)
    Seq(
      text + " {}",
      text.replace("\"format\":2", "\"format\":2,\"format\":2"),
      text.replace("additive-merge-v1", "other"),
      text.replace("\"skipped\":false", "\"skipped\":true")
    ).foreach { changed =>
      Files.write(path, changed.getBytes(StandardCharsets.UTF_8))
      assertThrows(
        classOf[SecurityException],
        () => DeltaRangerMergeJournal.peek(directory, intent.transactionIdentity)
      )
    }
    Files.write(path, bytes)
    journal.unknown()
    assertEquals(
      DeltaRangerMergeJournal.Unknown,
      DeltaRangerMergeJournal.peek(directory, intent.transactionIdentity).get.state
    )
    assertThrows(classOf[SecurityException], () => journal.prepare(proof(intent), 5L))
  }

  @Test
  def legacyFormatOneEncodingIsByteIdentical(): Unit = withDirectory { directory =>
    val journal = new DeltaRangerMergeJournal(directory, Some("legacy" -> 2L), "legacy-command")
    journal.prepare("{\"actions\":[]}", 3L)
    val expected =
      "{\"format\":1,\"command\":\"legacy-command\",\"proof\":\"{\\\"actions\\\":[]}\",\"version\":3,\"state\":\"PREPARED\",\"digest\":null,\"skipped\":false}"
    assertEquals(Vector(expected), entries(directory))
    assertEquals(None, DeltaRangerMergeJournal.peek(directory, Some("legacy" -> 2L)).get.evolution)
  }

  private def fixture(
      current: Boolean = false,
      head: Long = 5L,
      drift: Boolean = false,
      literal: String = "value",
      star: Boolean = true,
      projection: String = "extra"
  ): Capture = {
    val baseSchema = StructType(Seq(StructField("id", LongType), StructField("value", StringType)))
    val finalSchema = baseSchema.add("extra", StringType)
    val base = Metadata(
      id = "native-id",
      schemaString = baseSchema.json,
      configuration = Map("application" -> "retained")
    )
    val expected = base.copy(schemaString = finalSchema.json)
    val currentMetadata =
      if (drift) expected.copy(schemaString = finalSchema.add("later", StringType).json)
      else if (current) expected
      else base
    def attributes(schema: StructType) = schema.fields.toVector.map(f =>
      AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()
    )
    val targetLeaf = LocalRelation(attributes(currentMetadata.schema))
    val sourceLeaf = LocalRelation(attributes(currentMetadata.schema))
    val source = Project(
      Seq(sourceLeaf.output(0), sourceLeaf.output(1), Alias(Literal(literal), projection)()),
      sourceLeaf
    )
    val target = SubqueryAlias("t", targetLeaf)
    val sourceAlias = SubqueryAlias("s", source)
    val condition = EqualTo(target.output(0), sourceAlias.output(0))
    val update =
      if (star) UpdateStarAction(None)
      else UpdateAction(None, Seq(Assignment(target.output(1), Literal(literal))))
    val command =
      MergeIntoTable(target, sourceAlias, condition, Seq(update), Seq(InsertStarAction(None)), Nil)
    val actions =
      sourceAlias.output.map(a => DeltaMergeAction(Seq(a.name), a, targetColNameResolved = true))
    val nativeFinal =
      if (current) currentMetadata.schema
      else
        StructType(
          finalSchema.fields.dropRight(1) :+ finalSchema.fields.last.copy(nullable = false)
        )
    val resolved = DeltaMergeInto(
      target,
      sourceAlias,
      condition,
      Seq(DeltaMergeIntoMatchedUpdateClause(None, actions)),
      Seq(DeltaMergeIntoNotMatchedInsertClause(None, actions)),
      Nil,
      withSchemaEvolution = true,
      Some(nativeFinal)
    )
    val observation = Observation(
      Vector("m", "a", "db", "t"),
      Map("registration" -> "r1"),
      "file:/table",
      "native-id",
      Some("owner"),
      currentMetadata,
      Protocol(1, 2),
      if (current) head else 4L
    )
    Capture(
      authored(command, sourceLeaf, targetLeaf),
      resolved,
      resolved,
      sourceLeaf,
      targetLeaf,
      sourceAlias,
      target,
      observation,
      observation,
      expected,
      0L,
      4L,
      Set("id", "value"),
      Set("id", "value"),
      Set("id", "value", "extra"),
      Context(
        Some("consumer" -> 7L),
        "user",
        Set("group"),
        Map("spark.sql.ansi.enabled" -> "false")
      )
    )
  }

  private def proof(intent: DeltaRangerMergeEvolutionIntent): String = {
    val node = mapper.createObjectNode()
    node.put("readVersion", 4L); node.put("nativeId", "native-transaction")
    node.set("expectedMetadata", mapper.readTree(intent.expectedMetadata.json).get("metaData"))
    node
      .putArray("actions")
      .add(mapper.readTree("{\"txn\":{\"appId\":\"consumer\",\"version\":7}}"))
    node.toString
  }
  private def withDirectory(body: Path => Unit): Unit = {
    val directory = Files.createTempDirectory(
      "merge-evolution",
      PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"))
    )
    try body(directory)
    finally {
      val paths = Files.walk(directory)
      try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.delete)
      finally paths.close()
    }
  }
  private def entries(directory: Path): Vector[String] = {
    val files = Files.list(directory.resolve("merge"))
    try
      files
        .iterator()
        .asScala
        .toVector
        .map(p => new String(Files.readAllBytes(p), StandardCharsets.UTF_8))
    finally files.close()
  }
}

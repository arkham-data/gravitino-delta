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

import java.nio.file.Files
import java.util.{List => JavaList}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions._
import scala.collection.JavaConverters._

class TestDeltaRangerLiveAppendRequest {
  private def input: ObjectNode = new ObjectMapper()
    .readTree("""{
    "schemaVersion":1, "phase":"baseline-denied", "frontend":"insertInto",
    "sourceSecurityDirectory":"/tmp/source", "driverSecurityDirectory":"/tmp/driver",
    "sparkConfigurationFile":"/tmp/spark.properties", "resourceService":"service",
    "resourceDefinition":"definition", "metalake":"lake", "subject":"reader",
    "metadataPrincipal":"metadata", "minPolicy":1, "minRole":0,
    "catalog":"catalog", "database":"db", "source":"source", "target":"target",
    "transactionAppId":"app", "transactionVersion":1,
    "expectedSparkVersion":"3.5.4", "expectedDeltaVersion":"3.3.0",
    "publicStateSha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "rangerStateSha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    "stateEvidenceFile":"/tmp/state.json", "packageManifest":"/tmp/package.json",
    "expectedPreimage":{"version":0,"rows":[[0,"sentinel","seed"]]},
    "expectedPostimage":{"version":0,"rows":[[0,"sentinel","seed"]]},
    "warmOrder":"none", "duplicate":false, "barrier":null
  }""")
    .asInstanceOf[ObjectNode]

  @Test def boxedLongRowsMatchParsedExpectedJson(): Unit = {
    val expected = new ObjectMapper().readTree(
      """[[-2147483648,"secret","east"],[-1,"secret","east"],[0,"secret","east"],
        [1,"secret","east"],[2147483647,"secret","east"]]"""
    )
    val rows = Seq(Int.MinValue.toLong, -1L, 0L, 1L, Int.MaxValue.toLong)
      .map(id => Seq[Any](Long.box(id), "secret", "east"))
    assertEquals(expected, DeltaRangerLiveAppend.rowListsToJson(rows.map(_.asJava).asJava))
  }

  @Test def preservesExactLongRowImagePrecision(): Unit = {
    for (
      (id, expectedId) <- Seq(
        Long.MinValue -> "-9223372036854775808",
        -2147483649L -> "-2147483649",
        2147483648L -> "2147483648",
        9007199254740993L -> "9007199254740993",
        Long.MaxValue -> "9223372036854775807"
      )
    ) {
      val rows = Seq(Seq[Any](Long.box(id), "secret", "east").asJava).asJava
      val expected = new ObjectMapper().readTree(s"""[[$expectedId,"secret","east"]]""")
      val actual = DeltaRangerLiveAppend.rowListsToJson(rows)
      assertEquals(expected, actual, expectedId)
      assertEquals(id, actual.get(0).get(0).longValue())
    }
  }

  @Test def preservesEmptyRowImagesAndNullCells(): Unit = {
    val mapper = new ObjectMapper()
    assertEquals(
      mapper.readTree("[]"),
      DeltaRangerLiveAppend.rowListsToJson(Seq.empty[JavaList[Any]].asJava)
    )
    val rows = Seq(
      Seq[Any](Long.box(1L), null, "east"),
      Seq[Any](Long.box(2L), "null", null)
    ).map(_.asJava).asJava
    assertEquals(
      mapper.readTree("""[[1,null,"east"],[2,"null",null]]"""),
      DeltaRangerLiveAppend.rowListsToJson(rows)
    )
  }

  @Test def rejectsDifferentRowImagesWithoutRelaxingEquality(): Unit = {
    def row(id: Long, secret: String, region: String): Seq[Any] =
      Seq(Long.box(id), secret, region)
    val first = row(1L, "masked", "east")
    val second = row(2L, "null", "west")
    val expected = new ObjectMapper().readTree("""[[1,"masked","east"],[2,"null","west"]]""")
    for (
      (change, rows) <- Seq(
        "changed ID" -> Seq(row(3L, "masked", "east"), second),
        "removed row" -> Seq(first),
        "extra row" -> Seq(first, second, row(3L, "secret", "east")),
        "duplicate row" -> Seq(first, first, second),
        "replaced row with duplicate" -> Seq(first, first),
        "wrong string" -> Seq(first, row(2L, "changed", "west")),
        "wrong mask" -> Seq(row(1L, "unmasked", "east"), second),
        "wrong region" -> Seq(row(1L, "masked", "west"), second),
        "null versus string" -> Seq(first, row(2L, null, "west")),
        "reordered rows" -> Seq(second, first)
      )
    ) {
      assertNotEquals(
        expected,
        DeltaRangerLiveAppend.rowListsToJson(rows.map(_.asJava).asJava),
        change
      )
    }
  }

  @Test def admitsExactDiscoveryExtensionOrderAndRejectsReorderDuplicateAndStock(): Unit = {
    val authorization =
      "org.apache.gravitino.spark.connector.authorization.GravitinoAuthorizationSparkSessionExtensions"
    val companion = "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerSparkExtension"
    val delta = "io.delta.sql.DeltaSparkSessionExtension"
    DeltaRangerLiveAppend.validateExtensions(Seq(authorization, companion, delta).mkString(","))
    for (
      extensions <- Seq(
        Seq(companion, delta),
        Seq(authorization, delta, companion),
        Seq(authorization, companion, companion, delta),
        Seq(
          authorization,
          companion,
          delta,
          "org.apache.kyuubi.plugin.spark.authz.ranger.RangerSparkExtension"
        )
      )
    ) {
      assertThrows(
        classOf[IllegalArgumentException],
        () => DeltaRangerLiveAppend.validateExtensions(extensions.mkString(","))
      )
    }
  }

  @Test def cyclicUnexpectedCauseClassificationIsFinite(): Unit = {
    var calls = 0
    val cyclic = new IllegalStateException("secret-cycle") {
      override def getCause: Throwable = {
        calls += 1
        if (calls > 10) throw new AssertionError("Cause traversal did not terminate")
        this
      }
    }
    assertFalse(DeltaRangerLiveAppend.exactDenial(cyclic, false))
    assertEquals(1, DeltaRangerLiveAppend.failureEvidence(cyclic).size())
    assertTrue(calls <= 2)
  }

  @Test def rejectsAnUnrelatedBoundAppendFrameAsTargetDenial(): Unit = {
    val error = new SecurityException("Delta Ranger access denied")
    def stack(method: String) = Array(
      new StackTraceElement(
        classOf[DeltaRangerPolicyEngine].getName,
        "authorize",
        "DeltaRangerPolicyEngine.java",
        200
      ),
      new StackTraceElement(
        "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerRegistrationBindings$BoundAppendTransaction",
        method,
        "DeltaRangerRegistrationBindings.java",
        614
      )
    )
    error.setStackTrace(stack("requireLive"))
    assertFalse(DeltaRangerLiveAppend.exactDenial(error, false))
    error.setStackTrace(stack("<init>"))
    assertTrue(DeltaRangerLiveAppend.exactDenial(error, false))
    assertFalse(DeltaRangerLiveAppend.exactDenial(error, true))
  }

  @Test def acceptsExactReceiptCaptureForBothEntryPhasesAndCauseWrappers(): Unit = {
    for (
      phase <- Seq("baseline-denied", "revoked-denied");
      wrapped <- Seq(false, true);
      trailing <- Seq(false, true)
    ) {
      val frames =
        if (trailing) receiptCaptureFrames :+ ("unrelated.Caller" -> "call")
        else receiptCaptureFrames
      val denial = entryFailure(frames)
      val failure = if (wrapped) new IllegalStateException("wrapper", denial) else denial
      assertTrue(DeltaRangerLiveAppend.exactDenial(failure, false))
      verifyFreshWriterOutcome(failure, phase)
    }
  }

  @Test def receiptCaptureRequiresEveryExactAdjacentOrderedBoundary(): Unit = {
    val frames = receiptCaptureFrames
    for (index <- frames.indices) {
      assertFalse(
        DeltaRangerLiveAppend.exactDenial(entryFailure(frames.patch(index, Nil, 1)), false)
      )
      for (
        (name, method) <- Seq(
          (frames(index)._1 + "Other", frames(index)._2),
          ("unrelated." + frames(index)._1, frames(index)._2),
          (frames(index)._1, frames(index)._2 + "Other")
        )
      ) {
        assertFalse(
          DeltaRangerLiveAppend.exactDenial(
            entryFailure(frames.updated(index, (name, method))),
            false
          )
        )
      }
      assertFalse(
        DeltaRangerLiveAppend.exactDenial(
          entryFailure(frames.patch(index, Seq("unrelated.Guard" -> "call"), 0)),
          false
        )
      )
    }
    for (index <- 0 until frames.size - 1) {
      assertFalse(
        DeltaRangerLiveAppend.exactDenial(
          entryFailure(frames.updated(index, frames(index + 1)).updated(index + 1, frames(index))),
          false
        )
      )
    }
    assertFalse(DeltaRangerLiveAppend.exactDenial(entryFailure(frames.reverse), false))
    assertFalse(
      DeltaRangerLiveAppend.exactDenial(
        entryFailure(
          Seq(
            frames.head,
            "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerReadRule" -> "apply"
          )
        ),
        false
      )
    )
  }

  @Test def receiptCaptureRejectsWrongThrowableSuppressedAndSplitCauses(): Unit = {
    val exact = entryFailure(receiptCaptureFrames)
    for (
      failure <- Seq(
        new SecurityException("different message"),
        new SecurityException("Delta Ranger access denied") {},
        new IllegalArgumentException("Delta Ranger access denied")
      )
    ) {
      failure.setStackTrace(exact.getStackTrace)
      assertFalse(DeltaRangerLiveAppend.exactDenial(failure, false))
    }
    val suppressed = new IllegalStateException("wrapper")
    suppressed.addSuppressed(exact)
    assertFalse(DeltaRangerLiveAppend.exactDenial(suppressed, false))
    val split = entryFailure(receiptCaptureFrames.take(3))
    split.initCause(entryFailure(receiptCaptureFrames.drop(3)))
    assertFalse(DeltaRangerLiveAppend.exactDenial(split, false))
    var calls = 0
    val cyclic = new SecurityException("Delta Ranger access denied") {
      override def getCause: Throwable = {
        calls += 1
        if (calls > 10) throw new AssertionError("Cyclic denial did not terminate")
        this
      }
    }
    cyclic.setStackTrace(split.getStackTrace)
    assertFalse(DeltaRangerLiveAppend.exactDenial(cyclic, false))
    assertTrue(calls <= 1)
  }

  @Test def keepsTransactionAndAfterHelperAlternativesDistinctAndRejectsGrantedFailures(): Unit = {
    val prefix = "org.apache.gravitino.spark.connector.delta.ranger."
    val transaction = entryFailure(
      Seq(
        prefix + "DeltaRangerPolicyEngine" -> "authorize",
        prefix + "DeltaRangerRegistrationBindings$BoundAppendTransaction" -> "<init>"
      )
    )
    val stale = entryFailure(
      Seq(
        prefix + "DeltaRangerPolicyEngine" -> "requireSnapshot",
        prefix + "DeltaRangerRegistrationBindings$BoundAppendTransaction" -> "requireContext"
      ),
      "Delta Ranger publication changed; reanalyze the query"
    )
    for (phase <- Seq("baseline-denied", "revoked-denied")) {
      verifyFreshWriterOutcome(transaction, phase)
      assertRejectedWriterOutcome(stale, phase)
    }
    assertTrue(DeltaRangerLiveAppend.exactDenial(stale, true))
    verifyFreshWriterOutcome(stale, "after-helper-revoked")
    for (entry <- Seq(transaction, entryFailure(receiptCaptureFrames))) {
      assertFalse(DeltaRangerLiveAppend.exactDenial(entry, true))
      assertRejectedWriterOutcome(entry, "after-helper-revoked")
      assertRejectedWriterOutcome(entry, "granted-append")
    }
    val staleReceipt = entryFailure(
      receiptCaptureFrames.updated(0, prefix + "DeltaRangerPolicyEngine" -> "requireSnapshot"),
      "Delta Ranger publication changed; reanalyze the query"
    )
    assertFalse(DeltaRangerLiveAppend.exactDenial(staleReceipt, true))
    verifyFreshWriterOutcome(null, "granted-append")
    val unrelated = stale.getStackTrace.clone()
    unrelated(1) = new StackTraceElement(
      prefix + "DeltaRangerRegistrationBindings$BoundAppendTransaction",
      "requireLive",
      "Source.scala",
      1
    )
    stale.setStackTrace(unrelated)
    assertFalse(DeltaRangerLiveAppend.exactDenial(stale, true))
  }

  @Test def persistsOriginalFailureBeforeRejectingUnexpectedWriterOutcome(): Unit = {
    val output = Files.createTempDirectory("writer-failure-evidence")
    try {
      val original = new UnsupportedOperationException("secret-token-must-not-escape")
      original.setStackTrace(
        Array(new StackTraceElement("RealProductGuard", "validate", "Guard.scala", 73))
      )
      assertThrows(
        classOf[IllegalArgumentException],
        () => DeltaRangerLiveAppend.verifyWriterOutcome(original, "baseline-denied", output)
      )
      val path = output.resolve("writer-failure.json")
      assertTrue(Files.isRegularFile(path))
      val evidence = new ObjectMapper().readTree(path.toFile)
      assertEquals(
        classOf[UnsupportedOperationException].getName,
        evidence.get(0).path("class").asText()
      )
      assertEquals(
        "RealProductGuard.validate(Guard.scala:73)",
        evidence.get(0).path("stack").get(0).asText()
      )
      assertFalse(evidence.toString.contains("secret-token"))
    } finally {
      Files.deleteIfExists(output.resolve("writer-failure.json"))
      Files.deleteIfExists(output)
    }
  }

  @Test def retainsOriginalFailureClassesAndStacksWithoutMessages(): Unit = {
    val cause = new SecurityException("private credential must not escape")
    cause.setStackTrace(Array(new StackTraceElement("ProductionGuard", "reject", "Guard.java", 42)))
    val wrapper = new IllegalStateException("private endpoint must not escape", cause)
    val evidence = new ObjectMapper().valueToTree[JsonNode](
      DeltaRangerLiveAppend.failureEvidence(wrapper)
    )
    assertEquals(2, evidence.size())
    assertEquals(classOf[SecurityException].getName, evidence.get(1).path("class").asText())
    assertEquals(
      "ProductionGuard.reject(Guard.java:42)",
      evidence.get(1).path("stack").get(0).asText()
    )
    assertFalse(evidence.toString.contains("private"))
  }

  @Test def acceptsOnlyTheFourExplicitPhases(): Unit = {
    for (
      phase <- Seq("baseline-denied", "granted-append", "revoked-denied", "after-helper-revoked")
    ) {
      val value = input.put("phase", phase)
      if (phase == "granted-append") value.withObject("expectedPostimage").put("version", 1)
      if (phase == "after-helper-revoked")
        value.set(
          "barrier",
          new ObjectMapper().readTree(
            """{"controlDirectory":"/tmp/control","deadlineSeconds":45}"""
          )
        )
      assertEquals(phase, DeltaRangerLiveAppend.parse(value.toString).path("phase").asText())
    }
  }

  @Test def rejectsUnknownMissingDuplicateAndTrailingFields(): Unit = {
    val unknown = input.put("allowOwner", true)
    assertThrows(
      classOf[IllegalArgumentException],
      () => DeltaRangerLiveAppend.parse(unknown.toString)
    )
    val missing = input; missing.remove("minRole")
    assertThrows(
      classOf[IllegalArgumentException],
      () => DeltaRangerLiveAppend.parse(missing.toString)
    )
    assertThrows(
      classOf[IllegalArgumentException],
      () =>
        DeltaRangerLiveAppend.parse(input.toString.dropRight(1) + ",\"phase\":\"granted-append\"}")
    )
    assertThrows(
      classOf[IllegalArgumentException],
      () => DeltaRangerLiveAppend.parse(input.toString + " {}")
    )
  }

  @Test def rejectsInvalidPinsIdentitiesTransactionsAndWarmDenials(): Unit = {
    for (
      (name, value) <- Seq(
        "subject" -> "metadata",
        "source" -> "target",
        "expectedSparkVersion" -> "3.5.5",
        "expectedDeltaVersion" -> "3.3.1",
        "frontend" -> "save",
        "warmOrder" -> "target-first",
        "phase" -> "unknown",
        "target" -> "target;drop",
        "publicStateSha256" -> "bad"
      )
    ) {
      assertThrows(
        classOf[IllegalArgumentException],
        () => DeltaRangerLiveAppend.parse(input.put(name, value).toString)
      )
    }
    for (version <- Seq("-1", "9223372036854775808", "1.5", "\"1\"")) {
      val value = input; value.set("transactionVersion", new ObjectMapper().readTree(version))
      assertThrows(
        classOf[IllegalArgumentException],
        () => DeltaRangerLiveAppend.parse(value.toString)
      )
    }
    assertThrows(
      classOf[IllegalArgumentException],
      () => DeltaRangerLiveAppend.parse(input.put("duplicate", true).toString)
    )
    val malformed = input; malformed.withObject("expectedPreimage").put("version", -1)
    assertThrows(
      classOf[IllegalArgumentException],
      () => DeltaRangerLiveAppend.parse(malformed.toString)
    )
  }

  private val disassembly = """  public void inTransaction(example.Input);
    Code:
     225: invokevirtual #241 // Method org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerAppendHelper.write:()LTagged;
     228: astore 11
     230: aload_1
     233: invokestatic #86 // Method validate$1:()V
     236: aload 10
     238: invokevirtual #242 // Method org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerAppendHelper.close:()V
     241: new #244 // class org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerAppendCommit
    LineNumberTable:
      line 75: 223
      line 76: 230
      line 77: 236
      line 78: 241
    LocalVariableTable:
  public void another();
  """

  @Test def pinsValidationAfterHelperReturnBeforeCommitCreation(): Unit = {
    assertEquals(76, DeltaRangerLivePhasePin.postReturnLine(disassembly))
  }

  @Test def rejectsMissingDebugLinesAndChangedCallOrder(): Unit = {
    for (
      bad <- Seq(
        disassembly.replace("line 76: 230", "line 75: 230"),
        disassembly.replace("DeltaRangerAppendHelper.write:", "Changed.write:"),
        disassembly.replace("233: invokestatic #86", "233: invokevirtual #86"),
        disassembly.replace("line 76: 230", "")
      )
    ) {
      assertThrows(
        classOf[IllegalArgumentException],
        () => DeltaRangerLivePhasePin.postReturnLine(bad)
      )
    }
  }

  @Test def rejectsUnappliedOrStaleRevokeWitness(): Unit = {
    val mapper = new ObjectMapper()
    val valid = mapper.readTree("""{"initialPolicy":2,"policy":3,"role":4,"downloadVersion":3,
      "responseSha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "publicStateSha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
      "rangerStateSha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
      "publicRevokeComplete":true,"updateAbsent":true,"roleMatchesInitialDownload":true}""")
    DeltaRangerLiveMetadataBarrier.validateRelease(valid, 2, 4)
    for (
      (key, replacement) <- Seq(
        "policy" -> "2",
        "downloadVersion" -> "2",
        "role" -> "5",
        "responseSha256" -> "\"\"",
        "publicRevokeComplete" -> "false",
        "updateAbsent" -> "false",
        "roleMatchesInitialDownload" -> "false"
      )
    ) {
      val changed = valid.deepCopy[ObjectNode](); changed.set(key, mapper.readTree(replacement))
      assertThrows(
        classOf[IllegalArgumentException],
        () => DeltaRangerLiveMetadataBarrier.validateRelease(changed, 2, 4)
      )
    }
  }

  @Test def rendersNamedLoaderAndModuleFramesCanonically(): Unit = {
    val frames = Array(
      new StackTraceElement("app", null, null, "example.Reader", "load", "Reader.scala", 42),
      new StackTraceElement(
        "worker",
        "metadata",
        "1.2",
        "example.Writer$",
        "append$1",
        "Writer.scala",
        76
      ),
      new StackTraceElement("example.Reader", "load", "Reader.scala", 42)
    )
    assertEquals(
      Vector(
        "example.Reader.load(Reader.scala:42)",
        "example.Writer$.append$1(Writer.scala:76)",
        "example.Reader.load(Reader.scala:42)"
      ),
      DeltaRangerLiveMetadataBarrier.renderStack(frames)
    )
  }

  @Test def retainsNativeAndUnknownSourceStackNotation(): Unit = {
    val frames = Array(
      new StackTraceElement("app", null, null, "example.Native", "invoke", "Native.java", -2),
      new StackTraceElement("app", "metadata", "1.2", "example.Unknown", "call", null, -1),
      new StackTraceElement("app", null, null, "example.NoLine", "read", "NoLine.scala", -1)
    )
    assertEquals(
      Vector(
        "example.Native.invoke(Native Method)",
        "example.Unknown.call(Unknown Source)",
        "example.NoLine.read(NoLine.scala)"
      ),
      DeltaRangerLiveMetadataBarrier.renderStack(frames)
    )
    assertEquals(Vector.empty[String], DeltaRangerLiveMetadataBarrier.renderStack(Array.empty))
  }

  @Test def requiresEntirePinnedMetadataCallChain(): Unit = {
    val prefix = "org.apache.gravitino.spark.connector.delta.ranger."
    val methods = Seq(
      "org.apache.gravitino.client.HTTPClient" -> "execute",
      "org.apache.gravitino.client.RelationalCatalog" -> "loadTable",
      (prefix + "DeltaRangerRegistrationReader") -> "read",
      (prefix + "DeltaRangerRegistrationBindings$BoundAppendTransaction") -> "requireLive",
      (prefix + "DeltaRangerAppendCommandBinding") -> "validateOwned",
      (prefix + "DeltaRangerAppendCommandBinding") -> "validateRetainedOwned",
      (prefix + "DeltaRangerAppendCoordinator$") -> "validate$1",
      (prefix + "DeltaRangerAppendCoordinator$") -> "inTransaction"
    )
    val frames = methods.map { case (c, m) =>
      new StackTraceElement(
        c,
        m,
        if (c.endsWith("Coordinator$")) "DeltaRangerAppendCoordinator.scala" else "Source.java",
        if (m == "inTransaction") 76 else 10
      )
    }.toArray
    val pin = DeltaRangerLivePhasePin.Pin("a" * 64, "b" * 64, 76)
    val namedFrames = frames.map(f =>
      new StackTraceElement(
        "app",
        null,
        null,
        f.getClassName,
        f.getMethodName,
        f.getFileName,
        f.getLineNumber
      )
    )
    val expected = Vector(
      "org.apache.gravitino.client.HTTPClient.execute(Source.java:10)",
      "org.apache.gravitino.client.RelationalCatalog.loadTable(Source.java:10)",
      "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerRegistrationReader.read(Source.java:10)",
      "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerRegistrationBindings$BoundAppendTransaction.requireLive(Source.java:10)",
      "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerAppendCommandBinding.validateOwned(Source.java:10)",
      "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerAppendCommandBinding.validateRetainedOwned(Source.java:10)",
      "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerAppendCoordinator$.validate$1(DeltaRangerAppendCoordinator.scala:10)",
      "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerAppendCoordinator$.inTransaction(DeltaRangerAppendCoordinator.scala:76)"
    )
    for (stack <- Seq(frames, namedFrames)) {
      assertTrue(pin.matches(stack))
      assertEquals(expected, DeltaRangerLiveMetadataBarrier.renderStack(stack))
      for (index <- stack.indices)
        assertFalse(pin.matches(stack.take(index) ++ stack.drop(index + 1)))
      assertFalse(pin.matches(stack.reverse))
      assertFalse(pin.matches(stack.updated(2, stack(3)).updated(3, stack(2))))
      assertFalse(pin.matches(stack :+ stack.last))
      assertFalse(pin.copy(line = 75).matches(stack))
      for (
        (method, file, line) <- Seq(
          ("inTransaction", "DeltaRangerAppendCoordinator.scala", 75),
          ("inTransaction", "Wrong.scala", 76),
          ("wrongMethod", "DeltaRangerAppendCoordinator.scala", 76)
        )
      ) {
        val wrong = new StackTraceElement(
          "app",
          null,
          null,
          stack.last.getClassName,
          method,
          file,
          line
        )
        assertFalse(pin.matches(stack.updated(stack.length - 1, wrong)))
      }
    }
  }

  @Test def metadataGatewayForwardsSuccessfulBytesAndAbortsTerminally(): Unit = {
    val server =
      com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress("127.0.0.1", 0), 0)
    val calls = new java.util.concurrent.atomic.AtomicInteger()
    server.createContext(
      "/",
      (exchange: com.sun.net.httpserver.HttpExchange) => {
        calls.incrementAndGet()
        assertEquals("separate-metadata", exchange.getRequestHeaders.getFirst("X-Fixture-Identity"))
        val bytes = "ordinary-metadata".getBytes("UTF-8")
        exchange.getResponseHeaders.add("X-Upstream", "preserved")
        exchange.sendResponseHeaders(200, bytes.length)
        exchange.getResponseBody.write(bytes); exchange.close()
      }
    )
    server.start()
    val dir = java.nio.file.Files.createTempDirectory("metadata-barrier-test")
    val gateway = new DeltaRangerLiveMetadataBarrier.Gateway(
      java.net.URI.create("http://127.0.0.1:" + server.getAddress.getPort),
      dir
    )
    val path = "/api/metalakes/l/catalogs/c/schemas/d/tables/t"
    def read() = java.net.http.HttpClient
      .newHttpClient()
      .send(
        java.net.http.HttpRequest
          .newBuilder(gateway.uri.resolve(path + "?x=unchanged"))
          .header("X-Fixture-Identity", "separate-metadata")
          .GET()
          .build(),
        java.net.http.HttpResponse.BodyHandlers.ofString()
      )
    try {
      val response = read(); assertEquals(200, response.statusCode());
      assertEquals("ordinary-metadata", response.body())
      assertEquals("preserved", response.headers().firstValue("X-Upstream").orElseThrow())
      gateway.arm(
        Thread.currentThread(),
        path,
        DeltaRangerLivePhasePin.Pin("a" * 64, "b" * 64, 76),
        5
      )
      gateway.abort()
      assertEquals(503, read().statusCode()); assertEquals(503, read().statusCode())
      assertEquals(1, calls.get(), "Aborted target retries must not reach upstream")
      assertThrows(classOf[IllegalArgumentException], () => gateway.requireSuccessfulForward())
    } finally { gateway.close(); server.stop(0); java.nio.file.Files.delete(dir) }
  }
  private def assertRejectedWriterOutcome(error: Throwable, phase: String): Unit = {
    val rejected =
      assertThrows(classOf[IllegalArgumentException], () => verifyFreshWriterOutcome(error, phase))
    assertEquals(
      "requirement failed: " + (if (phase == "granted-append") "Public writer failed"
                                else "Unexpected writer failure"),
      rejected.getMessage
    )
  }

  private def verifyFreshWriterOutcome(error: Throwable, phase: String): Unit = {
    val output = Files.createTempDirectory("single-writer-outcome")
    try DeltaRangerLiveAppend.verifyWriterOutcome(error, phase, output)
    finally {
      Files.deleteIfExists(output.resolve("writer-failure.json"))
      Files.deleteIfExists(output)
    }
  }

  private def receiptCaptureFrames: Seq[(String, String)] = {
    val prefix = "org.apache.gravitino.spark.connector.delta.ranger."
    Seq(
      prefix + "DeltaRangerPolicyEngine" -> "authorize",
      prefix + "DeltaRangerAppendTargetReceipt" -> "validate",
      prefix + "DeltaRangerAppendTargetReceipt$" -> "capture",
      prefix + "DeltaRangerAppendCommandBinding$" -> "capture",
      prefix + "DeltaRangerAppendV1Input$Owner" -> "<init>"
    )
  }

  private def entryFailure(
      frames: Seq[(String, String)],
      message: String = "Delta Ranger access denied"
  ): SecurityException = {
    val error = new SecurityException(message)
    error.setStackTrace(frames.zipWithIndex.map { case ((name, method), index) =>
      new StackTraceElement(name, method, "Synthetic.scala", 900 + index)
    }.toArray)
    error
  }

}

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

import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._

/** Read-only test witness for the exact frozen coordinator call site. */
private[ranger] object DeltaRangerLivePhasePin {
  private val prefix = "org.apache.gravitino.spark.connector.delta.ranger."
  private val coordinator = prefix + "DeltaRangerAppendCoordinator$"

  final case class Pin(classSha256: String, artifactSha256: String, line: Int) {
    def matches(stack: Array[StackTraceElement]): Boolean = {
      val frames = stack.toVector
      val sites = frames.zipWithIndex.filter { case (f, _) =>
        f.getClassName == coordinator && f.getMethodName == "inTransaction"
      }
      if (sites.size != 1) return false
      val (site, index) = sites.head
      if (site.getLineNumber != line || site.getFileName != "DeltaRangerAppendCoordinator.scala")
        return false
      val chain = Vector(
        "org.apache.gravitino.client.HTTPClient" -> "execute",
        "org.apache.gravitino.client.RelationalCatalog" -> "loadTable",
        (prefix + "DeltaRangerRegistrationReader") -> "read",
        (prefix + "DeltaRangerRegistrationBindings$BoundAppendTransaction") -> "requireLive",
        (prefix + "DeltaRangerAppendCommandBinding") -> "validateOwned",
        (prefix + "DeltaRangerAppendCommandBinding") -> "validateRetainedOwned",
        coordinator -> "validate$1"
      )
      var cursor = 0
      chain.forall { case (name, method) =>
        val found = frames.indexWhere(
          f => f.getClassName == name && f.getMethodName == method && f.getLineNumber > 0,
          cursor
        )
        cursor = found + 1
        found >= 0 && found < index
      }
    }
  }

  def inspect(output: Path): Pin = {
    val cls = Class.forName(coordinator)
    val origin = Paths.get(cls.getProtectionDomain.getCodeSource.getLocation.toURI)
    require(
      Files.isRegularFile(origin) && origin.toString.endsWith(".jar"),
      "Phase pin requires actual packaged coordinator"
    )
    require(
      cls.getClassLoader eq DeltaRangerLiveAppend.getClass.getClassLoader,
      "Coordinator loader differs"
    )
    val disassembly = output.resolve("coordinator-javap.txt")
    val process = new ProcessBuilder(
      Paths.get(System.getProperty("java.home"), "bin", "javap").toString,
      "-classpath",
      origin.toString,
      "-c",
      "-l",
      "-p",
      coordinator
    )
      .redirectErrorStream(true)
      .redirectOutput(disassembly.toFile)
      .start()
    if (!process.waitFor(15, TimeUnit.SECONDS)) {
      process.destroyForcibly(); throw new IllegalStateException("Phase inspection timed out")
    }
    require(process.exitValue() == 0, "Phase inspection failed")
    val bytes = cls.getResourceAsStream("DeltaRangerAppendCoordinator$.class")
    require(bytes != null, "Coordinator class bytes unavailable")
    val hash =
      try digest(bytes.readAllBytes())
      finally bytes.close()
    Pin(
      hash,
      digest(Files.readAllBytes(origin)),
      postReturnLine(new String(Files.readAllBytes(disassembly), "UTF-8"))
    )
  }

  def postReturnLine(disassembly: String): Int = {
    val methods = disassembly
      .split("\n  (?=public |private |protected )")
      .filter(_.startsWith("public void inTransaction("))
    // javap may place the first method at the beginning of a supplied unit fixture.
    val candidates =
      if (methods.nonEmpty) methods
      else
        disassembly.trim
          .split("\n  (?=public |private |protected )")
          .filter(_.startsWith("public void inTransaction("))
    require(candidates.length == 1, "Missing unique coordinator method")
    val body = candidates.head
    val code = "(?m)^[ \t]*(\\d+):[ \t]+(\\w+)[ \t]*(.*)$".r
      .findAllMatchIn(body)
      .map(m => (m.group(1).toInt, m.group(2), m.group(3)))
      .toVector
    val writes = code.zipWithIndex.filter(_._1._3.contains("DeltaRangerAppendHelper.write:"))
    require(writes.size == 1 && writes.head._1._2 == "invokevirtual", "Missing unique helper write")
    val after = code.drop(writes.head._2 + 1)
    val calls = after.filter(c => c._2.startsWith("invoke") || c._2 == "new")
    require(
      calls.size >= 3 && calls(0)._2 == "invokestatic" && calls(0)._3
        .contains("Method validate$1:") &&
        calls(1)._3.contains("DeltaRangerAppendHelper.close:") && calls(2)._2 == "new" &&
        calls(2)._3.endsWith("DeltaRangerAppendCommit"),
      "Changed post-helper call ordering"
    )
    require(
      after
        .takeWhile(_._1 < calls.head._1)
        .forall(c => c._2.startsWith("aload") || c._2.startsWith("astore")),
      "Unexpected post-helper branch"
    )
    val lines = "(?m)^\\s*line (\\d+): (\\d+)\\s*$".r
      .findAllMatchIn(body)
      .map(m => m.group(2).toInt -> m.group(1).toInt)
      .toVector
      .sortBy(_._1)
    def lineAt(offset: Int): Int = {
      val prior = lines.filter(_._1 <= offset); require(prior.nonEmpty, "Missing line table");
      prior.last._2
    }
    val selected = lineAt(calls.head._1)
    require(
      selected > 0 && selected != lineAt(writes.head._1._1) && selected != lineAt(calls(1)._1),
      "Ambiguous post-return line"
    )
    require(
      code
        .filter(c => c._1 < writes.head._1._1 && c._3.contains("Method validate$1:"))
        .forall(c => lineAt(c._1) != selected),
      "Validation line aliases earlier phase"
    )
    selected
  }

  private def digest(bytes: Array[Byte]): String =
    MessageDigest.getInstance("SHA-256").digest(bytes).map(b => f"${b & 0xff}%02x").mkString
}

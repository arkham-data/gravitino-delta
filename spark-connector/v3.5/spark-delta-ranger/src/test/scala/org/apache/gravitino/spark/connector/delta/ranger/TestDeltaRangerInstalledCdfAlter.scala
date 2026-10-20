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

import java.io.{PrintWriter, StringWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.nio.file.attribute.PosixFilePermissions
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableChange}
import org.apache.spark.sql.delta.{DeltaLog, OptimisticTransaction}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** Installed-extension characterization of named CDF property mutation, not CDF qualification. */
class TestDeltaRangerInstalledCdfAlter {
  @Test
  def namedCdfAlterRejectsBeforePolicyAndNativeMutation(): Unit = {
    Seq(false, true).foreach { denied =>
      val policy = new TestDeltaRangerNativeSelect.Fixture
      val label = if (denied) "deny-alter" else "allow-alter"
      val evidenceRoot = Option(System.getenv("GRAVITINO_CDF_GOVERNED_EVIDENCE"))
        .map(Paths.get(_))
        .getOrElse(Files.createTempDirectory("delta-ranger-cdf-alter"))
      val evidence = evidenceRoot.toAbsolutePath.normalize().resolve(label)
      Files.createDirectories(evidence)
      Files.setPosixFilePermissions(evidence, PosixFilePermissions.fromString("rwx------"))
      try {
        NativeSelectFixture.withSpark(
          spark => {
            spark.conf.set("spark.sql.catalog.a", classOf[CdfAlterForwardingCatalog].getName)
            CdfAlterForwardingCatalog.alters.set(0)
            val fixture = NativeSelectFixture.bindingFixture(spark, "a")
            val registration = fixture.properties.asScala.toMap
            val location = NativeSelectFixture.locations("a")
            val snapshot = DeltaLog.forTable(spark, location).unsafeVolatileSnapshot
            assertFalse(
              snapshot.metadata.configuration.get("delta.enableChangeDataFeed").contains("true")
            )
            val before = manifest(Paths.get(location))
            if (denied) policy.denied.add("a:*")
            val key = new DeltaRangerResourceKey("a", "db", "t", null, "alter", "same-owner")
            val authorize =
              () => policy.engine.authorize(DeltaRangerQuerySubject.current(spark), key)
            if (denied) assertThrows(classOf[SecurityException], () => authorize())
            else authorize()
            assertEquals(Vector("alter"), policy.requests.asScala.map(_.getAccessType).toVector)
            policy.requests.clear()
            policy.events.clear()
            val failure = assertThrows(
              classOf[Exception],
              () => {
                spark
                  .sql("ALTER TABLE a.db.t SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true')")
                  .collect()
                ()
              }
            )
            val trace = new StringWriter
            val writer = new PrintWriter(trace)
            try failure.printStackTrace(writer)
            finally writer.close()
            write(evidence.resolve("failure-private.txt"), trace.toString)
            val causes =
              Iterator.iterate[Throwable](failure)(_.getCause).takeWhile(_ != null).toVector
            val admission = causes.find(e =>
              e.isInstanceOf[SecurityException] &&
                Option(e.getMessage).exists(_.startsWith("Unqualified Delta Ranger route:"))
            )
            assertTrue(
              admission.isDefined,
              "Expected installed admission failure; inspect private failure evidence"
            )
            val admissionNode =
              admission.get.getMessage.stripPrefix("Unqualified Delta Ranger route: ")
            assertTrue(
              admissionNode.matches("[A-Za-z][A-Za-z0-9]*"),
              "Expected a safe admission node name"
            )
            assertTrue(
              policy.requests.isEmpty,
              "Named ALTER must reject before Ranger policy evaluation"
            )
            assertEquals(
              0,
              CdfAlterForwardingCatalog.alters.get(),
              "Catalog ALTER must not execute"
            )
            assertEquals(registration, fixture.properties.asScala.toMap)
            assertEquals(
              before,
              manifest(Paths.get(location)),
              "Rejected ALTER changed target bytes"
            )
            val after = DeltaLog.forTable(spark, location).unsafeVolatileSnapshot
            assertEquals(snapshot.version, after.version)
            assertEquals(snapshot.metadata, after.metadata)
            assertEquals(snapshot.protocol, after.protocol)
            assertNull(DeltaRangerNativeOperation.current())
            assertTrue(OptimisticTransaction.getActive.isEmpty)
            write(evidence.resolve("manifest-before.txt"), before.toSeq.sorted.mkString("\n"))
            write(
              evidence.resolve("manifest-after.txt"),
              manifest(Paths.get(location)).toSeq.sorted.mkString("\n")
            )
            write(
              evidence.resolve("outcome.txt"),
              s"policyControl=$label\nfirstAdmissionNode=$admissionNode\n" +
                s"commandPolicyRequests=${policy.requests.size}\n" +
                s"catalogAlterCalls=${CdfAlterForwardingCatalog.alters.get()}\n" +
                s"nativeVersion=${snapshot.version}\nreader=${snapshot.protocol.minReaderVersion}\n" +
                s"writer=${snapshot.protocol.minWriterVersion}\n" +
                "targetBytesUnchanged=true\nregistrationPropertiesUnchanged=true\n" +
                "metadataProtocolUnchanged=true\n" +
                causes
                  .map(e => e.getClass.getName + ": " + Option(e.getMessage).getOrElse(""))
                  .mkString("\n")
            )
          },
          Some(NativeSelectFixture.extension(policy))
        )
      } finally policy.engine.close()
    }
  }

  private def manifest(root: Path): Map[String, String] = {
    val paths = Files.walk(root)
    try
      paths
        .iterator()
        .asScala
        .filter(Files.isRegularFile(_))
        .map { path =>
          val digest = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path))
          root.relativize(path).toString -> digest.map(b => f"${b & 0xff}%02x").mkString
        }
        .toMap
    finally paths.close()
  }

  private def write(path: Path, value: String): Unit = {
    Files.write(path, value.getBytes(StandardCharsets.UTF_8))
    Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-------"))
  }
}

/** Forwards ALTER to the production wrapper if installed plan admission permits execution. */
class CdfAlterForwardingCatalog extends NativeBoundForwardingCatalog {

  /** Delegates the exact Spark changes without adding authorization or changing admission. */
  override def alterTable(id: Identifier, changes: TableChange*): Table = {
    CdfAlterForwardingCatalog.alters.incrementAndGet()
    NativeSelectFixture.wrapper(SparkSession.active, name()).alterTable(id, changes: _*)
  }
}

private[ranger] object CdfAlterForwardingCatalog {
  val alters = new AtomicInteger()
}

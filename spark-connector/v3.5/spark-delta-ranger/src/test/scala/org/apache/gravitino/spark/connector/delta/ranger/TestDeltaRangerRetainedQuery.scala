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
import com.sun.net.httpserver.HttpServer
import java.io.{BufferedReader, StringReader}
import java.net.{InetSocketAddress, URI}
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.nio.file.attribute.PosixFilePermissions
import java.util.LinkedHashMap
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import org.apache.ranger.plugin.util.RangerRESTUtils

import scala.collection.JavaConverters._

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.Mockito.when

class TestDeltaRangerRetainedQuery {
  private val sql = "SELECT secret FROM a.db.t WHERE id = 1"

  @Test
  def driverLoopWritesExactRetainedEnvelopesAndCleansOnCloseOrMalformedInput(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      withSecurity { (security, _, _) =>
        val output = Files.createTempDirectory("retained-driver-")
        val held = new DeltaRangerRetainedQueries(spark)
        val lines =
          s"""{"id":"p1","operation":"prepare","retentionId":"held","sql":"$sql","minPolicy":1,"minRole":1}
             |{"id":"c1","operation":"collect","retentionId":"held","action":"collect","expectedRows":[["a-one-masked"]],"minPolicy":1,"minRole":1,"requiredDownloads":"none"}
             |{"id":"r1","operation":"release","retentionId":"held"}
             |{"close":true}
             |""".stripMargin
        DeltaRangerDownloadedQuery.runCommands(
          spark,
          policy.engine,
          security,
          output,
          new BufferedReader(new StringReader(lines)),
          held,
          () => ()
        )
        val events = output.resolve("retained-events")
        assertEquals(
          PosixFilePermissions.fromString("rwx------"),
          Files.getPosixFilePermissions(events)
        )
        val json = new ObjectMapper()
        val prepare = json.readTree(events.resolve("000001.json").toFile)
        assertEquals(
          Set(
            "schemaVersion",
            "sequence",
            "id",
            "operation",
            "retentionId",
            "sql",
            "versionsBefore",
            "versionsAfter",
            "completedCursor",
            "prepared"
          ),
          prepare.fieldNames().asScala.toSet
        )
        assertEquals(1, prepare.path("schemaVersion").asInt())
        assertEquals(1, prepare.path("sequence").asInt())
        assertEquals("p1", prepare.path("id").asText())
        assertEquals("prepare", prepare.path("operation").asText())
        assertEquals("held", prepare.path("retentionId").asText())
        assertEquals(sql, prepare.path("sql").asText())
        assertEquals(
          Set("service", "definition", "policy", "role"),
          prepare.path("versionsAfter").fieldNames().asScala.toSet
        )
        assertEquals(0L, prepare.path("completedCursor").asLong())
        assertTrue(prepare.path("prepared").asBoolean())
        val collect = json.readTree(events.resolve("000002.json").toFile)
        assertEquals(
          Set(
            "schemaVersion",
            "sequence",
            "id",
            "operation",
            "retentionId",
            "versionsBefore",
            "versionsAfter",
            "downloads",
            "refreshQualified",
            "actionResult"
          ),
          collect.fieldNames().asScala.toSet
        )
        assertEquals("collect", collect.path("operation").asText())
        assertEquals(
          Set("requested", "effective", "cursorBefore", "cursorAfter", "policy", "role"),
          collect.path("downloads").fieldNames().asScala.toSet
        )
        assertEquals("none", collect.path("downloads").path("requested").asText())
        assertEquals("none", collect.path("downloads").path("effective").asText())
        assertTrue(collect.path("downloads").path("policy").isNull())
        assertTrue(collect.path("downloads").path("role").isNull())
        assertFalse(collect.path("refreshQualified").asBoolean())
        assertEquals(
          Set(
            "retentionId",
            "action",
            "rows",
            "failureDetails",
            "actionEntryDenied",
            "sameDatasetBefore",
            "sameRootBefore",
            "sameDatasetAfter",
            "sameRootAfter",
            "jobGroup",
            "jobIdsBefore",
            "jobIdsAfter",
            "userScanOutputRows",
            "passed"
          ),
          collect.path("actionResult").fieldNames().asScala.toSet
        )
        assertEquals(
          "a-one-masked",
          collect.path("actionResult").path("rows").get(0).get(0).asText()
        )
        val release = json.readTree(events.resolve("000003.json").toFile)
        assertEquals(
          Set("schemaVersion", "sequence", "id", "operation", "retentionId", "released"),
          release.fieldNames().asScala.toSet
        )
        assertTrue(release.path("released").asBoolean())
        (1 to 3).foreach { n =>
          val marker = json.readTree(events.resolve(f"$n%06d-passed.json").toFile)
          assertEquals(
            Set("schemaVersion", "sequence", "id", "retentionId", "passed"),
            marker.fieldNames().asScala.toSet
          )
          assertTrue(marker.path("passed").asBoolean())
          assertEquals(
            PosixFilePermissions.fromString("rw-------"),
            Files.getPosixFilePermissions(events.resolve(f"$n%06d.json"))
          )
          assertEquals(
            PosixFilePermissions.fromString("rw-------"),
            Files.getPosixFilePermissions(events.resolve(f"$n%06d-passed.json"))
          )
        }
        assertFalse(Files.exists(events.resolve("000004.json")))
        assertThrows(classOf[IllegalStateException], () => held.require("held"))

        val malformedOutput = Files.createTempDirectory("retained-malformed-")
        val malformedHeld = new DeltaRangerRetainedQueries(spark)
        val malformed =
          s"""{"id":"p2","operation":"prepare","retentionId":"second","sql":"$sql","minPolicy":1,"minRole":1}
             |{"close":true,"id":"extra"}
             |""".stripMargin
        assertThrows(
          classOf[IllegalArgumentException],
          () =>
            DeltaRangerDownloadedQuery.runCommands(
              spark,
              policy.engine,
              security,
              malformedOutput,
              new BufferedReader(new StringReader(malformed)),
              malformedHeld,
              () => ()
            )
        )
        assertThrows(classOf[IllegalStateException], () => malformedHeld.require("second"))
        assertFalse(Files.exists(malformedOutput.resolve("retained-events/000002.json")))

        val cappedOutput = Files.createTempDirectory("retained-cap-")
        val cappedHeld = new DeltaRangerRetainedQueries(spark)
        cappedHeld.prepare("cap", sql, security.completedCursor(), policy.engine.versions())
        assertThrows(
          classOf[IllegalStateException],
          () =>
            DeltaRangerDownloadedQuery.runCommands(
              spark,
              policy.engine,
              security,
              cappedOutput,
              new BufferedReader(
                new StringReader("""{"id":"limit","operation":"release","retentionId":"cap"}""")
              ),
              cappedHeld,
              () => (),
              4096
            )
        )
        assertFalse(Files.exists(cappedOutput.resolve("retained-events/004097.json")))
        assertThrows(classOf[IllegalStateException], () => cappedHeld.require("cap"))
      }
    }

  @Test
  def driverUnionRequiresRealPolicyAndRoleDownloadsForAppliedVersions(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      withSecurity { (security, policyVersion, roleVersion) =>
        val output = Files.createTempDirectory("retained-union-")
        val held = new DeltaRangerRetainedQueries(spark)
        val lines =
          s"""{"id":"p1","operation":"prepare","retentionId":"held","sql":"$sql","minPolicy":1,"minRole":1}
             |{"id":"c1","operation":"collect","retentionId":"held","action":"collect","deniedAtEntry":true,"expectedFailureContains":"publication changed","minPolicy":2,"minRole":2,"requiredDownloads":"policy"}
             |{"close":true}
             |""".stripMargin
        val refreshCount = new AtomicInteger()
        val refresh: Runnable = () => {
          if (refreshCount.incrementAndGet() == 2) {
            policyVersion.set(2L)
            roleVersion.set(2L)
            val roles = policy.plugin.getRoles
            roles.setRoleVersion(2L)
            when(policy.plugin.getRolesVersion).thenReturn(2L)
            when(policy.plugin.getPoliciesVersion).thenReturn(2L)
            policy.start()
            val client = HttpClient.newHttpClient()
            Seq(
              RangerRESTUtils.REST_URL_POLICY_GET_FOR_SECURE_SERVICE_IF_UPDATED + "service",
              RangerRESTUtils.REST_URL_SERVICE_SERCURE_GET_USER_GROUP_ROLES + "service"
            ).foreach(path => {
              val response = client.send(
                HttpRequest.newBuilder(security.uri().resolve(path)).GET().build(),
                HttpResponse.BodyHandlers.ofByteArray()
              )
              assertEquals(200, response.statusCode())
            })
          }
        }
        DeltaRangerDownloadedQuery.runCommands(
          spark,
          policy.engine,
          security,
          output,
          new BufferedReader(new StringReader(lines)),
          held,
          refresh
        )
        val event =
          new ObjectMapper().readTree(output.resolve("retained-events/000002.json").toFile)
        assertEquals("policy", event.path("downloads").path("requested").asText())
        assertEquals("both", event.path("downloads").path("effective").asText())
        assertTrue(event.path("refreshQualified").asBoolean())
        assertEquals(1L, event.path("versionsBefore").path("policy").asLong())
        assertEquals(1L, event.path("versionsBefore").path("role").asLong())
        assertEquals(2L, event.path("versionsAfter").path("policy").asLong())
        assertEquals(2L, event.path("versionsAfter").path("role").asLong())
        assertEquals(2L, event.path("downloads").path("policy").path("version").asLong())
        assertEquals(2L, event.path("downloads").path("role").path("version").asLong())
        assertEquals(1L, event.path("downloads").path("policy").path("attempt").asLong())
        assertEquals(2L, event.path("downloads").path("role").path("attempt").asLong())
        assertEquals(2L, event.path("downloads").path("cursorAfter").asLong())
        assertTrue(event.path("actionResult").path("actionEntryDenied").asBoolean())
        assertTrue(Files.exists(output.resolve("retained-events/000002-passed.json")))
      }
    }

  private def withSecurity(
      test: (DeltaRangerDownloadedSecurity, AtomicLong, AtomicLong) => Unit
  ): Unit = {
    val policyVersion = new AtomicLong(1L)
    val roleVersion = new AtomicLong(1L)
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext(
      "/",
      exchange => {
        val path = exchange.getRequestURI.getPath
        val body = (if (path.contains("roles"))
                      s"""{"serviceName":"service","roleVersion":${roleVersion.get()}}"""
                    else s"""{"serviceName":"service","policyVersion":${policyVersion.get()}}""")
          .getBytes(StandardCharsets.UTF_8)
        exchange.sendResponseHeaders(200, body.length)
        exchange.getResponseBody.write(body)
        exchange.close()
      }
    )
    server.start()
    val base = Files.createTempDirectory("retained-security-")
    val source = Files.createDirectory(base.resolve("source"))
    Files.writeString(source.resolve("log4j2.xml"), "safe logger")
    Files.writeString(
      source.resolve("ranger-spark-security.xml"),
      s"""<configuration><property><name>ranger.plugin.spark.policy.rest.url</name><value>http://127.0.0.1:${server.getAddress.getPort}</value></property><property><name>ranger.plugin.spark.policy.cache.dir</name><value>/old/cache</value></property></configuration>"""
    )
    try {
      val security = new DeltaRangerDownloadedSecurity(
        source,
        Files.createDirectory(base.resolve("driver")),
        Files.createDirectory(base.resolve("evidence")),
        "service"
      )
      try test(security, policyVersion, roleVersion)
      finally security.close()
    } finally server.stop(0)
  }

  @Test
  def sameDatasetAndRootRepeatBothActionsThenRejectDecisionDrift(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      val held = new DeltaRangerRetainedQueries(spark)
      val slot = held.prepare("one", sql, 0L, policy.engine.versions())
      val dataset = slot.dataset()
      val root = slot.root()
      Seq("two", "three", "four").foreach(id => held.prepare(id, sql, 0L, policy.engine.versions()))
      assertThrows(
        classOf[IllegalStateException],
        () => held.prepare("five", sql, 0L, policy.engine.versions())
      )
      val ledger = new DeltaRangerDownloadedCommand.Ledger()
      val firstCommand = collect("c1", "one", "collect", ledger)
      val firstAction = DeltaRangerRetainedAction.execute(spark, slot, firstCommand)
      assertTrue(firstAction.passed())
      val evidence = Files.createTempDirectory("retained-write-")
      val blockedMarker = Files.createDirectory(evidence.resolve("000001-passed.json"))
      val envelope = new LinkedHashMap[String, Object]()
      envelope.put("schemaVersion", Integer.valueOf(1))
      assertThrows(
        classOf[java.nio.file.FileAlreadyExistsException],
        () =>
          DeltaRangerDownloadedQuery.completeCollect(
            evidence,
            1,
            envelope,
            firstCommand,
            firstAction,
            slot,
            3L,
            policy.engine.versions()
          )
      )
      assertEquals(0L, slot.cursor())
      assertTrue(Files.exists(evidence.resolve("000001.json")))
      Files.delete(blockedMarker)
      DeltaRangerDownloadedQuery.completeCollect(
        evidence,
        2,
        envelope,
        firstCommand,
        firstAction,
        slot,
        4L,
        policy.engine.versions()
      )
      assertEquals(4L, slot.cursor())
      assertEquals(
        PosixFilePermissions.fromString("rw-------"),
        Files.getPosixFilePermissions(evidence.resolve("000002-passed.json"))
      )
      val unexpected = new DeltaRangerRetainedAction.Result(false, firstAction.evidence(), null)
      assertThrows(
        classOf[AssertionError],
        () =>
          DeltaRangerDownloadedQuery.completeCollect(
            evidence,
            3,
            envelope,
            firstCommand,
            unexpected,
            slot,
            5L,
            policy.engine.versions()
          )
      )
      assertTrue(Files.exists(evidence.resolve("000003.json")))
      assertFalse(Files.exists(evidence.resolve("000003-passed.json")))
      assertEquals(4L, slot.cursor())
      assertTrue(
        DeltaRangerRetainedAction
          .execute(spark, slot, collect("c2", "one", "collectAsList", ledger))
          .passed()
      )
      assertTrue(
        DeltaRangerRetainedAction
          .execute(spark, slot, collect("c3", "one", "collect", ledger))
          .passed()
      )
      assertSame(dataset, held.require("one").dataset())
      assertSame(root, held.require("one").root())
      policy.denied.add("a:secret")
      val denied = DeltaRangerRetainedAction.execute(spark, slot, denial("c4", "one", ledger))
      assertTrue(denied.passed(), denied.evidence().toString)
      assertEquals(true, denied.evidence().get("actionEntryDenied"))
      assertTrue(
        denied
          .evidence()
          .get("userScanOutputRows")
          .asInstanceOf[java.util.List[java.lang.Long]]
          .stream()
          .allMatch(_ == 0L)
      )
      held.release("one")
      assertThrows(classOf[IllegalStateException], () => held.require("one"))
      assertThrows(
        classOf[IllegalStateException],
        () => held.prepare("one", sql, 0L, policy.engine.versions())
      )
      held.close()
    }

  @Test
  def beforeFirstCollectionAndRolePublicationRejectAtEntry(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      val held = new DeltaRangerRetainedQueries(spark)
      val first = held.prepare("first", sql, 0L, policy.engine.versions())
      policy.denied.add("a:secret")
      val ledger = new DeltaRangerDownloadedCommand.Ledger()
      assertTrue(
        DeltaRangerRetainedAction.execute(spark, first, denial("d1", "first", ledger)).passed()
      )
      policy.denied.remove("a:secret")
      val role = held.prepare("role", sql, 0L, policy.engine.versions())
      val context = policy.plugin.getCurrentRangerAuthContext
      val roles = policy.plugin.getRoles
      val version = policy.plugin.getRolesVersion + 1L
      roles.setRoleVersion(version)
      org.mockito.Mockito.when(policy.plugin.getRolesVersion).thenReturn(version)
      context.setRoles(roles)
      policy.listener.contextChanged()
      val command = DeltaRangerDownloadedCommand.parse(
        "{\"id\":\"d2\",\"operation\":\"collect\",\"retentionId\":\"role\",\"action\":\"collect\",\"deniedAtEntry\":true,\"expectedFailureContains\":\"publication changed\",\"minPolicy\":1,\"minRole\":1,\"requiredDownloads\":\"none\"}",
        ledger
      )
      assertTrue(DeltaRangerRetainedAction.execute(spark, role, command).passed())
      held.close()
      assertThrows(classOf[IllegalStateException], () => held.require("role"))
    }

  private def collect(
      id: String,
      retained: String,
      action: String,
      ledger: DeltaRangerDownloadedCommand.Ledger
  ): DeltaRangerDownloadedCommand.Parsed =
    DeltaRangerDownloadedCommand.parse(
      s"""{"id":"$id","operation":"collect","retentionId":"$retained","action":"$action","expectedRows":[["a-one-masked"]],"minPolicy":1,"minRole":1,"requiredDownloads":"none"}""",
      ledger
    )

  private def denial(
      id: String,
      retained: String,
      ledger: DeltaRangerDownloadedCommand.Ledger
  ): DeltaRangerDownloadedCommand.Parsed =
    DeltaRangerDownloadedCommand.parse(
      s"""{"id":"$id","operation":"collect","retentionId":"$retained","action":"collect","deniedAtEntry":true,"expectedFailureContains":"denied","minPolicy":1,"minRole":1,"requiredDownloads":"none"}""",
      ledger
    )
}

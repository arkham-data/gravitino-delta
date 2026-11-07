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

import java.net.{InetSocketAddress, URI}
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.file.{Files, Path}
import java.security.MessageDigest
import java.time.Duration
import java.util.UUID
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import scala.collection.JavaConverters._

/** Transparent finite test transport; never a product authority. */
private[ranger] object DeltaRangerLiveMetadataBarrier {
  private val json = new ObjectMapper()
  private val hopHeaders = Set(
    "connection",
    "content-length",
    "expect",
    "host",
    "http2-settings",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade"
  )

  def renderStack(stack: Array[StackTraceElement]): Vector[String] =
    stack.toVector.map(f =>
      new StackTraceElement(
        f.getClassName,
        f.getMethodName,
        f.getFileName,
        f.getLineNumber
      ).toString
    )

  /** Configuration controls require healthy upstream metadata and unchanged policy versions. */
  def validateMetadataRelease(value: JsonNode, initialPolicy: Long, initialRole: Long): Unit = {
    require(
      value.fieldNames().asScala.toSet == Set(
        "initialPolicy",
        "policy",
        "role",
        "publicMetadataChanged",
        "upstreamHealthy"
      )
    )
    require(
      value.path("initialPolicy").asLong(-1) == initialPolicy && value
        .path("policy")
        .asLong(-1) == initialPolicy && value.path("role").asLong(-1) == initialRole
    )
    require(
      value
        .path("publicMetadataChanged")
        .isBoolean && value.path("publicMetadataChanged").asBoolean()
    )
    require(value.path("upstreamHealthy").isBoolean && value.path("upstreamHealthy").asBoolean())
  }

  /** A closed external scope is independent of policy changes and public metadata. */
  def validateScopeRelease(value: JsonNode, initialPolicy: Long, initialRole: Long): Unit = {
    require(
      value.fieldNames().asScala.toSet == Set("initialPolicy", "policy", "role", "scopeClosed")
    )
    require(
      value.path("initialPolicy").asLong(-1) == initialPolicy && value
        .path("policy")
        .asLong(-1) == initialPolicy && value.path("role").asLong(-1) == initialRole
    )
    require(value.path("scopeClosed").isBoolean && value.path("scopeClosed").asBoolean())
  }

  /** Exact Ranger deny evidence is distinct from public privilege revocation. */
  def validateSharedDenyRelease(value: JsonNode, initialPolicy: Long, initialRole: Long): Unit = {
    val fields = Set(
      "initialPolicy",
      "policy",
      "role",
      "downloadVersion",
      "responseSha256",
      "rangerStateSha256",
      "exactRangerDenyComplete",
      "unrelatedPoliciesUnchanged",
      "roleMatchesInitialDownload",
      "service",
      "catalog",
      "database",
      "table",
      "actor",
      "applicationRole",
      "denyPolicyId",
      "policyPriority",
      "access"
    )
    require(value.isObject && value.fieldNames().asScala.toSet == fields)
    Seq("initialPolicy", "policy", "role", "downloadVersion", "denyPolicyId").foreach(k =>
      require(
        value.path(k).isIntegralNumber && value
          .path(k)
          .canConvertToLong && value.path(k).asLong() >= 0
      )
    )
    require(
      value.path("initialPolicy").asLong() == initialPolicy && value
        .path("policy")
        .asLong() > initialPolicy
    )
    require(
      value.path("downloadVersion").asLong() == value.path("policy").asLong() && value
        .path("role")
        .asLong() == initialRole
    )
    Seq("responseSha256", "rangerStateSha256").foreach(k =>
      require(value.path(k).isTextual && value.path(k).asText().matches("[0-9a-f]{64}"))
    )
    Seq("exactRangerDenyComplete", "unrelatedPoliciesUnchanged", "roleMatchesInitialDownload")
      .foreach(k => require(value.path(k).isBoolean && value.path(k).asBoolean()))
    Seq("service", "catalog", "database", "table", "actor", "applicationRole").foreach(k =>
      require(
        value
          .path(k)
          .isTextual && value.path(k).asText().nonEmpty && !value.path(k).asText().contains("*")
      )
    )
    require(value.path("access").asText() == "update")
    require(
      value.path("policyPriority").isIntegralNumber && value.path("policyPriority").asInt() == 1
    )
  }

  def validateRelease(value: JsonNode, initialPolicy: Long, initialRole: Long): Unit = {
    val numbers = Set("initialPolicy", "policy", "role", "downloadVersion")
    val digests = Set("responseSha256", "publicStateSha256", "rangerStateSha256")
    val absence = if (value.has("sourceSelectAbsent")) "sourceSelectAbsent" else "updateAbsent"
    val flags = Set("publicRevokeComplete", absence, "roleMatchesInitialDownload")
    require(
      value.isObject && value.fieldNames().asScala.toSet == numbers ++ digests ++ flags,
      "Unexpected release witness"
    )
    numbers.foreach(k =>
      require(
        value
          .path(k)
          .isIntegralNumber && value.path(k).canConvertToLong && value.path(k).longValue() >= 0,
        "Invalid release version"
      )
    )
    require(
      value
        .path("initialPolicy")
        .longValue() == initialPolicy && value.path("policy").longValue() > initialPolicy &&
        value.path("policy").longValue() == value.path("downloadVersion").longValue() && value
          .path("role")
          .longValue() == initialRole,
      "Unapplied or stale revoke"
    )
    digests.foreach(k =>
      require(
        value.path(k).isTextual && value.path(k).asText().matches("[0-9a-f]{64}"),
        "Missing release digest"
      )
    )
    flags.foreach(k =>
      require(value.path(k).isBoolean && value.path(k).booleanValue(), "Incomplete revoke evidence")
    )
  }

  final class Gateway(upstream: URI, evidence: Path) extends AutoCloseable {
    private val executor = Executors.newCachedThreadPool()
    private val client = HttpClient
      .newBuilder()
      .version(HttpClient.Version.HTTP_1_1)
      .connectTimeout(Duration.ofSeconds(10))
      .followRedirects(HttpClient.Redirect.NEVER)
      .build()
    private val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    private val held = new CountDownLatch(1)
    private val forwarded = new CountDownLatch(1)
    private val runId = UUID.randomUUID().toString
    private var worker: Thread = _
    private var target: String = _
    private var pin: DeltaRangerLivePhasePin.Pin = _
    private var deadline: Long = 0L
    private var state = "idle"
    private var releasedStatus = 0
    private var metadataPrincipal: String = _
    private var metadataObserved = false
    private var metadataPrefixes = Set.empty[String]
    private var metadataPaths = Set.empty[String]
    private var sharedActorPrefixes = Map.empty[String, String]
    private var namedAttempts = Vector.empty[Map[String, Any]]
    private var witness: Vector[String] = Vector.empty
    server.createContext("/", (exchange: HttpExchange) => forward(exchange))
    server.setExecutor(executor)
    server.start()

    def uri: URI = URI.create("http://127.0.0.1:" + server.getAddress.getPort)

    def observeTarget(path: String, principal: String): Unit = synchronized {
      require(
        state == "idle" && path.startsWith("/api/metalakes/") && principal.nonEmpty,
        "Invalid metadata observation"
      )
      target = path; metadataPrincipal = principal
    }

    def observeTables(prefix: String, principal: String): Unit = synchronized {
      require(
        state == "idle" && prefix.startsWith("/api/metalakes/") && prefix.endsWith(
          "/tables/"
        ) && principal.nonEmpty,
        "Invalid named metadata observation"
      )
      require(metadataPrincipal == null || metadataPrincipal == principal)
      metadataPrefixes += prefix; metadataPrincipal = principal
    }

    /** Observe shared actor visibility without treating it as the separate metadata identity. */
    def observeSharedTables(prefix: String, principal: String, actor: String): Unit = synchronized {
      require(actor.nonEmpty && actor != principal && !sharedActorPrefixes.contains(prefix))
      observeTables(prefix, principal)
      sharedActorPrefixes += prefix -> actor
    }

    /** Detached snapshots of every relevant attempted request, including non-200 responses. */
    def namedRequests(): java.util.List[java.util.Map[String, Any]] = synchronized {
      namedAttempts.map(_.asJava).asJava
    }

    def requireMetadataObserved(fileName: String = "metadata-identity.json"): Unit = synchronized {
      require(metadataObserved, "Separately authenticated metadata lookup was not observed")
      json.writeValue(
        evidence.resolve(fileName).toFile,
        Map(
          "principal" -> metadataPrincipal,
          "successfulTargetGet" -> Boolean.box(true),
          "successfulNamedGets" -> metadataPaths.toVector.sorted.asJava
        ).asJava
      )
    }

    def arm(
        writer: Thread,
        exactPath: String,
        phase: DeltaRangerLivePhasePin.Pin,
        seconds: Long
    ): Unit = synchronized {
      require(
        state == "idle" && writer != null && seconds >= 5 && seconds <= 120 &&
          exactPath.startsWith("/api/metalakes/") && !exactPath.contains("?") && phase.line > 0,
        "Invalid barrier arm"
      )
      worker = writer; target = exactPath; pin = phase
      deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds)
      state = "armed"
    }

    def awaitHeld(seconds: Long): Unit = {
      if (!held.await(seconds, TimeUnit.SECONDS)) {
        abort(); throw new IllegalStateException("Post-helper barrier did not hold")
      }
      synchronized { require(state == "held" && live(), "Post-helper barrier lost its worker") }
    }

    def releaseAfter(value: JsonNode, initialPolicy: Long, initialRole: Long): Unit = synchronized {
      validateRelease(value, initialPolicy, initialRole)
      require(state == "held" && live(), "Release lacks a live held phase")
      json.writeValue(evidence.resolve("release-witness.json").toFile, value)
      state = "released"; notifyAll()
    }

    /** Release after the shared caller proves an exact downloaded Ranger UPDATE deny. */
    def releaseSharedRangerDeny(value: JsonNode, initialPolicy: Long, initialRole: Long): Unit =
      synchronized {
        validateSharedDenyRelease(value, initialPolicy, initialRole)
        require(state == "held" && live(), "Release lacks a live held phase")
        json.writeValue(evidence.resolve("release-witness.json").toFile, value)
        state = "released"; notifyAll()
      }

    /** Release a live held request after a verified metadata-only fixture change. */
    def releaseMetadataChange(value: JsonNode, initialPolicy: Long, initialRole: Long): Unit =
      synchronized {
        validateMetadataRelease(value, initialPolicy, initialRole)
        require(state == "held" && live(), "Release lacks a live held phase")
        json.writeValue(evidence.resolve("release-witness.json").toFile, value)
        state = "released"; notifyAll()
      }

    /** Release only after the caller's external scope has actually been closed. */
    def releaseScopeClosed(value: JsonNode, initialPolicy: Long, initialRole: Long): Unit =
      synchronized {
        validateScopeRelease(value, initialPolicy, initialRole)
        require(state == "held" && live(), "Release lacks a live held phase")
        json.writeValue(evidence.resolve("release-witness.json").toFile, value)
        state = "released"; notifyAll()
      }

    def requireSuccessfulForward(): Unit = {
      require(forwarded.await(20, TimeUnit.SECONDS), "Released metadata request did not complete")
      synchronized {
        require(
          state == "forwarded" && releasedStatus == 200,
          "Metadata transport failure cannot prove policy denial"
        )
      }
    }

    def abort(): Unit = synchronized {
      state = "aborted"; notifyAll(); held.countDown()
    }

    override def close(): Unit = {
      synchronized { if (state != "forwarded") abort() }
      server.stop(0); executor.shutdownNow()
      require(executor.awaitTermination(15, TimeUnit.SECONDS), "Metadata handlers remain active")
    }

    private def live(): Boolean =
      worker.isAlive && System.nanoTime() < deadline && pin.matches(worker.getStackTrace)

    private def hold(exchange: HttpExchange): Boolean = synchronized {
      val matching =
        exchange.getRequestMethod == "GET" && exchange.getRequestURI.getRawPath == target
      if (matching && state == "aborted") return false
      if (matching && state == "armed" && pin.matches(worker.getStackTrace)) {
        require(live(), "Dead or expired post-helper worker")
        state = "held"
        witness = renderStack(worker.getStackTrace)
        json.writeValue(
          evidence.resolve("held.json").toFile,
          Map(
            "runId" -> runId,
            "stack" -> witness.asJava,
            "classSha256" -> pin.classSha256,
            "artifactSha256" -> pin.artifactSha256,
            "postReturnLine" -> Int.box(pin.line),
            "writerThreadId" -> Long.box(worker.getId)
          ).asJava
        )
        held.countDown()
        while (state == "held") {
          if (!live()) { abort(); return false }
          wait(25L)
        }
        return state == "released" && live()
      }
      if (matching && (state == "held" || state == "released")) { abort(); return false }
      true
    }

    private def forward(exchange: HttpExchange): Unit = {
      var releasing = false
      val attempt = synchronized {
        val path = exchange.getRequestURI.getRawPath
        if (path == target || metadataPrefixes.exists(path.startsWith)) {
          val index = namedAttempts.size
          namedAttempts :+= Map[String, Any](
            "sequence" -> (index.toLong + 1L),
            "method" -> exchange.getRequestMethod,
            "path" -> path,
            "status" -> 0
          )
          Some(index)
        } else None
      }
      try {
        val metadataPath = synchronized {
          metadataPrincipal != null && exchange.getRequestMethod == "GET" && (exchange.getRequestURI.getRawPath == target || metadataPrefixes
            .exists(exchange.getRequestURI.getRawPath.startsWith))
        }
        val metadata = if (metadataPath) {
          val header = Option(exchange.getRequestHeaders.getFirst("Authorization")).getOrElse("")
          require(header.startsWith("Basic "), "Metadata authentication was not explicit")
          val decoded = new String(java.util.Base64.getDecoder.decode(header.substring(6)), "UTF-8")
          val principal = decoded.takeWhile(_ != ':')
          val actor = synchronized {
            sharedActorPrefixes.exists { case (prefix, expected) =>
              exchange.getRequestURI.getRawPath.startsWith(prefix) && principal == expected
            }
          }
          require(
            principal == metadataPrincipal || actor,
            "Metadata request used another principal"
          )
          if (actor) synchronized {
            attempt.foreach(index =>
              namedAttempts = namedAttempts.updated(
                index,
                namedAttempts(index).updated("identityKind", "shared-application")
              )
            )
          }
          principal == metadataPrincipal
        } else false
        if (!hold(exchange)) { exchange.sendResponseHeaders(503, -1); return }
        releasing = synchronized {
          state == "released" && exchange.getRequestMethod == "GET" && exchange.getRequestURI.getRawPath == target
        }
        val request = HttpRequest
          .newBuilder(upstream.resolve(exchange.getRequestURI))
          .timeout(Duration.ofSeconds(15))
        exchange.getRequestHeaders.asScala.foreach { case (name, values) =>
          if (!hopHeaders(name.toLowerCase(java.util.Locale.ROOT)))
            values.asScala.foreach(v => request.header(name, v))
        }
        val body = exchange.getRequestBody.readAllBytes()
        request.method(
          exchange.getRequestMethod,
          if (body.isEmpty) HttpRequest.BodyPublishers.noBody()
          else HttpRequest.BodyPublishers.ofByteArray(body)
        )
        val response = client.send(request.build(), HttpResponse.BodyHandlers.ofByteArray())
        synchronized {
          attempt.foreach(index =>
            namedAttempts = namedAttempts.updated(
              index,
              namedAttempts(index).updated("status", response.statusCode())
            )
          )
        }
        response.headers().map().asScala.foreach { case (name, values) =>
          if (!hopHeaders(name.toLowerCase(java.util.Locale.ROOT)))
            values.asScala.foreach(v => exchange.getResponseHeaders.add(name, v))
        }
        exchange.sendResponseHeaders(
          response.statusCode(),
          if (response.body().isEmpty) -1L else response.body().length.toLong
        )
        if (response.body().nonEmpty) exchange.getResponseBody.write(response.body())
        exchange.getResponseBody.close()
        if (metadata && response.statusCode() == 200) synchronized {
          metadataObserved = true; metadataPaths += exchange.getRequestURI.getRawPath
        }
        if (releasing) synchronized {
          require(state == "released", "Barrier aborted during forwarding")
          releasedStatus = response.statusCode(); state = "forwarded"
          val digest = MessageDigest
            .getInstance("SHA-256")
            .digest(response.body())
            .map(b => f"${b & 0xff}%02x")
            .mkString
          json.writeValue(
            evidence.resolve("forwarded.json").toFile,
            Map(
              "runId" -> runId,
              "status" -> Int.box(releasedStatus),
              "responseSha256" -> digest,
              "transparent" -> Boolean.box(true)
            ).asJava
          )
          forwarded.countDown()
        }
      } catch {
        case _: Throwable =>
          abort()
          try exchange.sendResponseHeaders(503, -1L)
          catch { case _: Throwable => () }
      } finally exchange.close()
    }
  }
}

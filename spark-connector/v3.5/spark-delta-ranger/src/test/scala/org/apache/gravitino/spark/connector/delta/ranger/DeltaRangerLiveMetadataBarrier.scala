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

  def validateRelease(value: JsonNode, initialPolicy: Long, initialRole: Long): Unit = {
    val numbers = Set("initialPolicy", "policy", "role", "downloadVersion")
    val digests = Set("responseSha256", "publicStateSha256", "rangerStateSha256")
    val flags = Set("publicRevokeComplete", "updateAbsent", "roleMatchesInitialDownload")
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

    def requireMetadataObserved(): Unit = synchronized {
      require(metadataObserved, "Separately authenticated metadata lookup was not observed")
      json.writeValue(
        evidence.resolve("metadata-identity.json").toFile,
        Map("principal" -> metadataPrincipal, "successfulTargetGet" -> Boolean.box(true)).asJava
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
      try {
        val metadata = synchronized {
          metadataPrincipal != null && exchange.getRequestMethod == "GET" && exchange.getRequestURI.getRawPath == target
        }
        if (metadata) {
          val header = Option(exchange.getRequestHeaders.getFirst("Authorization")).getOrElse("")
          require(header.startsWith("Basic "), "Metadata authentication was not explicit")
          val decoded = new String(java.util.Base64.getDecoder.decode(header.substring(6)), "UTF-8")
          require(
            decoded.takeWhile(_ != ':') == metadataPrincipal,
            "Metadata request used another principal"
          )
        }
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
        if (metadata && response.statusCode() == 200) synchronized { metadataObserved = true }
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

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
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest
import java.time.Instant
import java.util.{Collections, IdentityHashMap, Optional, OptionalLong}
import java.util.concurrent.ScheduledExecutorService
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal
import com.fasterxml.jackson.databind.ObjectMapper
import io.delta.storage.LogStore
import org.apache.gravitino.{Catalog, NameIdentifier}
import org.apache.gravitino.catalog.lakehouse.delta.registration.{
  DeltaOperationMarker,
  DeltaRegistrationCommand,
  DeltaRegistrationIdentity,
  DeltaRegistrationReceipt
}
import org.apache.gravitino.client.GravitinoClient
import org.apache.gravitino.exceptions.NoSuchTableException
import org.apache.gravitino.rel.{
  Column,
  Table => MetadataTable,
  TableCatalog => MetadataTables,
  TableChange => MetadataChange
}
import org.apache.gravitino.spark.connector.delta.GravitinoDeltaCatalogSpark35
import org.apache.ranger.admin.client.RangerAdminRESTClient
import org.apache.ranger.plugin.model.RangerServiceDef
import org.apache.ranger.plugin.service.RangerBasePlugin
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.{LogicalRDD, QueryExecution}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.connector.catalog.{Identifier, Table}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.delta.OptimisticTransaction
import org.apache.spark.sql.delta.commands.CreateDeltaTableCommand
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.junit.jupiter.api.Assertions._
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._

/** Test-only tracing and typed transport; nothing in this object grants native authority. */
private[ranger] object InstalledSchemaCreateBoundaryProbe {
  val mapper = new ObjectMapper
  val fixturePath: Path = sys.env
    .get("DELTA_CREATE_OPTIONS_FIXTURE")
    .map(Paths.get(_))
    .getOrElse(Paths.get("../spark-delta/src/test/resources/native-create/spark-hub-options.json"))
    .toAbsolutePath
    .normalize()
  def fixtureMap(name: String): Map[String, String] =
    mapper
      .readTree(Files.readAllBytes(fixturePath))
      .get(name)
      .fields()
      .asScala
      .map(e => e.getKey -> e.getValue.asText())
      .toMap
  def write(path: Path, value: String): Unit = { Files.write(path, value.getBytes(UTF_8)); () }
  def sha(bytes: Array[Byte]): String =
    MessageDigest.getInstance("SHA-256").digest(bytes).map(b => f"${b & 0xff}%02x").mkString
  def fresh(name: String): Trace = {
    val root = sys.env
      .get("GRAVITINO_DELTA_CREATE_PROBE_EVIDENCE_DIR")
      .map(Paths.get(_))
      .getOrElse(Files.createTempDirectory("installed-create-evidence"))
    require(root.isAbsolute, "Evidence directory must be absolute")
    Files.createDirectories(root)
    new Trace(Files.createDirectory(root.resolve(name)))
  }
  final class Trace(val directory: Path) extends AutoCloseable {
    val target: Path = directory.resolve("native-target")
    val log0: Path = target.resolve("_delta_log/00000000000000000000.json")
    val events = ArrayBuffer.empty[String]
    val errors = ArrayBuffer.empty[Throwable]
    val observations = ArrayBuffer.empty[(Boolean, Long, Boolean)]
    val plans = ArrayBuffer.empty[LogicalPlan]
    private val identities = new IdentityHashMap[AnyRef, java.lang.Integer]()
    private val origins = scala.collection.mutable.Set.empty[Class[_]]
    private val jarHashes = scala.collection.mutable.Map.empty[Path, String]
    private var sequence = 0L
    private var closed = false
    def id(value: AnyRef): String = synchronized {
      if (value == null) "NONE"
      else {
        if (!identities.containsKey(value)) identities.put(value, identities.size() + 1)
        value.getClass.getName + "@" + identities.get(value)
      }
    }
    def record(phase: String, text: String): Unit = synchronized {
      require(!closed)
      sequence += 1
      events += s"event=$sequence time=${Instant.now()} thread=${Thread.currentThread().getName} phase=$phase $text"
    }
    def origin(clazz: Class[_]): Unit = synchronized {
      if (origins.add(clazz)) {
        val resource = "/" + clazz.getName.replace('.', '/') + ".class"
        val stream = clazz.getResourceAsStream(resource)
        val bytes =
          try stream.readAllBytes()
          finally stream.close()
        val location = clazz.getProtectionDomain.getCodeSource.getLocation
        val path = Paths.get(location.toURI)
        val jar =
          if (Files.isRegularFile(path))
            jarHashes.getOrElseUpdate(path, sha(Files.readAllBytes(path)))
          else "DIRECTORY"
        record(
          "runtime-artifact",
          s"class=${clazz.getName} classSha256=${sha(bytes)} origin=$location artifactSha256=$jar"
        )
      }
    }
    def activate(spark: SparkSession): Unit = {
      spark.listenerManager.register(new QueryExecutionListener {
        override def onSuccess(name: String, query: QueryExecution, duration: Long): Unit =
          consumer("success", name, query)
        override def onFailure(name: String, query: QueryExecution, failure: Exception): Unit =
          consumer("failure", name, query)
        private def consumer(outcome: String, name: String, query: QueryExecution): Unit =
          Trace.this.synchronized {
            if (!closed) {
              // The listener observes returned plans only, without requesting analysis or execution.
              record(
                "query-consumer",
                s"outcome=$outcome name=$name logical=${query.logical.treeString}"
              )
            }
          }
      })
      record(
        "runtime",
        s"spark=${spark.version} scala=${scala.util.Properties.versionNumberString} java=${System.getProperty("java.version")} session=${id(spark)}"
      )
      Seq(
        classOf[SparkSession],
        classOf[CreateDeltaTableCommand],
        classOf[OptimisticTransaction],
        classOf[RangerBasePlugin],
        classOf[RangerAdminRESTClient],
        classOf[LogStore],
        classOf[RangerServiceDef],
        classOf[scala.Option[_]],
        classOf[GravitinoDeltaCatalogSpark35],
        classOf[DeltaRangerSparkExtension],
        classOf[DeltaRangerCatalogSpark35],
        classOf[DeltaRangerCreateCommand.Owner],
        classOf[DeltaRangerCreateMutation],
        classOf[DeltaRangerCreateMetadata],
        classOf[DeltaRangerCreateExec],
        classOf[ResolutionObserver],
        classOf[TestDeltaRangerInstalledSchemaCreateBoundary]
      ).foreach(origin)
      Files.copy(fixturePath, directory.resolve("spark-hub-options.json"))
      assertEquals(
        "789fbd8fa8f22f85daa6105c233c0f34bf239bcfc5b62d7044708ccea9030f18",
        sha(Files.readAllBytes(fixturePath)),
        "Unexpected native CREATE fixture bytes"
      )
      record(
        "fixture",
        s"path=$fixturePath sha256=${sha(Files.readAllBytes(fixturePath))} writerOptions=UNUSED"
      )
    }
    def observe(spark: SparkSession, phase: String, plan: Option[LogicalPlan]): Unit =
      synchronized {
        try {
          val txn = OptimisticTransaction.getActive
          val exists = Files.exists(log0)
          record(
            phase,
            s"session=${id(spark)} operation=${id(DeltaRangerNativeOperation.current())} activeTxn=${id(txn.orNull)} log0=$exists"
          )
          txn.foreach { t =>
            val snapshot = t.snapshot
            origin(t.getClass); origin(snapshot.getClass); origin(t.deltaLog.getClass)
            record(
              phase + "-transaction",
              s"txn=${id(t)} log=${id(t.deltaLog)} snapshot=${id(snapshot)} snapshotClass=${snapshot.getClass.getName} snapshotVersion=${snapshot.version} readVersion=${t.readVersion} catalogTablePresent=${t.catalogTable.nonEmpty} log0=$exists"
            )
            observations += ((exists, snapshot.version, t.catalogTable.nonEmpty))
          }
          plan.foreach { root =>
            plans += root
            root.foreach {
              case logical: LogicalRDD if logical.getClass == classOf[LogicalRDD] =>
                record(
                  "public-logical-rdd",
                  s"logical=${id(logical)} rdd=${id(logical.rdd)} schema=${logical.schema.json} streaming=${logical.isStreaming} partitioning=${logical.outputPartitioning} ordering=${logical.outputOrdering}"
                )
                val visited = new IdentityHashMap[RDD[_], java.lang.Boolean]()
                def graph(rdd: RDD[_]): Unit = {
                  if (!visited.containsKey(rdd)) {
                    visited.put(rdd, true)
                    origin(rdd.getClass)
                    val known = rdd.getClass == Class.forName(
                      "org.apache.spark.rdd.MapPartitionsRDD",
                      false,
                      classOf[RDD[_]].getClassLoader
                    ) ||
                      rdd.getClass == Class.forName(
                        "org.apache.spark.rdd.EmptyRDD",
                        false,
                        classOf[RDD[_]].getClassLoader
                      ) ||
                      rdd.getClass == Class.forName(
                        "org.apache.spark.rdd.ParallelCollectionRDD",
                        false,
                        classOf[RDD[_]].getClassLoader
                      )
                    if (!known) record("public-rdd-unknown", s"rdd=${id(rdd)} getters=NOT_INVOKED")
                    else {
                      val dependencies = rdd.dependencies.toVector
                      record(
                        "public-rdd",
                        s"rdd=${id(rdd)} context=${id(rdd.context)} partitions=${rdd.partitions.toVector.map(p => id(p) + ":" + p.index)} dependencies=${dependencies.map(d => id(d) + "->" + id(d.rdd))}"
                      )
                      dependencies.foreach(d => graph(d.rdd))
                    }
                  }
                }
                graph(logical.rdd)
              case _ =>
            }
            record(
              phase + "-plan",
              root
                .map(n =>
                  s"node=${n.nodeName} id=${id(n)} receipt=${DeltaRangerDecisionReceipt.present(n)}"
                )
                .mkString("\n")
            )
          }
        } catch {
          case NonFatal(error) =>
            errors += error; failed("instrumentation", error);
            throw new AssertionError("CREATE trace instrumentation failed", error)
        }
      }
    def failed(phase: String, error: Throwable): Unit = {
      val stack = new StringWriter
      error.printStackTrace(new PrintWriter(stack))
      record("failure-" + phase, stack.toString)
      write(directory.resolve(phase + "-exception.txt"), stack.toString)
    }
    def files(): Map[String, String] = {
      if (!Files.exists(target)) Map.empty
      else {
        val paths = Files.walk(target)
        try
          paths
            .iterator()
            .asScala
            .filter(Files.isRegularFile(_))
            .map(p => target.relativize(p).toString -> sha(Files.readAllBytes(p)))
            .toMap
        finally paths.close()
      }
    }
    def inspectFiles(): Unit = {
      val manifest = files()
      write(directory.resolve("native-manifest.txt"), manifest.toSeq.sorted.mkString("\n"))
      record("native-outcome", s"files=${manifest.size} log0=${Files.exists(log0)}")
      if (Files.exists(log0)) {
        val actions = Files.readAllLines(log0, UTF_8).asScala.map(mapper.readTree)
        val metadata = actions.find(_.has("metaData")).get.get("metaData")
        val commit = actions.find(_.has("commitInfo")).get.get("commitInfo")
        record(
          "native-log0",
          s"operation=${commit.get("operation")} uuid=${metadata.get("id")} schema=${metadata.get("schemaString")} configuration=${metadata.get("configuration")} actionTypes=${actions.map(_.fieldNames().asScala.mkString(","))}"
        )
      }
    }
    override def close(): Unit = synchronized {
      if (!closed) { write(directory.resolve("events.txt"), events.mkString("\n")); closed = true }
    }
  }
  final class ResolutionObserver(spark: SparkSession, trace: Trace) extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      val tag =
        TreeNodeTag[DeltaRangerDecisionReceipt.Reference]("gravitino.delta.ranger.decision-receipt")
      val receipts = plan.map(n => n.getTagValue(tag))
      val operation = DeltaRangerNativeOperation.current()
      val transaction = OptimisticTransaction.getActive
      val settings = spark.sessionState.conf.getAllConfs
      trace.observe(spark, "resolution", Some(plan))
      assertEquals(receipts, plan.map(n => n.getTagValue(tag)), "Observer changed receipt context")
      assertSame(operation, DeltaRangerNativeOperation.current())
      assertEquals(transaction, OptimisticTransaction.getActive)
      assertEquals(settings, spark.sessionState.conf.getAllConfs, "Observer changed settings")
      plan
    }
  }
  def observer(trace: Trace): SparkSessionExtensions => Unit = extensions =>
    extensions.injectResolutionRule(spark => new ResolutionObserver(spark, trace))

  /** Scripted typed registration transport; never evidence of live provider publication. */
  final class Transport(trace: Trace, schema: StructType) {
    val metadata = mock(classOf[Catalog])
    val actor = mock(classOf[Catalog])
    private val metadataTables = mock(classOf[MetadataTables])
    private val actorTables = mock(classOf[MetadataTables])
    private val reservedTable = mock(classOf[MetadataTable])
    val commands = ArrayBuffer.empty[DeltaRegistrationCommand]
    var reads = 0
    var publishSuccess = false
    var metadataFailure: RuntimeException = null
    var reserveFailure: RuntimeException = null
    var afterPublishedResponse: () => Unit = () => ()
    var receipt: DeltaRegistrationReceipt = _
    val sentinel = new IllegalStateException("INSTALLED_SCHEMA_CREATE_PUBLISH_STOP")
    Seq(metadata, actor).foreach { catalog =>
      when(catalog.name()).thenReturn("a")
      when(catalog.provider()).thenReturn("lakehouse-delta")
    }
    when(metadata.asTableCatalog()).thenReturn(metadataTables)
    when(actor.asTableCatalog()).thenReturn(actorTables)
    when(metadataTables.loadTable(any(classOf[NameIdentifier]))).thenAnswer(call => {
      val name = call.getArgument[NameIdentifier](0)
      reads += 1
      if (metadataFailure != null) throw metadataFailure
      trace.record("metadata-absence", s"name=$name type=${classOf[NoSuchTableException].getName}")
      throw new NoSuchTableException("Absent installed schema CREATE fixture: %s", name)
    })
    when(
      actorTables.createTable(
        any(classOf[NameIdentifier]),
        any(classOf[Array[Column]]),
        any(),
        any()
      )
    ).thenAnswer(call => {
      assertEquals(NameIdentifier.of("db", "created"), call.getArgument[NameIdentifier](0))
      assertEquals(0, call.getArgument[Array[Column]](1).length)
      assertNull(call.getArgument[String](2))
      val properties = call.getArgument[java.util.Map[String, String]](3)
      assertEquals(
        Set(DeltaRegistrationCommand.COMMAND_PROPERTY),
        properties.keySet().asScala.toSet
      )
      val command =
        DeltaRegistrationCommand.decode(properties.get(DeltaRegistrationCommand.COMMAND_PROPERTY))
      commands += command
      trace.record("actor-command", command.encode())
      if (reserveFailure != null) throw reserveFailure
      assertEquals(DeltaRegistrationCommand.Operation.RESERVE, command.operation())
      assertEquals(mapper.readTree(schema.json), mapper.readTree(command.exactSchemaJson().get()))
      assertFalse(command.schemaContract().isPresent)
      assertTrue(command.token().isPresent); assertTrue(command.runId().isPresent)
      val marker = DeltaOperationMarker.create(
        11,
        0,
        command.token().get(),
        trace.target.toUri.toString,
        command.exactSchemaJson().get()
      )
      receipt = DeltaRegistrationReceipt.of(
        DeltaRegistrationIdentity.of(11, 0, command.token().get()),
        DeltaRegistrationCommand.Operation.RESERVE,
        DeltaRegistrationReceipt.State.RESERVED,
        command.runId().get(),
        DeltaRegistrationReceipt.Evidence.NONE,
        "NONE",
        Optional.of(marker),
        Optional.empty[String](),
        Optional.empty[String](),
        OptionalLong.empty()
      )
      trace.record("reserved-receipt", receipt.toProperties().asScala.toSeq.sorted.mkString("\n"))
      when(reservedTable.properties()).thenReturn(receipt.toProperties())
      reservedTable
    })
    when(actorTables.alterTable(any(classOf[NameIdentifier]), any(classOf[MetadataChange])))
      .thenAnswer(call => {
        assertEquals(NameIdentifier.of("db", "created"), call.getArgument[NameIdentifier](0))
        val property = call.getArgument[MetadataChange.SetProperty](1)
        assertEquals(DeltaRegistrationCommand.COMMAND_PROPERTY, property.getProperty)
        val command = DeltaRegistrationCommand.decode(property.getValue)
        commands += command
        trace.record("actor-command", command.encode())
        assertEquals(DeltaRegistrationCommand.Operation.PUBLISH, command.operation())
        assertEquals(receipt.identity(), command.expected().get())
        if (!publishSuccess) {
          trace.failed("publish-sentinel", sentinel)
          throw sentinel
        }
        val actions = Files.readAllLines(trace.log0).asScala.map(mapper.readTree)
        val nativeId = actions.find(_.has("metaData")).get.get("metaData").get("id").asText()
        receipt = DeltaRegistrationReceipt.of(
          receipt.identity(),
          receipt.operation(),
          DeltaRegistrationReceipt.State.PUBLISHED,
          receipt.runId(),
          DeltaRegistrationReceipt.Evidence.EXACT_NATIVE_COMMIT,
          "NONE",
          Optional.empty[DeltaOperationMarker](),
          Optional.of(trace.target.toUri.toString),
          Optional.of(nativeId),
          OptionalLong.of(0)
        )
        trace.record(
          "scripted-published-receipt",
          receipt.toProperties().asScala.toSeq.sorted.mkString("\n")
        )
        when(reservedTable.properties()).thenReturn(receipt.toProperties())
        afterPublishedResponse()
        reservedTable
      })
  }
  final class Wrapper(
      transport: Transport,
      policy: TestDeltaRangerNativeSelect.Fixture,
      trace: Trace
  ) extends AutoCloseable {
    private val client = mock(classOf[GravitinoClient])
    when(client.loadCatalog("a")).thenReturn(transport.metadata)
    private val metadata =
      new DeltaRangerMetadataClient(() => 0L, mock(classOf[ScheduledExecutorService]), _ => client)
    val catalog = new DeltaRangerCatalogSpark35(
      new DeltaRangerCatalogSpark35.State(
        metadata,
        () => transport.actor,
        () => transport.metadata,
        _ => policy.engine
      )
    )
    catalog.initialize(
      "a",
      new CaseInsensitiveStringMap(
        Collections.singletonMap(
          "journal-directory",
          trace.directory.resolve("wrapper-journal").toString
        )
      )
    )
    override def close(): Unit = catalog.close()
  }
  var installer: SparkSessionExtensions => Unit = _
  var wrapper: DeltaRangerCatalogSpark35 = _
  var trace: Trace = _
  var loads = 0
  var exists = 0
  var creates = 0
}

/** Spark-loadable test extension that preserves the actual companion installation. */
class InstalledSchemaCreateExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit =
    InstalledSchemaCreateBoundaryProbe.installer(extensions)
}

/** Test forwarding entries call the real Ranger wrapper without translating its result. */
class InstalledSchemaCreateForwardingCatalog extends NativeSelectTestCatalog {
  import InstalledSchemaCreateBoundaryProbe._
  override def loadTable(id: Identifier): Table = {
    loads += 1; trace.record("forward-loadTable", id.toString); wrapper.loadTable(id)
  }
  override def tableExists(id: Identifier): Boolean = {
    exists += 1; trace.record("forward-tableExists", id.toString); wrapper.tableExists(id)
  }
  override def createTable(
      id: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: java.util.Map[String, String]
  ): Table = {
    creates += 1; trace.record("forward-createTable", id.toString);
    wrapper.createTable(id, schema, partitions, properties)
  }
}

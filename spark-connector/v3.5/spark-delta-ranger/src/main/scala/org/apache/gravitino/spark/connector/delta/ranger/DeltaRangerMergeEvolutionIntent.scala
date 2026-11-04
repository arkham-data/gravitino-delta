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
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import scala.collection.JavaConverters._
import scala.util.control.NonFatal
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.delta.actions.{Action, Metadata, Protocol}
import org.apache.spark.sql.types._

/** Detached comparison evidence only. It cannot issue a transaction, read, or publication permit.
  */
private[ranger] final class DeltaRangerMergeEvolutionIntent private (
    val encoded: String,
    private val captured: Boolean
) {
  import DeltaRangerMergeEvolutionIntent._
  private def node: JsonNode = parse(encoded)
  def baseMetadata: Metadata = metadata(node.get("target").get("metadata"))
  def expectedMetadata: Metadata = metadata(node.get("expectedMetadata"))
  def protocol: Protocol = nativeProtocol(node.get("target").get("protocol"))
  def baseVersion: Long = node.get("target").get("version").longValue()
  def sameRegistration: Boolean = node.get("sameRegistration").booleanValue()
  def requiresAlter: Boolean = node.get("requiresAlter").booleanValue()
  def requiredUpdates: Set[String] = strings(node.get("targetUpdated")).toSet
  def transactionIdentity: Option[(String, Long)] = {
    val txn = node.get("context").get("transaction")
    if (txn.isNull) None else Some(txn.get("appId").textValue() -> txn.get("version").longValue())
  }
  def fingerprint: String = DeltaRangerMergeJournal.hash(encoded)

  /** A fresh expected-schema analysis is comparison data, never permission to execute a retry. */
  def requireCurrent(candidate: DeltaRangerMergeEvolutionIntent): Unit = {
    if (!candidate.captured) rejected("current comparison requires fresh native capture")
    val retained = node
    val fresh = candidate.node
    if (!requiresAlter || baseMetadata == expectedMetadata) rejected("missing original evolution")
    if (
      candidate.baseMetadata != expectedMetadata || candidate.protocol != protocol ||
      candidate.baseVersion <= baseVersion || candidate.expectedMetadata != expectedMetadata
    )
      rejected("current native transition changed")
    val target = fresh.get("target").asInstanceOf[ObjectNode]
    target.set[JsonNode]("metadata", retained.get("target").get("metadata"))
    target.set[JsonNode]("version", retained.get("target").get("version"))
    if (sameRegistration) {
      if (
        !candidate.sameRegistration || metadata(
          fresh.get("source").get("metadata")
        ) != expectedMetadata ||
        nativeProtocol(fresh.get("source").get("protocol")) != protocol
      )
        rejected("same-registration source transition changed")
      fresh
        .get("source")
        .asInstanceOf[ObjectNode]
        .set[JsonNode]("metadata", retained.get("source").get("metadata"))
    }
    // Data-only source advancement is not part of a bounded CDF interval's identity.
    fresh
      .get("source")
      .asInstanceOf[ObjectNode]
      .set[JsonNode]("version", retained.get("source").get("version"))
    if (fresh != retained) rejected("authored or governed evolution intent changed")
  }

  /** Cross-check detached proof fields; native commit bytes and duplicate marker remain mandatory.
    * expectedMetadata is a bare native Metadata object, outside the supplied actions array.
    */
  def requireProofBinding(encodedProof: String, committedVersion: Long): Unit = {
    val proof = parse(encodedProof)
    if (
      !proof.isObject || !proof.has("readVersion") || !proof.has("nativeId") ||
      !proof.has("expectedMetadata") || !proof.has("actions")
    ) rejected("incomplete evolution proof")
    if (
      long(proof.get("readVersion")) != baseVersion || baseVersion == Long.MaxValue ||
      committedVersion != baseVersion + 1 || !proof.get("nativeId").isTextual ||
      proof.get("nativeId").textValue().isEmpty ||
      proof.get("expectedMetadata") != node.get("expectedMetadata")
    ) rejected("evolution proof transition changed")
    val actions = array(proof.get("actions"))
    if (
      actions.exists(a =>
        !a.isObject || a.size() != 1 ||
          !Set("add", "remove", "cdc", "txn")(a.fieldNames().next())
      )
    ) rejected("evolution supplied actions changed")
    val transactions = actions.filter(_.has("txn")).map(_.get("txn"))
    transactionIdentity match {
      case Some((app, version)) =>
        if (transactions.size != 1) rejected("missing exact native transaction action")
        val txn = transactions.head
        val names = txn.fieldNames().asScala.toSet
        if (
          !txn.isObject || !(names == Set("appId", "version") || names == Set(
            "appId",
            "version",
            "lastUpdated"
          )) ||
          !txn.get("appId").isTextual || txn.get("appId").textValue() != app || long(
            txn.get("version")
          ) != version ||
          (txn.has("lastUpdated") && long(txn.get("lastUpdated")) < 0)
        ) rejected("native transaction identity changed")
      case None => if (transactions.nonEmpty) rejected("unexpected native transaction action")
    }
  }
}

private[ranger] object DeltaRangerMergeEvolutionIntent {
  private val MaxBytes = 8 * 1024 * 1024
  private val mapper = new ObjectMapper()
    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)

  /** Named observations are inputs only; capture snapshots every native value immediately. */
  final case class Observation(
      name: Vector[String],
      receipt: Map[String, String],
      location: String,
      nativeId: String,
      owner: Option[String],
      metadata: Metadata,
      protocol: Protocol,
      version: Long
  )
  final case class Context(
      transaction: Option[(String, Long)],
      user: String,
      groups: Set[String],
      settings: Map[String, String]
  )
  final class Authored private[DeltaRangerMergeEvolutionIntent] (
      private[DeltaRangerMergeEvolutionIntent] val encoded: String
  )
  final case class Capture(
      authored: Authored,
      resolved: DeltaMergeInto,
      governed: DeltaMergeInto,
      sourceLeaf: LogicalPlan,
      targetLeaf: LogicalPlan,
      sourcePolicy: LogicalPlan,
      targetPolicy: LogicalPlan,
      target: Observation,
      source: Observation,
      expectedMetadata: Metadata,
      start: Long,
      end: Long,
      sourceSelected: Set[String],
      targetSelected: Set[String],
      targetUpdated: Set[String],
      context: Context
  )

  /** Call at childrenResolved MergeIntoTable interception, before Delta star expansion. */
  def authored(
      command: MergeIntoTable,
      sourceLeaf: LogicalPlan,
      targetLeaf: LogicalPlan
  ): Authored = {
    if (command.notMatchedBySourceActions.nonEmpty || command.matchedActions.isEmpty)
      rejected("unsupported authored clauses")
    val roles = commandRoles(command.sourceTable.output, command.targetTable.output)
    def expr(value: Expression): JsonNode = expression(value, roles, unresolved = true)
    def assignments(values: Seq[Assignment]): JsonNode = list(
      values.map(a => list(Seq(expr(a.key), expr(a.value))))
    )
    def clause(action: MergeAction): JsonNode = action match {
      case UpdateStarAction(condition) => list(Seq(text("update-star"), optional(condition, expr)))
      case InsertStarAction(condition) => list(Seq(text("insert-star"), optional(condition, expr)))
      case UpdateAction(condition, values) =>
        list(Seq(text("update"), optional(condition, expr), assignments(values)))
      case InsertAction(condition, values) =>
        list(Seq(text("insert"), optional(condition, expr), assignments(values)))
      case DeleteAction(condition) => list(Seq(text("delete"), optional(condition, expr)))
      case _                       => rejected("unsupported authored action")
    }
    new Authored(
      canonical(
        list(
          Seq(
            text("authored-v1"),
            sourcePlan(command.sourceTable, sourceLeaf),
            targetAliases(command.targetTable, targetLeaf),
            expr(command.mergeCondition),
            list(command.matchedActions.map(clause)),
            list(command.notMatchedActions.map(clause))
          )
        )
      ).toString
    )
  }

  def capture(input: Capture): DeltaRangerMergeEvolutionIntent = {
    val node = mapper.createObjectNode()
    node.put("encoding", "additive-merge-v1")
    node.set[JsonNode]("authored", parse(input.authored.encoded))
    val expected = metadata(parse(input.expectedMetadata.json).get("metaData"))
    validateTransition(input.target.metadata, expected, input.target.protocol)
    def semantics(command: DeltaMergeInto): JsonNode = {
      if (
        command.notMatchedBySourceClauses.nonEmpty || command.matchedClauses.isEmpty || command.finalSchema.isEmpty
      )
        rejected("unsupported resolved clauses or final schema")
      val nativeSchema = command.finalSchema.get
      val old = input.target.metadata.schema
      if (
        nativeSchema.length != expected.schema.length || !nativeSchema.fields
          .take(old.length)
          .sameElements(old.fields) ||
        !nativeSchema.fields.drop(old.length).zip(expected.schema.fields.drop(old.length)).forall {
          case (a, b) => a.copy(nullable = true) == b
        }
      )
        rejected("native final schema differs from exact expectation")
      val roles = commandRoles(command.source.output, command.target.output)
      def expr(value: Expression): JsonNode = expression(value, roles)
      def clause(value: DeltaMergeIntoClause): JsonNode = {
        val kind = value match {
          case _: DeltaMergeIntoMatchedUpdateClause    => "update"
          case _: DeltaMergeIntoMatchedDeleteClause    => "delete"
          case _: DeltaMergeIntoNotMatchedInsertClause => "insert"
          case _                                       => rejected("unsupported native clause")
        }
        val actions = value.actions.map {
          case a: DeltaMergeAction if a.targetColNameResolved && a.targetColNameParts.size == 1 =>
            list(Seq(stringList(a.targetColNameParts), expr(a.expr)))
          case _ => rejected("unresolved or nested native assignment")
        }
        list(Seq(text(kind), optional(value.condition, expr), list(actions)))
      }
      list(
        Seq(
          text("resolved-v1"),
          parse(expected.schema.json),
          expr(command.condition),
          list(command.matchedClauses.map(clause)),
          list(command.notMatchedClauses.map(clause)),
          bool(command.withSchemaEvolution)
        )
      )
    }
    node.set[JsonNode]("resolved", semantics(input.resolved))
    node.set[JsonNode]("governed", semantics(input.governed))
    node.set[JsonNode]("sourcePolicy", sourcePlan(input.sourcePolicy, input.sourceLeaf))
    node.set[JsonNode](
      "targetPolicy",
      sourcePlan(input.targetPolicy, input.targetLeaf, target = true)
    )
    node.set[JsonNode]("target", observation(input.target))
    node.set[JsonNode]("source", observation(input.source))
    node.set[JsonNode]("expectedMetadata", parse(expected.json).get("metaData"))
    node.put("sameRegistration", identity(node.get("target")) == identity(node.get("source")))
    node.put("start", input.start); node.put("end", input.end)
    node.put("requiresAlter", true)
    node.set[JsonNode]("sourceSelected", stringList(input.sourceSelected.toVector.sorted))
    node.set[JsonNode]("targetSelected", stringList(input.targetSelected.toVector.sorted))
    node.set[JsonNode]("targetUpdated", stringList(input.targetUpdated.toVector.sorted))
    val context = node.putObject("context")
    input.context.transaction match {
      case Some((app, version)) =>
        val txn = context.putObject("transaction"); txn.put("appId", app);
        txn.put("version", version)
      case None => context.putNull("transaction")
    }
    context.put("user", input.context.user)
    context.set[JsonNode]("groups", stringList(input.context.groups.toVector.sorted))
    context.set[JsonNode](
      "settings",
      stringMap(
        input.context.settings.filterNot(
          _._1 == "spark.databricks.delta.lastCommitVersionInSession"
        )
      )
    )
    val validated = decode(canonical(node).toString)
    new DeltaRangerMergeEvolutionIntent(validated.encoded, captured = true)
  }

  def decode(encoded: String): DeltaRangerMergeEvolutionIntent = {
    val node = parse(encoded)
    fields(
      node,
      Set(
        "encoding",
        "authored",
        "resolved",
        "governed",
        "sourcePolicy",
        "targetPolicy",
        "target",
        "source",
        "expectedMetadata",
        "sameRegistration",
        "start",
        "end",
        "requiresAlter",
        "sourceSelected",
        "targetSelected",
        "targetUpdated",
        "context"
      )
    )
    if (
      string(node.get("encoding")) != "additive-merge-v1" || !node
        .get("sameRegistration")
        .isBoolean ||
      !node.get("requiresAlter").isBoolean || !node.get("requiresAlter").booleanValue() ||
      long(node.get("start")) < 0 || long(node.get("end")) < long(node.get("start"))
    ) rejected("invalid evolution envelope")
    Seq("target", "source").foreach(k => validateObservation(node.get(k)))
    val same = identity(node.get("target")) == identity(node.get("source"))
    if (same != node.get("sameRegistration").booleanValue())
      rejected("same-registration identity changed")
    if (
      same && (node.get("target").get("metadata") != node.get("source").get("metadata") || node
        .get("target")
        .get("protocol") != node.get("source").get("protocol"))
    )
      rejected("inconsistent same-registration native state")
    val base = metadata(node.get("target").get("metadata"))
    val expected = metadata(node.get("expectedMetadata"))
    validateTransition(base, expected, nativeProtocol(node.get("target").get("protocol")))
    Seq("sourceSelected", "targetSelected", "targetUpdated").foreach(k =>
      distinctStrings(node.get(k))
    )
    if (
      !expected.schema.fields
        .drop(base.schema.length)
        .map(_.name)
        .toSet
        .subsetOf(strings(node.get("targetUpdated")).toSet)
    )
      rejected("missing added-column UPDATE obligations")
    val context = node.get("context")
    fields(context, Set("transaction", "user", "groups", "settings"))
    nonempty(context.get("user")); distinctStrings(context.get("groups"));
    validateStringMap(context.get("settings"))
    if (context.get("settings").has("spark.databricks.delta.lastCommitVersionInSession"))
      rejected("unstable context encoding")
    val txn = context.get("transaction")
    if (!txn.isNull) {
      fields(txn, Set("appId", "version")); nonempty(txn.get("appId"));
      if (long(txn.get("version")) < 0) rejected("negative transaction version")
    }
    validateAuthored(node.get("authored"))
    Seq("resolved", "governed").foreach(k => validateSemantics(node.get(k), expected.schema))
    Seq("sourcePolicy", "targetPolicy").foreach(k => validatePlan(node.get(k)))
    new DeltaRangerMergeEvolutionIntent(canonical(node).toString, captured = false)
  }

  private def observation(value: Observation): JsonNode = {
    val node = mapper.createObjectNode()
    node.set[JsonNode]("name", stringList(value.name))
    node.set[JsonNode](
      "receipt",
      stringMap(value.receipt.filterNot(_._1 == "delta.registration.receipt.nativeVersion"))
    )
    node.put("location", value.location); node.put("nativeId", value.nativeId);
    node.put("owner", value.owner.orNull)
    node.set[JsonNode]("metadata", parse(value.metadata.json).get("metaData"))
    node.set[JsonNode]("protocol", parse(value.protocol.json).get("protocol"))
    node.put("version", value.version)
    node
  }
  private def validateObservation(node: JsonNode): Unit = {
    fields(
      node,
      Set("name", "receipt", "location", "nativeId", "owner", "metadata", "protocol", "version")
    )
    if (
      strings(node.get("name")).size != 4 || strings(node.get("name"))
        .exists(_.isEmpty) || long(node.get("version")) < 0
    ) rejected("invalid named observation")
    validateStringMap(node.get("receipt")); nonempty(node.get("location"));
    nonempty(node.get("nativeId"))
    if (!node.get("owner").isNull) nonempty(node.get("owner"))
    if (metadata(node.get("metadata")).id != string(node.get("nativeId")))
      rejected("native table UUID changed")
    nativeProtocol(node.get("protocol"))
  }
  private def identity(node: JsonNode): JsonNode = {
    val result = node.deepCopy[ObjectNode]()
    Seq("metadata", "protocol", "version").foreach(result.remove)
    result
  }
  private def validateTransition(base: Metadata, expected: Metadata, protocol: Protocol): Unit = {
    val old = base.schema; val next = expected.schema
    if (
      base.copy(schemaString = expected.schemaString) != expected || next.length < old.length ||
      !next.fields.take(old.length).sameElements(old.fields) ||
      !next.fields.drop(old.length).forall(f => f.nullable && scalar(f.dataType)) ||
      next.fieldNames.map(_.toLowerCase(java.util.Locale.ROOT)).distinct.length != next.length
    )
      rejected("non-additive native metadata transition")
    val ordinary = protocol == Protocol(1, 2)
    val cdf =
      protocol.minReaderVersion == 1 && protocol.minWriterVersion == 7 && protocol.readerFeatures.isEmpty &&
        protocol.writerFeatures.contains(Set("changeDataFeed", "appendOnly", "invariants"))
    if (!ordinary && !cdf) rejected("unsupported evolving protocol")
  }
  private def scalar(value: DataType): Boolean = value match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
        StringType | BinaryType | DateType | TimestampType =>
      true
    case _: DecimalType => true
    case _              => false
  }
  private def metadata(node: JsonNode): Metadata = {
    if (node == null || !node.isObject) rejected("invalid metadata")
    val envelope = mapper.createObjectNode(); envelope.set[JsonNode]("metaData", node)
    try {
      val value = Action.fromJson(envelope.toString).asInstanceOf[Metadata]
      if (parse(value.json).get("metaData") != node) rejected("noncanonical metadata fields")
      if (parse(value.schemaString) != parse(value.schema.json))
        rejected("noncanonical native schema fields")
      value
    } catch { case NonFatal(e) => rejected("invalid native metadata", e) }
  }
  private def nativeProtocol(node: JsonNode): Protocol = {
    if (node == null || !node.isObject) rejected("invalid protocol")
    val envelope = mapper.createObjectNode(); envelope.set[JsonNode]("protocol", node)
    try {
      val value = Action.fromJson(envelope.toString).asInstanceOf[Protocol]
      if (parse(value.json).get("protocol") != node) rejected("noncanonical protocol fields")
      value
    } catch { case NonFatal(e) => rejected("invalid native protocol", e) }
  }

  private type Roles = Map[ExprId, JsonNode]
  private def field(attribute: Attribute): JsonNode = list(
    Seq(
      text(attribute.name),
      parse(attribute.dataType.json),
      bool(attribute.nullable),
      parse(attribute.metadata.json)
    )
  )
  private def roles(output: Seq[Attribute], role: String, ordinal: Boolean): Roles = {
    if (output.map(_.exprId).distinct.size != output.size)
      rejected("ambiguous attribute occurrence")
    output.zipWithIndex.map { case (a, i) =>
      a.exprId -> list(
        Seq(text("attribute"), text(role), if (ordinal) number(i) else text(a.name), field(a))
      )
    }.toMap
  }
  private def commandRoles(source: Seq[Attribute], target: Seq[Attribute]): Roles = {
    if (source.map(_.exprId).toSet.intersect(target.map(_.exprId).toSet).nonEmpty)
      rejected("ambiguous source/target occurrence")
    roles(source, "source", ordinal = true) ++ roles(target, "target", ordinal = false)
  }
  private def expression(value: Expression, bound: Roles, unresolved: Boolean = false): JsonNode =
    value match {
      case a: AttributeReference =>
        val result = bound.getOrElse(a.exprId, rejected("unbound attribute occurrence"))
        if (result.get(3) != field(a)) rejected("attribute type or metadata changed")
        result
      case a: UnresolvedAttribute if unresolved =>
        list(Seq(text("unresolved"), stringList(a.nameParts)))
      case function: UnresolvedFunction if unresolved =>
        // Preserve authored lookup/flags; only native resolution decides the admitted operator.
        list(
          Seq(
            text("unresolved-function"),
            stringList(function.nameParts),
            bool(function.isDistinct),
            bool(function.ignoreNulls),
            optional(function.filter, (e: Expression) => expression(e, bound, unresolved)),
            list(function.arguments.map(expression(_, bound, unresolved)))
          )
        )
      case literal: Literal =>
        list(Seq(text("literal"), parse(literal.dataType.json), text(literal.sql)))
      case alias: Alias =>
        list(
          Seq(
            text("alias"),
            text(alias.name),
            stringList(alias.qualifier),
            parse(alias.metadata.json),
            expression(alias.child, bound, unresolved)
          )
        )
      case cast: Cast =>
        list(
          Seq(
            text("cast"),
            parse(cast.dataType.json),
            optional(cast.timeZoneId, text),
            text(cast.evalMode.toString),
            expression(cast.child, bound, unresolved)
          )
        )
      case arithmetic: BinaryArithmetic =>
        val mode = arithmetic match {
          case value: Add      => value.evalMode
          case value: Subtract => value.evalMode
          case value: Multiply => value.evalMode
          case value: Divide   => value.evalMode
          case _               => rejected("unsupported arithmetic")
        }
        list(
          Seq(
            text("operator"),
            text(arithmetic.getClass.getSimpleName),
            text(mode.toString),
            list(arithmetic.children.map(expression(_, bound, unresolved)))
          )
        )
      case _: EqualTo | _: EqualNullSafe | _: GreaterThan | _: GreaterThanOrEqual | _: LessThan |
          _: LessThanOrEqual | _: And | _: Or | _: Not | _: IsNull | _: IsNotNull | _: In |
          _: Upper | _: Lower | _: Concat | _: Coalesce =>
        list(
          Seq(
            text("operator"),
            text(value.getClass.getSimpleName),
            text(""),
            list(value.children.map(expression(_, bound, unresolved)))
          )
        )
      case _ => rejected("unsupported expression " + value.nodeName)
    }
  private def sourcePlan(
      plan: LogicalPlan,
      leaf: LogicalPlan,
      target: Boolean = false
  ): JsonNode = {
    if (leaf.children.nonEmpty || leaf.isStreaming || !leaf.resolved)
      rejected("source anchor is not a resolved logical leaf")
    def physical(value: LogicalPlan): Boolean = (value eq leaf) || (value match {
      case a: SubqueryAlias => physical(a.child)
      case _                => false
    })
    def inputRoles(child: LogicalPlan): Roles =
      roles(child.output, "input", ordinal = !physical(child))
    def loop(value: LogicalPlan): JsonNode = {
      if (value eq leaf) return list(Seq(text("leaf")))
      value match {
        case p: Project =>
          list(
            Seq(
              text("project"),
              list(
                p.projectList.map(expression(_, inputRoles(p.child)))
              ),
              loop(p.child)
            )
          )
        case f: Filter =>
          list(
            Seq(
              text("filter"),
              expression(f.condition, inputRoles(f.child)),
              loop(f.child)
            )
          )
        case a: SubqueryAlias =>
          list(
            Seq(
              text("subquery-alias"),
              stringList(a.identifier.qualifier :+ a.identifier.name),
              loop(a.child)
            )
          )
        case _ => rejected("unsupported source or policy plan " + value.nodeName)
      }
    }
    // A projected source binds precisely its authored output. A target policy may copy through
    // unselected fields; preserve the expressions and referenced field shapes, not leaf growth.
    list(
      Seq(
        text(if (target) "target-plan-v1" else "source-plan-v1"),
        if (target) list(Nil) else list(plan.output.map(field)),
        loop(plan)
      )
    )
  }
  private def targetAliases(plan: LogicalPlan, leaf: LogicalPlan): JsonNode = {
    if (plan eq leaf) return list(Nil)
    plan match {
      case a: SubqueryAlias =>
        list(
          Seq(stringList(a.identifier.qualifier :+ a.identifier.name), targetAliases(a.child, leaf))
        )
      case _ => rejected("unsupported target shape")
    }
  }

  // Semantic subtrees are inert comparison data, not a second expression deserializer. Their
  // roots are strict/versioned; requireCurrent must compare them to freshly captured finite
  // native semantics. No parsed expression, plan, caller hash, or journal record is executable.
  private def validateAuthored(node: JsonNode): Unit = {
    val values = array(node)
    if (values.size != 6 || string(values.head) != "authored-v1")
      rejected("invalid authored encoding")
    validatePlan(values(1))
    values.drop(2).foreach(array)
  }
  private def validateSemantics(node: JsonNode, schema: StructType): Unit = {
    val values = array(node)
    if (
      values.size != 6 || string(values.head) != "resolved-v1" ||
      values(1) != parse(schema.json) || !values(5).isBoolean
    ) rejected("invalid resolved encoding")
    values.slice(2, 5).foreach(array)
  }
  private def validatePlan(node: JsonNode): Unit = {
    val values = array(node)
    if (values.size != 3 || !Set("source-plan-v1", "target-plan-v1")(string(values.head)))
      rejected("invalid source encoding")
    values.drop(1).foreach(array)
  }
  private def fields(node: JsonNode, names: Set[String]): Unit = if (
    node == null || !node.isObject || node.fieldNames().asScala.toSet != names
  ) rejected("invalid object fields")
  private def array(node: JsonNode): Vector[JsonNode] = {
    if (node == null || !node.isArray) rejected("expected array"); node.elements().asScala.toVector
  }
  private def string(node: JsonNode): String = {
    if (node == null || !node.isTextual) rejected("expected string"); node.textValue()
  }
  private def nonempty(node: JsonNode): Unit = if (string(node).isEmpty) rejected("empty identity")
  private def long(node: JsonNode): Long = {
    if (node == null || !node.isIntegralNumber || !node.canConvertToLong) rejected("expected long");
    node.longValue()
  }
  private def strings(node: JsonNode): Vector[String] = array(node).map(string)
  private def distinctStrings(node: JsonNode): Unit = {
    val values = strings(node);
    if (values != values.distinct.sorted || values.exists(_.isEmpty))
      rejected("noncanonical inventory")
  }
  private def validateStringMap(node: JsonNode): Unit = if (
    !node.isObject || node.elements().asScala.exists(!_.isTextual)
  ) rejected("invalid string map")
  private def stringMap(values: Map[String, String]): JsonNode = {
    val node = mapper.createObjectNode();
    values.toVector.sortBy(_._1).foreach { case (k, v) => node.put(k, v) }; node
  }
  private def text(value: String): JsonNode = mapper.getNodeFactory.textNode(value)
  private def bool(value: Boolean): JsonNode = mapper.getNodeFactory.booleanNode(value)
  private def number(value: Long): JsonNode = mapper.getNodeFactory.numberNode(value)
  private def list(values: Seq[JsonNode]): ArrayNode = {
    val node = mapper.createArrayNode(); values.foreach(node.add); node
  }
  private def stringList(values: Seq[String]): JsonNode = list(values.map(text))
  private def optional[T](value: Option[T], encode: T => JsonNode): JsonNode =
    value.map(encode).getOrElse(mapper.getNodeFactory.nullNode())
  private def canonical(node: JsonNode): JsonNode = {
    if (node.isObject) {
      val result = mapper.createObjectNode();
      node
        .fields()
        .asScala
        .toVector
        .sortBy(_.getKey)
        .foreach(e => result.set[JsonNode](e.getKey, canonical(e.getValue))); result
    } else if (node.isArray) list(node.elements().asScala.toVector.map(canonical))
    else node
  }
  private def parse(value: String): JsonNode = {
    if (value == null || value.getBytes(StandardCharsets.UTF_8).length > MaxBytes)
      rejected("oversized evolution encoding")
    try {
      val node = mapper.readTree(value); if (node == null) rejected("empty evolution encoding");
      node
    } catch { case NonFatal(e) => rejected("invalid evolution JSON", e) }
  }
  private def rejected(reason: String, cause: Throwable = null): Nothing =
    throw new SecurityException("Delta MERGE evolution intent rejected: " + reason, cause)
}

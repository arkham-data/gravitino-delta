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

package org.apache.gravitino.spark.connector.delta.ranger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;

/** Closed test-only protocol for one failed ordinary-policy callback. */
final class DeltaRangerPolicyCallbackCase {
  private static final String OWNER = "GRAVITINO_OWNER_ROLE";
  private static final Set<String> CONFIG_FIELDS =
      Set.of("caseId", "kind", "role", "subject", "visibilityRole", "tableA", "tableB");

  enum Kind {
    FIRST_GRANT,
    ADDITIONAL_GRANT,
    LAST_REVOKE
  }

  private enum State {
    NEW,
    PREPARED,
    ARMED,
    QUARANTINED,
    CLOSED
  }

  final String id;
  final Kind kind;
  final String role;
  final String subject;
  final String visibilityRole;
  final JsonNode tableA;
  final JsonNode tableB;
  private State state = State.NEW;
  @Nullable private String active;

  private DeltaRangerPolicyCallbackCase(JsonNode config) {
    requireFields(config, CONFIG_FIELDS);
    id = token(config, "caseId");
    kind = Kind.valueOf(text(config, "kind"));
    role = token(config, "role");
    subject = token(config, "subject");
    visibilityRole = token(config, "visibilityRole");
    require(
        !role.equals(visibilityRole) && !role.startsWith("GRAVITINO_"),
        "Dedicated ordinary role required");
    tableA = table(config.path("tableA"));
    tableB = table(config.path("tableB"));
    require(!fullName(tableA).equals(fullName(tableB)), "Different registered tables required");
  }

  static DeltaRangerPolicyCallbackCase parse(JsonNode config) {
    return new DeltaRangerPolicyCallbackCase(config);
  }

  JsonNode target() {
    return kind == Kind.ADDITIONAL_GRANT ? tableB : tableA;
  }

  String mutation() {
    return kind == Kind.LAST_REVOKE ? "revoke-select" : "grant-select";
  }

  void begin(JsonNode command) {
    requireFields(command, Set.of("operation", "caseId", "kind", "id"));
    require(
        id.equals(text(command, "caseId")) && kind.name().equals(text(command, "kind")),
        "Case identity mismatch");
    token(command, "id");
    require(active == null && state != State.CLOSED, "Command unresolved or closed");
    String operation = text(command, "operation");
    boolean allowed =
        "close".equals(operation)
            || "snapshot".equals(operation)
            || (state == State.NEW && "prepare-policy-case".equals(operation))
            || (state == State.PREPARED && "fault-policy-write".equals(operation))
            || (state == State.ARMED && mutation().equals(operation));
    require(allowed, "Command violates finite case order");
    active = operation;
  }

  void finish(boolean completed) {
    require(active != null, "No active command");
    String operation = active;
    active = null;
    if ("close".equals(operation)) state = State.CLOSED;
    else if (!completed || mutation().equals(operation)) state = State.QUARANTINED;
    else if ("prepare-policy-case".equals(operation)) state = State.PREPARED;
    else if ("fault-policy-write".equals(operation)) state = State.ARMED;
  }

  boolean quarantined() {
    return state == State.QUARANTINED;
  }

  Selection select(
      String service,
      String metalakeId,
      String canonicalRole,
      List<SecurableObject> local,
      List<String> memberships,
      JsonNode policies,
      JsonNode roles) {
    require(
        canonicalRole.equals("GRAVITINO_METALAKE_" + metalakeId + "_ROLE_" + role),
        "Canonical role mismatch");
    require(
        memberships.size() == 2 && Set.copyOf(memberships).equals(Set.of(role, visibilityRole)),
        "Isolated local membership required");
    List<String> localNames = new ArrayList<>();
    for (SecurableObject object : local) {
      require(
          object.type() == MetadataObject.Type.TABLE
              && object.privileges().equals(List.of(Privileges.SelectTable.allow())),
          "Only exact table SELECT allowed");
      localNames.add(object.fullName());
    }
    require(
        localNames.equals(kind == Kind.FIRST_GRANT ? List.of() : List.of(fullName(tableA))),
        "Wrong local pre-state");
    validateRole(roles, canonicalRole);
    JsonNode targetPolicy = ownedPolicy(policies, target(), service, metalakeId);
    if (kind == Kind.ADDITIONAL_GRANT) {
      require(
          ordinaryItem(ownedPolicy(policies, tableA, service, metalakeId), canonicalRole) >= 0,
          "Healthy first table grant missing");
    }
    managedSelectItem(targetPolicy);
    int index = ordinaryItem(targetPolicy, canonicalRole);
    require(
        (index >= 0) == (kind == Kind.LAST_REVOKE), "Target privilege already converged or absent");
    ObjectNode after = targetPolicy.deepCopy();
    ArrayNode items = (ArrayNode) after.path("policyItems");
    if (kind == Kind.LAST_REVOKE) {
      ObjectNode item = (ObjectNode) items.get(index);
      ArrayNode rolesAfter = (ArrayNode) item.path("roles");
      for (int i = rolesAfter.size() - 1; i >= 0; i--) {
        if (canonicalRole.equals(rolesAfter.get(i).asText())) rolesAfter.remove(i);
      }
      if (rolesAfter.isEmpty() && empty(item.path("users")) && empty(item.path("groups")))
        items.remove(index);
    } else if (managedSelectItem(targetPolicy) >= 0) {
      ((ArrayNode) items.get(managedSelectItem(targetPolicy)).path("roles")).add(canonicalRole);
    } else {
      ObjectNode item = items.addObject().put("delegateAdmin", false);
      item.putArray("roles").add(canonicalRole);
      item.putArray("users");
      item.putArray("groups");
      item.putArray("conditions");
      item.putArray("accesses").addObject().put("type", "select").put("isAllowed", true);
    }
    return new Selection(
        "PUT",
        "/service/public/v2/api/policy/" + targetPolicy.path("id").longValue(),
        targetPolicy.deepCopy(),
        after);
  }

  static void verifyMutationEvidence(
      List<JsonNode> callbacks,
      List<JsonNode> publicEvents,
      String method,
      String path,
      String publicPath) {
    JsonNode callback = completedPair(callbacks, method, path);
    require(
        callback.path("faultStatus").isIntegralNumber()
            && callback.path("faultStatus").intValue() == 503
            && callback.path("upstreamAttempted").isBoolean()
            && !callback.path("upstreamAttempted").booleanValue()
            && callback.path("downstreamResponseCompleted").asBoolean(),
        "Exactly one completed pre-forwarding 503 required");
    JsonNode response = completedPair(publicEvents, "PUT", publicPath);
    require(
        response.path("upstreamAttempted").asBoolean()
            && response.path("upstreamStatus").isIntegralNumber()
            && response.path("upstreamStatus").intValue() >= 100
            && response.path("upstreamStatus").intValue() <= 599
            && response.path("upstreamResponseCompleted").isTextual()
            && response.path("downstreamResponseCompleted").asBoolean()
            && response.path("responseBodyFile").isTextual()
            && !response.path("responseBodyFile").textValue().isBlank()
            && !response.path("upstreamCompletionUnresolved").asBoolean(),
        "Completed public HTTP response and body required");
  }

  static String fullName(JsonNode table) {
    return text(table, "name") + "." + text(table, "database") + "." + text(table, "table");
  }

  static final class Selection {
    final String method;
    final String path;
    final JsonNode before;
    final JsonNode after;

    private Selection(String method, String path, JsonNode before, JsonNode after) {
      this.method = method;
      this.path = path;
      this.before = before;
      this.after = after;
    }
  }

  private static JsonNode completedPair(List<JsonNode> events, String method, String path) {
    List<JsonNode> matches = new ArrayList<>();
    for (JsonNode event : events) {
      if (method.equals(event.path("method").asText()) && path.equals(event.path("path").asText()))
        matches.add(event);
    }
    require(matches.size() == 2, "Exactly one start/completion pair required; no retry");
    JsonNode start = matches.get(0);
    JsonNode end = matches.get(1);
    require(
        !start.has("finished")
            && end.path("finished").isTextual()
            && start.path("run").isTextual()
            && !start.path("run").textValue().isBlank()
            && start.path("attempt").isIntegralNumber()
            && start.path("attempt").longValue() > 0
            && start.path("run").equals(end.path("run"))
            && start.path("attempt").equals(end.path("attempt"))
            && start.path("started").isTextual()
            && start.path("started").equals(end.path("started")),
        "Mismatched or incomplete attempt");
    return end;
  }

  private void validateRole(JsonNode roles, String canonicalRole) {
    require(roles.isArray(), "Complete role array required");
    List<JsonNode> matches = new ArrayList<>();
    roles.forEach(
        r -> {
          if (canonicalRole.equals(r.path("name").asText())) matches.add(r);
        });
    require(matches.size() <= 1, "Duplicate remote role");
    if (matches.isEmpty()) {
      require(kind == Kind.FIRST_GRANT, "Mirrored role required");
      return;
    }
    JsonNode remote = matches.get(0);
    require(
        remote.path("id").isIntegralNumber() && remote.path("id").asLong() > 0,
        "Remote role ID required");
    require(empty(remote.path("groups")) && empty(remote.path("roles")), "Foreign role membership");
    JsonNode users = remote.path("users");
    require(users.isArray() || empty(users), "Role users required");
    boolean mirrored =
        users.size() == 1
            && subject.equals(users.get(0).path("name").asText())
            && users.get(0).path("isAdmin").isBoolean()
            && !users.get(0).path("isAdmin").booleanValue();
    require(
        mirrored || (kind == Kind.FIRST_GRANT && empty(users)),
        "Dedicated mirrored member required");
  }

  private static JsonNode ownedPolicy(
      JsonNode policies, JsonNode table, String service, String metalakeId) {
    require(policies.isArray(), "Complete policy array required");
    List<JsonNode> matches = new ArrayList<>();
    Set<String> labels =
        Set.of(
            "gravitino-managed",
            "gravitino-metalake-id:" + metalakeId,
            "gravitino-catalog-id:" + text(table, "catalogId"));
    for (JsonNode p : policies) {
      if (!service.equals(p.path("service").asText())
          || !p.path("policyType").isIntegralNumber()
          || p.path("policyType").intValue() != 0) continue;
      Set<String> actual = new HashSet<>();
      p.path("policyLabels").forEach(l -> actual.add(l.asText()));
      if (!actual.containsAll(labels)) continue;
      JsonNode resources = p.path("resources");
      if (!fields(resources).equals(Set.of("catalog", "schema", "table", "column"))) continue;
      String[] keys = {"catalog", "schema", "table", "column"};
      String[] values = {text(table, "name"), text(table, "database"), text(table, "table"), "*"};
      boolean exact = true;
      for (int i = 0; i < keys.length; i++) {
        JsonNode r = resources.path(keys[i]);
        exact &=
            r.path("values").isArray()
                && r.path("values").size() == 1
                && values[i].equals(r.path("values").get(0).asText())
                && r.path("isExcludes").isBoolean()
                && !r.path("isExcludes").booleanValue()
                && r.path("isRecursive").isBoolean()
                && !r.path("isRecursive").booleanValue();
      }
      if (exact) matches.add(p);
    }
    require(matches.size() == 1, "Exactly one owned registered-table policy required");
    JsonNode p = matches.get(0);
    require(
        p.path("id").isIntegralNumber()
            && p.path("id").canConvertToLong()
            && p.path("id").longValue() > 0,
        "Verified policy ID required");
    require(
        p.path("policyItems").isArray()
            && (p.path("denyPolicyItems").isArray() || empty(p.path("denyPolicyItems"))),
        "Policy item arrays required");
    int owners = 0;
    for (JsonNode item : p.path("policyItems")) {
      if (contains(item.path("roles"), OWNER)) {
        require(
            item.path("roles").size() == 1 && access(item, "all"),
            "Protected owner item malformed");
        owners++;
      }
    }
    require(owners == 1, "Protected owner item required");
    return p;
  }

  private static int managedSelectItem(JsonNode policy) {
    int found = -1;
    JsonNode items = policy.path("policyItems");
    for (int i = 0; i < items.size(); i++) {
      JsonNode item = items.get(i);
      boolean managed = false;
      for (JsonNode role : item.path("roles")) managed |= role.asText().startsWith("GRAVITINO_");
      boolean selects = false;
      for (JsonNode access : item.path("accesses"))
        selects |= "select".equals(access.path("type").asText());
      if (managed && selects) {
        require(
            found < 0
                && access(item, "select")
                && empty(item.path("conditions"))
                && !item.path("delegateAdmin").asBoolean()
                && !contains(item.path("roles"), OWNER),
            "Ambiguous or mixed managed SELECT item");
        found = i;
      }
    }
    return found;
  }

  private static int ordinaryItem(JsonNode policy, String role) {
    int found = -1;
    JsonNode items = policy.path("policyItems");
    for (int i = 0; i < items.size(); i++) {
      JsonNode item = items.get(i);
      if (contains(item.path("roles"), role)) {
        require(
            found < 0
                && empty(item.path("conditions"))
                && access(item, "select")
                && !item.path("delegateAdmin").asBoolean(),
            "Unexpected ordinary item");
        found = i;
      }
    }
    for (String list : List.of("denyPolicyItems", "allowExceptions", "denyExceptions")) {
      for (JsonNode item : policy.path(list))
        require(!contains(item.path("roles"), role), "Unexpected ordinary deny/exception item");
    }
    return found;
  }

  private static boolean access(JsonNode item, String type) {
    JsonNode accesses = item.path("accesses");
    return accesses.isArray()
        && accesses.size() == 1
        && type.equals(accesses.get(0).path("type").asText())
        && accesses.get(0).path("isAllowed").isBoolean()
        && accesses.get(0).path("isAllowed").booleanValue();
  }

  private static boolean empty(JsonNode value) {
    return value.isMissingNode() || value.isNull() || (value.isArray() && value.isEmpty());
  }

  private static boolean contains(JsonNode values, String value) {
    if (!values.isArray()) return false;
    for (JsonNode node : values) if (value.equals(node.asText())) return true;
    return false;
  }

  private static JsonNode table(JsonNode value) {
    requireFields(value, Set.of("name", "database", "table", "catalogId"));
    for (String field : List.of("name", "database", "table")) token(value, field);
    require(text(value, "catalogId").matches("[1-9][0-9]*"), "Catalog ID required");
    return value.deepCopy();
  }

  private static String token(JsonNode node, String field) {
    String value = text(node, field);
    require(value.matches("[A-Za-z_][A-Za-z0-9_-]*"), "Finite identifier required: " + field);
    return value;
  }

  private static String text(JsonNode node, String field) {
    JsonNode value = node.path(field);
    require(value.isTextual() && !value.textValue().isBlank(), "Required field: " + field);
    return value.textValue();
  }

  private static Set<String> fields(JsonNode node) {
    Set<String> result = new HashSet<>();
    Iterator<String> fields = node.fieldNames();
    fields.forEachRemaining(result::add);
    return result;
  }

  private static void requireFields(JsonNode node, Set<String> expected) {
    require(node.isObject() && fields(node).equals(expected), "Unexpected or missing fields");
  }

  private static void require(boolean condition, String message) {
    if (!condition) throw new IllegalArgumentException(message);
  }
}

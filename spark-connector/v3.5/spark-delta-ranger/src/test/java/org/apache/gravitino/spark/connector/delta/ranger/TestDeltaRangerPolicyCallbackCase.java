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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.client.ObjectMapperProvider;
import org.apache.gravitino.dto.responses.RoleResponse;
import org.junit.jupiter.api.Test;

/** Offline contracts for the finite ordinary-policy callback fault checkpoint. */
public class TestDeltaRangerPolicyCallbackCase {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final String ROLE = "GRAVITINO_METALAKE_7_ROLE_case_role";

  @Test
  void firstGrantAllowsAbsentRemoteRoleAndSelectsOwnedPut() throws Exception {
    DeltaRangerPolicyCallbackCase c = fixture("FIRST_GRANT");
    DeltaRangerPolicyCallbackCase.Selection selected =
        select(c, List.of(), policies(false, false), roles(false));
    assertEquals("PUT", selected.method);
    assertEquals("/service/public/v2/api/policy/31", selected.path);
    assertEquals(2, selected.after.path("policyItems").size());
  }

  @Test
  void additionalGrantRequiresSameRoleHealthyDifferentTableAndMember() throws Exception {
    DeltaRangerPolicyCallbackCase c = fixture("ADDITIONAL_GRANT");
    DeltaRangerPolicyCallbackCase.Selection selected =
        select(c, List.of(grant("a.sales.orders")), policies(true, false), roles(true));
    assertEquals("/service/public/v2/api/policy/32", selected.path);
    assertThrows(
        IllegalArgumentException.class,
        () -> select(c, List.of(), policies(true, false), roles(true)));
    assertThrows(
        IllegalArgumentException.class,
        () -> select(c, List.of(grant("a.sales.orders")), policies(false, false), roles(true)));
    assertThrows(
        IllegalArgumentException.class,
        () -> select(c, List.of(grant("a.sales.orders")), policies(true, false), roles(false)));
  }

  @Test
  void lastRevokePreservesOwnerAndEveryForeignField() throws Exception {
    DeltaRangerPolicyCallbackCase c = fixture("LAST_REVOKE");
    ArrayNode policies = policies(true, false);
    ObjectNode foreign = JSON.createObjectNode().put("opaque", "preserve");
    ((ArrayNode) policies.get(0).path("denyPolicyItems")).add(foreign);
    JsonNode before = policies.deepCopy();
    DeltaRangerPolicyCallbackCase.Selection selected =
        select(c, List.of(grant("a.sales.orders")), policies, roles(true));
    assertEquals("PUT", selected.method);
    assertEquals("/service/public/v2/api/policy/31", selected.path);
    assertEquals(before, policies);
    assertEquals(
        before.get(0).path("policyItems").get(0), selected.after.path("policyItems").get(0));
    assertEquals(before.get(0).path("denyPolicyItems"), selected.after.path("denyPolicyItems"));
    assertEquals(1, selected.after.path("policyItems").size());
    assertThrows(IllegalArgumentException.class, () -> select(c, List.of(), policies, roles(true)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            select(
                c,
                List.of(grant("a.sales.orders"), grant("b.sales.orders")),
                policies,
                roles(true)));
  }

  @Test
  void rejectsAlreadyConvergedOrOwnerMixedIntoOrdinaryItem() throws Exception {
    DeltaRangerPolicyCallbackCase first = fixture("FIRST_GRANT");
    assertThrows(
        IllegalArgumentException.class,
        () -> select(first, List.of(), policies(true, false), roles(false)));
    ArrayNode p = policies(true, false);
    ((ArrayNode) p.get(0).path("policyItems").get(1).path("roles")).add("GRAVITINO_OWNER_ROLE");
    assertThrows(
        IllegalArgumentException.class,
        () -> select(fixture("LAST_REVOKE"), List.of(grant("a.sales.orders")), p, roles(true)));
  }

  @Test
  void rejectsMissingAmbiguousForeignOrWrongResourcePolicy() throws Exception {
    for (String field :
        List.of(
            "absent",
            "duplicate",
            "service",
            "policyType",
            "policyLabels",
            "resources",
            "owner",
            "id")) {
      ArrayNode p = policies(false, false);
      ObjectNode a = (ObjectNode) p.get(0);
      switch (field) {
        case "absent":
          p.remove(0);
          break;
        case "duplicate":
          p.add(a.deepCopy());
          break;
        case "service":
          a.put("service", "foreign");
          break;
        case "policyType":
          a.put("policyType", 1);
          break;
        case "policyLabels":
          a.putArray("policyLabels").add("gravitino-managed");
          break;
        case "resources":
          ((ObjectNode) a.path("resources").path("column")).put("isRecursive", true);
          break;
        case "owner":
          a.putArray("policyItems");
          break;
        case "id":
          a.put("id", -1);
          break;
        default:
          throw new AssertionError(field);
      }
      assertThrows(
          IllegalArgumentException.class,
          () -> select(fixture("FIRST_GRANT"), List.of(), p, roles(false)),
          field);
    }
  }

  @Test
  void rejectsWrongLocalPrivilegesMembershipAndRemoteRoleShape() throws Exception {
    DeltaRangerPolicyCallbackCase c = fixture("LAST_REVOKE");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            c.select(
                "svc",
                "7",
                ROLE,
                List.of(grant("a.sales.orders")),
                List.of("visibility"),
                policies(true, false),
                roles(true)));
    SecurableObject wrong =
        SecurableObjects.parse(
            "a.sales.orders", MetadataObject.Type.TABLE, List.of(Privileges.ModifyTable.allow()));
    assertThrows(
        IllegalArgumentException.class,
        () -> select(c, List.of(wrong), policies(true, false), roles(true)));
    ArrayNode remote = roles(true);
    ((ObjectNode) remote.get(0))
        .putArray("groups")
        .add(JSON.createObjectNode().put("name", "foreign"));
    assertThrows(
        IllegalArgumentException.class,
        () -> select(c, List.of(grant("a.sales.orders")), policies(true, false), remote));
    ArrayNode duplicate = roles(true);
    duplicate.add(duplicate.get(0).deepCopy());
    assertThrows(
        IllegalArgumentException.class,
        () -> select(c, List.of(grant("a.sales.orders")), policies(true, false), duplicate));
  }

  @Test
  void closedProtocolAllowsExactlyOneMutationThenOnlySnapshotAndClose() throws Exception {
    DeltaRangerPolicyCallbackCase c = fixture("FIRST_GRANT");
    assertThrows(IllegalArgumentException.class, () -> c.begin(command("fault-policy-write")));
    c.begin(command("prepare-policy-case"));
    c.finish(true);
    assertThrows(IllegalArgumentException.class, () -> c.begin(command("prepare-policy-case")));
    c.begin(command("fault-policy-write"));
    c.finish(true);
    assertThrows(IllegalArgumentException.class, () -> c.begin(command("revoke-select")));
    c.begin(command("grant-select"));
    c.finish(false);
    assertTrue(c.quarantined());
    assertThrows(IllegalArgumentException.class, () -> c.begin(command("grant-select")));
    assertThrows(IllegalArgumentException.class, () -> c.begin(command("fault-policy-write")));
    c.begin(command("snapshot"));
    c.finish(true);
    c.begin(command("close"));
    c.finish(true);
    assertThrows(IllegalArgumentException.class, () -> c.begin(command("snapshot")));
  }

  @Test
  void setupFailureOrUnresolvedMutationNeverReplays() throws Exception {
    DeltaRangerPolicyCallbackCase c = fixture("FIRST_GRANT");
    c.begin(command("prepare-policy-case"));
    c.finish(false);
    assertThrows(IllegalArgumentException.class, () -> c.begin(command("prepare-policy-case")));
    c.begin(command("snapshot"));
    c.finish(true);
    c.begin(command("close"));
    c.finish(true);
    DeltaRangerPolicyCallbackCase inFlight = fixture("FIRST_GRANT");
    inFlight.begin(command("prepare-policy-case"));
    inFlight.finish(true);
    inFlight.begin(command("fault-policy-write"));
    inFlight.finish(true);
    inFlight.begin(command("grant-select"));
    assertThrows(IllegalArgumentException.class, () -> inFlight.begin(command("snapshot")));
    inFlight.finish(false);
    assertTrue(inFlight.quarantined());
  }

  @Test
  void commandIdentityAndFieldsCannotChooseUrlOrAnotherRole() throws Exception {
    for (String field : List.of("path", "policyId", "roleSuffix", "subject", "other")) {
      ObjectNode cmd = command("prepare-policy-case").put(field, "foreign");
      assertThrows(IllegalArgumentException.class, () -> fixture("FIRST_GRANT").begin(cmd));
    }
    assertThrows(
        IllegalArgumentException.class,
        () -> fixture("FIRST_GRANT").begin(command("prepare-policy-case").put("caseId", "other")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            fixture("FIRST_GRANT")
                .begin(command("prepare-policy-case").put("kind", "LAST_REVOKE")));
  }

  @Test
  void acceptsRangerWireModelsThatOmitEmptyCollections() throws Exception {
    ArrayNode p = policies(true, false);
    for (JsonNode policy : p) {
      ((ObjectNode) policy).remove(List.of("denyPolicyItems", "allowExceptions", "denyExceptions"));
      for (JsonNode item : policy.path("policyItems")) {
        ((ObjectNode) item).remove(List.of("users", "groups", "conditions"));
      }
    }
    ArrayNode remote = roles(true);
    ((ObjectNode) remote.get(0)).remove(List.of("groups", "roles"));
    DeltaRangerPolicyCallbackCase.Selection selected =
        select(fixture("LAST_REVOKE"), List.of(grant("a.sales.orders")), p, remote);
    assertEquals("PUT", selected.method);
    assertEquals(1, selected.after.path("policyItems").size());
    assertTrue(selected.after.path("denyPolicyItems").isMissingNode());
  }

  @Test
  void requiresExactlyOneConsumedFaultAndAnActualCompletedPublicResponse() throws Exception {
    ObjectNode callbackStart = event("PUT", "/service/public/v2/api/policy/31", 4);
    ObjectNode callbackEnd =
        callbackStart
            .deepCopy()
            .put("finished", "done")
            .put("faultStatus", 503)
            .put("upstreamAttempted", false)
            .put("downstreamResponseCompleted", true);
    ObjectNode publicStart = event("PUT", "/api/case/grant", 8);
    ObjectNode publicEnd =
        publicStart
            .deepCopy()
            .put("finished", "done")
            .put("upstreamStatus", 500)
            .put("upstreamAttempted", true)
            .put("upstreamResponseCompleted", "done")
            .put("responseBodyFile", "/private/public.body")
            .put("downstreamResponseCompleted", true);
    DeltaRangerPolicyCallbackCase.verifyMutationEvidence(
        List.of(callbackStart, callbackEnd),
        List.of(publicStart, publicEnd),
        "PUT",
        "/service/public/v2/api/policy/31",
        "/api/case/grant");
    for (String field :
        List.of("finished", "faultStatus", "upstreamAttempted", "downstreamResponseCompleted")) {
      ObjectNode missing = callbackEnd.deepCopy();
      missing.remove(field);
      assertThrows(
          IllegalArgumentException.class,
          () ->
              DeltaRangerPolicyCallbackCase.verifyMutationEvidence(
                  List.of(callbackStart, missing),
                  List.of(publicStart, publicEnd),
                  "PUT",
                  "/service/public/v2/api/policy/31",
                  "/api/case/grant"));
    }
    for (String field :
        List.of(
            "upstreamStatus",
            "upstreamResponseCompleted",
            "responseBodyFile",
            "downstreamResponseCompleted")) {
      ObjectNode missing = publicEnd.deepCopy();
      missing.remove(field);
      assertThrows(
          IllegalArgumentException.class,
          () ->
              DeltaRangerPolicyCallbackCase.verifyMutationEvidence(
                  List.of(callbackStart, callbackEnd),
                  List.of(publicStart, missing),
                  "PUT",
                  "/service/public/v2/api/policy/31",
                  "/api/case/grant"));
    }
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRangerPolicyCallbackCase.verifyMutationEvidence(
                List.of(callbackStart, callbackEnd, callbackStart.deepCopy().put("attempt", 5)),
                List.of(publicStart, publicEnd),
                "PUT",
                "/service/public/v2/api/policy/31",
                "/api/case/grant"));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRangerPolicyCallbackCase.verifyMutationEvidence(
                List.of(callbackStart, callbackEnd.deepCopy().put("run", "other")),
                List.of(publicStart, publicEnd),
                "PUT",
                "/service/public/v2/api/policy/31",
                "/api/case/grant"));
  }

  private static ObjectNode event(String method, String path, int attempt) {
    return JSON.createObjectNode()
        .put("run", "run1")
        .put("attempt", attempt)
        .put("method", method)
        .put("path", path)
        .put("started", "start");
  }

  @Test
  void sharedManagedSelectItemKeepsEveryForeignRoleAndUser() throws Exception {
    ArrayNode p = policies(true, false);
    ObjectNode shared = (ObjectNode) p.get(0).path("policyItems").get(1);
    ((ArrayNode) shared.path("roles")).add("GRAVITINO_METALAKE_7_ROLE_other");
    ((ArrayNode) shared.path("users")).add("foreign_user");
    JsonNode original = p.deepCopy();
    DeltaRangerPolicyCallbackCase.Selection revoke =
        select(fixture("LAST_REVOKE"), List.of(grant("a.sales.orders")), p, roles(true));
    assertEquals(2, revoke.after.path("policyItems").size());
    assertEquals(
        List.of("GRAVITINO_METALAKE_7_ROLE_other"),
        JSON.convertValue(revoke.after.path("policyItems").get(1).path("roles"), List.class));
    assertEquals(shared.path("users"), revoke.after.path("policyItems").get(1).path("users"));
    assertEquals(original, p);
    ArrayNode beforeGrant = policies(false, false);
    ((ArrayNode) beforeGrant.get(0).path("policyItems"))
        .add(revoke.after.path("policyItems").get(1).deepCopy());
    DeltaRangerPolicyCallbackCase.Selection grant =
        select(fixture("FIRST_GRANT"), List.of(), beforeGrant, roles(false));
    assertEquals(2, grant.after.path("policyItems").size());
    assertEquals(
        List.of("GRAVITINO_METALAKE_7_ROLE_other", ROLE),
        JSON.convertValue(grant.after.path("policyItems").get(1).path("roles"), List.class));
    assertEquals(shared.path("users"), grant.after.path("policyItems").get(1).path("users"));
  }

  @Test
  void rejectsAmbiguousOrMixedManagedSelectContributions() throws Exception {
    for (boolean mixed : List.of(false, true)) {
      ArrayNode p = policies(false, false);
      ArrayNode items = (ArrayNode) p.get(0).path("policyItems");
      ObjectNode first = item("GRAVITINO_METALAKE_7_ROLE_other", "select");
      items.add(first);
      if (mixed)
        ((ArrayNode) first.path("accesses"))
            .addObject()
            .put("type", "update")
            .put("isAllowed", true);
      else items.add(item("GRAVITINO_METALAKE_7_ROLE_other2", "select"));
      assertThrows(
          IllegalArgumentException.class,
          () -> select(fixture("FIRST_GRANT"), List.of(), p, roles(false)));
    }
  }

  @Test
  void rejectsSchemaAliasEvenWhenEveryOtherOwnedPolicyFieldMatches() throws Exception {
    ArrayNode candidates = policies(false, false);
    ObjectNode resources = (ObjectNode) candidates.get(0).path("resources");
    resources.set("schema", resources.remove("database"));
    assertThrows(
        IllegalArgumentException.class,
        () -> select(fixture("FIRST_GRANT"), List.of(), candidates, roles(false)));
  }

  @Test
  void actualOwnedPolicyWireShapeUsesDatabaseAndPreservesProtectedModels() throws Exception {
    // Sanitized recorded Ranger 2.8.0 owned policies 37/40: preserve wire keys and omitted fields.
    ArrayNode candidates = recordedOwnedPolicies();
    JsonNode original = candidates.deepCopy();
    DeltaRangerPolicyCallbackCase.Selection first =
        select(fixture("FIRST_GRANT"), List.of(), candidates, roles(false));
    assertEquals("/service/public/v2/api/policy/37", first.path);
    assertEquals(original, candidates);
    assertEquals(
        original.get(0).path("policyItems").get(0), first.after.path("policyItems").get(0));
    ((ArrayNode) candidates.get(0).path("policyItems").get(1).path("roles")).add(ROLE);
    DeltaRangerPolicyCallbackCase.Selection additional =
        select(
            fixture("ADDITIONAL_GRANT"), List.of(grant("a.sales.orders")), candidates, roles(true));
    assertEquals("/service/public/v2/api/policy/40", additional.path);
    assertEquals(
        original.get(1).path("policyItems").get(0), additional.after.path("policyItems").get(0));
    DeltaRangerPolicyCallbackCase.Selection revoke =
        select(fixture("LAST_REVOKE"), List.of(grant("a.sales.orders")), candidates, roles(true));
    assertEquals("/service/public/v2/api/policy/37", revoke.path);
    assertEquals(original.get(0), revoke.after);
  }

  @Test
  void deserializedPublicRoleSelectsAdditionalAndLastWithRecordedSharedPolicyAndRole()
      throws Exception {
    ArrayNode policies = recordedOwnedPolicies();
    JsonNode originalA = policies.get(0).deepCopy();
    ((ArrayNode) policies.get(0).path("policyItems").get(1).path("roles")).add(ROLE);
    List<SecurableObject> local = recordedLocalGrants("select_table", "allow", false);
    DeltaRangerPolicyCallbackCase.Selection additional =
        select(fixture("ADDITIONAL_GRANT"), local, policies, recordedRemoteRole());
    assertEquals("/service/public/v2/api/policy/40", additional.path);
    DeltaRangerPolicyCallbackCase.Selection revoke =
        select(fixture("LAST_REVOKE"), local, policies, recordedRemoteRole());
    assertEquals("/service/public/v2/api/policy/37", revoke.path);
    assertEquals(originalA, revoke.after);
  }

  @Test
  void deserializedPublicRoleRejectsWrongPrivilegeDenyAndMixedPrivileges() throws Exception {
    ArrayNode policies = recordedOwnedPolicies();
    ((ArrayNode) policies.get(0).path("policyItems").get(1).path("roles")).add(ROLE);
    for (String kind : List.of("ADDITIONAL_GRANT", "LAST_REVOKE")) {
      for (List<SecurableObject> local :
          List.of(
              recordedLocalGrants("modify_table", "allow", false),
              recordedLocalGrants("select_table", "deny", false),
              recordedLocalGrants("select_table", "allow", true))) {
        assertThrows(
            IllegalArgumentException.class,
            () -> select(fixture(kind), local, policies, recordedRemoteRole()));
      }
    }
  }

  private static ArrayNode recordedOwnedPolicies() throws Exception {
    return (ArrayNode)
        JSON.readTree(
            """
        [
          {
            "id": 37,
            "guid": "00000000-0000-0000-0000-00000000001",
            "isEnabled": true,
            "version": 12,
            "service": "svc",
            "name": "sanitized_managed_37",
            "policyType": 0,
            "policyPriority": 0,
            "isAuditEnabled": true,
            "resources": {
              "column": {
                "values": [
                  "*"
                ],
                "isExcludes": false,
                "isRecursive": false
              },
              "database": {
                "values": [
                  "sales"
                ],
                "isExcludes": false,
                "isRecursive": false
              },
              "catalog": {
                "values": [
                  "a"
                ],
                "isExcludes": false,
                "isRecursive": false
              },
              "table": {
                "values": [
                  "orders"
                ],
                "isExcludes": false,
                "isRecursive": false
              }
            },
            "policyItems": [
              {
                "accesses": [
                  {
                    "type": "all",
                    "isAllowed": true
                  }
                ],
                "users": [
                  "sanitized_owner"
                ],
                "roles": [
                  "GRAVITINO_OWNER_ROLE"
                ],
                "delegateAdmin": false
              },
              {
                "accesses": [
                  {
                    "type": "select",
                    "isAllowed": true
                  }
                ],
                "roles": [
                  "GRAVITINO_METALAKE_7_ROLE_foreign0",
                  "GRAVITINO_METALAKE_7_ROLE_foreign1",
                  "GRAVITINO_METALAKE_7_ROLE_foreign2",
                  "GRAVITINO_METALAKE_7_ROLE_foreign3",
                  "GRAVITINO_METALAKE_7_ROLE_foreign4"
                ],
                "delegateAdmin": false
              }
            ],
            "serviceType": "sanitized_delta_definition",
            "policyLabels": [
              "gravitino-catalog-id:11",
              "gravitino-managed",
              "gravitino-metalake-id:7"
            ],
            "isDenyAllElse": false
          },
          {
            "id": 40,
            "guid": "00000000-0000-0000-0000-00000000002",
            "isEnabled": true,
            "version": 2,
            "service": "svc",
            "name": "sanitized_managed_40",
            "policyType": 0,
            "policyPriority": 0,
            "isAuditEnabled": true,
            "resources": {
              "column": {
                "values": [
                  "*"
                ],
                "isExcludes": false,
                "isRecursive": false
              },
              "database": {
                "values": [
                  "sales"
                ],
                "isExcludes": false,
                "isRecursive": false
              },
              "catalog": {
                "values": [
                  "b"
                ],
                "isExcludes": false,
                "isRecursive": false
              },
              "table": {
                "values": [
                  "orders"
                ],
                "isExcludes": false,
                "isRecursive": false
              }
            },
            "policyItems": [
              {
                "accesses": [
                  {
                    "type": "all",
                    "isAllowed": true
                  }
                ],
                "users": [
                  "sanitized_owner"
                ],
                "roles": [
                  "GRAVITINO_OWNER_ROLE"
                ],
                "delegateAdmin": false
              },
              {
                "accesses": [
                  {
                    "type": "select",
                    "isAllowed": true
                  }
                ],
                "roles": [
                  "GRAVITINO_METALAKE_7_ROLE_foreign0"
                ],
                "delegateAdmin": false
              }
            ],
            "serviceType": "sanitized_delta_definition",
            "policyLabels": [
              "gravitino-catalog-id:12",
              "gravitino-managed",
              "gravitino-metalake-id:7"
            ],
            "isDenyAllElse": false
          }
        ]
        """);
  }

  private static List<SecurableObject> recordedLocalGrants(
      String name, String condition, boolean mixed) throws Exception {
    ObjectNode response =
        (ObjectNode)
            JSON.readTree(
                """
        {
          "code": 0,
          "role": {
            "name": "case_role",
            "audit": {
              "creator": "sanitized_admin",
              "createTime": "2026-09-27T14:49:07.144364Z"
            },
            "properties": {},
            "securableObjects": [
              {
                "type": "table",
                "privileges": [
                  {
                    "name": "select_table",
                    "condition": "allow"
                  }
                ],
                "fullName": "a.sales.orders"
              }
            ]
          }
        }
        """);
    ArrayNode privileges =
        (ArrayNode) response.path("role").path("securableObjects").get(0).path("privileges");
    ((ObjectNode) privileges.get(0)).put("name", name).put("condition", condition);
    if (mixed) privileges.addObject().put("name", "modify_table").put("condition", "allow");
    RoleResponse decoded =
        ObjectMapperProvider.objectMapper().treeToValue(response, RoleResponse.class);
    decoded.validate();
    return decoded.getRole().securableObjects();
  }

  private static ArrayNode recordedRemoteRole() throws Exception {
    return JSON.createArrayNode()
        .add(
            JSON.readTree(
                """
        {
          "id": 30,
          "isEnabled": true,
          "createdBy": "sanitized_admin",
          "updatedBy": "sanitized_admin",
          "createTime": 1790520547196,
          "updateTime": 1790520547609,
          "name": "GRAVITINO_METALAKE_7_ROLE_case_role",
          "description": "MANAGED_BY_GRAVITINO",
          "users": [
            {
              "name": "reader",
              "isAdmin": false
            }
          ]
        }
        """));
  }

  private static DeltaRangerPolicyCallbackCase fixture(String kind) throws Exception {
    ObjectNode f =
        JSON.createObjectNode()
            .put("caseId", "case1")
            .put("kind", kind)
            .put("role", "case_role")
            .put("subject", "reader")
            .put("visibilityRole", "visibility");
    f.set("tableA", table("a", "11"));
    f.set("tableB", table("b", "12"));
    return DeltaRangerPolicyCallbackCase.parse(f);
  }

  private static ObjectNode table(String catalog, String id) {
    return JSON.createObjectNode()
        .put("name", catalog)
        .put("database", "sales")
        .put("table", "orders")
        .put("catalogId", id);
  }

  private static SecurableObject grant(String name) {
    return SecurableObjects.parse(
        name, MetadataObject.Type.TABLE, List.of(Privileges.SelectTable.allow()));
  }

  private static ObjectNode command(String operation) {
    return JSON.createObjectNode()
        .put("operation", operation)
        .put("caseId", "case1")
        .put("kind", "FIRST_GRANT")
        .put("id", "step1");
  }

  private static DeltaRangerPolicyCallbackCase.Selection select(
      DeltaRangerPolicyCallbackCase c,
      List<SecurableObject> local,
      JsonNode policies,
      JsonNode roles) {
    return c.select("svc", "7", ROLE, local, List.of("visibility", "case_role"), policies, roles);
  }

  private static ArrayNode roles(boolean present) {
    ArrayNode roles = JSON.createArrayNode();
    if (present) {
      ObjectNode role = roles.addObject().put("name", ROLE).put("id", 8);
      role.putArray("users").addObject().put("name", "reader").put("isAdmin", false);
      role.putArray("groups");
      role.putArray("roles");
    }
    return roles;
  }

  private static ArrayNode policies(boolean aGrant, boolean bGrant) throws Exception {
    ArrayNode policies = JSON.createArrayNode();
    for (int i = 0; i < 2; i++) {
      ObjectNode p =
          policies.addObject().put("id", 31 + i).put("service", "svc").put("policyType", 0);
      p.putArray("policyLabels")
          .add("gravitino-managed")
          .add("gravitino-metalake-id:7")
          .add("gravitino-catalog-id:" + (11 + i));
      ObjectNode resources = p.putObject("resources");
      String[] keys = {"catalog", "database", "table", "column"};
      String[] values = {i == 0 ? "a" : "b", "sales", "orders", "*"};
      for (int k = 0; k < keys.length; k++) {
        ObjectNode resource =
            resources.putObject(keys[k]).put("isExcludes", false).put("isRecursive", false);
        resource.putArray("values").add(values[k]);
      }
      ArrayNode items = p.putArray("policyItems");
      items.add(item("GRAVITINO_OWNER_ROLE", "all"));
      if (i == 0 ? aGrant : bGrant) items.add(item(ROLE, "select"));
      p.putArray("denyPolicyItems");
      p.putArray("allowExceptions");
      p.putArray("denyExceptions");
    }
    return policies;
  }

  private static ObjectNode item(String role, String access) {
    ObjectNode item = JSON.createObjectNode().put("delegateAdmin", false);
    item.putArray("roles").add(role);
    item.putArray("users");
    item.putArray("groups");
    item.putArray("conditions");
    item.putArray("accesses").addObject().put("type", access).put("isAllowed", true);
    return item;
  }
}

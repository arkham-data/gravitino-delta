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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestDeltaRangerDownloadedQuery {
  @TempDir Path directory;

  @Test
  void retainedCommandLimitRejectsBeforeTheNextFilename() {
    assertEquals(4096, DeltaRangerDownloadedQuery.nextSequence(4095));
    assertThrows(IllegalStateException.class, () -> DeltaRangerDownloadedQuery.nextSequence(4096));
  }

  @Test
  void retainedActionEvidenceRecursivelyFreezesNestedCollections() {
    List<Object> row = new ArrayList<>();
    row.add("value");
    List<Object> rows = new ArrayList<>();
    rows.add(row);
    Map<String, Object> cause = new LinkedHashMap<>();
    cause.put("stack", new ArrayList<>(List.of("frame")));
    Map<String, Object> evidence = new LinkedHashMap<>();
    evidence.put("rows", rows);
    evidence.put("failureDetails", new ArrayList<>(List.of(cause)));
    evidence.put("userScanOutputRows", new ArrayList<>(List.of(1L)));
    DeltaRangerRetainedAction.Result result =
        new DeltaRangerRetainedAction.Result(true, evidence, null);
    row.add("late");
    cause.put("message", "late");
    assertEquals(1, ((List<?>) ((List<?>) result.evidence().get("rows")).get(0)).size());
    assertFalse(
        ((Map<?, ?>) ((List<?>) result.evidence().get("failureDetails")).get(0))
            .containsKey("message"));
    assertThrows(
        UnsupportedOperationException.class,
        () -> ((List<Object>) result.evidence().get("userScanOutputRows")).add(2L));
    assertThrows(
        UnsupportedOperationException.class,
        () -> ((List<Object>) result.evidence().get("rows")).clear());
  }

  @Test
  void effectiveRequirementIncludesEveryAdvancedGeneration() throws Exception {
    Constructor<DeltaRangerPolicyEngine.Versions> constructor =
        DeltaRangerPolicyEngine.Versions.class.getDeclaredConstructor(
            String.class, String.class, long.class, long.class);
    constructor.setAccessible(true);
    DeltaRangerPolicyEngine.Versions before =
        constructor.newInstance("owned", "definition", 1L, 1L);
    DeltaRangerPolicyEngine.Versions policy =
        constructor.newInstance("owned", "definition", 2L, 1L);
    DeltaRangerPolicyEngine.Versions both = constructor.newInstance("owned", "definition", 2L, 2L);
    assertEquals(
        DeltaRangerDownloadedSecurity.DownloadRequirement.NONE,
        DeltaRangerDownloadedQuery.effective(
            DeltaRangerDownloadedSecurity.DownloadRequirement.NONE, before, before));
    assertEquals(
        DeltaRangerDownloadedSecurity.DownloadRequirement.POLICY,
        DeltaRangerDownloadedQuery.effective(
            DeltaRangerDownloadedSecurity.DownloadRequirement.POLICY, before, policy));
    assertEquals(
        DeltaRangerDownloadedSecurity.DownloadRequirement.BOTH,
        DeltaRangerDownloadedQuery.effective(
            DeltaRangerDownloadedSecurity.DownloadRequirement.POLICY, before, both));
    assertThrows(
        IllegalStateException.class,
        () ->
            DeltaRangerDownloadedQuery.effective(
                DeltaRangerDownloadedSecurity.DownloadRequirement.NONE, before, policy));
    assertThrows(
        IllegalStateException.class,
        () ->
            DeltaRangerDownloadedQuery.effective(
                DeltaRangerDownloadedSecurity.DownloadRequirement.ROLE, before, policy));
  }

  @Test
  void strictCommandsPreserveCloseAndReserveSafeOutputNames() {
    DeltaRangerDownloadedCommand.Ledger ledger = new DeltaRangerDownloadedCommand.Ledger();
    DeltaRangerDownloadedCommand.Parsed close =
        DeltaRangerDownloadedCommand.parse("{\"close\":true}", ledger);
    assertEquals(DeltaRangerDownloadedCommand.Kind.CLOSE, close.kind());
    assertEquals(null, close.id());
    assertThrows(IllegalStateException.class, close::retentionId);
    assertEquals(
        DeltaRangerDownloadedCommand.Kind.PREPARE,
        DeltaRangerDownloadedCommand.parse(
                "{\"id\":\"p1\",\"operation\":\"prepare\",\"retentionId\":\"held\",\"sql\":\"SELECT secret FROM a.db.t\",\"minPolicy\":1,\"minRole\":1}",
                ledger)
            .kind());
    assertEquals(
        DeltaRangerDownloadedCommand.Kind.COLLECT,
        DeltaRangerDownloadedCommand.parse(
                "{\"id\":\"c1\",\"operation\":\"collect\",\"retentionId\":\"held\",\"action\":\"collectAsList\",\"deniedAtEntry\":true,\"expectedFailureContains\":\"denied\",\"minPolicy\":1,\"minRole\":1,\"requiredDownloads\":\"both\"}",
                ledger)
            .kind());
    for (String invalid :
        List.of(
            "{\"close\":false}",
            "{\"close\":true,\"id\":\"x\"}",
            "{\"close\":true,\"close\":true}",
            "{\"close\":true} {}",
            "{\"id\":\"../bad\",\"sql\":\"select 1\",\"minPolicy\":1,\"minRole\":1,\"expectedRows\":[]}",
            "{\"id\":\"ready\",\"sql\":\"select 1\",\"minPolicy\":1,\"minRole\":1,\"expectedRows\":[]}",
            "{\"id\":\"x-passed\",\"sql\":\"select 1\",\"minPolicy\":1,\"minRole\":1,\"expectedRows\":[]}",
            "{\"id\":null,\"sql\":\"select 1\",\"minPolicy\":1,\"minRole\":1,\"expectedRows\":[]}",
            "{\"id\":\"missing\",\"sql\":\"select 1\",\"minPolicy\":1,\"expectedRows\":[]}",
            "{\"id\":\"extra\",\"sql\":\"select 1\",\"minPolicy\":1,\"minRole\":1,\"expectedRows\":[],\"surprise\":1}",
            "{\"id\":\"dupe\",\"id\":\"dupe\",\"sql\":\"select 1\",\"minPolicy\":1,\"minRole\":1,\"expectedRows\":[]}",
            "{\"id\":\"bad\",\"operation\":\"collect\",\"retentionId\":\"held\",\"action\":\"count\",\"expectedRows\":[],\"minPolicy\":1,\"minRole\":1,\"requiredDownloads\":\"none\"}")) {
      assertThrows(
          IllegalArgumentException.class,
          () -> DeltaRangerDownloadedCommand.parse(invalid, ledger));
    }
    assertEquals(
        DeltaRangerDownloadedCommand.Kind.FRESH,
        DeltaRangerDownloadedCommand.parse(
                "{\"id\":\"x\",\"sql\":\"select 1\",\"minPolicy\":1,\"minRole\":1,\"expectedRows\":[]}",
                ledger)
            .kind());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRangerDownloadedCommand.parse(
                "{\"id\":\"x\",\"sql\":\"select 1\",\"minPolicy\":1,\"minRole\":1,\"denied\":true}",
                ledger));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRangerDownloadedCommand.parse(
                "{\"id\":\"x-passed\",\"sql\":\"select 1\",\"minPolicy\":1,\"minRole\":1,\"expectedRows\":[]}",
                ledger));
  }

  @Test
  void acceptsOnlyActualAuthorizeDenialIncludingWrappedFailure() throws Exception {
    try (TestDeltaRangerNativeSelect.Fixture fixture = new TestDeltaRangerNativeSelect.Fixture()) {
      fixture.denied.add("a:secret");
      DeltaRangerQuerySubject subject =
          DeltaRangerQuerySubject.current(TestDeltaRangerQuerySubject.session(null, "simple"));
      DeltaRangerResourceKey key =
          new DeltaRangerResourceKey("a", "db", "t", "secret", "select", null);
      SecurityException denied =
          assertThrows(SecurityException.class, () -> fixture.engine.authorize(subject, key));
      assertEquals("Delta Ranger access denied", denied.getMessage());
      assertTrue(classify(denied));
      assertTrue(classify(new RuntimeException("query wrapper", denied)));
      Method details =
          DeltaRangerDownloadedQuery.class.getDeclaredMethod("failureDetails", Throwable.class);
      details.setAccessible(true);
      List<?> recorded = (List<?>) details.invoke(null, denied);
      Map<?, ?> cause = (Map<?, ?>) recorded.get(0);
      assertEquals("Delta Ranger access denied", cause.get("message"));
      assertTrue(
          ((List<?>) cause.get("stack"))
              .get(0)
              .toString()
              .contains("DeltaRangerPolicyEngine.authorize"));
      assertFalse(classify(new SecurityException("Delta Ranger access denied")));
      assertFalse(classify(new SecurityException("Unqualified Delta Ranger route: Aggregate")));
      assertFalse(classify(new IllegalStateException("metadata setup failed")));
      assertFalse(classify(null));
    }
  }

  @Test
  void actualReadinessAndEvaluationFailuresAreNotPolicyDenials() throws Exception {
    try (TestDeltaRangerNativeSelect.Fixture fixture = new TestDeltaRangerNativeSelect.Fixture()) {
      DeltaRangerQuerySubject subject =
          DeltaRangerQuerySubject.current(TestDeltaRangerQuerySubject.session(null, "simple"));
      DeltaRangerResourceKey key =
          new DeltaRangerResourceKey("a", "db", "t", "secret", "select", null);
      when(fixture.plugin.isAccessAllowed(any(RangerAccessRequest.class)))
          .thenThrow(new IllegalStateException("evaluation failed"));
      SecurityException evaluation =
          assertThrows(SecurityException.class, () -> fixture.engine.authorize(subject, key));
      assertFalse(classify(evaluation));
      when(fixture.plugin.getPoliciesVersion()).thenReturn(-1L);
      SecurityException readiness =
          assertThrows(SecurityException.class, fixture.engine::requireReady);
      assertFalse(classify(readiness));
    }
  }

  @Test
  void privateDiagnosticsRetainEveryCauseAndOriginIncludingExpectedDenial() throws Exception {
    Method details =
        DeltaRangerDownloadedQuery.class.getDeclaredMethod("failureDetails", Throwable.class);
    details.setAccessible(true);
    var failure =
        new RuntimeException("outer diagnostic", new SecurityException("inner diagnostic"));
    List<?> causes = (List<?>) details.invoke(null, failure);
    assertEquals(2, causes.size());
    assertEquals("outer diagnostic", ((Map<?, ?>) causes.get(0)).get("message"));
    assertEquals("inner diagnostic", ((Map<?, ?>) causes.get(1)).get("message"));
    assertFalse(((List<?>) ((Map<?, ?>) causes.get(1)).get("stack")).isEmpty());
    assertTrue(((List<?>) details.invoke(null, new Object[] {null})).isEmpty());
    Method write =
        DeltaRangerDownloadedQuery.class.getDeclaredMethod("write", Path.class, Object.class);
    write.setAccessible(true);
    Path path = directory.resolve("private-query.json");
    write.invoke(null, path, causes);
    assertEquals(PosixFilePermissions.fromString("rw-------"), Files.getPosixFilePermissions(path));
    assertTrue(Files.readString(path).contains("inner diagnostic"));
  }

  private static boolean classify(Throwable failure) throws Exception {
    Method method =
        DeltaRangerDownloadedQuery.class.getDeclaredMethod("isSecurityFailure", Throwable.class);
    method.setAccessible(true);
    return (boolean) method.invoke(null, failure);
  }
}

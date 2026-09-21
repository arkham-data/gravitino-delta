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

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Map;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestDeltaRangerDownloadedQuery {
  @TempDir Path directory;

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

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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.HashSet;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

class TestDeltaRangerQuerySubject {
  @Test
  void currentAuthenticatedUgiKeepsActualGroupsAndImmutableSnapshot() throws Exception {
    UserGroupInformation ugi =
        UserGroupInformation.createUserForTesting(
            "query-reader", new String[] {"finance", "analysts"});
    DeltaRangerQuerySubject subject =
        ugi.doAs(
            (PrivilegedExceptionAction<DeltaRangerQuerySubject>)
                () -> DeltaRangerQuerySubject.current(session(null, "simple")));
    assertEquals("query-reader", subject.user());
    assertEquals(new HashSet<>(Arrays.asList("finance", "analysts")), subject.groups());
    assertThrows(UnsupportedOperationException.class, () -> subject.groups().add("admin"));
  }

  @Test
  void arbitrarySessionUserCannotReplaceAuthenticatedUgi() throws Exception {
    UserGroupInformation ugi =
        UserGroupInformation.createUserForTesting("query-reader", new String[] {"analysts"});
    ugi.doAs(
        (PrivilegedExceptionAction<Void>)
            () -> {
              assertThrows(
                  SecurityException.class,
                  () -> DeltaRangerQuerySubject.current(session("metadata-service", "simple")));
              return null;
            });
  }

  @Test
  void tokenCacheIdentityCannotEstablishAuthenticatedSubject() {
    assertThrows(
        SecurityException.class, () -> DeltaRangerQuerySubject.current(session(null, "token")));
  }

  @Test
  void missingSessionRejects() {
    assertThrows(SecurityException.class, () -> DeltaRangerQuerySubject.current(null));
  }

  static SparkSession session(String localUser, String mode) {
    SparkSession session = mock(SparkSession.class);
    SparkContext context = mock(SparkContext.class);
    SparkConf conf = new SparkConf(false).set(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, mode);
    when(session.sparkContext()).thenReturn(context);
    when(context.getConf()).thenReturn(conf);
    when(context.getLocalProperty("kyuubi.session.user")).thenReturn(localUser);
    RuntimeConfig runtime = mock(RuntimeConfig.class);
    when(session.conf()).thenReturn(runtime);
    when(runtime.get(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, mode)).thenReturn(mode);
    return session;
  }
}

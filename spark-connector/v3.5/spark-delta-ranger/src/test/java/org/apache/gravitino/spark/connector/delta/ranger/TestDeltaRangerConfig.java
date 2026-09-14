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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestDeltaRangerConfig {
  @TempDir Path directory;

  @Test
  void missingOrContradictoryServiceBindingRejects() throws Exception {
    Configuration conf = configuration(directory);
    assertThrows(
        IllegalArgumentException.class,
        () -> new DeltaRangerConfig("other", "delta", "lake", conf));
    assertThrows(
        IllegalArgumentException.class, () -> new DeltaRangerConfig("service", "", "lake", conf));
    conf.unset("ranger.plugin.spark.service.name");
    assertThrows(
        IllegalArgumentException.class,
        () -> new DeltaRangerConfig("service", "delta", "lake", conf));
  }

  @Test
  void incrementalRangerPolicyDownloadsRejectWithEitherEngineUpdateStrategy() throws Exception {
    Configuration conf = configuration(directory);
    conf.setBoolean("fixture.policy.deltas", true);
    conf.set("ranger.plugin.spark.supports.policy.deltas", "${fixture.policy.deltas}");
    for (boolean inPlace : new boolean[] {false, true}) {
      conf.setBoolean("ranger.plugin.spark.supports.in.place.policy.updates", inPlace);
      IllegalArgumentException failure =
          assertThrows(
              IllegalArgumentException.class,
              () -> new DeltaRangerConfig("service", "delta", "lake", conf));
      assertTrue(failure.getMessage().contains("Full Ranger policy snapshots required"));
    }
  }

  @Test
  void defaultAndExplicitFullRangerPolicySnapshotsRemainSupported() throws Exception {
    Configuration conf = configuration(directory);
    assertDoesNotThrow(() -> new DeltaRangerConfig("service", "delta", "lake", conf));
    conf.setBoolean("ranger.plugin.spark.supports.policy.deltas", false);
    // In-place policy updates cannot activate without incremental downloads.
    conf.setBoolean("ranger.plugin.spark.supports.in.place.policy.updates", true);
    assertDoesNotThrow(() -> new DeltaRangerConfig("service", "delta", "lake", conf));
  }

  @Test
  void cacheMustBePrivateAndGroupOverrideIsUnqualified() throws Exception {
    Configuration conf = configuration(directory);
    Files.setPosixFilePermissions(directory, PosixFilePermissions.fromString("rwxr-x---"));
    assertThrows(
        IllegalArgumentException.class,
        () -> new DeltaRangerConfig("service", "delta", "lake", conf));
    Files.setPosixFilePermissions(directory, PosixFilePermissions.fromString("rwx------"));
    conf.setBoolean("ranger.plugin.spark.use.usergroups.from.userstore.enabled", true);
    assertThrows(
        IllegalArgumentException.class,
        () -> new DeltaRangerConfig("service", "delta", "lake", conf));
  }

  @Test
  void leasesCompareCompleteImmutableConfigurationWithoutExposingSecrets() throws Exception {
    Configuration conf = configuration(directory);
    conf.set("ranger.plugin.spark.policy.rest.client.password", "private-value");
    DeltaRangerConfig first = new DeltaRangerConfig("service", "delta", "lake", conf);
    assertEquals(first, new DeltaRangerConfig("service", "delta", "lake", conf));
    conf.set("ranger.plugin.spark.policy.rest.url", "http://localhost:1235");
    assertNotEquals(first, new DeltaRangerConfig("service", "delta", "lake", conf));
    assertFalse(first.toString().contains("private-value"));
  }

  @Test
  void basicDownloaderSecretsRequireSafeAuditFactoryLoggingBeforeInitialization() throws Exception {
    Configuration conf = configuration(directory);
    conf.set("ranger.plugin.spark.policy.rest.client.password", "fixture-only-secret");
    String logger = "org.apache.ranger.audit.provider.AuditProviderFactory";
    Level previous = LogManager.getLogger(logger).getLevel();
    try {
      Configurator.setLevel(logger, Level.INFO);
      assertThrows(IllegalArgumentException.class, DeltaRangerConfig::requireSafeLogging);
      Configurator.setLevel(logger, Level.WARN);
      DeltaRangerConfig.requireSafeLogging();
    } finally {
      Configurator.setLevel(logger, previous);
    }
  }

  static Configuration configuration(Path directory) throws Exception {
    Files.setPosixFilePermissions(directory, PosixFilePermissions.fromString("rwx------"));
    Configuration conf = new Configuration(false);
    conf.set("ranger.plugin.spark.service.name", "service");
    conf.set("ranger.plugin.spark.policy.rest.url", "http://localhost:1234");
    conf.set("ranger.plugin.spark.policy.cache.dir", directory.toAbsolutePath().toString());
    return conf;
  }
}

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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.slf4j.LoggerFactory;

/** Immutable trusted deployment binding; never construct from query or catalog user options. */
public final class DeltaRangerConfig {
  private static final String PREFIX = "ranger.plugin.spark.";
  final String service;
  final String definition;
  final String metalake;
  private final byte[] fingerprint;

  /**
   * Snapshots trusted driver configuration without retaining credential maps.
   *
   * @param service provisioned Ranger service name
   * @param definition provisioned four-level service definition name
   * @param metalake canonical metalake bound to that service
   * @param configuration trusted effective Ranger configuration, including private cache path
   */
  public DeltaRangerConfig(
      String service, String definition, String metalake, Configuration configuration) {
    this.service = DeltaRangerResourceKey.literal(service);
    this.definition = DeltaRangerResourceKey.literal(definition);
    this.metalake = DeltaRangerResourceKey.literal(metalake);
    require(
        configuration != null && service.equals(configuration.get(PREFIX + "service.name")),
        "Ranger service configuration mismatch");
    require(
        configuration.get(PREFIX + "policy.rest.url") != null
            && !configuration.get(PREFIX + "policy.rest.url").trim().isEmpty(),
        "Ranger Admin download endpoint required");
    require(
        !configuration.getBoolean(PREFIX + "supports.policy.deltas", false),
        "Full Ranger policy snapshots required; ranger.plugin.spark.supports.policy.deltas must be false");
    for (String option :
        Arrays.asList(
            "use.usergroups.from.userstore.enabled",
            "use.rangerGroups",
            "use.only.rangerGroups",
            "convert.emailToUser")) {
      require(
          !configuration.getBoolean(PREFIX + option, false),
          "Query UGI group and identity overrides are not qualified");
    }
    String directory = configuration.get(PREFIX + "policy.cache.dir");
    require(directory != null, "Private Ranger policy cache required");
    privateDirectory(Paths.get(directory));
    fingerprint = fingerprint(configuration);
  }

  /**
   * Reads ordinary ranger-spark-security.xml resources before the Kyuubi singleton initializes. The
   * driver must place these resources on its startup classpath and use Ranger's normal private
   * credential mechanisms. Never supply Ranger Admin credentials to queries.
   *
   * @param service provisioned service
   * @param definition expected downloaded definition
   * @param metalake canonical metalake
   * @return trusted immutable binding
   */
  public static DeltaRangerConfig fromClasspath(
      String service, String definition, String metalake) {
    return new DeltaRangerConfig(
        service,
        definition,
        metalake,
        new RangerPluginConfig("spark", null, "sparkSql", null, null, null));
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof DeltaRangerConfig)) {
      return false;
    }
    DeltaRangerConfig that = (DeltaRangerConfig) other;
    return service.equals(that.service)
        && definition.equals(that.definition)
        && metalake.equals(that.metalake)
        && Arrays.equals(fingerprint, that.fingerprint);
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hash(service, definition, metalake, Arrays.hashCode(fingerprint));
  }

  /**
   * @return only the nonsecret binding; credentials and configuration maps are never rendered
   */
  @Override
  public String toString() {
    return "DeltaRangerConfig{service="
        + service
        + ", definition="
        + definition
        + ", metalake="
        + metalake
        + "}";
  }

  static void requireSafeLogging() {
    require(
        !LoggerFactory.getLogger("org.apache.ranger.audit.provider.AuditProviderFactory")
            .isInfoEnabled(),
        "Configure org.apache.ranger.audit.provider.AuditProviderFactory at WARN or stricter before starting the driver; Ranger INFO logging dumps credential-bearing properties");
  }

  void requireCompatible(Configuration configuration) {
    require(
        Arrays.equals(fingerprint, fingerprint(configuration)),
        "Effective Ranger configuration differs from trusted lease configuration");
  }

  static void require(boolean condition, String message) {
    if (!condition) {
      throw new IllegalArgumentException(message);
    }
  }

  private static byte[] fingerprint(Configuration configuration) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      Map<String, String> entries = new TreeMap<>();
      configuration.forEach(entry -> entries.put(entry.getKey(), entry.getValue()));
      entries.forEach(
          (key, value) -> {
            digest.update(key.getBytes(StandardCharsets.UTF_8));
            digest.update((byte) 0);
            digest.update(value.getBytes(StandardCharsets.UTF_8));
            digest.update((byte) 0);
          });
      return digest.digest();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 unavailable", e);
    }
  }

  private static void privateDirectory(Path directory) {
    require(
        directory.isAbsolute() && Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS),
        "Private absolute Ranger cache directory must exist and cannot be a symbolic link");
    try {
      Set<PosixFilePermission> permissions =
          Files.getPosixFilePermissions(directory, LinkOption.NOFOLLOW_LINKS);
      require(
          Collections.disjoint(
              permissions,
              Arrays.asList(
                  PosixFilePermission.GROUP_READ,
                  PosixFilePermission.GROUP_WRITE,
                  PosixFilePermission.GROUP_EXECUTE,
                  PosixFilePermission.OTHERS_READ,
                  PosixFilePermission.OTHERS_WRITE,
                  PosixFilePermission.OTHERS_EXECUTE)),
          "Ranger cache must be accessible only to its owner");
      require(
          Files.isReadable(directory) && Files.isWritable(directory),
          "Ranger cache is not readable and writable");
    } catch (IOException | UnsupportedOperationException e) {
      throw new IllegalArgumentException("Cannot verify private Ranger cache permissions", e);
    }
  }
}

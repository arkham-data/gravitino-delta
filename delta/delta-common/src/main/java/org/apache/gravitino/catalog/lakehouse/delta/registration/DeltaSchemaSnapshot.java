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

package org.apache.gravitino.catalog.lakehouse.delta.registration;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Bounded detached caller-supplied snapshot observation, not proof of native validation. The
 * production metadata reader constructs this value from its exact pinned Kernel reads.
 */
public final class DeltaSchemaSnapshot {
  private final long version;
  private final String nativeId;
  private final String rawSchemaJson;
  private final Map<String, String> configuration;
  private final DeltaProtocolDescriptor protocol;

  private DeltaSchemaSnapshot(
      long version,
      String nativeId,
      String rawSchemaJson,
      Map<String, String> configuration,
      DeltaProtocolDescriptor protocol) {
    if (version < 0 || protocol == null) {
      throw new IllegalArgumentException("INVALID_SNAPSHOT_CONTEXT");
    }
    DeltaJson.requiredText(nativeId);
    DeltaJson.schema(rawSchemaJson);
    this.version = version;
    this.nativeId = nativeId;
    this.rawSchemaJson = rawSchemaJson;
    this.configuration = copyConfiguration(configuration);
    this.protocol = protocol;
  }

  /**
   * Copies a complete bounded observation without interpreting its schema feature policy.
   *
   * @param version nonnegative observed version
   * @param nativeId nonblank native table identity
   * @param rawSchemaJson raw observed schema object, at most 128 KiB UTF-8
   * @param configuration observed configuration, copied and never logged
   * @param protocol detached effective protocol
   * @return immutable snapshot context
   * @throws IllegalArgumentException for malformed, null or oversized input
   */
  public static DeltaSchemaSnapshot of(
      long version,
      String nativeId,
      String rawSchemaJson,
      Map<String, String> configuration,
      DeltaProtocolDescriptor protocol) {
    return new DeltaSchemaSnapshot(version, nativeId, rawSchemaJson, configuration, protocol);
  }

  /**
   * @return observed native version
   */
  public long version() {
    return version;
  }

  /**
   * @return observed native table identity
   */
  public String nativeId() {
    return nativeId;
  }

  /**
   * @return unchanged observed schema JSON
   */
  public String rawSchemaJson() {
    return rawSchemaJson;
  }

  /**
   * @return immutable bounded configuration; may contain sensitive values
   */
  public Map<String, String> configuration() {
    return configuration;
  }

  /**
   * @return detached effective protocol
   */
  public DeltaProtocolDescriptor protocol() {
    return protocol;
  }

  static Map<String, String> copyConfiguration(Map<String, String> values) {
    if (values == null || values.size() > 4096) {
      throw new IllegalArgumentException("CONFIGURATION_LIMIT");
    }
    Map<String, String> copy = new LinkedHashMap<>();
    long total = 0;
    for (Map.Entry<String, String> entry : values.entrySet()) {
      DeltaJson.boundedText(entry.getKey(), 16384);
      DeltaJson.boundedText(entry.getValue(), 1048576);
      total += entry.getKey().getBytes(StandardCharsets.UTF_8).length;
      total += entry.getValue().getBytes(StandardCharsets.UTF_8).length;
      if (total > 16777216) {
        throw new IllegalArgumentException("CONFIGURATION_LIMIT");
      }
      copy.put(entry.getKey(), entry.getValue());
    }
    return Collections.unmodifiableMap(copy);
  }
}

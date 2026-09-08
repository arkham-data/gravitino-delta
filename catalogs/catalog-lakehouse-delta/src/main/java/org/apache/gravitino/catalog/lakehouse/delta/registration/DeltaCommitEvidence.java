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

import java.util.Optional;
import java.util.OptionalLong;

/**
 * Immutable action-correlation result produced by {@link DeltaCommitActionReader}.
 *
 * <p>MATCHED is conditional on the reader's trusted input preconditions. It proves neither actor
 * authorization nor independent base lineage, and is not a publication decision. AMBIGUOUS exposes
 * no partial native observation. Only this package can construct results.
 */
public final class DeltaCommitEvidence {
  /** The only outcomes of action correlation; absence of a commit is never inferred. */
  public enum Kind {
    /** Every bounded correlation check passed after consuming the stream. */
    MATCHED,
    /** Required evidence was missing, inconsistent, unsupported or unreadable. */
    AMBIGUOUS
  }

  private final Kind kind;
  private final String reason;
  private final OptionalLong version;
  private final Optional<String> uuid;
  private final Optional<String> schemaJson;

  private DeltaCommitEvidence(
      Kind kind,
      String reason,
      OptionalLong version,
      Optional<String> uuid,
      Optional<String> schemaJson) {
    this.kind = kind;
    this.reason = reason;
    this.version = version;
    this.uuid = uuid;
    this.schemaJson = schemaJson;
  }

  /**
   * @return MATCHED or AMBIGUOUS
   */
  public Kind kind() {
    return kind;
  }

  /**
   * @return stable bounded diagnostic code without input payloads or exception messages
   */
  public String reason() {
    return reason;
  }

  /**
   * @return observed candidate version only for MATCHED
   */
  public OptionalLong version() {
    return version;
  }

  /**
   * @return actual metadata-action UUID only for MATCHED
   */
  public Optional<String> uuid() {
    return uuid;
  }

  /**
   * @return actual metadata schema with sorted object keys only for MATCHED
   */
  public Optional<String> schemaJson() {
    return schemaJson;
  }

  static DeltaCommitEvidence matched(long version, String uuid, String schemaJson) {
    return new DeltaCommitEvidence(
        Kind.MATCHED,
        "MATCHED",
        OptionalLong.of(version),
        Optional.of(uuid),
        Optional.of(schemaJson));
  }

  static DeltaCommitEvidence ambiguous(String reason) {
    return new DeltaCommitEvidence(
        Kind.AMBIGUOUS, reason, OptionalLong.empty(), Optional.empty(), Optional.empty());
  }
}

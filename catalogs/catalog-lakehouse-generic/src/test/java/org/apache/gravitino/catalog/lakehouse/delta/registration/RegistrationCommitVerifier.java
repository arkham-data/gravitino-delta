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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Column;

/** Read-only evidence boundary: only a trusted verifier can assert operation correlation. */
interface RegistrationCommitVerifier {
  /**
   * Inspects the stored location outside any entity update callback.
   *
   * @param storedReservation immutable snapshot to correlate
   * @param candidateVersion optional search hint, or -1
   * @return a verdict distinct from diagnostic native observations
   */
  Result verify(TableEntity storedReservation, long candidateVersion);

  /** Correlation verdicts; observation alone never implies MATCHED. */
  enum Kind {
    MATCHED,
    NOT_COMMITTED,
    AMBIGUOUS
  }

  /** Immutable native observation; public Kernel APIs may not expose a native UUID. */
  final class Observation {
    private final long nativeVersion;
    @Nullable private final String nativeUuid;
    private final String schemaFingerprint;
    private final List<Column> columns;

    Observation(
        long nativeVersion,
        @Nullable String nativeUuid,
        String schemaFingerprint,
        List<Column> columns) {
      if (nativeVersion < 0) {
        throw new IllegalArgumentException("Native version must be nonnegative");
      }
      this.nativeVersion = nativeVersion;
      this.nativeUuid = nativeUuid;
      this.schemaFingerprint = Objects.requireNonNull(schemaFingerprint);
      List<Column> copy = new ArrayList<>();
      for (Column column : columns) {
        copy.add(
            Column.of(
                column.name(),
                column.dataType(),
                column.comment(),
                column.nullable(),
                column.autoIncrement(),
                column.defaultValue()));
      }
      this.columns = Collections.unmodifiableList(copy);
    }

    long nativeVersion() {
      return nativeVersion;
    }

    @Nullable
    String nativeUuid() {
      return nativeUuid;
    }

    String schemaFingerprint() {
      return schemaFingerprint;
    }

    List<Column> columns() {
      return columns;
    }
  }

  /**
   * Immutable result. AMBIGUOUS may retain useful parsed metadata without authorizing publication.
   */
  final class Result {
    private final Kind kind;
    @Nullable private final Observation observation;
    private final String reason;

    private Result(Kind kind, @Nullable Observation observation, String reason) {
      this.kind = kind;
      this.observation = observation;
      this.reason = Objects.requireNonNull(reason);
      if (reason.trim().isEmpty()) {
        throw new IllegalArgumentException("Evidence needs a reason");
      }
    }

    static Result matched(Observation observation, String reason) {
      if (observation == null
          || observation.nativeUuid() == null
          || observation.nativeUuid().isEmpty()) {
        throw new IllegalArgumentException("MATCHED requires native identity and observation");
      }
      return new Result(Kind.MATCHED, observation, reason);
    }

    static Result notCommitted(String reason) {
      return new Result(Kind.NOT_COMMITTED, null, reason);
    }

    static Result ambiguous(String reason, @Nullable Observation observation) {
      return new Result(Kind.AMBIGUOUS, observation, reason);
    }

    Kind kind() {
      return kind;
    }

    @Nullable
    Observation observation() {
      return observation;
    }

    String reason() {
      return reason;
    }
  }
}

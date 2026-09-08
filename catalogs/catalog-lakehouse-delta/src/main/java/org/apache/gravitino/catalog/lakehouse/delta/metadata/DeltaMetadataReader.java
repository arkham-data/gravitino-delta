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

package org.apache.gravitino.catalog.lakehouse.delta.metadata;

import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import java.util.Objects;
import java.util.OptionalLong;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaCommitActionReader;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaCommitEvidence;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaOperationMarker;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/** Eager, checkpoint-aware native metadata reads with per-instance cached filesystem ownership. */
public final class DeltaMetadataReader implements AutoCloseable {
  private final DeltaStorageContext storage;

  /**
   * Creates an isolated reader from trusted server storage settings.
   *
   * @param trustedConfiguration server-owned settings, copied defensively; never arbitrary caller
   *     properties
   * @throws DeltaMetadataException if the actual Kernel runtime is unsupported
   */
  public DeltaMetadataReader(Configuration trustedConfiguration) {
    DeltaKernel33MetadataAdapter.validateRuntime();
    storage = new DeltaStorageContext(trustedConfiguration);
  }

  /**
   * Reads the latest native snapshot through Kernel's checkpoint discovery and protocol validation.
   *
   * @param normalizedLocation trusted normalized table location
   * @return detached immutable native metadata
   * @throws DeltaMetadataException if the snapshot cannot be observed or the reader is closed
   */
  public DeltaSnapshotMetadata latest(String normalizedLocation) {
    return storage.read(
        "LATEST",
        normalizedLocation,
        (engine, configuration) ->
            DeltaKernel33MetadataAdapter.read(engine, normalizedLocation, OptionalLong.empty()));
  }

  /**
   * Reads a retained snapshot at exactly the requested native version.
   *
   * @param normalizedLocation trusted normalized table location
   * @param version nonnegative native version
   * @return detached immutable native metadata
   * @throws DeltaMetadataException if history is unavailable, the version is invalid, or the reader
   *     is closed
   */
  public DeltaSnapshotMetadata atVersion(String normalizedLocation, long version) {
    return storage.read(
        "AT_VERSION",
        normalizedLocation,
        (engine, configuration) -> {
          if (version < 0) {
            throw new DeltaMetadataException("INVALID_VERSION", "AT_VERSION");
          }
          return DeltaKernel33MetadataAdapter.read(
              engine, normalizedLocation, OptionalLong.of(version));
        });
  }

  /**
   * Correlates the exact native CREATE or REPLACE commit after validating snapshot and base
   * lineage. No storage write, authorization decision, or registration-state transition occurs
   * here.
   *
   * @param expected trusted durable reservation marker containing the authorized stored location
   * @return shared action-correlation evidence, including AMBIGUOUS for contradictory commit
   *     actions
   * @throws DeltaMetadataException if native evidence is unavailable or base lineage is
   *     inconsistent
   * @throws NullPointerException if the marker is null
   */
  public DeltaCommitEvidence verify(DeltaOperationMarker expected) {
    Objects.requireNonNull(expected, "expected");
    return storage.read(
        "VERIFY",
        expected.normalizedLocation(),
        (engine, configuration) -> {
          boolean replace = expected.operation() == DeltaOperationMarker.Operation.REPLACE;
          long candidateVersion =
              replace ? Math.addExact(expected.baseVersion().getAsLong(), 1) : 0;
          String location = expected.normalizedLocation();
          DeltaSnapshotMetadata latest =
              DeltaKernel33MetadataAdapter.read(engine, location, OptionalLong.empty());
          DeltaSnapshotMetadata candidate =
              DeltaKernel33MetadataAdapter.read(
                  engine, location, OptionalLong.of(candidateVersion));
          if (replace) {
            DeltaSnapshotMetadata base =
                DeltaKernel33MetadataAdapter.read(engine, location, expected.baseVersion());
            if (!base.uuid().equals(expected.baseUuid().get())) {
              throw new DeltaMetadataException("BASE_UUID_MISMATCH", "VERIFY");
            }
          }
          Path log = new Path(new Path(location), "_delta_log");
          Path exactCommit =
              new Path(log, String.format(Locale.ROOT, "%020d.json", candidateVersion));
          DeltaCommitEvidence evidence;
          try (InputStream input = exactCommit.getFileSystem(configuration).open(exactCommit)) {
            evidence =
                new DeltaCommitActionReader()
                    .inspect(
                        expected,
                        candidateVersion,
                        latest.version(),
                        candidate.version(),
                        candidate.schemaJson(),
                        input);
          } catch (IOException | RuntimeException e) {
            throw new DeltaMetadataException("COMMIT_UNAVAILABLE", "VERIFY");
          }
          if (evidence.kind() == DeltaCommitEvidence.Kind.MATCHED) {
            if (!candidate.uuid().equals(evidence.uuid().orElse(null))
                || evidence.version().orElse(-1) != candidate.version()) {
              throw new DeltaMetadataException("CANDIDATE_ID_MISMATCH", "VERIFY");
            }
            if (!expected
                .encode()
                .equals(candidate.configuration().get(DeltaOperationMarker.PROPERTY_KEY))) {
              throw new DeltaMetadataException("CANDIDATE_MARKER_MISMATCH", "VERIFY");
            }
          }
          return evidence;
        });
  }

  /** Drains admitted reads and closes this reader's cached filesystems; repeated calls are safe. */
  @Override
  public void close() {
    storage.close();
  }
}

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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/** Detached effective protocol observation; feature admission is a separate contract decision. */
public final class DeltaProtocolDescriptor {
  private final int minReaderVersion;
  private final int minWriterVersion;
  private final Set<String> readerFeatures;
  private final Set<String> writerFeatures;

  private DeltaProtocolDescriptor(
      int reader, int writer, Set<String> readers, Set<String> writers) {
    if (reader < 1 || writer < 1 || (reader >= 3 && writer < 7)) {
      throw new IllegalArgumentException("INVALID_PROTOCOL_VERSION");
    }
    minReaderVersion = reader;
    minWriterVersion = writer;
    readerFeatures = copy(readers);
    writerFeatures = copy(writers);
    if ((reader < 3 && !readerFeatures.isEmpty()) || (writer < 7 && !writerFeatures.isEmpty())) {
      throw new IllegalArgumentException("INVALID_PROTOCOL_FEATURE_STATE");
    }
  }

  /**
   * Copies an effective protocol without claiming validation by a native engine.
   *
   * @param minReaderVersion positive effective reader version
   * @param minWriterVersion positive effective writer version
   * @param readerFeatures reader features, empty for absent legacy fields
   * @param writerFeatures writer features, empty for absent legacy fields
   * @return immutable detached protocol
   * @throws IllegalArgumentException for invalid structure, nulls or exceeded bounds
   */
  public static DeltaProtocolDescriptor of(
      int minReaderVersion,
      int minWriterVersion,
      Set<String> readerFeatures,
      Set<String> writerFeatures) {
    return new DeltaProtocolDescriptor(
        minReaderVersion, minWriterVersion, readerFeatures, writerFeatures);
  }

  /**
   * @return effective minimum reader version
   */
  public int minReaderVersion() {
    return minReaderVersion;
  }

  /**
   * @return effective minimum writer version
   */
  public int minWriterVersion() {
    return minWriterVersion;
  }

  /**
   * @return immutable effective reader features
   */
  public Set<String> readerFeatures() {
    return readerFeatures;
  }

  /**
   * @return immutable effective writer features
   */
  public Set<String> writerFeatures() {
    return writerFeatures;
  }

  /**
   * Compares effective protocol versions and feature sets.
   *
   * @param other value to compare
   * @return whether both effective states agree
   */
  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof DeltaProtocolDescriptor)) {
      return false;
    }
    DeltaProtocolDescriptor that = (DeltaProtocolDescriptor) other;
    return minReaderVersion == that.minReaderVersion
        && minWriterVersion == that.minWriterVersion
        && readerFeatures.equals(that.readerFeatures)
        && writerFeatures.equals(that.writerFeatures);
  }

  /**
   * @return hash of the effective protocol state
   */
  @Override
  public int hashCode() {
    return Objects.hash(minReaderVersion, minWriterVersion, readerFeatures, writerFeatures);
  }

  private static Set<String> copy(Set<String> values) {
    if (values == null || values.size() > 4096) {
      throw new IllegalArgumentException("INVALID_PROTOCOL_FEATURES");
    }
    Set<String> copy = new LinkedHashSet<>();
    for (String feature : values) {
      DeltaJson.requiredText(feature);
      copy.add(feature);
    }
    return Collections.unmodifiableSet(copy);
  }
}

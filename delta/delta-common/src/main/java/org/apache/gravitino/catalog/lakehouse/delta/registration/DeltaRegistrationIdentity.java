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

import java.util.Objects;

/** Immutable expected registration identity; its correlation token is not bearer authority. */
public final class DeltaRegistrationIdentity {
  private final long id;
  private final long generation;
  private final String token;

  private DeltaRegistrationIdentity(long id, long generation, String token) {
    if (id <= 0 || generation < 0) {
      throw new IllegalArgumentException("INVALID_REGISTRATION_IDENTITY");
    }
    requiredText(token, 1024);
    this.id = id;
    this.generation = generation;
    this.token = token;
  }

  /**
   * Creates an expected registration reference.
   *
   * @param id positive authoritative registration ID
   * @param generation nonnegative generation; increment overflow is checked by the server
   * @param token nonblank correlation token, at most 1024 UTF-8 bytes
   * @return immutable identity
   * @throws IllegalArgumentException if any field is invalid
   */
  public static DeltaRegistrationIdentity of(long id, long generation, String token) {
    return new DeltaRegistrationIdentity(id, generation, token);
  }

  /**
   * @return positive registration ID
   */
  public long id() {
    return id;
  }
  /**
   * @return nonnegative generation
   */
  public long generation() {
    return generation;
  }
  /**
   * @return correlation token, not authority
   */
  public String token() {
    return token;
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof DeltaRegistrationIdentity)) {
      return false;
    }
    DeltaRegistrationIdentity that = (DeltaRegistrationIdentity) other;
    return id == that.id && generation == that.generation && token.equals(that.token);
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hash(id, generation, token);
  }

  static void requiredText(String value, int limit) {
    DeltaJson.boundedText(value, limit);
    if (value.codePoints().allMatch(c -> Character.isWhitespace(c) || Character.isSpaceChar(c))) {
      throw new IllegalArgumentException("EMPTY_REGISTRATION_TEXT");
    }
  }
}

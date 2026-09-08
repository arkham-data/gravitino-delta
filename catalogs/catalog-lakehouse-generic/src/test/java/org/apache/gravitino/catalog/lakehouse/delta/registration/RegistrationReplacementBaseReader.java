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

import org.apache.gravitino.meta.TableEntity;

/** Trusted actual-base observation outside the registration update callback. Test-only seam. */
interface RegistrationReplacementBaseReader {
  /**
   * Observes the real latest base while the test's sole native writer is quiescent.
   *
   * @param published complete immutable publication row
   * @return native version, UUID and canonical schema from trusted storage
   */
  BaseObservation read(TableEntity published);

  /** Immutable observed base; construction does not perform I/O. */
  final class BaseObservation {
    final long version;
    final String uuid;
    final String schemaJson;

    BaseObservation(long version, String uuid, String schemaJson) {
      if (version < 0
          || version == Long.MAX_VALUE
          || uuid == null
          || uuid.isEmpty()
          || schemaJson == null
          || schemaJson.isEmpty()) {
        throw new IllegalArgumentException("Incomplete actual replacement base");
      }
      this.version = version;
      this.uuid = uuid;
      this.schemaJson = schemaJson;
    }
  }
}

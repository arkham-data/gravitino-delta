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

package org.apache.gravitino.catalog.lakehouse.delta;

import java.util.Map;
import java.util.Set;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationCommand;
import org.apache.gravitino.connector.BasePropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

/** Only registration commands are admitted as table request properties. */
final class DeltaTablePropertiesMetadata extends BasePropertiesMetadata {
  static final String INTERNAL_PREFIX = "delta.registration.internal.";
  static final String RECEIPT_PREFIX = "delta.registration.receipt.";

  static void validateRequestProperties(Map<String, String> properties) {
    if (properties == null
        || !properties.keySet().equals(Set.of(DeltaRegistrationCommand.COMMAND_PROPERTY))
        || properties.get(DeltaRegistrationCommand.COMMAND_PROPERTY) == null) {
      throw new IllegalArgumentException("INVALID_DELTA_TABLE_PROPERTIES");
    }
  }
  /** {@inheritDoc} */
  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return Map.of(
        DeltaRegistrationCommand.COMMAND_PROPERTY,
        PropertyEntry.stringPropertyEntry(
            DeltaRegistrationCommand.COMMAND_PROPERTY,
            "Transient registration operation request",
            false,
            false,
            null,
            false,
            false),
        INTERNAL_PREFIX,
        PropertyEntry.stringPropertyPrefixEntry(
            INTERNAL_PREFIX, "Private durable registration state", false, true, null, true, true),
        RECEIPT_PREFIX,
        PropertyEntry.stringPropertyPrefixEntry(
            RECEIPT_PREFIX, "Read-only registration receipt", false, true, null, false, true));
  }
}

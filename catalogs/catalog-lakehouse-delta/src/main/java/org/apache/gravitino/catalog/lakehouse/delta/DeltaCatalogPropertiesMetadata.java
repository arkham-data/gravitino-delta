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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.connector.BaseCatalogPropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;
import org.apache.gravitino.storage.S3Properties;

/** Immutable catalog storage policy and inherited shared catalog metadata. */
final class DeltaCatalogPropertiesMetadata extends BaseCatalogPropertiesMetadata {
  /** {@inheritDoc} */
  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    Map<String, PropertyEntry<?>> entries = new LinkedHashMap<>();
    for (String key :
        List.of(
            "admin.url",
            "service.type",
            "service.name",
            "auth.type",
            "username",
            "service.create-if-absent")) {
      add(entries, "authorization.ranger." + key, false, null, false);
    }
    add(entries, "authorization.ranger.password", false, null, true);
    add(entries, "authorization.ranger.delta.catalog", false, null, false);
    add(entries, "authorization.ranger.delta.metalake", false, null, false);
    add(entries, DeltaCatalogConfig.WAREHOUSE, true, null, false);
    add(entries, DeltaCatalogConfig.ADOPTION_ROOTS, false, "[]", false);
    add(entries, DeltaCatalogConfig.RECOVERY_PRINCIPALS, false, "[]", false);
    add(entries, DeltaCatalogConfig.CREDENTIAL_MODE, false, "instance", false);
    add(entries, S3Properties.GRAVITINO_S3_ENDPOINT, false, null, false);
    add(entries, S3Properties.GRAVITINO_S3_REGION, false, null, false);
    add(entries, S3Properties.GRAVITINO_S3_PATH_STYLE_ACCESS, false, "false", false);
    add(entries, S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, false, null, true);
    add(entries, S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, false, null, true);
    add(entries, DeltaCatalogConfig.SESSION_TOKEN, false, null, true);
    return Map.copyOf(entries);
  }

  private static void add(
      Map<String, PropertyEntry<?>> entries,
      String key,
      boolean required,
      @Nullable String defaultValue,
      boolean hidden) {
    entries.put(
        key,
        PropertyEntry.stringPropertyEntry(
            key, "Trusted Delta catalog setting", required, true, defaultValue, hidden, false));
  }
}

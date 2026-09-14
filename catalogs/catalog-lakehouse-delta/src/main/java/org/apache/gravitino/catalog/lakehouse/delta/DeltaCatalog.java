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
import java.util.Objects;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.utils.IsolatedClassLoader;

/** Managed Delta registration catalog with read-only native metadata observation. */
public final class DeltaCatalog extends BaseCatalog<DeltaCatalog> {
  private Map<String, String> authorizationConfig = Map.of();

  /** Creates the production ServiceLoader provider. */
  public DeltaCatalog() {}
  /** {@inheritDoc} */
  @Override
  public DeltaCatalog withCatalogConf(Map<String, String> config) {
    authorizationConfig = Map.copyOf(config);
    return super.withCatalogConf(config);
  }

  /** {@inheritDoc} */
  @Override
  public void initAuthorizationPluginInstance(IsolatedClassLoader classLoader, long metalakeId) {
    if ("ranger-delta".equalsIgnoreCase(authorizationConfig.get(Catalog.AUTHORIZATION_PROVIDER))) {
      if (!Objects.equals(
              entity().name(), authorizationConfig.get("authorization.ranger.delta.catalog"))
          || !Objects.equals(
              entity().namespace().level(0),
              authorizationConfig.get("authorization.ranger.delta.metalake"))) {
        throw new IllegalArgumentException(
            "Ranger Delta bindings must match the actual catalog and metalake");
      }
    }
    super.initAuthorizationPluginInstance(classLoader, metalakeId);
  }

  /** {@inheritDoc} */
  @Override
  public String shortName() {
    return "lakehouse-delta";
  }
  /** {@inheritDoc} */
  @Override
  public Capability newCapability() {
    return new DeltaCatalogCapability();
  }
  /** {@inheritDoc} */
  @Override
  public PropertiesMetadata catalogPropertiesMetadata() {
    return new DeltaCatalogPropertiesMetadata();
  }
  /** {@inheritDoc} */
  @Override
  public PropertiesMetadata schemaPropertiesMetadata() {
    return new DeltaSchemaPropertiesMetadata();
  }
  /** {@inheritDoc} */
  @Override
  public PropertiesMetadata tablePropertiesMetadata() {
    return new DeltaTablePropertiesMetadata();
  }
  /** {@inheritDoc} */
  @Override
  protected CatalogOperations newOps(Map<String, String> config) {
    return new DeltaCatalogOperations();
  }
  /** {@inheritDoc} */
  @Override
  protected boolean shouldBackfillCredential() {
    return false;
  }
}

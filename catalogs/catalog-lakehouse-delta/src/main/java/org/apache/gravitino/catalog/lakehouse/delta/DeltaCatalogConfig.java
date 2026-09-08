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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.gravitino.storage.S3Properties;
import org.apache.hadoop.conf.Configuration;

/** Trusted, resolved catalog settings; raw secrets never appear in diagnostics. */
final class DeltaCatalogConfig {
  static final String WAREHOUSE = "delta.warehouse";
  static final String ADOPTION_ROOTS = "delta.adoption-roots";
  static final String RECOVERY_PRINCIPALS = "delta.recovery-principals";
  static final String CREDENTIAL_MODE = "delta.storage-credential-mode";
  static final String SESSION_TOKEN = "s3-session-token";
  private final Configuration storage;
  private final DeltaLocationPolicy locations;
  private final Set<String> recoveryPrincipals;

  private DeltaCatalogConfig(
      Configuration storage, DeltaLocationPolicy locations, Set<String> principals) {
    this.storage = new Configuration(storage);
    this.locations = locations;
    this.recoveryPrincipals = Set.copyOf(principals);
  }

  static DeltaCatalogConfig from(Map<String, String> configuration) {
    try {
      DeltaCatalogPropertiesMetadata metadata = new DeltaCatalogPropertiesMetadata();
      for (Map.Entry<String, String> entry : configuration.entrySet()) {
        if (entry.getValue() == null || !metadata.containsProperty(entry.getKey())) {
          throw invalid();
        }
      }
      DeltaLocationPolicy locations =
          new DeltaLocationPolicy(
              configuration.get(WAREHOUSE),
              array(configuration.getOrDefault(ADOPTION_ROOTS, "[]")));
      Set<String> principals =
          Set.copyOf(array(configuration.getOrDefault(RECOVERY_PRINCIPALS, "[]")));
      String mode = configuration.getOrDefault(CREDENTIAL_MODE, "instance");
      String access = configuration.get(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID);
      String secret = configuration.get(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY);
      String token = configuration.get(SESSION_TOKEN);
      Configuration storage = new Configuration(false);
      storage.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
      storage.setBoolean("fs.s3a.impl.disable.cache", false);
      String provider;
      switch (mode) {
        case "instance":
          if (access != null || secret != null || token != null) {
            throw invalid();
          }
          provider = "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider";
          break;
        case "static":
          if (blank(access) || blank(secret) || token != null) {
            throw invalid();
          }
          provider = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider";
          break;
        case "session":
          if (blank(access) || blank(secret) || blank(token)) {
            throw invalid();
          }
          provider = "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider";
          break;
        default:
          throw invalid();
      }
      storage.set("fs.s3a.aws.credentials.provider", provider);
      if (access != null) {
        storage.set("fs.s3a.access.key", access);
        storage.set("fs.s3a.secret.key", secret);
      }
      if (token != null) {
        storage.set("fs.s3a.session.token", token);
      }
      String endpoint = configuration.get(S3Properties.GRAVITINO_S3_ENDPOINT);
      if (endpoint != null) {
        URI uri = new URI(endpoint);
        boolean http = "http".equals(uri.getScheme());
        if (!(http || "https".equals(uri.getScheme()))
            || uri.getHost() == null
            || uri.getUserInfo() != null
            || uri.getRawQuery() != null
            || uri.getRawFragment() != null
            || uri.getPort() > 65535
            || uri.getPort() == 0
            || !(uri.getPath().isEmpty() || uri.getPath().equals("/"))) {
          throw invalid();
        }
        storage.set("fs.s3a.endpoint", endpoint);
        storage.setBoolean("fs.s3a.connection.ssl.enabled", !http);
      }
      String region = configuration.get(S3Properties.GRAVITINO_S3_REGION);
      if (region != null) {
        if (!region.matches("[a-z0-9]+(?:-[a-z0-9]+)*")) {
          throw invalid();
        }
        storage.set("fs.s3a.endpoint.region", region);
      }
      String pathStyle =
          configuration.getOrDefault(S3Properties.GRAVITINO_S3_PATH_STYLE_ACCESS, "false");
      if (!pathStyle.equals("true") && !pathStyle.equals("false")) {
        throw invalid();
      }
      storage.set("fs.s3a.path.style.access", pathStyle);
      return new DeltaCatalogConfig(storage, locations, principals);
    } catch (Exception e) {
      throw invalid();
    }
  }

  Configuration storageConfiguration() {
    return new Configuration(storage);
  }

  DeltaLocationPolicy locations() {
    return locations;
  }

  Set<String> recoveryPrincipals() {
    return recoveryPrincipals;
  }

  private static List<String> array(String value) throws Exception {
    JsonNode array =
        new ObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).readTree(value);
    if (array == null || !array.isArray()) {
      throw invalid();
    }
    List<String> result = new ArrayList<>();
    for (JsonNode element : array) {
      if (!element.isTextual() || element.textValue().isBlank()) {
        throw invalid();
      }
      result.add(element.textValue());
    }
    return result;
  }

  private static boolean blank(@Nullable String value) {
    return value == null || value.isBlank();
  }

  private static IllegalArgumentException invalid() {
    return new IllegalArgumentException("INVALID_DELTA_CATALOG_CONFIGURATION");
  }
}

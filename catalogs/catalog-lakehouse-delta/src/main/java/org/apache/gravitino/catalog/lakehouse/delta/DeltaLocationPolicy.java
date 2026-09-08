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

import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;

/** Canonical S3 locations admitted by catalog administrator policy. */
final class DeltaLocationPolicy {
  private final String warehouse;
  private final List<String> roots;

  DeltaLocationPolicy(String warehouse, List<String> roots) {
    this.warehouse = canonical(warehouse);
    this.roots =
        List.copyOf(
            roots.stream().map(DeltaLocationPolicy::canonical).collect(Collectors.toList()));
  }

  String newTableLocation(NameIdentifier ident, long tableId) {
    if (ident == null || ident.namespace().length() != 3 || tableId <= 0) {
      throw invalid();
    }
    String schema = ident.namespace().level(2);
    if (!schema.matches("[A-Za-z0-9_][A-Za-z0-9_-]*")) {
      throw invalid();
    }
    return warehouse + "/" + schema + "/" + tableId;
  }

  String admittedExistingLocation(String location) {
    String normalized = canonical(location);
    if (roots.stream()
        .noneMatch(root -> normalized.equals(root) || normalized.startsWith(root + "/"))) {
      throw invalid();
    }
    return normalized;
  }

  private static String canonical(String location) {
    try {
      URI uri = new URI(location);
      String scheme = uri.getScheme();
      String authority = uri.getRawAuthority();
      String path = uri.getRawPath();
      if (!("s3".equals(scheme) || "s3a".equals(scheme))
          || authority == null
          || uri.getUserInfo() != null
          || uri.getPort() != -1
          || uri.getRawQuery() != null
          || uri.getRawFragment() != null
          || location.indexOf('%') >= 0
          || path == null) {
        throw invalid();
      }
      String bucket = authority.toLowerCase(Locale.ROOT);
      if (bucket.length() < 3
          || bucket.length() > 63
          || !bucket.matches("[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?")
          || bucket.contains("..")
          || bucket.contains(".-")
          || bucket.contains("-.")
          || bucket.matches("[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+")) {
        throw invalid();
      }
      if (path.endsWith("/")) {
        path = path.substring(0, path.length() - 1);
      }
      if (!path.isEmpty()) {
        if (!path.startsWith("/")) {
          throw invalid();
        }
        for (String segment : path.substring(1).split("/", -1)) {
          if (segment.isEmpty()
              || segment.equals(".")
              || segment.equals("..")
              || segment.indexOf('\\') >= 0) {
            throw invalid();
          }
        }
      }
      return "s3a://" + bucket + path;
    } catch (Exception e) {
      throw invalid();
    }
  }

  private static IllegalArgumentException invalid() {
    return new IllegalArgumentException("INVALID_DELTA_LOCATION");
  }
}

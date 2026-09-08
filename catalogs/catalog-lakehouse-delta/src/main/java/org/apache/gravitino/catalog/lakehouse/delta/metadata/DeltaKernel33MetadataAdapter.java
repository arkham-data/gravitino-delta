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

import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.types.DataTypeJsonSerDe;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import javax.annotation.Nullable;
import org.apache.gravitino.rel.Column;

/** The sole compatibility seam for the exact Kernel 3.3.0 and 3.3.2 metadata implementation. */
final class DeltaKernel33MetadataAdapter {
  private DeltaKernel33MetadataAdapter() {}

  static void validateRuntime() {
    validateVersions(
        Snapshot.class.getPackage().getImplementationVersion(),
        DefaultEngine.class.getPackage().getImplementationVersion());
  }

  static void validateVersions(@Nullable String api, @Nullable String defaults) {
    if (!("3.3.0".equals(api) || "3.3.2".equals(api)) || !api.equals(defaults)) {
      throw new DeltaMetadataException("UNSUPPORTED_RUNTIME", "VALIDATE_RUNTIME");
    }
  }

  static List<Column> projectSchema(String rawSchemaJson) {
    validateRuntime();
    try {
      if (rawSchemaJson == null
          || rawSchemaJson.length() > 128 * 1024
          || rawSchemaJson.getBytes(StandardCharsets.UTF_8).length > 128 * 1024) {
        throw new DeltaMetadataException("INVALID_DESIRED_SCHEMA", "PROJECT_SCHEMA");
      }
      return DeltaSchemaProjection.copyColumns(
          DeltaSchemaProjection.columns(DataTypeJsonSerDe.deserializeStructType(rawSchemaJson)));
    } catch (RuntimeException e) {
      throw new DeltaMetadataException("INVALID_DESIRED_SCHEMA", "PROJECT_SCHEMA");
    } catch (LinkageError e) {
      throw new DeltaMetadataException("UNSUPPORTED_RUNTIME", "PROJECT_SCHEMA");
    }
  }

  static DeltaSnapshotMetadata read(Engine engine, String location, OptionalLong version) {
    validateRuntime();
    try {
      Table table = Table.forPath(engine, location);
      Snapshot snapshot =
          version.isPresent()
              ? table.getSnapshotAsOfVersion(engine, version.getAsLong())
              : table.getLatestSnapshot(engine);
      return copy(engine, location, snapshot, version);
    } catch (DeltaMetadataException e) {
      throw e;
    } catch (RuntimeException e) {
      // Kernel 3.3 uses untyped KernelException for unsupported protocol and other failures.
      // Do not infer a reason from SDK messages or retain their potentially sensitive cause chain.
      throw new DeltaMetadataException("SNAPSHOT_UNAVAILABLE", "READ_SNAPSHOT");
    } catch (LinkageError e) {
      throw new DeltaMetadataException("UNSUPPORTED_RUNTIME", "READ_SNAPSHOT");
    }
  }

  static DeltaSnapshotMetadata copy(
      Engine engine, String location, Snapshot snapshot, OptionalLong version) {
    validateRuntime();
    if (snapshot == null || snapshot.getClass() != SnapshotImpl.class) {
      throw new DeltaMetadataException("UNSUPPORTED_RUNTIME", "COPY_METADATA");
    }
    SnapshotImpl nativeSnapshot = (SnapshotImpl) snapshot;
    Metadata metadata = nativeSnapshot.getMetadata();
    Map<String, String> configuration = metadata.getConfiguration();
    // These exact patches have no coordinator handler and otherwise silently omit unbackfilled
    // commits. A conforming activation is backfilled, so reject even empty/orphan coordinator keys.
    if (configuration.containsKey("delta.coordinatedCommits.commitCoordinator-preview")
        || configuration.containsKey("delta.coordinatedCommits.commitCoordinatorConf-preview")
        || configuration.containsKey("delta.coordinatedCommits.tableConf-preview")) {
      throw new DeltaMetadataException("UNSUPPORTED_COORDINATED_COMMITS", "COPY_METADATA");
    }
    Protocol protocol = nativeSnapshot.getProtocol();
    List<String> partitions = new ArrayList<>();
    ArrayValue array = metadata.getPartitionColumns();
    ColumnVector elements = array.getElements();
    for (int i = 0; i < array.getSize(); i++) {
      partitions.add(elements.getString(i));
    }
    if ((version.isPresent() && snapshot.getVersion(engine) != version.getAsLong())
        || !metadata.getSchema().toJson().equals(snapshot.getSchema(engine).toJson())
        || !partitions.equals(snapshot.getPartitionColumnNames(engine))) {
      throw new DeltaMetadataException("INCONSISTENT_SNAPSHOT", "COPY_METADATA");
    }
    return new DeltaSnapshotMetadata(
        location,
        snapshot.getVersion(engine),
        metadata.getId(),
        metadata.getSchemaString(),
        DeltaSchemaProjection.columns(metadata.getSchema()),
        partitions,
        configuration,
        metadata.getDescription(),
        metadata.getCreatedTime().isPresent()
            ? OptionalLong.of(metadata.getCreatedTime().get())
            : OptionalLong.empty(),
        protocol.getMinReaderVersion(),
        protocol.getMinWriterVersion(),
        new HashSet<>(protocol.getReaderFeatures()),
        new HashSet<>(protocol.getWriterFeatures()));
  }
}

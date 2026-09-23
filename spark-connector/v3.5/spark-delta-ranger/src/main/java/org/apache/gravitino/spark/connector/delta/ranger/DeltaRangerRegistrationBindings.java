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

package org.apache.gravitino.spark.connector.delta.ranger;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerRegistrationReader.PublishedObservation;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.SnapshotDescriptor;
import org.apache.spark.sql.delta.actions.Metadata;
import org.apache.spark.sql.delta.catalog.DeltaTableV2;
import org.apache.spark.sql.delta.files.TahoeLogFileIndex;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Seq;

/** Driver-local provenance of completed ordinary named loads; no retained-action admission. */
final class DeltaRangerRegistrationBindings {
  private final DeltaRangerRegistrationReader reader;
  private final DeltaRangerPolicyEngine engine;
  private final ReferenceQueue<CatalogTable> collected = new ReferenceQueue<>();
  private final Map<IdentityKey<CatalogTable>, RegistrationBinding> entries = new HashMap<>();
  @Nullable private PublishedObservation latest;
  private boolean closed;

  DeltaRangerRegistrationBindings(
      DeltaRangerRegistrationReader reader, DeltaRangerPolicyEngine engine) {
    this.reader = Objects.requireNonNull(reader);
    this.engine = Objects.requireNonNull(engine);
  }

  @SuppressWarnings(
      "ReferenceEquality") // Native provenance requires object identity, not case-class equality.
  Table load(Identifier id, Supplier<Table> loader) {
    try {
      requireLive();
      PublishedObservation before = reader.read(id);
      Table loaded = loader.get();
      if (!(loaded instanceof DeltaTableV2)) throw denied();
      DeltaTableV2 table = (DeltaTableV2) loaded;
      if (table.timeTravelOpt().isDefined()
          || !table.options().isEmpty()
          || table.hasPartitionFilters()
          || table.catalogTable().isEmpty()
          || table.ttSafeCatalogTable().isEmpty()
          || table.catalogTable().get() != table.ttSafeCatalogTable().get()) throw denied();
      Snapshot snapshot = table.initialSnapshot();
      BaseRelation base = table.toBaseRelation();
      if (!(base instanceof HadoopFsRelation)) throw denied();
      HadoopFsRelation relation = (HadoopFsRelation) base;
      if (!(relation.location() instanceof TahoeLogFileIndex)) throw denied();
      TahoeLogFileIndex index = (TahoeLogFileIndex) relation.location();
      if (snapshot.deltaLog() != table.deltaLog() || index.deltaLog() != table.deltaLog())
        throw denied();
      CatalogTable descriptor = table.catalogTable().get();
      PublishedObservation after = reader.read(id);
      if (!before.samePublication(after)) throw denied();
      validateDescriptor(descriptor, after, snapshot);
      if (!locationMatches(after.location(), table.path().toUri())
          || !locationMatches(after.location(), table.deltaLog().dataPath().toUri()))
        throw denied();
      validateIndex(index, after, snapshot);
      // DeltaLog.buildHadoopFsRelationWithFileIndex passes the full snapshot schema as dataSchema,
      // including partition columns, and passes the ordered partition schema separately.
      if (!logicalSchema(snapshot.schema()).equals(logicalSchema(relation.dataSchema()))
          || !logicalSchema(snapshot.metadata().partitionSchema())
              .equals(logicalSchema(relation.partitionSchema()))) throw denied();
      RegistrationBinding binding =
          new RegistrationBinding(after, descriptor, relation, index, snapshot);
      // Network and lazy native work above never hold this monitor. Close and insertion share it.
      synchronized (this) {
        requireLive();
        reader.requireLive(before);
        reader.requireLive(after);
        drain(collected, entries);
        entries.put(new IdentityKey<>(descriptor, collected), binding);
        latest = after;
      }
      return loaded;
    } catch (Exception | LinkageError failure) {
      throw denied();
    }
  }

  @SuppressWarnings(
      "ReferenceEquality") // Equal descriptor/index copies have no registration authority.
  BoundRegistrationScan resolve(LogicalRelation logical) {
    try {
      if (logical == null
          || logical.isStreaming()
          || logical.catalogTable().isEmpty()
          || !(logical.relation() instanceof HadoopFsRelation)) throw denied();
      CatalogTable descriptor = logical.catalogTable().get();
      HadoopFsRelation relation = (HadoopFsRelation) logical.relation();
      RegistrationBinding binding;
      synchronized (this) {
        requireLive();
        drain(collected, entries);
        binding = entries.get(new IdentityKey<>(descriptor, null));
      }
      if (binding == null
          || binding.relation.get() != relation
          || !(relation.location() instanceof TahoeLogFileIndex)) throw denied();
      TahoeLogFileIndex index = (TahoeLogFileIndex) relation.location();
      DeltaLog log = index.deltaLog();
      if (binding.index.get() != index || binding.log.get() != log) throw denied();
      validateDescriptor(descriptor, binding.observation, index.snapshotAtAnalysis());
      validateIndex(index, binding.observation, index.snapshotAtAnalysis());
      if (!binding.nativeSchemaJson.equals(index.snapshotAtAnalysis().metadata().schemaString())
          || binding.nativeVersion != index.snapshotAtAnalysis().version()
          || !binding.protocolJson.equals(index.snapshotAtAnalysis().protocol().json())
          || !binding.partitions.equals(sequenceStamp(index.metadata().partitionColumns()))
          || !binding.mapping.equals(mappingStamp(index.metadata()))
          || !binding.descriptorSchemaJson.equals(descriptor.schema().json())
          || !binding.dataSchemaJson.equals(relation.dataSchema().json())
          || !binding.partitionSchemaJson.equals(relation.partitionSchema().json())
          || !logicalSchema(outputSchema(logical)).equals(logicalSchema(relation.schema()))
          || !locationMatches(binding.observation.location(), log.dataPath().toUri()))
        throw denied();
      synchronized (this) {
        requireLive();
        reader.requireLive(binding.observation);
        return new BoundRegistrationScan(this, descriptor, relation, index, log, binding);
      }
    } catch (Exception | LinkageError failure) {
      throw denied();
    }
  }

  synchronized void requireLive() {
    try {
      if (closed) throw denied();
      engine.snapshotStamp();
      if (latest != null) reader.requireLive(latest);
    } catch (Exception | LinkageError failure) {
      throw denied();
    }
  }

  synchronized void close() {
    closed = true;
    entries.clear();
    latest = null;
    drain(collected, entries);
  }

  static <K, V> void drain(ReferenceQueue<K> queue, Map<IdentityKey<K>, V> entries) {
    Reference<? extends K> key;
    while ((key = queue.poll()) != null) entries.remove(key);
  }

  private static void validateDescriptor(
      CatalogTable descriptor, PublishedObservation observation, SnapshotDescriptor snapshot) {
    TableIdentifier id = descriptor.identifier();
    if (!id.catalog().contains(observation.catalog())
        || !id.database().contains(observation.database())
        || !id.table().equals(observation.table())
        || !descriptor.provider().contains("delta")
        || descriptor.storage().locationUri().isEmpty()
        || !locationMatches(observation.location(), descriptor.storage().locationUri().get())
        || !logicalSchema(descriptor.schema()).equals(observation.schemaJson())
        || !logicalSchema(snapshot.schema()).equals(observation.schemaJson())
        || !sequenceStamp(descriptor.partitionColumnNames())
            .equals(sequenceStamp(snapshot.metadata().partitionColumns()))) throw denied();
  }

  private static void validateIndex(
      TahoeLogFileIndex index, PublishedObservation observation, SnapshotDescriptor snapshot) {
    SnapshotDescriptor analysis = index.snapshotAtAnalysis();
    if (index.isTimeTravelQuery()
        || !index.partitionFilters().isEmpty()
        || analysis.deltaLog() != index.deltaLog()
        || analysis.deltaLog() != snapshot.deltaLog()
        || !locationMatches(observation.location(), index.path().toUri())
        || !observation.nativeId().equals(snapshot.metadata().id())
        || !observation.nativeId().equals(analysis.metadata().id())
        || snapshot.version() < observation.nativeVersion()
        || analysis.version() != snapshot.version()
        || !analysis.metadata().schemaString().equals(snapshot.metadata().schemaString())
        || !analysis.protocol().json().equals(snapshot.protocol().json())
        || !sequenceStamp(analysis.metadata().partitionColumns())
            .equals(sequenceStamp(snapshot.metadata().partitionColumns()))
        || !mappingStamp(analysis.metadata()).equals(mappingStamp(snapshot.metadata())))
      throw denied();
  }

  private static boolean locationMatches(String expected, URI actual) {
    if (expected.equals(actual.toString())) return true;
    URI configured = URI.create(expected);
    // Local fixtures may express the same absolute file path with or without the file scheme.
    // Production object-store URIs retain exact canonical text; no path/authority aliases are used.
    boolean localExpected = configured.getScheme() == null || "file".equals(configured.getScheme());
    boolean localActual = actual.getScheme() == null || "file".equals(actual.getScheme());
    return localExpected
        && localActual
        && configured.getAuthority() == null
        && actual.getAuthority() == null
        && configured.getQuery() == null
        && actual.getQuery() == null
        && configured.getFragment() == null
        && actual.getFragment() == null
        && configured.getRawPath() != null
        && configured.getRawPath().startsWith("/")
        && configured.getRawPath().equals(actual.getRawPath());
  }

  private static String sequenceStamp(Seq<String> values) {
    StringBuilder stamp = new StringBuilder();
    Iterator<String> iterator = values.iterator();
    while (iterator.hasNext()) append(stamp, iterator.next());
    return stamp.toString();
  }

  private static String mappingStamp(Metadata metadata) {
    Map<String, String> mapping = new TreeMap<>();
    Iterator<Tuple2<String, String>> iterator = metadata.configuration().iterator();
    while (iterator.hasNext()) {
      Tuple2<String, String> entry = iterator.next();
      if (entry._1().startsWith("delta.columnMapping.")) mapping.put(entry._1(), entry._2());
    }
    StringBuilder stamp = new StringBuilder();
    mapping.forEach(
        (key, value) -> {
          append(stamp, key);
          append(stamp, value);
        });
    return stamp.toString();
  }

  private static void append(StringBuilder stamp, String value) {
    stamp.append(value.length()).append(':').append(value);
  }

  private static StructType outputSchema(LogicalRelation logical) {
    StructField[] fields = new StructField[logical.output().size()];
    Iterator<AttributeReference> attributes = logical.output().iterator();
    int i = 0;
    while (attributes.hasNext()) {
      AttributeReference attribute = attributes.next();
      fields[i++] =
          DataTypes.createStructField(attribute.name(), attribute.dataType(), attribute.nullable());
    }
    return DataTypes.createStructType(fields);
  }

  private static String logicalSchema(StructType schema) {
    return withoutMetadata(schema).json();
  }

  private static DataType withoutMetadata(DataType type) {
    if (type instanceof StructType) {
      StructField[] fields = ((StructType) type).fields();
      StructField[] result = new StructField[fields.length];
      for (int i = 0; i < fields.length; i++) {
        StructField field = fields[i];
        result[i] =
            DataTypes.createStructField(
                field.name(), withoutMetadata(field.dataType()), field.nullable());
      }
      return DataTypes.createStructType(result);
    }
    if (type instanceof ArrayType) {
      ArrayType array = (ArrayType) type;
      return DataTypes.createArrayType(withoutMetadata(array.elementType()), array.containsNull());
    }
    if (type instanceof MapType) {
      MapType map = (MapType) type;
      return DataTypes.createMapType(
          withoutMetadata(map.keyType()),
          withoutMetadata(map.valueType()),
          map.valueContainsNull());
    }
    return type;
  }

  private static SecurityException denied() {
    return new SecurityException("Native Delta registration binding is unavailable");
  }

  static final class IdentityKey<T> extends WeakReference<T> {
    private final int hash;

    IdentityKey(T value, @Nullable ReferenceQueue<T> queue) {
      this(value, queue, System.identityHashCode(value));
    }

    IdentityKey(T value, @Nullable ReferenceQueue<T> queue, int identityHash) {
      super(value, queue);
      hash = identityHash;
    }
    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      return hash;
    }
    /** Compares live referents by identity; a cleared key matches only itself. */
    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (!(other instanceof IdentityKey)) return false;
      Object value = get();
      return value != null && value == ((IdentityKey<?>) other).get();
    }
    /** Returns no descriptor information. */
    @Override
    public String toString() {
      return "Weak native descriptor identity";
    }
  }

  static final class RegistrationBinding {
    private final PublishedObservation observation;
    private final String nativeSchemaJson;
    private final String protocolJson;
    private final String partitions;
    private final String mapping;
    private final String descriptorSchemaJson;
    private final String dataSchemaJson;
    private final String partitionSchemaJson;
    private final long nativeVersion;
    private final WeakReference<HadoopFsRelation> relation;
    private final WeakReference<TahoeLogFileIndex> index;
    private final WeakReference<DeltaLog> log;

    private RegistrationBinding(
        PublishedObservation observation,
        CatalogTable descriptor,
        HadoopFsRelation relation,
        TahoeLogFileIndex index,
        Snapshot snapshot) {
      this.observation = observation;
      this.nativeSchemaJson = snapshot.metadata().schemaString();
      this.protocolJson = snapshot.protocol().json();
      this.partitions = sequenceStamp(snapshot.metadata().partitionColumns());
      this.mapping = mappingStamp(snapshot.metadata());
      this.descriptorSchemaJson = descriptor.schema().json();
      this.dataSchemaJson = relation.dataSchema().json();
      this.partitionSchemaJson = relation.partitionSchema().json();
      this.nativeVersion = snapshot.version();
      this.relation = new WeakReference<>(relation);
      this.index = new WeakReference<>(index);
      this.log = new WeakReference<>(index.deltaLog());
    }
    /** Returns a constant description without registration or native metadata. */
    @Override
    public String toString() {
      return "Native registration binding";
    }
  }

  static final class BoundRegistrationScan {
    private final DeltaRangerRegistrationBindings owner;
    private final CatalogTable descriptor;
    private final HadoopFsRelation relation;
    private final TahoeLogFileIndex index;
    private final DeltaLog log;
    private final RegistrationBinding binding;

    private BoundRegistrationScan(
        DeltaRangerRegistrationBindings owner,
        CatalogTable descriptor,
        HadoopFsRelation relation,
        TahoeLogFileIndex index,
        DeltaLog log,
        RegistrationBinding binding) {
      this.owner = owner;
      this.descriptor = descriptor;
      this.relation = relation;
      this.index = index;
      this.log = log;
      this.binding = binding;
    }

    CatalogTable descriptor() {
      return descriptor;
    }

    HadoopFsRelation relation() {
      return relation;
    }

    TahoeLogFileIndex index() {
      return index;
    }

    DeltaLog log() {
      return log;
    }

    PublishedObservation observation() {
      return binding.observation;
    }

    String nativeSchemaJson() {
      return binding.nativeSchemaJson;
    }

    String nativeProtocolJson() {
      return binding.protocolJson;
    }

    String nativePartitionStamp() {
      return binding.partitions;
    }

    String mappingConfigurationStamp() {
      return binding.mapping;
    }

    long nativeVersion() {
      return binding.nativeVersion;
    }

    void requireLive() {
      owner.requireLive();
      owner.reader.requireLive(binding.observation);
    }
    /** Returns a constant description without registration or native metadata. */
    @Override
    public String toString() {
      return "Bound native registration scan";
    }
  }
}

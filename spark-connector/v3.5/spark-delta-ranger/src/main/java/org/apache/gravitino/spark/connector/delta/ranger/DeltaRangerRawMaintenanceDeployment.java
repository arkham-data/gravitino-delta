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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerPolicyEngine.SnapshotStamp;
import org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerRawMaintenanceConfig.TableKey;
import org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerRegistrationReader.PublishedObservation;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.internal.SharedState;

/** Catalog-owned metadata enrollment; observations do not admit any native operation. */
final class DeltaRangerRawMaintenanceDeployment implements AutoCloseable {
  private static final String CONFIG =
      DeltaRangerCatalogSpark35.PREFIX + "rawCacheMaintenance.config";
  private static final IdentityHashMap<SharedState, Scope> SCOPES = new IdentityHashMap<>();
  private final DeltaRangerCatalogSpark35 catalog;
  private final SparkContext context;
  private final SharedState sharedState;
  private final Scope scope;
  private final DeltaRangerRegistrationReader reader;
  private final DeltaRangerPolicyEngine engine;
  private final Map<TableKey, PublishedObservation> anchors;
  private volatile boolean closed;

  private DeltaRangerRawMaintenanceDeployment(
      DeltaRangerCatalogSpark35 catalog,
      SparkSession session,
      Scope scope,
      DeltaRangerRegistrationReader reader,
      DeltaRangerPolicyEngine engine,
      Map<TableKey, PublishedObservation> anchors) {
    this.catalog = catalog;
    this.context = session.sparkContext();
    this.sharedState = session.sharedState();
    this.scope = scope;
    this.reader = reader;
    this.engine = engine;
    this.anchors = Collections.unmodifiableMap(new LinkedHashMap<>(anchors));
  }

  @Nullable
  static DeltaRangerRawMaintenanceDeployment install(
      DeltaRangerCatalogSpark35 catalog,
      SparkSession session,
      String metalake,
      String catalogName,
      DeltaRangerRegistrationReader reader,
      DeltaRangerPolicyEngine engine) {
    String configured = session.sparkContext().getConf().get(CONFIG, null);
    if (configured == null) return null;
    final Path path;
    try {
      if (configured.trim().isEmpty()) throw new IllegalArgumentException();
      path = Paths.get(configured);
      if (!path.isAbsolute()) throw new IllegalArgumentException();
    } catch (RuntimeException failure) {
      throw invalid();
    }
    SharedState sharedState = session.sharedState();
    DeltaRangerRawMaintenanceConfig config;
    final Scope initialScope;
    synchronized (SCOPES) {
      Scope existing = SCOPES.get(sharedState);
      initialScope = existing;
      if (existing == null) config = null;
      else {
        requireScope(existing, session, path);
        config = existing.config;
      }
    }
    if (config == null) config = DeltaRangerRawMaintenanceConfig.read(path);
    Map<TableKey, PublishedObservation> anchors = new LinkedHashMap<>();
    for (TableKey key : config.tables()) if (!metalake.equals(key.metalake())) throw invalid();
    try {
      for (TableKey key : config.tables()) {
        if (!catalogName.equals(key.catalog())) continue;
        PublishedObservation before = reader.read(identifier(key));
        reader.requireLive(before);
        PublishedObservation after = reader.read(identifier(key));
        reader.requireLive(before);
        reader.requireLive(after);
        if (!before.samePublication(after)) throw denied();
        anchors.put(key, after);
      }
      if (anchors.isEmpty()) return null;
      if (session.sparkContext().isStopped()) throw denied();
      synchronized (SCOPES) {
        Scope current = SCOPES.get(sharedState);
        // A borrowed frozen snapshot cannot start a new lifecycle after its scope retires.
        if (initialScope != null && current != initialScope) throw denied();
        if (current != null) {
          requireScope(current, session, path);
          if (!current.config.tables().equals(config.tables())) throw denied();
          for (DeltaRangerRawMaintenanceDeployment owner : current.owners) {
            if (owner.catalog == catalog) throw denied();
            for (TableKey key : anchors.keySet())
              if (owner.anchors.containsKey(key)) throw denied();
          }
        } else {
          current = new Scope(session.sparkContext(), path, config);
          SCOPES.put(sharedState, current);
        }
        DeltaRangerRawMaintenanceDeployment owner =
            new DeltaRangerRawMaintenanceDeployment(
                catalog, session, current, reader, engine, anchors);
        current.owners.add(owner);
        current.generation++;
        return owner;
      }
    } catch (RuntimeException | LinkageError failure) {
      throw denied();
    }
  }

  static List<RegistrationObservation> observeRegistrations(SparkSession session) {
    try {
      SharedState sharedState = session.sharedState();
      SparkContext context = session.sparkContext();
      List<DeltaRangerRawMaintenanceDeployment> owners;
      long generation;
      synchronized (SCOPES) {
        Scope scope = SCOPES.get(sharedState);
        if (scope == null || scope.context != context || scope.owners.isEmpty()) throw denied();
        owners = new ArrayList<>(scope.owners);
        generation = scope.generation;
      }
      List<RegistrationObservation> observations = new ArrayList<>();
      for (DeltaRangerRawMaintenanceDeployment owner : owners) {
        owner.requireMember(session, generation);
        SnapshotStamp stamp = owner.engine.snapshotStamp();
        for (Map.Entry<TableKey, PublishedObservation> anchor : owner.anchors.entrySet()) {
          owner.requireMember(session, generation);
          owner.reader.requireLive(anchor.getValue());
          PublishedObservation before = owner.reader.read(identifier(anchor.getKey()));
          owner.requireMember(session, generation);
          PublishedObservation after = owner.reader.read(identifier(anchor.getKey()));
          owner.requireMember(session, generation);
          owner.reader.requireLive(before);
          owner.reader.requireLive(after);
          if (!before.samePublication(after) || !compatible(anchor.getValue(), after))
            throw denied();
          owner.engine.requireSnapshot(stamp);
          observations.add(new RegistrationObservation(owner, session, stamp, after, generation));
        }
      }
      for (RegistrationObservation observation : observations) observation.requireLive(session);
      return Collections.unmodifiableList(observations);
    } catch (RuntimeException | LinkageError failure) {
      throw denied();
    }
  }

  /** Retires this owner before releasing catalog-owned metadata and engine leases. */
  @Override
  public void close() {
    closed = true;
    synchronized (SCOPES) {
      if (scope.owners.remove(this)) {
        scope.generation++;
        if (scope.owners.isEmpty() && SCOPES.get(sharedState) == scope) SCOPES.remove(sharedState);
      }
    }
  }

  private void requireMember(SparkSession session, long generation) {
    if (closed
        || context.isStopped()
        || session.sparkContext() != context
        || session.sharedState() != sharedState) throw denied();
    synchronized (SCOPES) {
      if (closed
          || SCOPES.get(sharedState) != scope
          || scope.generation != generation
          || !scope.owners.contains(this)) throw denied();
    }
  }

  private static boolean compatible(PublishedObservation anchor, PublishedObservation current) {
    return anchor.identity().equals(current.identity())
        && anchor.nativeId().equals(current.nativeId())
        && anchor.location().equals(current.location())
        && anchor.schemaJson().equals(current.schemaJson())
        && current.nativeVersion() >= anchor.nativeVersion();
  }

  private static Identifier identifier(TableKey key) {
    return Identifier.of(new String[] {key.database()}, key.table());
  }

  private static void requireScope(Scope scope, SparkSession session, Path path) {
    if (scope.context != session.sparkContext()
        || !scope.path.equals(path)
        || scope.context.isStopped()) throw denied();
  }

  private static IllegalArgumentException invalid() {
    return new IllegalArgumentException("Invalid raw maintenance configuration");
  }

  private static SecurityException denied() {
    return new SecurityException("Raw maintenance registration observation is unavailable");
  }

  private static final class Scope {
    private final SparkContext context;
    private final Path path;
    private final DeltaRangerRawMaintenanceConfig config;
    private final List<DeltaRangerRawMaintenanceDeployment> owners = new ArrayList<>();
    private long generation;

    private Scope(SparkContext context, Path path, DeltaRangerRawMaintenanceConfig config) {
      this.context = context;
      this.path = path;
      this.config = config;
    }
  }

  /** Session-bound optimistic publication evidence, never a standalone native capability. */
  static final class RegistrationObservation {
    private final DeltaRangerRawMaintenanceDeployment owner;
    private final SparkSession session;
    private final SnapshotStamp stamp;
    private final PublishedObservation publication;
    private final long generation;

    private RegistrationObservation(
        DeltaRangerRawMaintenanceDeployment owner,
        SparkSession session,
        SnapshotStamp stamp,
        PublishedObservation publication,
        long generation) {
      this.owner = owner;
      this.session = session;
      this.stamp = stamp;
      this.publication = publication;
      this.generation = generation;
    }

    PublishedObservation publication() {
      return publication;
    }

    void requireCurrent(SparkSession actualSession) {
      try {
        requireLive(actualSession);
        PublishedObservation current =
            owner.reader.read(
                Identifier.of(new String[] {publication.database()}, publication.table()));
        requireLive(actualSession);
        owner.reader.requireLive(current);
        if (!publication.samePublication(current)) throw denied();
      } catch (RuntimeException | LinkageError failure) {
        throw denied();
      }
    }

    private void requireLive(SparkSession actualSession) {
      if (actualSession != session) throw denied();
      owner.requireMember(actualSession, generation);
      owner.reader.requireLive(publication);
      owner.engine.requireSnapshot(stamp);
      owner.requireMember(actualSession, generation);
    }
  }
}

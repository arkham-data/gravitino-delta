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

import java.security.Principal;
import java.util.Optional;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;

/** Additional registration policy checks using the authenticated Gravitino Subject. */
final class DeltaAuthorization {
  private final Supplier<GravitinoEnv> environment;
  private final Supplier<Principal> principals;

  DeltaAuthorization() {
    this(GravitinoEnv::getInstance, PrincipalUtils::getCurrentPrincipal);
  }

  DeltaAuthorization(Supplier<GravitinoEnv> environment, Supplier<Principal> principals) {
    this.environment = environment;
    this.principals = principals;
  }

  String currentActor() {
    try {
      return actor(principals.get());
    } catch (RuntimeException e) {
      throw denied();
    }
  }

  void requirePrivileged(NameIdentifier ident) {
    if (!isPrivileged(ident, currentActor())) {
      throw denied();
    }
  }

  boolean isPrivileged(NameIdentifier ident, String requestedActor) {
    try {
      Principal principal = principals.get();
      if (!actor(principal).equals(requestedActor)) {
        return false;
      }
      GravitinoEnv env = enabledEnvironment();
      GravitinoAuthorizer authorizer = env.gravitinoAuthorizer();
      String metalake = ident.namespace().level(0);
      AuthorizationRequestContext context = new AuthorizationRequestContext();
      return authorizer.isServiceAdmin()
          || authorizer.isOwner(
              principal,
              metalake,
              NameIdentifierUtil.toMetadataObject(
                  NameIdentifierUtil.ofMetalake(metalake), Entity.EntityType.METALAKE),
              context)
          || authorizer.isOwner(
              principal,
              metalake,
              NameIdentifierUtil.toMetadataObject(
                  NameIdentifierUtil.getCatalogIdentifier(ident), Entity.EntityType.CATALOG),
              context);
    } catch (RuntimeException e) {
      throw denied();
    }
  }

  void requireInitialOwner(NameIdentifier ident) {
    currentActor();
    if (!owner(ident).isPresent()) {
      throw denied();
    }
  }

  void requirePendingReplayOwner(NameIdentifier ident, String creator) {
    if (!currentActor().equals(creator)) {
      throw denied();
    }
    Optional<Owner> owner = owner(ident);
    if (owner.isPresent()
        && (owner.get().type() != Owner.Type.USER || !creator.equals(owner.get().name()))) {
      throw denied();
    }
  }

  private Optional<Owner> owner(NameIdentifier ident) {
    try {
      Optional<Owner> owner =
          enabledEnvironment()
              .internalOwnerDispatcher()
              .getOwner(
                  ident.namespace().level(0),
                  NameIdentifierUtil.toMetadataObject(ident, Entity.EntityType.TABLE));
      if (owner.isPresent()) {
        Owner value = owner.get();
        if (value.name() == null
            || value.name().isBlank()
            || (value.type() != Owner.Type.USER && value.type() != Owner.Type.GROUP)) {
          throw denied();
        }
      }
      return owner;
    } catch (RuntimeException e) {
      throw denied();
    }
  }

  private GravitinoEnv enabledEnvironment() {
    GravitinoEnv env = environment.get();
    if (env == null
        || env.config() == null
        || !Boolean.TRUE.equals(env.config().get(Configs.ENABLE_AUTHORIZATION))
        || env.gravitinoAuthorizer() == null
        || env.internalAccessControlDispatcher() == null
        || env.internalOwnerDispatcher() == null) {
      throw denied();
    }
    for (Class<?> type = env.gravitinoAuthorizer().getClass();
        type != null;
        type = type.getSuperclass()) {
      if (type.getName()
          .equals("org.apache.gravitino.server.authorization.PassThroughAuthorizer")) {
        throw denied();
      }
    }
    return env;
  }

  private static String actor(@Nullable Principal principal) {
    if (principal == null
        || principal.getName() == null
        || principal.getName().isBlank()
        || AuthConstants.ANONYMOUS_USER.equals(principal.getName())) {
      throw denied();
    }
    return principal.getName();
  }

  private static SecurityException denied() {
    return new SecurityException("DELTA_AUTHORIZATION_REQUIRED");
  }
}

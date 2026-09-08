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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.UserGroup;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.AccessControlDispatcher;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.server.authorization.PassThroughAuthorizer;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.Test;

class TestDeltaAuthorization {
  private final NameIdentifier ident = NameIdentifier.of("m", "c", "s", "t");

  @Test
  void rejectsAnonymousAndBlank() {
    for (Principal principal :
        new Principal[] {null, () -> "", () -> " ", PrincipalUtils.ANONYMOUS_PRINCIPAL}) {
      assertThrows(
          SecurityException.class,
          () -> new DeltaAuthorization(() -> null, () -> principal).currentActor());
    }
  }

  @Test
  void actualSubjectPrincipalIsUsed() throws Exception {
    assertThrows(SecurityException.class, () -> new DeltaAuthorization().currentActor());
    assertEquals(
        "alice",
        PrincipalUtils.doAs(
            new UserPrincipal("alice"), () -> new DeltaAuthorization().currentActor()));
  }

  @Test
  void cannotBorrowAdminFromAnotherActorOrMissingEnvironment() {
    GravitinoEnv env = mock(GravitinoEnv.class);
    DeltaAuthorization auth = new DeltaAuthorization(() -> env, () -> () -> "alice");
    assertFalse(auth.isPrivileged(ident, "bob"));
    assertThrows(SecurityException.class, () -> auth.requirePrivileged(ident));
    assertThrows(SecurityException.class, () -> auth.requireInitialOwner(ident));
  }

  @Test
  void disabledAndIncompleteAuthorizationFailBeforeAuthorizer() {
    for (int missing = 0; missing < 5; missing++) {
      Fixture f = new Fixture();
      switch (missing) {
        case 0:
          when(f.config.get(Configs.ENABLE_AUTHORIZATION)).thenReturn(false);
          break;
        case 1:
          when(f.env.config()).thenReturn(null);
          break;
        case 2:
          when(f.env.internalAccessControlDispatcher()).thenReturn(null);
          break;
        case 3:
          when(f.env.internalOwnerDispatcher()).thenReturn(null);
          break;
        case 4:
          when(f.env.gravitinoAuthorizer()).thenReturn(null);
          break;
        default:
          throw new AssertionError();
      }
      assertThrows(SecurityException.class, () -> f.auth.requirePrivileged(ident));
      assertThrows(SecurityException.class, () -> f.auth.requireInitialOwner(ident));
      verifyNoInteractions(f.authorizer, f.owners);
    }
  }

  @Test
  void actualPassThroughAndItsSubclassCannotProveAuthorization() {
    for (GravitinoAuthorizer authorizer :
        List.of(new PassThroughAuthorizer(), new DerivedPassThrough())) {
      Fixture f = new Fixture();
      when(f.env.gravitinoAuthorizer()).thenReturn(authorizer);
      assertThrows(SecurityException.class, () -> f.auth.requirePrivileged(ident));
      assertThrows(SecurityException.class, () -> f.auth.requireInitialOwner(ident));
    }
  }

  @Test
  void catalogAndMetalakeOwnershipUseOriginalPrincipalWithGroups() {
    Fixture f = new Fixture();
    MetadataObject catalog =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog("m", "c"), Entity.EntityType.CATALOG);
    when(f.authorizer.isOwner(
            same(f.principal), eq("m"), eq(catalog), any(AuthorizationRequestContext.class)))
        .thenAnswer(
            call -> {
              assertSame(f.principal, call.getArgument(0));
              assertEquals("analysts", f.principal.getGroups().get(0).getGroupName());
              return true;
            });
    assertDoesNotThrow(() -> f.auth.requirePrivileged(ident));
    assertFalse(f.auth.isPrivileged(ident, "other"));
    Fixture metalake = new Fixture();
    MetadataObject root =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofMetalake("m"), Entity.EntityType.METALAKE);
    when(metalake.authorizer.isOwner(
            same(metalake.principal), eq("m"), eq(root), any(AuthorizationRequestContext.class)))
        .thenReturn(true);
    assertTrue(metalake.auth.isPrivileged(ident, "alice"));
    Fixture admin = new Fixture();
    when(admin.authorizer.isServiceAdmin()).thenReturn(true);
    assertTrue(admin.auth.isPrivileged(ident, "alice"));
    verify(admin.authorizer, never()).isOwner(any(), any(), any(), any());
    Fixture ordinary = new Fixture();
    assertThrows(SecurityException.class, () -> ordinary.auth.requirePrivileged(ident));
  }

  @Test
  void persistedOwnershipAndPendingReplayAreSeparateChecks() {
    Fixture f = new Fixture();
    when(f.owners.getOwner(eq("m"), any())).thenReturn(Optional.empty());
    assertThrows(SecurityException.class, () -> f.auth.requireInitialOwner(ident));
    assertDoesNotThrow(() -> f.auth.requirePendingReplayOwner(ident, "alice"));
    for (Owner.Type type : Owner.Type.values()) {
      Owner owner = mock(Owner.class);
      when(owner.name()).thenReturn("transferred");
      when(owner.type()).thenReturn(type);
      when(f.owners.getOwner(eq("m"), any())).thenReturn(Optional.of(owner));
      assertDoesNotThrow(() -> f.auth.requireInitialOwner(ident));
      assertThrows(SecurityException.class, () -> f.auth.requirePendingReplayOwner(ident, "alice"));
      when(owner.name()).thenReturn("alice");
      if (type == Owner.Type.USER) {
        assertDoesNotThrow(() -> f.auth.requirePendingReplayOwner(ident, "alice"));
      } else {
        assertThrows(
            SecurityException.class, () -> f.auth.requirePendingReplayOwner(ident, "alice"));
      }
    }
    assertThrows(SecurityException.class, () -> f.auth.requirePendingReplayOwner(ident, "other"));
    when(f.owners.getOwner(eq("m"), any())).thenThrow(new IllegalStateException("fixture-secret"));
    SecurityException error =
        assertThrows(SecurityException.class, () -> f.auth.requireInitialOwner(ident));
    assertFalse(error.toString().contains("fixture-secret"));
    verify(f.owners, never()).setOwner(any(), any(), any(), any());
  }

  private static final class DerivedPassThrough extends PassThroughAuthorizer {}

  private static final class Fixture {
    private final Config config = mock(Config.class);
    private final GravitinoEnv env = mock(GravitinoEnv.class);
    private final GravitinoAuthorizer authorizer = mock(GravitinoAuthorizer.class);
    private final OwnerDispatcher owners = mock(OwnerDispatcher.class);
    private final UserPrincipal principal =
        new UserPrincipal("alice", List.of(new UserGroup(Optional.empty(), "analysts")));
    private final DeltaAuthorization auth = new DeltaAuthorization(() -> env, () -> principal);

    private Fixture() {
      when(config.get(Configs.ENABLE_AUTHORIZATION)).thenReturn(true);
      when(env.config()).thenReturn(config);
      when(env.gravitinoAuthorizer()).thenReturn(authorizer);
      when(env.internalOwnerDispatcher()).thenReturn(owners);
      when(env.internalAccessControlDispatcher()).thenReturn(mock(AccessControlDispatcher.class));
    }
  }

  @Test
  void freshDecisionContextAndOnlyCatalogOrMetalakeOwnership() {
    Fixture f = new Fixture();
    List<AuthorizationRequestContext> contexts = new ArrayList<>();
    when(f.authorizer.isOwner(same(f.principal), eq("m"), any(), any()))
        .thenAnswer(
            call -> {
              MetadataObject object = call.getArgument(2);
              assertTrue(
                  object.type() == MetadataObject.Type.METALAKE
                      || object.type() == MetadataObject.Type.CATALOG);
              contexts.add(call.getArgument(3));
              return false;
            });
    assertFalse(f.auth.isPrivileged(ident, "alice"));
    assertFalse(f.auth.isPrivileged(ident, "alice"));
    assertEquals(4, contexts.size());
    assertSame(contexts.get(0), contexts.get(1));
    assertNotSame(contexts.get(0), contexts.get(2));
  }
}

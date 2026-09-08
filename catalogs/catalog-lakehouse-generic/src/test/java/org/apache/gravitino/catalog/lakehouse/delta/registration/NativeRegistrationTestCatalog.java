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
package org.apache.gravitino.catalog.lakehouse.delta.registration;

import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.utils.PrincipalUtils;

/** Distinct test provider: every open constructs real native evidence, never synthetic outcomes. */
public final class NativeRegistrationTestCatalog extends RegistrationTestCatalog {
  /** Creates a ServiceLoader test provider. */
  public NativeRegistrationTestCatalog() {}

  /** {@inheritDoc} */
  @Override
  public String shortName() {
    return "delta-native-registration-test";
  }

  /** {@inheritDoc} */
  @Override
  protected RegistrationLifecycleFixture createTables(EntityStore store, SupportsSchemas schemas) {
    NativeRegistrationCommitVerifier verifier =
        new NativeRegistrationCommitVerifier(NativeRegistrationTestSupport.storageConfiguration());
    return new RegistrationLifecycleFixture(
        store,
        schemas,
        GravitinoEnv.getInstance().idGenerator(),
        PrincipalUtils::getCurrentUserName,
        "registration-recovery",
        NativeRegistrationTestSupport::writerStopped,
        verifier,
        verifier,
        true);
  }
}

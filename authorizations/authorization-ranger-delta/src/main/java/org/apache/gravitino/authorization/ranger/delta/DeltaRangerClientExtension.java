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
package org.apache.gravitino.authorization.ranger.delta;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import javax.annotation.Nullable;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Response;
import org.apache.gravitino.authorization.ranger.RangerClientExtension;
import org.apache.gravitino.authorization.ranger.reference.VXGroup;
import org.apache.gravitino.authorization.ranger.reference.VXGroupList;
import org.apache.gravitino.authorization.ranger.reference.VXUser;
import org.apache.gravitino.authorization.ranger.reference.VXUserList;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.ranger.RangerClient;
import org.codehaus.jackson.map.ObjectMapper;

/** Ranger 2.8 Map boundary for the existing Jackson 1 principal models. */
final class DeltaRangerClientExtension extends RangerClientExtension {
  private static final API SEARCH_USER =
      new API("/service/xusers/users", HttpMethod.GET, Response.Status.OK);
  private static final API SEARCH_GROUP =
      new API("/service/xusers/groups", HttpMethod.GET, Response.Status.OK);
  private static final API CREATE_USER =
      new API("/service/xusers/users/external", HttpMethod.POST, Response.Status.OK);
  private static final API CREATE_GROUP =
      new API("/service/xusers/groups", HttpMethod.POST, Response.Status.OK);
  private final Method mapCall;
  private final ObjectMapper legacy = new ObjectMapper();

  DeltaRangerClientExtension(String host, String auth, String user, String password) {
    super(host, auth, user, password);
    try {
      mapCall =
          RangerClient.class.getDeclaredMethod(
              "callAPI", API.class, Map.class, Object.class, Class.class);
      mapCall.setAccessible(true);
    } catch (ReflectiveOperationException e) {
      throw new AuthorizationPluginException("Pinned Ranger 2.8 principal API unavailable");
    }
  }
  /** {@inheritDoc} */
  @Override
  public VXUserList searchUser(Map<String, String> filters) {
    return legacy.convertValue(call(SEARCH_USER, filters, null), VXUserList.class);
  }
  /** {@inheritDoc} */
  @Override
  public VXGroupList searchGroup(Map<String, String> filters) {
    return legacy.convertValue(call(SEARCH_GROUP, filters, null), VXGroupList.class);
  }
  /** {@inheritDoc} */
  @Override
  public Boolean createUser(VXUser user) {
    call(CREATE_USER, null, legacy.convertValue(user, Map.class));
    return true;
  }
  /** {@inheritDoc} */
  @Override
  public Boolean createGroup(VXGroup group) {
    call(CREATE_GROUP, null, legacy.convertValue(group, Map.class));
    return true;
  }

  private Object call(API api, @Nullable Map<String, String> filters, @Nullable Map<?, ?> request) {
    try {
      return mapCall.invoke(this, api, filters, request, Map.class);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new AuthorizationPluginException(
          "Ranger principal request failed; authoritative readback required");
    }
  }
}

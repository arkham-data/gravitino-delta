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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.base.Preconditions;
import com.sun.jersey.api.client.ClientResponse;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
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
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerBaseModelObject;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.codehaus.jackson.map.ObjectMapper;

/** Ranger 2.8 wire boundaries for legacy principals and timestamp-bearing policy models. */
final class DeltaRangerClientExtension extends RangerClientExtension {
  private static final API SEARCH_USER =
      new API("/service/xusers/users", HttpMethod.GET, Response.Status.OK);
  private static final API SEARCH_GROUP =
      new API("/service/xusers/groups", HttpMethod.GET, Response.Status.OK);
  private static final API CREATE_USER =
      new API("/service/xusers/users/external", HttpMethod.POST, Response.Status.NO_CONTENT);
  private static final API CREATE_GROUP =
      new API("/service/xusers/groups", HttpMethod.POST, Response.Status.OK);
  private final Method mapCall;
  private final Method principalResponse;
  private final ObjectMapper legacy = new ObjectMapper();
  private final JsonMapper wire = new JsonMapper();

  DeltaRangerClientExtension(String host, String auth, String user, String password) {
    super(host, auth, user, password);
    try {
      mapCall =
          RangerClient.class.getDeclaredMethod(
              "callAPI", API.class, Map.class, Object.class, Class.class);
      mapCall.setAccessible(true);
      principalResponse =
          RangerClient.class.getDeclaredMethod(
              "responseHandler", API.class, Map.class, Object.class);
      principalResponse.setAccessible(true);
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
    VXUserList observed = searchUser(Collections.singletonMap("name", user.getName()));
    if (observed.getListSize() != 1
        || !user.getName().equals(observed.getList().get(0).getName())) {
      throw new AuthorizationPluginException(
          "Ranger external user readback was not an exact match; authoritative reconciliation required");
    }
    return true;
  }
  /** {@inheritDoc} */
  @Override
  public Boolean createGroup(VXGroup group) {
    call(CREATE_GROUP, null, legacy.convertValue(group, Map.class));
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public RangerRole createRole(String service, RangerRole role) throws RangerServiceException {
    return write(
        CREATE_ROLE, Collections.singletonMap("serviceName", service), role, RangerRole.class);
  }

  /** {@inheritDoc} */
  @Override
  public RangerRole updateRole(long id, RangerRole role) throws RangerServiceException {
    return write(UPDATE_ROLE_BY_ID.applyUrlFormat(id), null, role, RangerRole.class);
  }

  /** {@inheritDoc} */
  @Override
  public RangerPolicy createPolicy(RangerPolicy policy) throws RangerServiceException {
    requireResources(policy);
    return write(CREATE_POLICY, null, policy, RangerPolicy.class);
  }

  /** {@inheritDoc} */
  @Override
  public RangerPolicy updatePolicy(long id, RangerPolicy policy) throws RangerServiceException {
    requireResources(policy);
    return write(UPDATE_POLICY_BY_ID.applyUrlFormat(id), null, policy, RangerPolicy.class);
  }

  /** {@inheritDoc} */
  @Override
  public RangerPolicy updatePolicy(String service, String name, RangerPolicy policy)
      throws RangerServiceException {
    requireResources(policy);
    return write(
        UPDATE_POLICY_BY_NAME.applyUrlFormat(service, name), null, policy, RangerPolicy.class);
  }

  private void requireResources(RangerPolicy policy) {
    Preconditions.checkArgument(
        !policy.getResources().isEmpty(), "Ranger policy resources cannot be empty!");
  }

  private <T> T write(
      API api,
      @Nullable Map<String, String> filters,
      RangerBaseModelObject model,
      Class<T> response)
      throws RangerServiceException {
    // Ranger 2.8 JsonUtils formats Date values in a form its Admin rejects. A detached Map
    // preserves every model field while retaining the epoch-millisecond format returned by Admin.
    Map<String, Object> request =
        wire.convertValue(model, new TypeReference<Map<String, Object>>() {});
    if (model.getCreateTime() != null) {
      request.put("createTime", model.getCreateTime().toInstant().toEpochMilli());
    }
    if (model.getUpdateTime() != null) {
      request.put("updateTime", model.getUpdateTime().toInstant().toEpochMilli());
    }
    try {
      return response.cast(mapCall.invoke(this, api, filters, request, response));
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof RangerServiceException) {
        throw (RangerServiceException) e.getCause();
      }
      throw new AuthorizationPluginException(
          "Ranger model request failed; authoritative readback required");
    } catch (IllegalAccessException e) {
      throw new AuthorizationPluginException("Pinned Ranger 2.8 model API unavailable");
    }
  }

  @Nullable
  private Object call(API api, @Nullable Map<String, String> filters, @Nullable Map<?, ?> request) {
    ClientResponse response = null;
    try {
      response = (ClientResponse) principalResponse.invoke(this, api, filters, request);
      // Pinned responseHandler returns a raw 503 response instead of throwing. Never decode an
      // error body as a successful principal result, even when it happens to look like valid JSON.
      if (response == null || response.getStatus() != api.getExpectedStatus().getStatusCode()) {
        throw new AuthorizationPluginException(
            "Ranger principal status failed; authoritative readback required");
      }
      if (api == CREATE_USER) {
        return null;
      }
      Map<?, ?> decoded = JsonUtilsV2.readResponse(response, Map.class);
      if (decoded == null) {
        throw new AuthorizationPluginException("Ranger principal response must be a JSON object");
      }
      return decoded;
    } catch (Exception e) {
      throw new AuthorizationPluginException(
          "Ranger principal request failed; authoritative readback required");
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }
}

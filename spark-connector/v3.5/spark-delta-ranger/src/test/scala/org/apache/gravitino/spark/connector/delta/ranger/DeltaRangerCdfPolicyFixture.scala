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

package org.apache.gravitino.spark.connector.delta.ranger

import java.util.Collections
import scala.collection.JavaConverters._
import org.apache.hadoop.security.UserGroupInformation
import org.apache.ranger.plugin.model.{RangerPolicy, RangerRole, RangerValiditySchedule}
import org.apache.ranger.plugin.model.RangerPolicy._
import org.apache.ranger.plugin.service.RangerBasePlugin
import org.apache.ranger.plugin.util.{RangerRoles, ServicePolicies}

/** Real Ranger 2.8 evaluator with local policy publication and the existing named catalog fixture.
  */
private[ranger] final class DeltaRangerCdfPolicyFixture extends DeltaRangerPolicyEngine.Backend {
  val transport = new TestDeltaRangerNativeSelect.Fixture
  val nativePlugin = new RangerBasePlugin(transport.plugin.getConfig)
  private var version = 0L
  val user = UserGroupInformation.getCurrentUser.getShortUserName
  val engine = new DeltaRangerPolicyEngine.Owner(() => this)
    .acquire(new DeltaRangerConfig("service", "spark", "lake", nativePlugin.getConfig))

  override def plugin(): RangerBasePlugin = nativePlugin
  override def start(): Unit = { roles(granted = true); publish() }
  override def close(): Unit = { nativePlugin.cleanup(); transport.engine.close() }

  def roles(granted: Boolean): Unit = {
    val role = new RangerRole
    role.setName("cdf-reader")
    role.setUsers(
      Collections.singletonList(new RangerRole.RoleMember(if (granted) user else "nobody", false))
    )
    val roles = new RangerRoles
    roles.setServiceName("service")
    roles.setRoleVersion(1L)
    roles.setRangerRoles(Collections.singleton(role))
    nativePlugin.setRoles(roles)
  }

  def publish(
      denied: Set[String] = Set.empty,
      filter: Option[String] = None,
      masks: Map[String, String] = Map.empty,
      schedule: Option[RangerValiditySchedule] = None,
      grantSchedule: Option[RangerValiditySchedule] = None
  ): Unit = {
    def resource(id: Long, kind: Int, columns: Seq[String]): RangerPolicy = {
      val policy = new RangerPolicy
      policy.setId(id); policy.setName("cdf-" + id); policy.setService("service")
      policy.setPolicyType(kind); policy.setIsEnabled(true)
      val resources = Map(
        "catalog" -> new RangerPolicyResource("a"),
        "database" -> new RangerPolicyResource("db"),
        "table" -> new RangerPolicyResource("t")
      ) ++
        (if (columns.isEmpty) Map.empty[String, RangerPolicyResource]
         else
           Map("column" -> new RangerPolicyResource(columns.asJava, false, false)))
      policy.setResources(resources.asJava)
      policy
    }
    def access(item: RangerPolicyItem): Unit = {
      item.setRoles(Collections.singletonList("cdf-reader"))
      item.setAccesses(Collections.singletonList(new RangerPolicyItemAccess("select", true)))
    }
    val grant = resource(1L, 0, Seq("*"))
    val allowed = new RangerPolicyItem; access(allowed)
    grant.setPolicyItems(Collections.singletonList(allowed))
    grantSchedule.foreach(s => grant.setValiditySchedules(Collections.singletonList(s)))
    val policies = scala.collection.mutable.ArrayBuffer(grant)
    if (denied.nonEmpty) {
      val deny = resource(2L, 0, denied.toSeq)
      val item = new RangerPolicyItem; access(item)
      deny.setDenyPolicyItems(Collections.singletonList(item)); policies += deny
    }
    filter.foreach { value =>
      val policy = resource(3L, 2, Seq.empty)
      val item = new RangerRowFilterPolicyItem; access(item)
      item.setRowFilterInfo(new RangerPolicyItemRowFilterInfo(value))
      policy.setRowFilterPolicyItems(Collections.singletonList(item))
      schedule.foreach(s => policy.setValiditySchedules(Collections.singletonList(s)))
      policies += policy
    }
    masks.toSeq.zipWithIndex.foreach { case ((column, expression), index) =>
      val policy = resource(10L + index, 1, Seq(column))
      val item = new RangerDataMaskPolicyItem; access(item)
      item.setDataMaskInfo(new RangerPolicyItemDataMaskInfo("CUSTOM", null, expression))
      policy.setDataMaskPolicyItems(Collections.singletonList(item)); policies += policy
    }
    val published = new ServicePolicies
    published.setServiceName("service"); published.setServiceDef(transport.plugin.getServiceDef)
    version += 1; published.setPolicyVersion(version)
    published.setPolicies(policies.asJava)
    published.setServiceConfig(
      Collections.singletonMap("ranger.plugin.spark.gravitino.metalake", "lake")
    )
    nativePlugin.setPolicies(published)
  }

  def withSpark(
      setup: org.apache.spark.sql.SparkSession => Unit
  )(test: org.apache.spark.sql.SparkSession => Unit): Unit =
    try {
      NativeSelectFixture.catalogEngines = Map("a" -> engine)
      NativeSelectFixture.withSpark(
        test,
        Some(NativeSelectFixture.extension(transport)),
        beforeExtension = setup
      )
    } finally engine.close()
}

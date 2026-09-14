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
// Optional production authorization distribution with an independently pinned Ranger closure.
plugins {
  id("java")
}
configurations.configureEach {
  resolutionStrategy.eachDependency {
    if (requested.group == "org.apache.ranger") { useVersion("2.8.0") }
  }
}
dependencies {
  compileOnly(project(":api"))
  compileOnly(project(":core"))
  implementation(project(":authorizations:authorization-common")) { isTransitive = false }
  implementation(project(":authorizations:authorization-ranger")) { isTransitive = false }
  implementation(libs.jackson.databind)
  implementation(libs.guava)
  implementation(libs.commons.lang3)
  implementation("commons-lang:commons-lang:2.6")
  implementation(libs.slf4j.api)
  implementation(libs.javax.jaxb.api)
  implementation(libs.javax.ws.rs.api)
  implementation(libs.jettison)
  implementation(libs.mail)
  implementation(libs.rome)
  implementation(libs.hadoop3.client.api)
  implementation(libs.hadoop3.client.runtime)
  implementation("org.codehaus.jackson:jackson-mapper-asl:1.9.13")
  implementation("org.apache.ranger:ranger-intg:2.8.0") {
    exclude("org.apache.hive", "hive-storage-api")
    exclude("org.apache.lucene")
    exclude("org.apache.solr")
    exclude("org.apache.kafka")
    exclude("org.elasticsearch")
    exclude("org.elasticsearch.client")
    exclude("org.elasticsearch.plugin")
    exclude("org.apache.ranger", "ranger-plugins-audit")
    exclude("org.apache.ranger", "ranger-plugins-cred")
    exclude("org.apache.ranger", "ranger-plugin-classloader")
    exclude("org.eclipse.jetty")
    exclude("org.apache.hadoop", "hadoop-common")
    exclude("javax.ws.rs")
  }
  testImplementation(project(":common"))
  testImplementation(project(":delta:delta-common"))
  testImplementation(project(":server-common"))
  testImplementation(project(":clients:client-java"))
  testImplementation(project(":server"))
  testImplementation(project(":integration-test-common", "testArtifacts"))
  testImplementation(project(":catalogs:catalog-lakehouse-delta"))

  testImplementation(project(":api"))
  testImplementation(project(":core"))
  testImplementation(project(":authorizations:authorization-ranger"))
  testImplementation(project(":authorizations:authorization-common"))
  testImplementation("org.apache.ranger:ranger-intg:2.8.0") { isTransitive = false }
  testImplementation("org.apache.ranger:ranger-plugins-common:2.8.0") { isTransitive = false }
  testImplementation("org.codehaus.jackson:jackson-mapper-asl:1.9.13")
  testImplementation(libs.jackson.annotations)
  testImplementation(libs.javax.jaxb.api)
  testImplementation(libs.hadoop3.client.api)
  testImplementation(libs.guava)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.slf4j.api)
  // RangerRESTUtils initializes gethostname4j, which needs JNA at runtime.
  testRuntimeOnly("net.java.dev.jna:jna:5.13.0")
  testRuntimeOnly(libs.junit.jupiter.engine)
  testRuntimeOnly(libs.slf4j.simple)
}

val rangerDeltaAdminEnabled = providers.gradleProperty("rangerDeltaAdminEnabled").orNull == "true"
val rangerDeltaAdminUrl = providers.gradleProperty("rangerDeltaAdminUrl").orNull
val rangerDeltaAdminCredentialsFile = providers.gradleProperty("rangerDeltaAdminCredentialsFile").orNull
if (rangerDeltaAdminEnabled) {
  require(providers.gradleProperty("skipDockerTests").orNull == "false") {
    "rangerDeltaAdminEnabled requires -PskipDockerTests=false"
  }
  require(!rangerDeltaAdminUrl.isNullOrBlank()) {
    "rangerDeltaAdminEnabled requires rangerDeltaAdminUrl"
  }
  require(!rangerDeltaAdminCredentialsFile.isNullOrBlank()) {
    "rangerDeltaAdminEnabled requires rangerDeltaAdminCredentialsFile"
  }
}
tasks.test {
  if (rangerDeltaAdminEnabled) {
    extensions.extraProperties["gravitinoExternalTestEndpoints"] = true
    systemProperty("gravitino.ranger.delta.admin.enabled", "true")
    systemProperty("gravitino.ranger.delta.admin.url", requireNotNull(rangerDeltaAdminUrl))
    systemProperty(
      "gravitino.ranger.delta.admin.credentialsFile",
      requireNotNull(rangerDeltaAdminCredentialsFile)
    )
  }
}

val runtimeJars by tasks.registering(Sync::class) {
  from(configurations.runtimeClasspath)
  into(layout.buildDirectory.dir("runtime-libs"))
}
val copyAuthorizationLibs by tasks.registering(Sync::class) {
  dependsOn(tasks.jar, runtimeJars)
  from(tasks.jar.flatMap { it.archiveFile })
  from(layout.buildDirectory.dir("runtime-libs")) {
    exclude("guava-*.jar", "log4j-*.jar", "slf4j-*.jar")
  }
  into("$rootDir/distribution/package/authorizations/ranger-delta/libs")
}
tasks.register("copyLibAndConfig") { dependsOn(copyAuthorizationLibs) }
tasks.withType<Javadoc>().configureEach {
  (options as CoreJavadocOptions).addStringOption("Xwerror", "-quiet")
  isFailOnError = true
}

providers.gradleProperty("rangerDeltaCallbackFixture").orNull?.let { fixture ->
  require(providers.gradleProperty("skipDockerTests").orNull == "false") {
    "rangerDeltaCallbackFixture requires -PskipDockerTests=false"
  }
  tasks.test {
    extensions.extraProperties["gravitinoExternalTestEndpoints"] = true
    systemProperty("delta.ranger.callback.fixture", fixture)
  }
}

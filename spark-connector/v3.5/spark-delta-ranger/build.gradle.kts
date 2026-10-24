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

plugins {
  id("java")
  id("scala")
}

val deltaSparkVersion = providers.gradleProperty("deltaSparkVersion").getOrElse("3.5.4")
val deltaVersion = providers.gradleProperty("deltaVersion").getOrElse("3.3.0")
require(deltaSparkVersion in setOf("3.5.4", "3.5.6")) { "Unsupported deltaSparkVersion" }
require(deltaVersion in setOf("3.3.0", "3.3.2")) { "Unsupported deltaVersion" }
require(providers.gradleProperty("scalaVersion").getOrElse("2.12") == "2.12") {
  "The Delta Ranger adapter requires Scala 2.12"
}
// Pin deployed Spark classpaths without changing Zinc or formatter tool Scala versions.
configurations.matching {
  it.name in setOf("compileClasspath", "runtimeClasspath", "testCompileClasspath", "testRuntimeClasspath")
}.configureEach {
  resolutionStrategy.eachDependency {
    if (requested.group == "org.apache.ranger") { useVersion("2.8.0") }
    if (requested.group == "org.apache.spark") { useVersion(deltaSparkVersion) }
    if (requested.group == "org.scala-lang") { useVersion("2.12.21") }
  }
}
dependencies {
  implementation("org.apache.kyuubi:kyuubi-spark-authz_2.12:1.10.2") {
    exclude("org.apache.hadoop")
    exclude("org.apache.spark")
    exclude("org.scala-lang")
    exclude("io.delta")
    exclude("org.apache.kyuubi", "kyuubi-util")
    exclude("org.apache.kyuubi", "kyuubi-util-scala_2.12")
  }
  listOf("ranger-plugins-common", "ranger-plugins-cred", "ranger-audit-core", "ranger-plugin-classloader").forEach {
    implementation("org.apache.ranger:$it:2.8.0") { exclude("org.apache.hadoop") }
  }
  // The base Delta runtime alone supplies the 33 compatible Kyuubi utility classes.
  compileOnly("org.apache.kyuubi:kyuubi-spark-connector-hive_2.12:1.11.0") { isTransitive = false }
  compileOnly(project(":spark-connector:spark-delta-3.5")) { isTransitive = false }
  compileOnly("io.delta:delta-spark_2.12:$deltaVersion")
  compileOnly(project(":api"))
  compileOnly(project(":delta:delta-common"))
  compileOnly(project(":clients:client-java")) { isTransitive = false }
  compileOnly(project(":spark-connector:spark-common")) { isTransitive = false }
  compileOnly("org.apache.spark:spark-sql_2.12:$deltaSparkVersion")
  compileOnly("org.scala-lang:scala-library:2.12.21")
  compileOnly("com.google.code.findbugs:jsr305:3.0.2")
  testCompileOnly("org.apache.kyuubi:kyuubi-spark-connector-hive_2.12:1.11.0") { isTransitive = false }
  testCompileOnly(project(":spark-connector:spark-delta-3.5")) { isTransitive = false }
  testImplementation("io.delta:delta-spark_2.12:$deltaVersion")
  testImplementation(project(":api"))
  testImplementation(project(":common"))
  testImplementation(project(":delta:delta-common"))
  testImplementation(project(":clients:client-java"))
  testImplementation(project(":spark-connector:spark-common")) { isTransitive = false }
  testImplementation("org.apache.spark:spark-sql_2.12:$deltaSparkVersion")
  testImplementation("org.scala-lang:scala-library:2.12.21")
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mockito.core)
  testRuntimeOnly(project(":spark-connector:spark-delta-runtime-3.5", configuration = "shadow"))
  // Live discovery uses the unshaded, non-transitive spark-common test dependency.
  testRuntimeOnly(libs.caffeine)
  testRuntimeOnly(libs.junit.jupiter.engine)
}
tasks.named<JavaCompile>("compileJava") { options.release.set(8) }
tasks.withType<Javadoc>().configureEach {
  (options as CoreJavadocOptions).addStringOption("Xwerror", "-quiet")
  isFailOnError = true
}
tasks.test {
  // The deterministic journal race mock delegates private Files helpers in the test JVM only.
  jvmArgs("--add-opens=java.base/java.nio.file=ALL-UNNAMED")
  maxParallelForks = 1
  environment("SPARK_LOCAL_IP", "127.0.0.1")
}

tasks.withType<ScalaCompile>().configureEach {
  scalaCompileOptions.additionalParameters = listOf("-target:jvm-1.8")
}

configure<com.diffplug.gradle.spotless.SpotlessExtension> {
  scala {
    scalafmt("3.8.3").configFile(file(".scalafmt.conf"))
  }
}

val rangerDeltaProductionEnabled = providers.gradleProperty("rangerDeltaProductionEnabled").orNull == "true"
val rangerDeltaProductionProperties = mapOf(
  "gravitinoUrl" to providers.gradleProperty("rangerDeltaProductionGravitinoUrl").orNull,
  "adminUrl" to providers.gradleProperty("rangerDeltaProductionAdminUrl").orNull,
  "fixtureFile" to providers.gradleProperty("rangerDeltaProductionFixtureFile").orNull
)
if (rangerDeltaProductionEnabled) {
  require(providers.gradleProperty("skipDockerTests").orNull == "false") {
    "rangerDeltaProductionEnabled requires -PskipDockerTests=false"
  }
  require(rangerDeltaProductionProperties.values.all { !it.isNullOrBlank() }) {
    "Production acceptance requires explicit Gravitino/Admin endpoints and a private fixture file"
  }
} else {
  require(rangerDeltaProductionProperties.values.all { it == null }) {
    "Production acceptance endpoints require explicit rangerDeltaProductionEnabled=true"
  }
}
tasks.test {
  if (rangerDeltaProductionEnabled) {
    extensions.extraProperties["gravitinoExternalTestEndpoints"] = true
    systemProperty("gravitino.ranger.delta.production.enabled", "true")
    rangerDeltaProductionProperties.forEach { (key, value) ->
      systemProperty("gravitino.ranger.delta.production.$key", requireNotNull(value))
    }
  }
}

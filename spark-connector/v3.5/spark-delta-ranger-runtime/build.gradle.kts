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
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
  `maven-publish`
  id("java")
  alias(libs.plugins.shadow)
}

val deltaSparkVersion = providers.gradleProperty("deltaSparkVersion").getOrElse("3.5.4")
val deltaVersion = providers.gradleProperty("deltaVersion").getOrElse("3.3.0")
require(deltaSparkVersion in setOf("3.5.4", "3.5.6")) { "Unsupported deltaSparkVersion" }
require(deltaVersion in setOf("3.3.0", "3.3.2")) { "Unsupported deltaVersion" }
require(providers.gradleProperty("scalaVersion").getOrElse("2.12") == "2.12") {
  "The Delta Ranger runtime requires Scala 2.12"
}
val baseName = "${rootProject.name}-spark-connector-delta-ranger-runtime-3.5_2.12"

configurations.configureEach {
  resolutionStrategy.eachDependency {
    if (requested.group == "org.apache.ranger") { useVersion("2.8.0") }
  }
  exclude(group = "org.apache.spark")
  exclude(group = "org.apache.hadoop")
  exclude(group = "org.scala-lang")
  exclude(group = "io.delta")
  exclude(group = "org.slf4j")
  // Both qualified Spark distributions provide Servlet 4 and JSR-305.
  exclude(group = "javax.servlet")
  exclude(group = "com.google.code.findbugs", module = "jsr305")
  exclude(group = "org.apache.logging.log4j")
  exclude(group = "org.apache.kyuubi", module = "kyuubi-util")
  exclude(group = "org.apache.kyuubi", module = "kyuubi-util-scala_2.12")
  // The bundle duplicates jersey-client/core; the normal client jars suffice for downloads.
  exclude(group = "com.sun.jersey", module = "jersey-bundle")
  // Kyuubi authz embeds the selected non-JNA Hostname implementation.
  exclude(group = "com.kstruct", module = "gethostname4j")
}

dependencies {
  implementation(project(":spark-connector:spark-delta-ranger-3.5"))
}

tasks.withType<ShadowJar>().configureEach {
  isZip64 = true
  configurations = listOf(project.configurations.runtimeClasspath.get())
  archiveFileName.set("$baseName-$version.jar")
  archiveClassifier.set("")
  exclude("META-INF/*.SF", "META-INF/*.RSA", "META-INF/*.DSA")
  exclude("META-INF/versions/**/module-info.class", "module-info.class")
  // Java 21 optimizations are outside the qualified Java 17 launcher boundary.
  exclude("META-INF/versions/21/**")
  manifest.attributes["Multi-Release"] = "true"
  // The base runtime remains the only Kyuubi utility/private Guava supplier.
  exclude("org/apache/kyuubi/util/**")
  // JNA retains its JNI class names; neither supported Spark distribution nor the base supplies it.
  // Keep other implementation dependencies private from Spark and the base connector.
  listOf(
    "com.fasterxml", "com.google", "com.sun.jersey",
    "com.kstruct", "com.nimbusds", "com.carrotsearch", "com.thoughtworks",
    "net.minidev", "org.apache.commons", "org.ow2", "org.objectweb.asm",
    "io.airlift", "org.joda", "javax.ws.rs", "javax.xml.bind", "javax.activation",
    "org.apache.http", "org.apache.hc"
  ).forEach { relocate(it, "org.apache.gravitino.ranger.shaded.$it") }
  listOf("KrbPasswordSaverLoginModule", "SecureClientLogin", "SecureClientLoginConfiguration").forEach {
    relocate("org.apache.hadoop.security.$it", "org.apache.gravitino.ranger.shaded.hadoop.$it")
  }
  mergeServiceFiles()
}

publishing {
  publications {
    withType<MavenPublication>().configureEach { artifactId = baseName }
  }
}

tasks.jar {
  dependsOn(tasks.named("shadowJar"))
  archiveClassifier.set("empty")
}

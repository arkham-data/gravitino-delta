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

val scalaVersion =
  providers.gradleProperty("scalaVersion").getOrElse(extra["defaultScalaVersion"].toString())
val deltaSparkVersion = providers.gradleProperty("deltaSparkVersion").getOrElse("3.5.4")
val deltaVersion = providers.gradleProperty("deltaVersion").getOrElse("3.3.0")
val sparkMajorVersion = deltaSparkVersion.substringBeforeLast(".")
val kyuubiVersion = libs.versions.kyuubi4spark.get()
val baseName =
  "${rootProject.name}-spark-connector-delta-runtime-${sparkMajorVersion}_$scalaVersion"

require(scalaVersion == "2.12") { "The Delta runtime requires Scala 2.12" }
require(deltaSparkVersion in setOf("3.5.4", "3.5.6")) { "Unsupported deltaSparkVersion" }
require(deltaVersion in setOf("3.3.0", "3.3.2")) { "Unsupported deltaVersion" }

configurations.configureEach {
  resolutionStrategy.eachDependency {
    if (requested.group == "org.apache.spark") {
      useVersion(deltaSparkVersion)
    }
    if (requested.group == "org.scala-lang") {
      useVersion("2.12.21")
    }
  }
}

dependencies {
  implementation(project(":clients:client-java-runtime", configuration = "shadow"))
  implementation(project(":spark-connector:spark-3.5")) {
    exclude(group = "com.google.guava", module = "guava")
    exclude(group = "com.google.errorprone", module = "error_prone_annotations")
    exclude(group = "org.apache.commons", module = "commons-lang3")
  }
  implementation(project(":spark-connector:spark-delta-3.5")) {
    exclude(group = "com.fasterxml.jackson.core")
  }
  implementation("org.apache.kyuubi:kyuubi-spark-connector-hive_$scalaVersion:$kyuubiVersion")
}

tasks.withType<ShadowJar>().configureEach {
  isZip64 = true
  configurations = listOf(project.configurations.runtimeClasspath.get())
  archiveFileName.set("$baseName-$version.jar")
  archiveClassifier.set("")

  exclude("org/slf4j/**")

  relocate(
    "com.google.thirdparty.publicsuffix",
    "org.apache.kyuubi.shade.com.google.thirdparty.publicsuffix"
  )
  relocate("com.google", "org.apache.gravitino.shaded.com.google")
  relocate("google", "org.apache.gravitino.shaded.google")
  relocate("com.fasterxml", "org.apache.gravitino.shaded.com.fasterxml")
  relocate("org.apache.commons", "org.apache.gravitino.shaded.org.apache.commons")
  relocate("org.apache.hc", "org.apache.gravitino.shaded.org.apache.hc")
  relocate(
    "com.github.benmanes.caffeine",
    "org.apache.gravitino.shaded.com.github.benmanes.caffeine"
  )

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

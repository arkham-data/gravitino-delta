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
}

val deltaKernelVersion = providers.gradleProperty("deltaKernelVersion").getOrElse("3.3.0")
require(deltaKernelVersion in setOf("3.3.0", "3.3.2")) {
  "deltaKernelVersion must be 3.3.0 or 3.3.2"
}

configurations.configureEach {
  // Hadoop HTTP services are unused; their Jersey 1 shaded ASM conflicts with server Jersey 2.
  exclude(group = "com.sun.jersey")
  exclude(group = "javax.ws.rs", module = "jsr311-api")
  exclude(group = "ch.qos.reload4j", module = "reload4j")
  exclude(group = "org.slf4j", module = "slf4j-reload4j")
  resolutionStrategy.eachDependency {
    if (requested.group == "io.delta" && (requested.name.startsWith("delta-kernel-") || requested.name == "delta-storage")) {
      useVersion(deltaKernelVersion)
    }
    if (requested.group == "org.apache.hadoop") {
      useVersion(libs.versions.hadoop3.asProvider().get())
      because("metadata storage lifecycle is characterized against Hadoop 3.3.6")
    }
  }
}

dependencies {
  compileOnly(project(":api"))
  compileOnly(project(":common"))
  compileOnly(project(":core"))
  compileOnly(project(":catalogs:catalog-common"))
  implementation(libs.hadoop3.aws)
  implementation(libs.jackson.databind)
  implementation(project(":delta:delta-common"))
  implementation("io.delta:delta-kernel-api:$deltaKernelVersion")
  implementation("io.delta:delta-kernel-defaults:$deltaKernelVersion")
  implementation(libs.hadoop3.common)
  compileOnly("com.google.code.findbugs:jsr305:3.0.2")
  testImplementation(project(":api"))
  testImplementation(project(":common"))
  testImplementation(project(":core"))
  testImplementation(project(":server-common"))
  testImplementation(project(":clients:client-java"))
  testImplementation(project(":server"))
  testImplementation(project(":integration-test-common", "testArtifacts"))
  testImplementation(libs.commons.lang3)
  testImplementation(libs.commons.io)
  testImplementation(libs.guava)
  testImplementation(project(":catalogs:catalog-common"))
  testImplementation(libs.mockito.inline)
  testImplementation(libs.jackson.databind)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.withType<Test>().configureEach {
  inputs.property("deltaKernelVersion", deltaKernelVersion)
  systemProperty("deltaKernelVersion", deltaKernelVersion)
  systemProperty("log4j.configurationFile", file("src/test/resources/log4j2.properties").absolutePath)
  if (project.hasProperty("skipITs")) { exclude("**/integration/test/**") }
}

tasks.withType<Javadoc>().configureEach {
  options {
    (this as CoreJavadocOptions).addStringOption("Xwerror", "-quiet")
    isFailOnError = true
  }
}

val nativePublicationFixture = sourceSets.create("nativePublicationFixture")
dependencies {
  add(nativePublicationFixture.implementationConfigurationName, project(":delta:delta-common"))
  add(nativePublicationFixture.implementationConfigurationName, "org.apache.spark:spark-sql_2.12:3.5.6")
  add(nativePublicationFixture.implementationConfigurationName, "io.delta:delta-spark_2.12:$deltaKernelVersion")
  add(nativePublicationFixture.implementationConfigurationName, libs.hadoop3.aws)
  add(nativePublicationFixture.implementationConfigurationName, libs.hadoop3.client.api)
  add(nativePublicationFixture.implementationConfigurationName, libs.hadoop3.client.runtime)
  add(nativePublicationFixture.runtimeOnlyConfigurationName, libs.bundles.log4j)
}
configurations.matching { it.name.startsWith("nativePublicationFixture") }.configureEach {
  resolutionStrategy.eachDependency {
    if (requested.group == "org.apache.spark") { useVersion("3.5.6") }
    if (requested.group == "org.scala-lang") { useVersion("2.12.21") }
  }
}
val prepareNativePublicationFixture by tasks.registering {
  dependsOn(nativePublicationFixture.classesTaskName)
  val classpathFile = layout.buildDirectory.file("native-publication/classpath.txt")
  val argumentsFile = layout.buildDirectory.file("native-publication/jvm-args.json")
  inputs.files(nativePublicationFixture.runtimeClasspath)
  inputs.property("deltaKernelVersion", deltaKernelVersion)
  outputs.files(classpathFile, argumentsFile)
  doLast {
    classpathFile.get().asFile.parentFile.mkdirs()
    classpathFile.get().asFile.writeText(nativePublicationFixture.runtimeClasspath.asPath)
    argumentsFile.get().asFile.writeText(groovy.json.JsonOutput.toJson(rootProject.extra["extraJvmArgs"]))
  }
}
val runtimeJars by tasks.registering(Sync::class) {
  from(configurations.runtimeClasspath)
  into(layout.buildDirectory.dir("runtime-libs"))
}
tasks.jar { finalizedBy(runtimeJars) }
val copyCatalogLibs by tasks.registering(Sync::class) {
  dependsOn(tasks.jar, runtimeJars)
  from(tasks.jar.flatMap { it.archiveFile })
  from(layout.buildDirectory.dir("runtime-libs")) {
    exclude("guava-*.jar", "log4j-*.jar", "slf4j-*.jar", "reload4j-*.jar", "error_prone_annotations-*.jar")
  }
  into("$rootDir/distribution/package/catalogs/lakehouse-delta/libs")
}
val copyCatalogConfig by tasks.registering(Sync::class) {
  from("src/main/resources") { include("lakehouse-delta.conf") }
  into("$rootDir/distribution/package/catalogs/lakehouse-delta/conf")
}
tasks.register("copyLibAndConfig") { dependsOn(copyCatalogLibs, copyCatalogConfig) }
tasks.withType<Test>().configureEach {
  listOf("endpoint", "region", "bucket", "credentialsFile", "prefix").forEach { suffix ->
    val key = "delta.provider.it.$suffix"
    providers.systemProperty(key).orNull?.let { systemProperty(key, it) }
  }
  if (!project.hasProperty("skipITs") && providers.systemProperty("delta.provider.it.endpoint").isPresent) {
    dependsOn(prepareNativePublicationFixture, "copyLibAndConfig")
    // The existing catalog loader delegates to its parent first. Exclude main output so
    // acceptance can only discover the provider through the packaged production service file.
    classpath = classpath.minus(sourceSets.main.get().output)
    include("**/TestDeltaCatalogNativeRestIT.class")
    systemProperty("delta.provider.fixture.classpathFile", layout.buildDirectory.file("native-publication/classpath.txt").get().asFile.absolutePath)
    systemProperty("delta.provider.fixture.jvmArgsFile", layout.buildDirectory.file("native-publication/jvm-args.json").get().asFile.absolutePath)
    systemProperty("delta.provider.package", "$rootDir/distribution/package/catalogs/lakehouse-delta")
  }
}

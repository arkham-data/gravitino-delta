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
  implementation(project(":delta:delta-common"))
  implementation("io.delta:delta-kernel-api:$deltaKernelVersion")
  implementation("io.delta:delta-kernel-defaults:$deltaKernelVersion")
  implementation(libs.hadoop3.common)
  compileOnly("com.google.code.findbugs:jsr305:3.0.2")
  testImplementation(project(":api"))
  testImplementation(libs.jackson.databind)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.withType<Test>().configureEach {
  inputs.property("deltaKernelVersion", deltaKernelVersion)
  systemProperty("deltaKernelVersion", deltaKernelVersion)
}

tasks.withType<Javadoc>().configureEach {
  options {
    (this as CoreJavadocOptions).addStringOption("Xwerror", "-quiet")
    isFailOnError = true
  }
}

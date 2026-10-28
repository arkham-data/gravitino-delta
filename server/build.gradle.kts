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

import java.io.ByteArrayOutputStream
plugins {
  `maven-publish`
  id("java")
  id("idea")
}

dependencies {
  implementation(project(":api"))
  implementation(project(":common"))
  implementation(project(":core"))
  implementation(project(":lineage"))
  implementation(project(":server-common"))
  implementation(libs.bundles.jetty)
  implementation(libs.bundles.jersey)
  implementation(libs.bundles.log4j)
  implementation(libs.commons.lang3)
  implementation(libs.guava)
  implementation(libs.jackson.annotations)
  implementation(libs.jackson.datatype.jdk8)
  implementation(libs.jackson.datatype.jsr310)
  implementation(libs.jackson.databind)
  implementation(libs.metrics.jersey2)
  implementation(libs.openlineage.java) {
    isTransitive = false
  }

  // As of Java 9 or newer, the javax.activation package (needed by the jetty server) is no longer part of the JDK. It was removed because it was part of the
  // JavaBeans Activation Framework (JAF) which has been removed from Java SE. So we need to add it as a dependency. For more,
  // please see: https://stackoverflow.com/questions/46493613/what-is-the-replacement-for-javax-activation-package-in-java-9
  implementation(libs.sun.activation)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)
  testAnnotationProcessor(libs.lombok)
  testCompileOnly(libs.lombok)

  testImplementation(libs.commons.io)
  testImplementation(project(":server-common", "testArtifacts"))
  testImplementation(libs.jersey.test.framework.core) {
    exclude(group = "org.junit.jupiter")
  }
  testImplementation(libs.jersey.test.framework.provider.jetty) {
    exclude(group = "org.junit.jupiter")
  }

  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.ognl)

  testRuntimeOnly(libs.junit.jupiter.engine)
}

fun getGitCommitId(): String {
  // Git resolves linked worktrees, packed refs and detached HEADs. Source archives or
  // environments without Git retain the historical empty commit-id fallback.
  return try {
    val output = ByteArrayOutputStream()
    val result = exec {
      workingDir(rootDir)
      commandLine("git", "rev-parse", "HEAD")
      standardOutput = output
      errorOutput = ByteArrayOutputStream()
      isIgnoreExitValue = true
    }
    check(result.exitValue == 0) { "git rev-parse HEAD failed" }
    output.toString("UTF-8").trim().also {
      check(it.matches(Regex("[0-9a-f]{40}|[0-9a-f]{64}"))) { "Invalid Git commit id" }
    }
  } catch (e: Exception) {
    logger.warn("Unable to get Git commit id; using empty build metadata", e)
    ""
  }
}

tasks {
  test {
    environment("GRAVITINO_HOME", rootDir.path)
    environment("GRAVITINO_TEST", "true")
  }
}

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
  "The Delta adapter requires Scala 2.12"
}

configurations.configureEach {
  resolutionStrategy.eachDependency {
    if (requested.group == "org.apache.spark") { useVersion(deltaSparkVersion) }
    if (requested.group == "org.scala-lang") { useVersion("2.12.21") }
  }
}

dependencies {
  implementation(project(":delta:delta-common"))
  compileOnly(project(":api"))
  compileOnly(project(":spark-connector:spark-common")) { isTransitive = false }
  compileOnly("io.delta:delta-spark_2.12:$deltaVersion")
  compileOnly("org.apache.spark:spark-sql_2.12:$deltaSparkVersion")
  compileOnly("org.scala-lang:scala-library:2.12.21")
  implementation(libs.jackson.databind)
  compileOnly("com.google.code.findbugs:jsr305:3.0.2")
  testImplementation(project(":api"))
  testImplementation(project(":spark-connector:spark-common")) { isTransitive = false }
  testImplementation("io.delta:delta-spark_2.12:$deltaVersion")
  testImplementation("org.apache.spark:spark-sql_2.12:$deltaSparkVersion")
  testImplementation("org.scala-lang:scala-library:2.12.21")
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mockito.core)
  testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.named<JavaCompile>("compileJava") { options.release.set(8) }
tasks.withType<ScalaCompile>().configureEach {
  scalaCompileOptions.additionalParameters = listOf("-target:jvm-1.8")
}
tasks.withType<Javadoc>().configureEach {
  (options as CoreJavadocOptions).addStringOption("Xwerror", "-quiet")
  isFailOnError = true
}
tasks.test {
  maxParallelForks = 1
  systemProperty("spark.master", "local[2]")
  environment("SPARK_LOCAL_IP", "127.0.0.1")
}

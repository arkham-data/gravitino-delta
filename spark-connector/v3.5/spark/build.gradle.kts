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
evaluationDependsOn(":spark-connector:spark-common")

plugins {
  `maven-publish`
  id("java")
  id("scala")
  id("idea")
  alias(libs.plugins.shadow)
}

repositories {
  mavenCentral()
}

val glueHiveJarsDir: String? =
  project(":spark-connector:spark-common").extra["glueHiveJarsDir"] as String?

val scalaVersion: String =
  project.properties["scalaVersion"] as? String ?: extra["defaultScalaVersion"].toString()
val deltaTestSparkVersion = providers.gradleProperty("deltaTestSparkVersion").orNull
val deltaTestVersion = providers.gradleProperty("deltaTestVersion").orNull
val deltaS3Enabled = providers.gradleProperty("deltaS3Enabled").orNull == "true"
val deltaS3Endpoint = providers.gradleProperty("deltaS3Endpoint").orNull
val deltaS3Bucket = providers.gradleProperty("deltaS3Bucket").orNull
val deltaS3CredentialsFile = providers.gradleProperty("deltaS3CredentialsFile").orNull
val skipDockerTests = providers.gradleProperty("skipDockerTests").getOrElse("true")
require((deltaTestSparkVersion == null) == (deltaTestVersion == null)) {
  "Specify both deltaTestSparkVersion and deltaTestVersion"
}
if (deltaTestSparkVersion != null) {
  require(deltaTestSparkVersion in setOf("3.5.4", "3.5.6"))
  require(deltaTestVersion in setOf("3.3.0", "3.3.2"))
}
if (deltaS3Enabled) {
  require(deltaTestSparkVersion != null) {
    "deltaS3Enabled requires deltaTestSparkVersion and deltaTestVersion"
  }
  require(skipDockerTests == "false") { "deltaS3Enabled requires -PskipDockerTests=false" }
  require(!deltaS3Endpoint.isNullOrBlank()) { "deltaS3Enabled requires deltaS3Endpoint" }
  require(!deltaS3Bucket.isNullOrBlank()) { "deltaS3Enabled requires deltaS3Bucket" }
  require(!deltaS3CredentialsFile.isNullOrBlank()) {
    "deltaS3Enabled requires deltaS3CredentialsFile"
  }
}
val sparkVersion: String = deltaTestSparkVersion ?: libs.versions.spark35.get()
val sparkMajorVersion: String = sparkVersion.substringBeforeLast(".")
val icebergVersion: String = libs.versions.iceberg4spark35.get()
val paimonVersion: String = libs.versions.paimon.get()
val kyuubiVersion: String = libs.versions.kyuubi4spark.get()
val scalaJava8CompatVersion: String = libs.versions.scala.java.compat.get()
val scalaCollectionCompatVersion: String = libs.versions.scala.collection.compat.get()
val artifactName = "${rootProject.name}-spark-${sparkMajorVersion}_$scalaVersion"

if (hasProperty("excludePackagesForSparkConnector")) {
  @Suppress("UNCHECKED_CAST")
  val configureFunc = properties["excludePackagesForSparkConnector"] as? (Project) -> Unit
  configureFunc?.invoke(project)
}

dependencies {
  if (deltaTestVersion != null) {
    testImplementation("io.delta:delta-spark_$scalaVersion:$deltaTestVersion")
    testImplementation("org.apache.kyuubi:kyuubi-spark-authz_$scalaVersion:${libs.versions.kyuubi4authz.get()}") { exclude("org.apache.hadoop") }
    testImplementation("org.apache.ranger:ranger-plugins-common:2.8.0") { exclude("org.apache.hadoop") }
    testImplementation("org.apache.ranger:ranger-audit-core:2.8.0") { exclude("org.apache.hadoop") }
    testImplementation("org.apache.ranger:ranger-plugins-cred:2.8.0") { exclude("org.apache.hadoop") }
    testImplementation("org.apache.ranger:ranger-intg:2.8.0") { isTransitive = false }
    testImplementation("org.scala-lang:scala-library:2.12.21")
    testImplementation("com.sun.jersey:jersey-client:1.19.4")
  }

  implementation(project(":spark-connector:spark-3.4"))
  implementation(project(":spark-connector:spark-common"))
  compileOnly("org.apache.kyuubi:kyuubi-spark-connector-hive_$scalaVersion:$kyuubiVersion")
  compileOnly("org.apache.spark:spark-catalyst_$scalaVersion:$sparkVersion") {
    exclude("com.fasterxml.jackson")
  }
  compileOnly(project(":clients:client-java-runtime", configuration = "shadow"))
  compileOnly("org.apache.iceberg:iceberg-spark-runtime-${sparkMajorVersion}_$scalaVersion:$icebergVersion")
  if (scalaVersion == "2.12") {
    compileOnly("org.apache.paimon:paimon-spark-$sparkMajorVersion:$paimonVersion") {
      exclude("org.apache.spark")
    }
  }

  testImplementation(project(":api")) {
    exclude("org.apache.logging.log4j")
  }
  testImplementation(project(":catalogs:catalog-jdbc-common")) {
    exclude("org.apache.logging.log4j")
  }
  testImplementation(project(":catalogs:hive-metastore-common")) {
    exclude("*")
  }
  testImplementation(project(":clients:client-java")) {
    exclude("org.apache.logging.log4j")
    exclude("org.slf4j")
  }
  testImplementation(project(":core")) {
    exclude("org.apache.logging.log4j")
    exclude("org.slf4j")
  }
  testImplementation(project(":common")) {
    exclude("org.apache.logging.log4j")
    exclude("org.slf4j")
  }
  testImplementation(project(":integration-test-common", "testArtifacts")) {
    exclude("org.apache.logging.log4j")
    exclude("org.slf4j")
  }
  testImplementation(project(":server")) {
    exclude("org.apache.logging.log4j")
    exclude("org.slf4j")
  }
  testImplementation(project(":server-common")) {
    exclude("org.apache.logging.log4j")
    exclude("org.slf4j")
  }
  testImplementation(project(":spark-connector:spark-common", "testArtifacts")) {
    exclude("com.fasterxml.jackson")
    exclude("org.apache.logging.log4j")
    exclude("org.slf4j")
  }

  testImplementation(libs.awaitility)
  // Iceberg's GlueCatalog references several AWS SDK modules at runtime; must be on test classpath
  testImplementation(libs.aws.dynamodb)
  testImplementation(libs.aws.glue)
  testImplementation(libs.aws.kms)
  testImplementation(libs.aws.s3)
  testImplementation(libs.aws.sts)
  testImplementation(libs.hadoop3.aws)
  // hadoop-aws declares hadoop-client-api as provided; add it explicitly so S3AFileSystem can load
  // org.apache.hadoop.fs.impl.prefetch.PrefetchingStatistics (added in 3.3.5) at runtime.
  testImplementation(libs.hadoop3.client.api)
  testImplementation(libs.hive2.common) {
    exclude("com.sun.jersey")
    exclude("org.apache.curator")
    exclude("org.apache.logging.log4j")
    // use hadoop from Spark
    exclude("org.apache.hadoop")
    exclude("org.eclipse.jetty.aggregate", "jetty-all")
    exclude("org.eclipse.jetty.orbit", "javax.servlet")
  }

  testImplementation(libs.hive2.metastore) {
    exclude("co.cask.tephra")
    exclude("com.github.joshelser")
    exclude("com.google.code.findbugs", "jsr305")
    exclude("com.google.code.findbugs", "sr305")
    exclude("com.tdunning", "json")
    exclude("com.zaxxer", "HikariCP")
    exclude("com.sun.jersey")
    exclude("io.dropwizard.metrics")
    exclude("javax.transaction", "transaction-api")
    exclude("org.apache.avro")
    exclude("org.apache.curator")
    exclude("org.apache.hbase")
    exclude("org.apache.hadoop")
    exclude("org.apache.hive", "hive-common")
    exclude("org.apache.hive", "hive-shims")
    exclude("org.apache.logging.log4j")
    exclude("org.apache.parquet", "parquet-hadoop-bundle")
    exclude("org.apache.zookeeper")
    exclude("org.eclipse.jetty.aggregate", "jetty-all")
    exclude("org.eclipse.jetty.orbit", "javax.servlet")
    exclude("org.slf4j")
  }
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mysql.driver)
  testImplementation(libs.postgresql.driver)
  testImplementation(libs.testcontainers)
  testImplementation(libs.aws.policy)
  testImplementation(project(":iceberg:iceberg-common"))

  // org.apache.iceberg.rest.RESTSerializers#registerAll(ObjectMapper) has different method signature for iceberg-core and iceberg-spark-runtime package, we must make sure iceberg-core is in front to start up MiniGravitino server.
  testImplementation("org.apache.iceberg:iceberg-core:$icebergVersion")
  testImplementation("org.apache.iceberg:iceberg-spark-runtime-${sparkMajorVersion}_$scalaVersion:$icebergVersion")
  testImplementation("org.apache.iceberg:iceberg-hive-metastore:$icebergVersion")
  if (scalaVersion == "2.12") {
    testImplementation("org.apache.paimon:paimon-spark-$sparkMajorVersion:$paimonVersion") {
      exclude("org.apache.spark")
    }
  }
  testImplementation("org.apache.kyuubi:kyuubi-spark-connector-hive_$scalaVersion:$kyuubiVersion")
  // include spark-sql,spark-catalyst,hive-common,hdfs-client
  testImplementation("org.apache.spark:spark-hive_$scalaVersion:$sparkVersion") {
    // conflict with Gravitino server jersey
    exclude("org.glassfish.jersey.core")
    exclude("org.glassfish.jersey.containers")
    exclude("org.glassfish.jersey.inject")
    exclude("com.sun.jersey")
    exclude("com.fasterxml.jackson")
    exclude("com.fasterxml.jackson.core")
  }
  testImplementation("org.scala-lang.modules:scala-collection-compat_$scalaVersion:$scalaCollectionCompatVersion")
  testImplementation("org.apache.spark:spark-catalyst_$scalaVersion:$sparkVersion")
  testImplementation("org.apache.spark:spark-core_$scalaVersion:$sparkVersion")
  testImplementation("org.apache.spark:spark-sql_$scalaVersion:$sparkVersion")

  testRuntimeOnly(libs.junit.jupiter.engine)
}

if (deltaTestSparkVersion != null) {
  configurations
    .matching { it.name == "testCompileClasspath" || it.name == "testRuntimeClasspath" }
    .configureEach {
      // Jersey 1 server/bundle embed an old copy of Jersey 2's shaded ASM namespace.
      // Keep Ranger's Jersey 1 client/core while the embedded server uses Jersey 2.41.
      exclude(group = "com.sun.jersey", module = "jersey-bundle")
      exclude(group = "com.sun.jersey", module = "jersey-server")
      exclude(group = "javax.ws.rs", module = "jsr311-api")
      resolutionStrategy.eachDependency {
        if (requested.group == "org.apache.ranger") {
          useVersion("2.8.0")
          because("The required Delta authorization fixture must execute against Ranger 2.8.0")
        }
        if (requested.group == "org.scala-lang") {
          useVersion("2.12.21")
          because("The opt-in Scala test helper compiles with one converged Scala toolchain")
        }
        if (requested.group == "org.apache.spark") {
          useVersion(deltaTestSparkVersion)
          because("The Delta experiment must run one Spark patch version")
        }
      }
    }
}

// These experimental helpers import Delta directly; execution exclusion alone cannot compile
// the default test source set without adding Delta to ordinary builds.
if (deltaTestSparkVersion == null) {
  sourceSets.test {
    java.exclude("**/integration/test/delta/**")
    scala.exclude("**/integration/test/delta/**")
    resources.exclude("ranger-spark-security.xml", "ranger-delta-local-policies.json")
  }
}

tasks.test {
  if (deltaTestSparkVersion == null) {
    exclude("**/connector/delta/TestDeltaNativeContract.class")
    exclude("**/integration/test/delta/**")
  } else {
    dependsOn(":catalogs:catalog-lakehouse-generic:jar")
    systemProperty("gravitino.delta.test.sparkVersion", deltaTestSparkVersion)
    systemProperty("gravitino.delta.test.deltaVersion", requireNotNull(deltaTestVersion))
  }
  if (deltaS3Enabled) {
    // This contract uses an explicitly supplied localhost endpoint and does not require the
    // macOS container-IP bridge checked by the root build's general Docker-test gate.
    extensions.extraProperties["gravitinoExternalTestEndpoints"] = true
    systemProperty("gravitino.delta.test.s3.enabled", "true")
    systemProperty("gravitino.delta.test.s3.endpoint", requireNotNull(deltaS3Endpoint))
    systemProperty("gravitino.delta.test.s3.bucket", requireNotNull(deltaS3Bucket))
    systemProperty(
      "gravitino.delta.test.s3.credentialsFile",
      requireNotNull(deltaS3CredentialsFile)
    )
  }

  val skipITs = project.hasProperty("skipITs")
  val enableSparkSQLITs = project.hasProperty("enableSparkSQLITs")
  if (!enableSparkSQLITs) {
    exclude("**/integration/test/sql/**")
  }
  if (skipITs) {
    // Exclude integration tests
    exclude("**/integration/test/**")
  } else {
    if (glueHiveJarsDir != null) {
      dependsOn(":spark-connector:spark-common:downloadGlueHiveJars")
      jvmArgs("-Dglue.hive-jars-dir=$glueHiveJarsDir")
    }
    dependsOn(tasks.jar)
    dependsOn(":catalogs:catalog-lakehouse-iceberg:jar")
    dependsOn(":catalogs:catalog-hive:jar")
    dependsOn(":iceberg:iceberg-rest-server:jar")
    dependsOn(":lance:lance-rest-server:jar")
    dependsOn(":catalogs:catalog-lakehouse-paimon:jar")
    dependsOn(":catalogs:catalog-jdbc-mysql:jar")
    dependsOn(":catalogs:catalog-jdbc-postgresql:jar")
  }
}

tasks.withType<Jar> {
  archiveBaseName.set(artifactName)
}

publishing {
  publications {
    withType<MavenPublication>().configureEach {
      artifactId = artifactName
    }
  }
}

tasks.clean {
  delete("derby.log")
  delete("metastore_db")
  delete("spark-warehouse")
}

tasks.named<Jar>("sourcesJar") {
  duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

// Gradle 8.2 otherwise emits the unsupported Scala spelling -target:jvm-1.17.
// Helpers use Java 8 bytecode (as Spark/Delta do), while compiling/running on JDK 17.
tasks.withType<ScalaCompile>().configureEach {
  scalaCompileOptions.additionalParameters = listOf("-target:jvm-1.8")
  doFirst {
    logger.lifecycle("DELTA SCALA compilerClasspath={} target=jvm-1.8 java={}", scalaClasspath.files, System.getProperty("java.version"))
  }
}

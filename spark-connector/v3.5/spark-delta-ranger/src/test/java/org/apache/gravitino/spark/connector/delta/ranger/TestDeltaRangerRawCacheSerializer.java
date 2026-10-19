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

package org.apache.gravitino.spark.connector.delta.ranger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.spark.package$;
import org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Runs every singleton history in a separate JVM without resetting Spark serializer state. */
public class TestDeltaRangerRawCacheSerializer {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "absent",
        "default",
        "foreign",
        "configured-default-actual-foreign",
        "actual-default-configured-foreign",
        "session-and-candidate-binding",
        "callback-before"
      })
  void observesActualSerializerWithoutFactoryOrCallback(String scenario) throws Exception {
    String evidence = System.getenv("GRAVITINO_DELTA_RAW_SERIALIZER_EVIDENCE_DIR");
    Path directory =
        evidence == null
            ? Files.createTempDirectory("raw-cache-serializer-")
            : Files.createDirectories(Paths.get(evidence).resolve("children").resolve(scenario));
    Path result = directory.resolve("result.properties");
    Path log = directory.resolve("child.log");
    List<String> command = new ArrayList<>();
    command.add(Paths.get(System.getProperty("java.home"), "bin", "java").toString());
    command.addAll(moduleAccessArguments());
    command.add("-Xms128m");
    command.add("-Xmx768m");
    command.add("-cp");
    command.add(childClasspath());
    command.add(RawCacheSerializerProbe.class.getName());
    command.add(scenario);
    command.add(result.toString());
    Process child =
        new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    String diagnostic = "Scenario " + scenario + "; result=" + result + "; log=" + log;
    try {
      assertTrue(child.waitFor(120, TimeUnit.SECONDS), "Child timeout: " + diagnostic);
      assertEquals(0, child.exitValue(), diagnostic);
      Properties properties = new Properties();
      try (InputStream input = Files.newInputStream(result)) {
        properties.load(input);
      }
      assertEquals(scenario, properties.getProperty("scenario"), diagnostic);
      assertEquals("true", properties.getProperty("complete"), diagnostic);
      assertEquals(
          package$.MODULE$.SPARK_VERSION(), properties.getProperty("spark.version"), diagnostic);
      assertEquals(
          DefaultCachedBatchSerializer.class
              .getProtectionDomain()
              .getCodeSource()
              .getLocation()
              .toExternalForm(),
          properties.getProperty("serializer.origin"),
          diagnostic);
      assertEquals(
          properties.getProperty("serializer.origin"),
          properties.getProperty("module.origin"),
          diagnostic);
      assertTrue(Integer.parseInt(properties.getProperty("assertions")) >= 5, diagnostic);
      for (String check : expectedChecks(scenario)) {
        assertEquals("true", properties.getProperty("check." + check), diagnostic + "; " + check);
      }
      assertTrue(properties.containsKey("constructors"), diagnostic);
      assertTrue(properties.containsKey("callbacks"), diagnostic);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw interrupted;
    } finally {
      if (child.isAlive()) {
        child.destroyForcibly();
      }
    }
  }

  private static List<String> expectedChecks(String scenario) {
    switch (scenario) {
      case "absent":
        return Arrays.asList("denied", "noConstruction", "noCallback");
      case "default":
        return Arrays.asList(
            "observed",
            "recacheBuilderChanged",
            "recacheSameSerializer",
            "clearRetainsSerializer",
            "candidateDenied",
            "noJobs");
      case "foreign":
        return Arrays.asList("actualForeign", "denied", "noConstruction", "noCallback");
      case "configured-default-actual-foreign":
        return Arrays.asList(
            "defaultConfig", "actualForeign", "denied", "noConstruction", "noCallback");
      case "actual-default-configured-foreign":
        return Arrays.asList(
            "foreignConfig", "actualDefault", "denied", "noConstruction", "noCallback");
      case "session-and-candidate-binding":
        return Arrays.asList(
            "exactCandidate",
            "otherSessionDenied",
            "freshSessionObservation",
            "candidateDenied",
            "nullCandidateDenied",
            "changedConfigDenied",
            "stoppedDenied");
      case "callback-before":
        return Arrays.asList(
            "foreign.recacheDenied",
            "foreign.exactKey",
            "foreign.noCallback",
            "foreign.standaloneDenied",
            "defaultConfig.recacheDenied",
            "defaultConfig.exactKey",
            "defaultConfig.noCallback",
            "defaultConfig.standaloneDenied");
      default:
        throw new IllegalArgumentException("Unknown scenario");
    }
  }

  private static List<String> moduleAccessArguments() {
    List<String> arguments = ManagementFactory.getRuntimeMXBean().getInputArguments();
    List<String> allowed = new ArrayList<>();
    for (int i = 0; i < arguments.size(); i++) {
      String argument = arguments.get(i);
      if (argument.startsWith("--add-opens=") || argument.startsWith("--add-exports=")) {
        allowed.add(argument);
      } else if (argument.equals("--add-opens") || argument.equals("--add-exports")) {
        if (++i >= arguments.size()) {
          throw new IllegalStateException("Incomplete module access flag");
        }
        allowed.add(argument);
        allowed.add(arguments.get(i));
      }
    }
    return allowed;
  }

  private static String childClasspath() throws Exception {
    Set<String> entries = new LinkedHashSet<>();
    for (ClassLoader loader = TestDeltaRangerRawCacheSerializer.class.getClassLoader();
        loader != null;
        loader = loader.getParent()) {
      if (loader instanceof URLClassLoader) {
        for (URL url : ((URLClassLoader) loader).getURLs()) {
          if (!"file".equals(url.getProtocol())) {
            throw new IllegalStateException("Unsupported child runtime classpath URL protocol");
          }
          entries.add(Paths.get(url.toURI()).toString());
        }
      }
    }
    entries.addAll(Arrays.asList(System.getProperty("java.class.path").split(File.pathSeparator)));
    return String.join(File.pathSeparator, entries);
  }
}

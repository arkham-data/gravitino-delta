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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.gravitino.spark.connector.plugin.GravitinoDriverPlugin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kyuubi.plugin.spark.authz.ranger.SparkRangerAdminPlugin;
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils;
import org.apache.kyuubi.util.reflect.ReflectUtils$;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.delta.catalog.DeltaTableV2;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;

/** Test application for actual archived Spark distributions; never installed in a plugin JAR. */
public final class DeltaRangerDownloadedQuery {
  private static final ObjectMapper JSON = new ObjectMapper();
  private final String helperField = "nonpublic-value";

  /**
   * Runs fresh named queries received on stdin against genuinely downloaded Ranger policies.
   *
   * @param args private request JSON and new evidence directory
   * @throws Exception if deployment setup or an executed acceptance assertion fails
   */
  public static void main(String[] args) throws Exception {
    JsonNode input = JSON.readTree(Path.of(args[0]).toFile());
    Path output = Path.of(args[1]);
    Files.createDirectory(output);
    Properties properties = new Properties();
    try (InputStream stream =
        Files.newInputStream(Path.of(input.path("sparkConfigurationFile").asText()))) {
      properties.load(stream);
    }
    SparkConf conf =
        new SparkConf(false)
            .setMaster("local[2]")
            .setAppName("delta-ranger-production-qualification");
    for (String key : properties.stringPropertyNames()) {
      conf.set(key, properties.getProperty(key));
    }
    Configuration hadoop = new Configuration();
    for (String key : properties.stringPropertyNames()) {
      if (key.startsWith("spark.hadoop.")) {
        hadoop.set(key.substring("spark.hadoop.".length()), properties.getProperty(key));
      }
    }
    UserGroupInformation.setConfiguration(hadoop);
    SparkSession spark = null;
    try {
      spark = SparkSession.builder().config(conf).getOrCreate();
      DeltaRangerQuerySubject subject = DeltaRangerQuerySubject.current(spark);
      check(
          subject.user().equals(input.path("subject").asText()),
          "Actual UGI differs from expected query subject");
      check(
          spark.sparkContext().sparkUser().equals(subject.user()),
          "Application discovery identity differs from actual UGI");
      check(
          !subject.user().equals(input.path("metadataPrincipal").asText()),
          "Metadata identity became the query identity");
      Map<String, Object> startup = new LinkedHashMap<>();
      startup.put("spark", spark.version());
      startup.put("user", subject.user());
      startup.put("groups", subject.groups());
      startup.put("applicationUser", spark.sparkContext().sparkUser());
      startup.put("origins", origins());
      startup.put("helpers", helpers());
      Map<String, Boolean> visible = new LinkedHashMap<>();
      for (JsonNode catalog : input.path("catalogs")) {
        visible.put(
            catalog.asText(), spark.conf().contains("spark.sql.catalog." + catalog.asText()));
      }
      startup.put("visibleCatalogs", visible);
      DeltaRangerConfig config =
          DeltaRangerConfig.fromClasspath(
              input.path("resourceService").asText(),
              input.path("resourceDefinition").asText(),
              input.path("metalake").asText());
      try (DeltaRangerPolicyEngine engine = DeltaRangerPolicyEngine.acquire(config);
          BufferedReader commands =
              new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
        await(engine, input.path("minPolicy").asLong(0), input.path("minRole").asLong(0));
        startup.put("versions", versions(engine));
        write(output.resolve("ready.json"), startup);
        String line;
        while ((line = commands.readLine()) != null) {
          JsonNode command = JSON.readTree(line);
          if (command.path("close").asBoolean()) {
            break;
          }
          SparkRangerAdminPlugin.refreshPoliciesAndTags();
          await(engine, command.path("minPolicy").asLong(0), command.path("minRole").asLong(0));
          Map<String, Object> result = new LinkedHashMap<>();
          result.put("versionsBefore", versions(engine));
          result.put("subject", DeltaRangerQuerySubject.current(spark).user());
          result.put("sql", command.path("sql").asText());
          List<List<Object>> rows = new ArrayList<>();
          Throwable queryFailure = null;
          try {
            for (Row row : spark.sql(command.path("sql").asText()).collectAsList()) {
              List<Object> values = new ArrayList<>();
              for (int i = 0; i < row.size(); i++) {
                values.add(row.isNullAt(i) ? null : row.get(i));
              }
              rows.add(values);
            }
          } catch (Exception failure) {
            queryFailure = failure;
          }
          result.put(
              "failureClass", queryFailure == null ? null : queryFailure.getClass().getName());
          result.put("authorizationDenied", isSecurityFailure(queryFailure));
          result.put("rows", rows);
          result.put("versionsAfter", versions(engine));
          write(output.resolve(command.path("id").asText() + ".json"), result);
          if (command.path("denied").asBoolean()) {
            check(
                isSecurityFailure(queryFailure),
                "Expected an authorization denial, not a setup or execution failure");
          } else if (command.path("hidden").asBoolean()) {
            check(queryFailure != null, "Hidden catalog unexpectedly resolved");
            check(
                !spark.conf().contains("spark.sql.catalog." + command.path("catalog").asText()),
                "Hidden catalog was registered by discovery");
          } else {
            if (queryFailure != null) {
              throw new AssertionError(
                  "Fresh native SELECT failed; private evidence retained", queryFailure);
            }
            check(
                normalized(JSON.valueToTree(rows)).equals(normalized(command.path("expectedRows"))),
                "Executed native rows differ from independent expected rows");
          }
          write(
              output.resolve(command.path("id").asText() + "-passed.json"),
              Collections.singletonMap("passed", true));
        }
      }
      write(output.resolve("completed.json"), Collections.singletonMap("completed", true));
    } catch (Throwable failure) {
      Map<String, Object> error = new LinkedHashMap<>();
      List<String> causes = new ArrayList<>();
      for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
        causes.add(cause.getClass().getName());
      }
      error.put("failureClasses", causes);
      write(output.resolve("failure.json"), error);
      throw failure;
    } finally {
      if (spark != null) {
        spark.stop();
      }
    }
  }

  /**
   * @return fixed value for the executed pinned reflection helper probe
   */
  public String helperMethod() {
    return "method-value";
  }

  private static Map<String, String> origins() throws Exception {
    Map<String, String> result = new LinkedHashMap<>(DeltaRangerPolicyEngine.runtimeOrigins());
    for (Class<?> type :
        new Class<?>[] {
          SparkSession.class,
          DeltaTableV2.class,
          DeltaRangerCatalogSpark35.class,
          GravitinoDriverPlugin.class,
          ReflectUtils$.class,
          Class.forName("org.apache.kyuubi.util.SemanticVersion"),
          Class.forName("org.apache.kyuubi.shade.com.google.common.net.InternetDomainName"),
          Class.forName(
              "org.apache.kyuubi.shade.com.google.thirdparty.publicsuffix.PublicSuffixPatterns")
        }) {
      result.put(
          type.getName(),
          type.getProtectionDomain().getCodeSource().getLocation().toExternalForm());
    }
    return result;
  }

  private static boolean helpers() throws Exception {
    check(
        AuthZUtils.isSparkV35OrGreater()
            && AuthZUtils.isScalaV212()
            && AuthZUtils.isRanger21orGreater(),
        "Pinned version helper linkage");
    check(
        "`odd`` name`".equals(AuthZUtils.quoteIfNeeded("odd` name")),
        "Pinned quoting helper linkage");
    DeltaRangerDownloadedQuery target = new DeltaRangerDownloadedQuery();
    check(
        target.helperField.equals(ReflectUtils$.MODULE$.getField(target, "helperField")),
        "Pinned nonpublic reflection helper linkage");
    check(
        "method-value"
            .equals(
                ReflectUtils$.MODULE$.invokeAs(
                    target,
                    "helperMethod",
                    JavaConverters.asScalaBufferConverter(
                            Collections.<scala.Tuple2<Class<?>, Object>>emptyList())
                        .asScala()
                        .toSeq())),
        "Pinned reflection/private Guava linkage");
    ReflectUtils$.MODULE$
        .loadFromServiceLoader(
            ReflectUtils$.MODULE$.loadFromServiceLoader$default$1(),
            ClassTag$.MODULE$.apply(Runnable.class))
        .hasNext();
    check(
        SparkRangerAdminPlugin.getRangerConf() != null, "Pinned Ranger config/reflection linkage");
    Class<?> domain =
        Class.forName("org.apache.kyuubi.shade.com.google.common.net.InternetDomainName");
    Object value = domain.getMethod("from", String.class).invoke(null, "www.example.co.uk");
    check(
        "example.co.uk".equals(domain.getMethod("topPrivateDomain").invoke(value).toString()),
        "Private Guava/publicsuffix linkage");
    return true;
  }

  private static void await(DeltaRangerPolicyEngine engine, long policy, long role)
      throws Exception {
    long end = System.nanoTime() + Duration.ofSeconds(60).toNanos();
    do {
      try {
        if (engine.versions().policy() >= policy && engine.versions().role() >= role) {
          return;
        }
      } catch (SecurityException incomplete) {
        /* Wait only for observation; never replay a mutation. */
      }
      Thread.sleep(100);
    } while (System.nanoTime() < end);
    throw new AssertionError("Required downloaded policy/role versions were not observed");
  }

  private static Map<String, Object> versions(DeltaRangerPolicyEngine engine) {
    DeltaRangerPolicyEngine.Versions value = engine.versions();
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("service", value.service());
    result.put("definition", value.definition());
    result.put("policy", value.policy());
    result.put("role", value.role());
    return result;
  }

  private static boolean isSecurityFailure(Throwable failure) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof SecurityException) {
        return true;
      }
    }
    return false;
  }

  private static List<String> normalized(JsonNode rows) {
    List<String> result = new ArrayList<>();
    for (JsonNode row : rows) {
      result.add(row.toString());
    }
    Collections.sort(result);
    return result;
  }

  private static void write(Path path, Object value) throws Exception {
    Files.writeString(path, JSON.writeValueAsString(value), StandardOpenOption.CREATE_NEW);
  }

  private static void check(boolean condition, String diagnostic) {
    if (!condition) {
      throw new AssertionError(diagnostic);
    }
  }
}

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
package org.apache.gravitino.spark.connector.integration.test.delta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests failure-safe cleanup behavior used by the live S3 contract. */
public class TestDeltaS3Cleanup {
  @Test
  void runsEveryActionAndSuppressesFailuresAfterTheFirst() {
    List<String> calls = new ArrayList<>();
    IOException firstFailure = new IOException("first");
    IllegalStateException laterFailure = new IllegalStateException("later");

    IOException thrown =
        Assertions.assertThrows(
            IOException.class,
            () ->
                DeltaS3Cleanup.runAll(
                    () -> {
                      calls.add("first");
                      throw firstFailure;
                    },
                    () -> calls.add("second"),
                    () -> {
                      calls.add("third");
                      throw laterFailure;
                    },
                    () -> calls.add("fourth")));

    Assertions.assertSame(firstFailure, thrown);
    Assertions.assertEquals(Arrays.asList("first", "second", "third", "fourth"), calls);
    Assertions.assertArrayEquals(new Throwable[] {laterFailure}, thrown.getSuppressed());
  }

  @Test
  void runsNestedSparkStorageAndMetadataCleanupAfterFailures() {
    List<String> calls = new ArrayList<>();
    IOException bridgeFailure = new IOException("bridge");
    IOException sparkStopFailure = new IOException("spark-stop");
    IOException deleteFailure = new IOException("delete");
    IOException fileSystemCloseFailure = new IOException("filesystem-close");
    IOException metalakeFailure = new IOException("metalake");

    IOException thrown =
        Assertions.assertThrows(
            IOException.class,
            () ->
                DeltaS3Cleanup.runAll(
                    () ->
                        DeltaS3Cleanup.runAll(
                            () -> {
                              calls.add("bridge-close");
                              throw bridgeFailure;
                            },
                            () -> {
                              calls.add("spark-stop");
                              throw sparkStopFailure;
                            },
                            () -> calls.add("clear-active"),
                            () -> calls.add("clear-default")),
                    () ->
                        DeltaS3Cleanup.runAll(
                            () -> {
                              calls.add("prefix-delete");
                              throw deleteFailure;
                            },
                            () -> {
                              calls.add("filesystem-close");
                              throw fileSystemCloseFailure;
                            }),
                    () -> {
                      calls.add("metalake-drop");
                      throw metalakeFailure;
                    }));

    Assertions.assertSame(bridgeFailure, thrown);
    Assertions.assertEquals(
        Arrays.asList(
            "bridge-close",
            "spark-stop",
            "clear-active",
            "clear-default",
            "prefix-delete",
            "filesystem-close",
            "metalake-drop"),
        calls);
    Assertions.assertArrayEquals(
        new Throwable[] {sparkStopFailure, deleteFailure, metalakeFailure}, thrown.getSuppressed());
    Assertions.assertArrayEquals(
        new Throwable[] {fileSystemCloseFailure}, deleteFailure.getSuppressed());
  }
}

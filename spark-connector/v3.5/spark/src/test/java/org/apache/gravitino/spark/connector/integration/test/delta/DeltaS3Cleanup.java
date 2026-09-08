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

final class DeltaS3Cleanup {
  @FunctionalInterface
  interface Action {
    void run() throws Exception;
  }

  private DeltaS3Cleanup() {}

  static void runAll(Action... actions) throws Exception {
    Throwable firstFailure = null;
    for (Action action : actions) {
      try {
        action.run();
      } catch (Throwable failure) {
        if (firstFailure == null) {
          firstFailure = failure;
        } else if (failure != firstFailure) {
          firstFailure.addSuppressed(failure);
        }
      }
    }

    if (firstFailure instanceof Exception) {
      throw (Exception) firstFailure;
    }
    if (firstFailure instanceof Error) {
      throw (Error) firstFailure;
    }
  }
}

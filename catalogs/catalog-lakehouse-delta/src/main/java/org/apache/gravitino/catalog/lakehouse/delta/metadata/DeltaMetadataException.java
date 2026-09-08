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

package org.apache.gravitino.catalog.lakehouse.delta.metadata;

/** A sanitized native metadata failure with no storage payload or SDK cause chain. */
public final class DeltaMetadataException extends RuntimeException {
  /** Stable diagnostic code without source payload. */
  private final String reason;
  /** Internal operation name without location or input data. */
  private final String operation;

  DeltaMetadataException(String reason, String operation) {
    super(operation + ":" + reason, null, false, true);
    this.reason = reason;
    this.operation = operation;
  }

  /**
   * @return stable diagnostic code; never an SDK message or location
   */
  public String reason() {
    return reason;
  }

  /**
   * @return safe operation name, without native input data
   */
  public String operation() {
    return operation;
  }
}

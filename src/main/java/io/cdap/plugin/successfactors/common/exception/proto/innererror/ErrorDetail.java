/*
 * Copyright © 2022 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.successfactors.common.exception.proto.innererror;

/**
 * ErrorDetail class
 */
public class ErrorDetail {

  private final String code;
  private final String message;
  private final String propertyref;
  private final String severity;
  private final String target;

  public ErrorDetail(String code, String message, String propertyref, String severity, String target) {
    this.code = code;
    this.message = message;
    this.propertyref = propertyref;
    this.severity = severity;
    this.target = target;
  }

  public String getCode() {
    return code;
  }

  public String getMessage() {
    return message;
  }

  public String getPropertyRef() {
    return propertyref;
  }

  public String getSeverity() {
    return severity;
  }

  public String getTarget() {
    return target;
  }
}

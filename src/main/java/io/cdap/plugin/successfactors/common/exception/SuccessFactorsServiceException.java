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

package io.cdap.plugin.successfactors.common.exception;

import io.cdap.plugin.successfactors.common.exception.proto.SuccessFactorsError;

/**
 * This {@code SapSuccessFactorsServiceException} class is used to capture all the errors that are related to
 * SuccessFactors
 * service issues.
 * e.g. Resource Not found, forbidden access, invalid query options etc.
 */

public class SuccessFactorsServiceException extends Exception {

  private final SuccessFactorsError successFactorsError;
  private final Integer errorCode;

  public SuccessFactorsServiceException(String message) {
    this(message, null, null, null);
  }

  public SuccessFactorsServiceException(String message, Integer errorCode) {
    this(message, errorCode, null, null);
  }

  public SuccessFactorsServiceException(String message, Integer errorCode, SuccessFactorsError successFactorsError) {
    this(message, errorCode, successFactorsError, null);
  }

  public SuccessFactorsServiceException(String message, Throwable cause) {
    this(message, null, null, cause);
  }

  public SuccessFactorsServiceException(String message, Integer errorCode, SuccessFactorsError successFactorsError,
                                        Throwable cause) {
    super(message, cause);
    this.errorCode = errorCode;
    this.successFactorsError = successFactorsError;
  }

  public SuccessFactorsError getSuccessFactorsError() {
    return this.successFactorsError;
  }

  public Integer getErrorCode() {
    return this.errorCode;
  }
}

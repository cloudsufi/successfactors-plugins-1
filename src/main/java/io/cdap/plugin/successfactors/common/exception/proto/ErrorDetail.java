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

package io.cdap.plugin.successfactors.common.exception.proto;

import io.cdap.plugin.successfactors.common.exception.proto.innererror.InnerError;

import javax.annotation.Nullable;

/**
 * ErrorDetail class
 */
public class ErrorDetail {
  private final String code;
  private final Message message;
  @Nullable
  private final InnerError innerError;

  public ErrorDetail(String code, Message message, @Nullable InnerError innerError) {
    this.code = code;
    this.message = message;
    this.innerError = innerError;
  }

  public String getCode() {
    return code;
  }

  public Message getMessage() {
    return message;
  }

  @Nullable
  public InnerError getInnerError() {
    return innerError;
  }
}

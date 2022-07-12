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
 * ErrorResolution class
 */
public class ErrorResolution {

  private final String successFactorsTransaction;
  private final String successFactorsNote;
  private final String additionalSuccessFactorsNote;

  public ErrorResolution(String successFactorsTransaction, String successFactorsNote,
                         String additionalSuccessFactorsNote) {
    this.successFactorsTransaction = successFactorsTransaction;
    this.successFactorsNote = successFactorsNote;
    this.additionalSuccessFactorsNote = additionalSuccessFactorsNote;
  }

  public String getSuccessFactorsTransaction() {
    return successFactorsTransaction;
  }

  public String getSuccessFactorsNote() {
    return successFactorsNote;
  }

  public String getAdditionalSuccessFactorsNote() {
    return additionalSuccessFactorsNote;
  }
}

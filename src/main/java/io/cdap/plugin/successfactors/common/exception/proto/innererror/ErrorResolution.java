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

  private final String sapTransaction;
  private final String sapNote;
  private final String additionalSAPNote;

  public ErrorResolution(String sapTransaction, String sapNote, String additionalSAPNote) {
    this.sapTransaction = sapTransaction;
    this.sapNote = sapNote;
    this.additionalSAPNote = additionalSAPNote;
  }

  public String getSapTransaction() {
    return sapTransaction;
  }

  public String getSapNote() {
    return sapNote;
  }

  public String getAdditionalSAPNote() {
    return additionalSAPNote;
  }
}

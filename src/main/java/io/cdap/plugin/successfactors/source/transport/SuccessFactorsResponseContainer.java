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

package io.cdap.plugin.successfactors.source.transport;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import javax.annotation.Nullable;

/**
 * This {@code SuccessFactorsResponseContainer} container class is used to contains request body of type
 * {@code InputStream},
 * along with the following:
 * - HTTP STATUS CODE,
 * - HTTP STATUS MESSAGE &
 * - SAP SuccessFactors service version number
 */

public class SuccessFactorsResponseContainer {

  private final int httpStatusCode;
  private final String httpStatusMsg;

  @Nullable
  private final String dataServiceVersion;
  private final byte[] responseStream;

  public SuccessFactorsResponseContainer(int httpStatusCode, String httpStatusMsg, @Nullable String dataServiceVersion,
                                         byte[] responseStream) {

    this.httpStatusCode = httpStatusCode;
    this.httpStatusMsg = httpStatusMsg;
    this.dataServiceVersion = dataServiceVersion;
    this.responseStream = responseStream;
  }

  public static Builder builder() {
    return new Builder();
  }

  public int getHttpStatusCode() {
    return this.httpStatusCode;
  }

  public String getHttpStatusMsg() {
    return this.httpStatusMsg;
  }

  @Nullable
  public String getDataServiceVersion() {
    return this.dataServiceVersion;
  }

  @Nullable
  public InputStream getResponseStream() {
    return new ByteArrayInputStream(responseStream);
  }

  /**
   * Helper class to simplify {@link SuccessFactorsResponseContainer} class creation.
   */
  public static class Builder {
    private int httpStatusCode;
    private String httpStatusMsg;
    @Nullable
    private String dataServiceVersion;
    private byte[] responseStream;

    public Builder httpStatusCode(int httpStatusCode) {
      this.httpStatusCode = httpStatusCode;
      return this;
    }

    public Builder httpStatusMsg(String httpStatusMsg) {
      this.httpStatusMsg = httpStatusMsg;
      return this;
    }

    public Builder dataServiceVersion(@Nullable String dataServiceVersion) {
      this.dataServiceVersion = dataServiceVersion;
      return this;
    }

    public Builder responseStream(byte[] responseStream) {
      this.responseStream = responseStream;
      return this;
    }

    public SuccessFactorsResponseContainer build() {
      return new SuccessFactorsResponseContainer(this.httpStatusCode, this.httpStatusMsg, this.dataServiceVersion,
                                                 this.responseStream);
    }
  }
}

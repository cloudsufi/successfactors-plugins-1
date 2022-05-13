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

import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * InnerError class
 */
public class InnerError {
  private final String transactionId;
  private final String timestamp;
  @Nullable
  private final Application application;
  @Nullable
  private final ErrorResolution errorResolution;
  @Nullable
  private final List<ErrorDetail> errordetails;

  public InnerError(String transactionId, String timestamp, @Nullable Application application,
                    @Nullable ErrorResolution errorResolution, @Nullable List<ErrorDetail> errordetails) {

    this.transactionId = transactionId;
    this.timestamp = timestamp;
    this.application = application;
    this.errorResolution = errorResolution;
    this.errordetails = errordetails;
  }

  public String getTransactionId() {
    return transactionId;
  }

  public String getTimestamp() {
    return timestamp;
  }

  @Nullable
  public Application getApplication() {
    return application;
  }

  @Nullable
  public ErrorResolution getErrorResolution() {
    return errorResolution;
  }

  @Nullable
  public List<ErrorDetail> getInnerErrorDetails() {
    return errordetails == null ? Collections.emptyList() : errordetails;
  }
  
  /**
   * Application class
   */
  public class Application {

    private final String componentId;
    private final String serviceNamespace;
    private final String serviceId;
    private final String serviceVersion;

    public Application(String componentId, String serviceNamespace, String serviceId, String serviceVersion) {

      this.componentId = componentId;
      this.serviceNamespace = serviceNamespace;
      this.serviceId = serviceId;
      this.serviceVersion = serviceVersion;
    }

    public String getComponentId() {
      return componentId;
    }

    public String getServiceNamespace() {
      return serviceNamespace;
    }

    public String getServiceId() {
      return serviceId;
    }

    public String getServiceVersion() {
      return serviceVersion;
    }
  }
}

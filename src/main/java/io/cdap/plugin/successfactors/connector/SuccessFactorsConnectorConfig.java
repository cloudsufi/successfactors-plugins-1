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
package io.cdap.plugin.successfactors.connector;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.successfactors.common.exception.SuccessFactorsServiceException;
import io.cdap.plugin.successfactors.common.exception.TransportException;
import io.cdap.plugin.successfactors.common.util.ResourceConstants;
import io.cdap.plugin.successfactors.common.util.SuccessFactorsUtil;
import io.cdap.plugin.successfactors.source.transport.SuccessFactorsResponseContainer;
import io.cdap.plugin.successfactors.source.transport.SuccessFactorsTransporter;
import okhttp3.HttpUrl;

import java.net.HttpURLConnection;
import java.net.URL;
import javax.ws.rs.core.MediaType;

/**
 * SuccessFactorsConnectorConfig Class
 */
public class SuccessFactorsConnectorConfig extends PluginConfig {

  public static final String BASE_URL = "baseURL";
  public static final String UNAME = "username";
  public static final String PASSWORD = "password";
  public static final String TEST = "TEST";
  private static final String COMMON_ACTION = ResourceConstants.ERR_MISSING_PARAM_OR_MACRO_ACTION.getMsgForKey();
  private static final String SAP_SUCCESSFACTORS_USERNAME = "SAP SuccessFactors Username";
  private static final String SAP_SUCCESSFACTORS_PASSWORD = "SAP SuccessFactors Password";
  private static final String SAP_SUCCESSFACTORS_BASE_URL = "SAP SuccessFactors Base URL";

  @Name(UNAME)
  @Macro
  @Description("SAP SuccessFactors Username for user authentication.")
  private final String username;

  @Name(PASSWORD)
  @Macro
  @Description("SAP SuccessFactors password for user authentication.")
  private final String password;

  @Macro
  @Name(BASE_URL)
  @Description("SuccessFactors Base URL.")
  private final String baseURL;

  public SuccessFactorsConnectorConfig(String username, String password, String baseURL) {
    this.username = username;
    this.password = password;
    this.baseURL = baseURL;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public String getBaseURL() {
    return baseURL;
  }

  public void validateBasicCredentials(FailureCollector failureCollector) {

    if (SuccessFactorsUtil.isNullOrEmpty(getUsername()) && !containsMacro(UNAME)) {
      String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey(SAP_SUCCESSFACTORS_USERNAME);
      failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(UNAME);
    }
    if (SuccessFactorsUtil.isNullOrEmpty(getPassword()) && !containsMacro(PASSWORD)) {
      String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey(SAP_SUCCESSFACTORS_PASSWORD);
      failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(PASSWORD);
    }
    if (SuccessFactorsUtil.isNullOrEmpty(getBaseURL()) && !containsMacro(BASE_URL)) {
      String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey(SAP_SUCCESSFACTORS_BASE_URL);
      failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(BASE_URL);
    }
    if (SuccessFactorsUtil.isNotNullOrEmpty(getBaseURL()) && !containsMacro(BASE_URL)) {
      if (HttpUrl.parse(getBaseURL()) == null) {
        String errMsg = ResourceConstants.ERR_INVALID_BASE_URL.getMsgForKey(SAP_SUCCESSFACTORS_BASE_URL);
        failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(BASE_URL);
      }
    }
  }

  /**
   * Method to validate the credential fields.
   */
  public void validateConnection(FailureCollector collector) throws SuccessFactorsServiceException, TransportException {
    SuccessFactorsTransporter successFactorsHttpClient = new SuccessFactorsTransporter(getUsername(), getPassword());
    URL testerURL = HttpUrl.parse(getBaseURL()).newBuilder().build().url();
    SuccessFactorsResponseContainer responseContainer =
      successFactorsHttpClient.callSuccessFactorsEntity(testerURL, MediaType.APPLICATION_JSON, TEST);
    if (responseContainer.getHttpStatusCode() == HttpURLConnection.HTTP_UNAUTHORIZED) {
      String errMsg = ResourceConstants.ERR_INVALID_CREDENTIAL.getMsgForKey();
      collector.addFailure(errMsg, COMMON_ACTION);
    } else if (responseContainer.getHttpStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      String errMsg = ResourceConstants.ERR_INVALID_BASE_URL.getMsgForKey();
      collector.addFailure(errMsg, COMMON_ACTION);
    }
  }
}

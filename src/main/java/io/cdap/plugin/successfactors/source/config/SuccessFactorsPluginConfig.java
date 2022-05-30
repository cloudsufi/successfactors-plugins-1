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

package io.cdap.plugin.successfactors.source.config;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.successfactors.common.util.ResourceConstants;
import io.cdap.plugin.successfactors.common.util.SuccessFactorsUtil;
import okhttp3.HttpUrl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * This {@code SuccessFactorsPluginConfig} contains all SAP SuccessFactors plugin UI configuration parameters.
 */
public class SuccessFactorsPluginConfig extends PluginConfig {
  public static final String BASE_URL = "baseURL";
  public static final String ENTITY_NAME = "entityName";
  public static final String ASSOCIATED_ENTITY_NAME = "associatedEntityName";
  public static final String UNAME = "username";
  public static final String PASSWORD = "password";
  public static final String NAME_SCHEMA = "schema";
  public static final String PAGINATION_TYPE = "paginationType";
  public static final String REFERENCE_NAME = "referenceName";
  public static final String REFERENCE_NAME_DESCRIPTION = "This will be used to uniquely identify this source/sink " +
    "for lineage, annotating metadata, etc.";
  private static final Logger LOG = LoggerFactory.getLogger(SuccessFactorsPluginConfig.class);
  private static final String COMMON_ACTION = ResourceConstants.ERR_MISSING_PARAM_OR_MACRO_ACTION.getMsgForKey();

  @Macro
  @Name(BASE_URL)
  @Description("SuccessFactors Base URL.")
  private final String baseURL;

  @Macro
  @Name(ENTITY_NAME)
  @Description("Name of the Entity which is being extracted.")
  private final String entityName;

  @Macro
  @Nullable
  @Name(ASSOCIATED_ENTITY_NAME)
  @Description("Name of the Associated Entity which is being extracted.")
  private final String associateEntityName;

  /**
   * Credentials parameters
   */
  @Name(UNAME)
  @Macro
  @Description("SAP SuccessFactors user ID.")
  private final String username;

  @Name(PASSWORD)
  @Macro
  @Description("SAP SuccessFactors password for user authentication.")
  private final String password;

  /**
   * Advanced parameters
   */
  @Nullable
  @Macro
  @Description("Filter condition to restrict the output data volume e.g. Price gt 200")
  private final String filterOption;

  @Nullable
  @Macro
  @Description("Fields to be preserved in the extracted data e.g.: Name,Gender,State/City")
  private final String selectOption;

  @Nullable
  @Macro
  @Description("List of navigation fields to be expanded in the extracted output data e.g.: State/City")
  private final String expandOption;
  
  /**
   * Basic parameters.
   */
  @Name(REFERENCE_NAME)
  @Description(REFERENCE_NAME_DESCRIPTION)
  public String referenceName;

  @Name(NAME_SCHEMA)
  @Macro
  @Nullable
  @Description("The schema of the table to read.")
  private String schema;

  @Name(PAGINATION_TYPE)
  @Macro
  @Description("The type of pagination to be used.")
  private String paginationType;

  SuccessFactorsPluginConfig(String referenceName,
                             String baseURL,
                             String entityName,
                             String associateEntityName,
                             @Nullable String username,
                             @Nullable String password,
                             @Nullable String filterOption,
                             @Nullable String selectOption,
                             @Nullable String expandOption,
                             String paginationType) {

    this.referenceName = referenceName;
    this.baseURL = baseURL;
    this.entityName = entityName;
    this.associateEntityName = associateEntityName;
    this.username = username;
    this.password = password;
    this.filterOption = filterOption;
    this.selectOption = selectOption;
    this.expandOption = expandOption;
    this.paginationType = paginationType;
  }

  public static Builder builder() {
    return new Builder();
  }

  public String getReferenceName() {
    return this.referenceName;
  }

  public String getBaseURL() {
    return SuccessFactorsUtil.trim(this.baseURL);
  }

  public String getEntityName() {
    return SuccessFactorsUtil.trim(this.entityName);
  }

  public String getAssociatedEntityName() {
    return SuccessFactorsUtil.trim(this.associateEntityName);
  }

  @Nullable
  public String getUsername() {
    return SuccessFactorsUtil.trim(this.username);
  }

  @Nullable
  public String getPassword() {
    return this.password;
  }

  @Nullable
  public String getFilterOption() {
    // Plugin UI field is 'textarea' so the user can input multiline filter statement
    // and as line break are not supported in URI
    // so any line break is removed from the filter option.
    return SuccessFactorsUtil.removeLinebreak(this.filterOption);
  }

  @Nullable
  public String getSelectOption() {
    // Plugin UI field is 'textarea' so the user can input multiline select statement
    // and as line break are not supported in URI and any select column name with extra
    // spaces are considered as actual column by SuccessFactors services and results in 'Not Found'
    // so to avoid the 'Not Found' scenarios
    // any line break and extra spaces is removed from the select option.
    // e.g.
    //    $select = col1, col2,
    //                col3,col4
    //    will be convert to col1,col2,col3,col4
    return SuccessFactorsUtil.removeWhitespace(this.selectOption);
  }

  @Nullable
  public String getExpandOption() {
    return SuccessFactorsUtil.removeWhitespace(this.expandOption);
  }

  public String getPaginationType() {
    return this.paginationType;
  }

  /**
   * Checks if the call to SuccessFactors service is required for metadata creation.
   * condition parameters: ['host' | 'serviceName' | 'entityName' | 'username' | 'password']
   * - any parameter is 'macro' then it returns 'false'
   *
   * @return boolean flag as per the check
   */
  public boolean isSchemaBuildRequired() {
    LOG.debug("Checking output schema creation is required or not.");
    if (containsMacro(BASE_URL) || containsMacro(ENTITY_NAME)) {
      return false;
    }

    return !containsMacro(UNAME) && !containsMacro(PASSWORD);
  }

  /**
   * @return the schema of the dataset
   */
  @Nullable
  public Schema getSchema(FailureCollector collector) {
    try {
      return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
    } catch (IOException e) {
      collector.addFailure("Invalid schema: " + e.getMessage(), null)
        .withConfigProperty(NAME_SCHEMA);
    }
    // if there was an error that was added, it will throw an exception, otherwise, this statement will not be executed
    throw collector.getOrThrowException();
  }

  public void validatePluginParameters(FailureCollector failureCollector) {
    LOG.debug("Validating mandatory parameters.");
    validateMandatoryParameters(failureCollector);
    LOG.debug("Validating the Basic Security Type parameters.");
    validateBasicCredentials(failureCollector);
    LOG.debug("Validating the advanced parameters.");
    validateEntityParameter(failureCollector);
    failureCollector.getOrThrowException();
  }

  /**
   * Validates the mandatory parameters.
   *
   * @param failureCollector {@code FailureCollector}
   */
  private void validateMandatoryParameters(FailureCollector failureCollector) {

    IdUtils.validateReferenceName(getReferenceName(), failureCollector);
    if (SuccessFactorsUtil.isNullOrEmpty(getBaseURL()) && !containsMacro(BASE_URL)) {
      String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("SAP SuccessFactor Base URL");
      failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(BASE_URL);
    }
    if (SuccessFactorsUtil.isNotNullOrEmpty(getBaseURL()) && !containsMacro(BASE_URL)) {
      if (HttpUrl.parse(getBaseURL()) == null) {
        String errMsg = ResourceConstants.ERR_INVALID_BASE_URL.getMsgForKey("SAP SuccessFactor Base URL");
        failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(BASE_URL);
      }
    }
    if (SuccessFactorsUtil.isNullOrEmpty(getEntityName()) && !containsMacro(ENTITY_NAME)) {
      String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("Entity Name");
      failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(ENTITY_NAME);
    }
  }

  /**
   * Validates the credentials parameters.
   *
   * @param failureCollector {@code FailureCollector}
   */
  private void validateBasicCredentials(FailureCollector failureCollector) {

    if (SuccessFactorsUtil.isNullOrEmpty(getUsername()) && !containsMacro(UNAME)) {
      String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("SAP SuccessFactors Username");
      failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(UNAME);
    }
    if (SuccessFactorsUtil.isNullOrEmpty(getPassword()) && !containsMacro(PASSWORD)) {
      String errMsg = ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("SAP SuccessFactors Password");
      failureCollector.addFailure(errMsg, COMMON_ACTION).withConfigProperty(PASSWORD);
    }
  }

  /**
   * Checks if the Entity field contains any 'Key' values e.g Products(2). Then throws the error as this is not
   * supported.
   *
   * @param failureCollector {@code FailureCollector}
   */
  private void validateEntityParameter(FailureCollector failureCollector) {
    if (SuccessFactorsUtil.isNotNullOrEmpty(getEntityName()) && !containsMacro(getEntityName())) {
      Pattern pattern = Pattern.compile("\\(.*\\)");
      if (pattern.matcher(getEntityName()).find()) {
        failureCollector.addFailure(ResourceConstants.ERR_FEATURE_NOT_SUPPORTED.getMsgForKey(), null)
          .withConfigProperty(ENTITY_NAME);
      }
    }
  }

  /**
   * Helper class to simplify {@link SuccessFactorsPluginConfig} class creation.
   */
  public static class Builder {
    private String referenceName;
    private String baseURL;
    private String entityName;
    private String associateEntityName;
    private String username;
    private String password;
    private String filterOption;
    private String selectOption;
    private String expandOption;
    private String paginationType;

    public Builder referenceName(String referenceName) {
      this.referenceName = referenceName;
      return this;
    }

    public Builder baseURL(String host) {
      this.baseURL = host;
      return this;
    }

    public Builder entityName(String entityName) {
      this.entityName = entityName;
      return this;
    }

    public Builder associateEntityName(String associateEntityName) {
      this.associateEntityName = associateEntityName;
      return this;
    }

    public Builder username(@Nullable String username) {
      this.username = username;
      return this;
    }

    public Builder password(@Nullable String password) {
      this.password = password;
      return this;
    }

    public Builder filterOption(@Nullable String filterOption) {
      this.filterOption = filterOption;
      return this;
    }

    public Builder selectOption(@Nullable String selectOption) {
      this.selectOption = selectOption;
      return this;
    }

    public Builder expandOption(@Nullable String expandOption) {
      this.expandOption = expandOption;
      return this;
    }
    
    public Builder paginationType(@Nullable String paginationType) {
      this.paginationType = paginationType;
      return this;
    }
    

    public SuccessFactorsPluginConfig build() {
      return new SuccessFactorsPluginConfig(referenceName, baseURL, entityName, associateEntityName, username, password,
                                            filterOption, selectOption, expandOption, paginationType);
    }
  }
}

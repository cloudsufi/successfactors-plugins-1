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

import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.successfactors.common.util.ResourceConstants;
import io.cdap.plugin.successfactors.connector.SuccessFactorsConnectorConfig;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class SuccessFactorsPluginConfigTest {

  private static final String REFERENCE_NAME = "unit-test-ref-name";
  private static final String BASE_URL = "http://localhost";
  private static final String ENTITY_NAME = "hunit-test-ref-name";
  private static final String ENTITY = "entity";
  private static final String USER_NAME = "username";
  private static final String PASSWORD = "password";
  private MockFailureCollector failureCollector;
  private SuccessFactorsPluginConfig.Builder pluginConfigBuilder;

  @Before
  public void setUp() {
    failureCollector = new MockFailureCollector();
    pluginConfigBuilder = SuccessFactorsPluginConfig.builder()
      .referenceName(REFERENCE_NAME)
      .baseURL(BASE_URL)
      .entityName(ENTITY_NAME)
      .authType(SuccessFactorsConnectorConfig.BASIC_AUTH)
      .username(USER_NAME)
      .password(PASSWORD);
  }

  @Test
  public void testSchemaBuildRequired() {
    SuccessFactorsPluginConfig pluginConfig = pluginConfigBuilder.build();
    Assert.assertTrue(pluginConfig.isSchemaBuildRequired());
  }

  @Test
  public void testValidatePluginParametersWithEmptyEntityName() {
    SuccessFactorsPluginConfig pluginConfig = pluginConfigBuilder
      .entityName(null)
      .username(USER_NAME)
      .password(PASSWORD)
      .build();

    try {
      pluginConfig.validatePluginParameters(failureCollector);
      Assert.fail("EntityName is null");
    } catch (ValidationException ve) {
      List<ValidationFailure> failures = ve.getFailures();
      Assert.assertEquals(1, failures.size());
      Assert.assertEquals(ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("Entity Name"),
                          failures.get(0).getMessage());
    }
  }

  @Test
  public void testValidatePluginParametersWithEmptyUserName() {
    SuccessFactorsPluginConfig pluginConfig = pluginConfigBuilder
      .entityName("entity")
      .username(null)
      .password(PASSWORD)
      .build();
    try {
      pluginConfig.validatePluginParameters(failureCollector);
      Assert.fail("Username is null");
    } catch (ValidationException ve) {
      List<ValidationFailure> failures = ve.getFailures();
      Assert.assertEquals(1, failures.size());
      Assert.assertEquals(ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("SAP SuccessFactors Username"),
                          failures.get(0).getMessage());
    }
  }

  @Test
  public void testValidatePluginParametersWithEmptyPassword() {
    SuccessFactorsPluginConfig pluginConfig = pluginConfigBuilder
      .entityName(ENTITY)
      .username(USER_NAME)
      .password(null)
      .build();
    try {
      pluginConfig.validatePluginParameters(failureCollector);
      Assert.fail("Password is null");
    } catch (ValidationException ve) {
      List<ValidationFailure> failures = ve.getFailures();
      Assert.assertEquals(1, failures.size());
      Assert.assertEquals(ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("SAP SuccessFactors Password"),
                          failures.get(0).getMessage());
    }
  }

  @Test
  public void testValidatePluginParametersWithEmptyURL() {
    SuccessFactorsPluginConfig pluginConfig = pluginConfigBuilder
      .entityName(ENTITY)
      .username(USER_NAME)
      .password(PASSWORD)
      .baseURL(null)
      .build();
    try {
      pluginConfig.validatePluginParameters(failureCollector);
      Assert.fail("URL is null");
    } catch (ValidationException ve) {
      List<ValidationFailure> failures = ve.getFailures();
      Assert.assertEquals(1, failures.size());
      Assert.assertEquals(ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("SAP SuccessFactors Base URL"),
                          failures.get(0).getMessage());
    }
  }

  @Test
  public void testInValidBaseURL() {
    SuccessFactorsPluginConfig pluginConfig = pluginConfigBuilder
      .baseURL("INVALID-URL")
      .build();
    try {
      pluginConfig.validatePluginParameters(failureCollector);
      Assert.fail("invalid URL");
    } catch (ValidationException ve) {
      List<ValidationFailure> failures = ve.getFailures();
      Assert.assertEquals(1, failures.size());
      Assert.assertEquals(ResourceConstants.ERR_INVALID_BASE_URL.getMsgForKey(), failures.get(0).getMessage());
    }
  }

  @Test
  public void testValidateEntityForKeyBasedExtraction() {
    SuccessFactorsPluginConfig pluginConfig = pluginConfigBuilder.entityName("Products(1)").build();
    try {
      pluginConfig.validatePluginParameters(failureCollector);
      Assert.fail("Type of entity is not supported");
    } catch (ValidationException ve) {
      List<ValidationFailure> failures = ve.getFailures();
      Assert.assertEquals(1, failures.size());
      Assert.assertEquals(ResourceConstants.ERR_FEATURE_NOT_SUPPORTED.getMsgForKey(), failures.get(0).getMessage());
    }
  }

  @Test
  public void testRefactoredPluginPropertyValues() {
    SuccessFactorsPluginConfig pluginConfig = pluginConfigBuilder
      .baseURL("http://localhost:5000")
      .entityName("entity-name")
      .associateEntityName("AssEntity")
      .filterOption("amount le 20 and amount gt 4")
      .expandOption("col1")
      .selectOption("col1,col2,   \n  parent/col1,\r       col3     ")
      .paginationType("Type1")
      .build();
    Assert.assertEquals("AssEntity", pluginConfig.getAssociatedEntityName());
    Assert.assertEquals("Type1", pluginConfig.getPaginationType());
    Assert.assertEquals("col1", pluginConfig.getExpandOption());
    Assert.assertEquals("amount le 20 and amount gt 4", pluginConfig.getFilterOption());
    Assert.assertEquals("Base URL not trimmed", "http://localhost:5000", pluginConfig.getConnection().getBaseURL());
    Assert.assertEquals("Entity name not trimmed", "entity-name", pluginConfig.getEntityName());
    Assert.assertEquals("Select option not trimmed", "col1,col2,parent/col1,col3", pluginConfig.getSelectOption());
  }

  @Test
  public void testValidateRetryConfigurationWithDefaultValues() {
    SuccessFactorsPluginConfig pluginConfig = pluginConfigBuilder
      .setInitialRetryDuration(SuccessFactorsPluginConfig.DEFAULT_INITIAL_RETRY_DURATION_SECONDS)
      .setMaxRetryDuration(SuccessFactorsPluginConfig.DEFAULT_MAX_RETRY_DURATION_SECONDS)
      .setMaxRetryCount(SuccessFactorsPluginConfig.DEFAULT_MAX_RETRY_COUNT)
      .setRetryMultiplier(SuccessFactorsPluginConfig.DEFAULT_RETRY_MULTIPLIER)
      .build();
    pluginConfig.validateRetryConfiguration(failureCollector);
    Assert.assertEquals(0, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testValidateRetryConfigurationWithInvalidInitialRetryDuration() {
    SuccessFactorsPluginConfig pluginConfig = pluginConfigBuilder
      .setInitialRetryDuration(-1)
      .setMaxRetryDuration(SuccessFactorsPluginConfig.DEFAULT_MAX_RETRY_DURATION_SECONDS)
      .setMaxRetryCount(SuccessFactorsPluginConfig.DEFAULT_MAX_RETRY_COUNT)
      .setRetryMultiplier(SuccessFactorsPluginConfig.DEFAULT_RETRY_MULTIPLIER)
      .build();
    pluginConfig.validateRetryConfiguration(failureCollector);
    Assert.assertEquals(1, failureCollector.getValidationFailures().size());
    Assert.assertEquals("Initial retry duration must be greater than 0.",
                        failureCollector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testValidateRetryConfigurationWithInvalidMaxRetryDuration() {
    SuccessFactorsPluginConfig pluginConfig = pluginConfigBuilder
      .setInitialRetryDuration(SuccessFactorsPluginConfig.DEFAULT_INITIAL_RETRY_DURATION_SECONDS)
      .setMaxRetryDuration(-1)
      .setMaxRetryCount(SuccessFactorsPluginConfig.DEFAULT_MAX_RETRY_COUNT)
      .setRetryMultiplier(SuccessFactorsPluginConfig.DEFAULT_RETRY_MULTIPLIER)
      .build();
    pluginConfig.validateRetryConfiguration(failureCollector);
    Assert.assertEquals(2, failureCollector.getValidationFailures().size());
    Assert.assertEquals("Max retry duration must be greater than 0.",
                        failureCollector.getValidationFailures().get(0).getMessage());
    Assert.assertEquals("Max retry duration must be greater than initial retry duration.",
                        failureCollector.getValidationFailures().get(1).getMessage());
  }

  @Test
  public void testValidateRetryConfigurationWithInvalidRetryMultiplier() {
    SuccessFactorsPluginConfig pluginConfig = pluginConfigBuilder
      .setInitialRetryDuration(SuccessFactorsPluginConfig.DEFAULT_INITIAL_RETRY_DURATION_SECONDS)
      .setMaxRetryDuration(SuccessFactorsPluginConfig.DEFAULT_MAX_RETRY_DURATION_SECONDS)
      .setMaxRetryCount(SuccessFactorsPluginConfig.DEFAULT_MAX_RETRY_COUNT)
      .setRetryMultiplier(-1)
      .build();
    pluginConfig.validateRetryConfiguration(failureCollector);
    Assert.assertEquals(1, failureCollector.getValidationFailures().size());
    Assert.assertEquals("Retry multiplier must be strictly greater than 1.",
                        failureCollector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testValidateRetryConfigurationWithInvalidRetryMultiplierAndMaxRetryCount() {
    SuccessFactorsPluginConfig pluginConfig = pluginConfigBuilder
      .setInitialRetryDuration(SuccessFactorsPluginConfig.DEFAULT_INITIAL_RETRY_DURATION_SECONDS)
      .setMaxRetryDuration(SuccessFactorsPluginConfig.DEFAULT_MAX_RETRY_DURATION_SECONDS)
      .setMaxRetryCount(1)
      .setRetryMultiplier(-1)
      .build();
    pluginConfig.validateRetryConfiguration(failureCollector);
    Assert.assertEquals(1, failureCollector.getValidationFailures().size());
    Assert.assertEquals("Retry multiplier must be strictly greater than 1.",
                        failureCollector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testValidateRetryConfigurationWithMultiplierOne() {
    SuccessFactorsPluginConfig pluginConfig = pluginConfigBuilder
      .setInitialRetryDuration(SuccessFactorsPluginConfig.DEFAULT_INITIAL_RETRY_DURATION_SECONDS)
      .setMaxRetryDuration(SuccessFactorsPluginConfig.DEFAULT_MAX_RETRY_DURATION_SECONDS)
      .setMaxRetryCount(SuccessFactorsPluginConfig.DEFAULT_MAX_RETRY_COUNT)
      .setRetryMultiplier(1)
      .build();
    pluginConfig.validateRetryConfiguration(failureCollector);
    Assert.assertEquals(1, failureCollector.getValidationFailures().size());
    Assert.assertEquals("Retry multiplier must be strictly greater than 1.",
                        failureCollector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testValidateRetryConfigurationWithMaxRetryLessThanInitialRetry() {
    SuccessFactorsPluginConfig pluginConfig = pluginConfigBuilder
      .setInitialRetryDuration(20)
      .setMaxRetryDuration(10)
      .setMaxRetryCount(1)
      .setRetryMultiplier(2)
      .build();
    pluginConfig.validateRetryConfiguration(failureCollector);
    Assert.assertEquals(1, failureCollector.getValidationFailures().size());
    Assert.assertEquals("Max retry duration must be greater than initial retry duration.",
                        failureCollector.getValidationFailures().get(0).getMessage());
  }
}

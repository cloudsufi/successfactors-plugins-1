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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class SuccessFactorsPluginConfigTest {

  private MockFailureCollector failureCollector;
  private SuccessFactorsPluginConfig.Builder pluginConfigBuilder;

  @Before
  public void setUp() {
    failureCollector = new MockFailureCollector();
    pluginConfigBuilder = SuccessFactorsPluginConfig.builder()
      .referenceName("unit-test-ref-name")
      .baseURL("http://localhost")
      .entityName("entity name")
      .username("username")
      .password("password");
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
      .username("username")
      .password("password")
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
      .password("password")
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
      .entityName("entity")
      .username("username")
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
      .entityName("entity")
      .username("username")
      .password("password")
      .baseURL(null)
      .build();
    try {
      pluginConfig.validatePluginParameters(failureCollector);
      Assert.fail("URL is null");
    } catch (ValidationException ve) {
      List<ValidationFailure> failures = ve.getFailures();
      Assert.assertEquals(1, failures.size());
      Assert.assertEquals(ResourceConstants.ERR_MISSING_PARAM_PREFIX.getMsgForKey("SAP SuccessFactor Base URL"),
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
      .baseURL(" http://localhost:5000 ")
      .entityName("entity-name")
      .selectOption("col1,col2,   \n  parent/col1,\r       col3     ")
      .build();
    Assert.assertEquals("Base URL not trimmed", "http://localhost:5000", pluginConfig.getBaseURL());
    Assert.assertEquals("Entity name not trimmed", "entity-name", pluginConfig.getEntityName());
    Assert.assertEquals("Select option not trimmed", "col1,col2,parent/col1,col3", pluginConfig.getSelectOption());
  }

}

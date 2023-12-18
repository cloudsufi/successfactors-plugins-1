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
package io.cdap.plugin.successfactors.source;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.successfactors.common.exception.SuccessFactorsServiceException;
import io.cdap.plugin.successfactors.common.exception.TransportException;
import io.cdap.plugin.successfactors.common.util.ResourceConstants;
import io.cdap.plugin.successfactors.source.config.SuccessFactorsPluginConfig;
import io.cdap.plugin.successfactors.source.input.SuccessFactorsInputSplit;
import io.cdap.plugin.successfactors.source.input.SuccessFactorsPartitionBuilder;
import io.cdap.plugin.successfactors.source.metadata.SuccessFactorsEntityProvider;
import io.cdap.plugin.successfactors.source.metadata.SuccessFactorsSchemaGenerator;
import io.cdap.plugin.successfactors.source.service.SuccessFactorsService;
import io.cdap.plugin.successfactors.source.transport.SuccessFactorsResponseContainer;
import io.cdap.plugin.successfactors.source.transport.SuccessFactorsTransporter;
import io.cdap.plugin.successfactors.source.transport.SuccessFactorsUrlContainer;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Tested;
import org.apache.olingo.odata2.api.edm.Edm;
import org.apache.olingo.odata2.api.ep.EntityProvider;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SuccessFactorsSourceTest {

  @Tested
  private SuccessFactorsPluginConfig.Builder pluginConfigBuilder;
  private SuccessFactorsSource successFactorsSource;
  private SuccessFactorsPluginConfig pluginConfig;
  private SuccessFactorsTransporter successFactorsTransporter;
  private SuccessFactorsService successFactorsService;
  private SuccessFactorsPartitionBuilder successFactorsPartitionBuilder;
  private Schema schema;
  private MockPipelineConfigurer pipelineConfigurer;
  @Mocked
  private Edm edm;
  @Mocked
  private BatchSourceContext context;
  @Mocked
  private EntityProvider entityProvider;
  private SuccessFactorsUrlContainer successFactorsUrlContainer;
  private SuccessFactorsSchemaGenerator successFactorsSchemaGenerator;


  @Before
  public void setUp() {
    pipelineConfigurer = new MockPipelineConfigurer(null);
    pluginConfigBuilder = SuccessFactorsPluginConfig.builder()
      .referenceName("unit-test-ref-name")
      .baseURL("http://localhost")
      .entityName("entity name")
      .username("username")
      .password("password")
      .selectOption("col1,col2,   \n  parent/col1,\r       col3     ");
  }

  @Test
  public void testConfigurePipelineWithInvalidUrl() throws Exception {
    pluginConfigBuilder = SuccessFactorsPluginConfig.builder()
      .referenceName("unit-test-ref-name")
      .baseURL("base_url")
      .entityName("entity name")
      .username("username")
      .password("password")
      .selectOption("col1,col2,   \n  parent/col1,\r       col3     ");
    SuccessFactorsPluginConfig pluginConfig = pluginConfigBuilder.build();
    successFactorsSource = new SuccessFactorsSource(pluginConfig);
    Map<String, Object> plugins = new HashMap<>();
    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(null, plugins);
    try {
      successFactorsSource.configurePipeline(mockPipelineConfigurer);
      Assert.fail("Exception is not thrown if valid url is provided");
    } catch (ValidationException e) {
      Assert.assertEquals("Please verify the provided base url is correct. Please contact the SAP " +
                            "administrator.", e.getFailures().get(0).getMessage());
    }
  }

  @Test
  public void testConfigurePipelineWithInvalidReferenceName() {
    pluginConfigBuilder = SuccessFactorsPluginConfig.builder()
      .referenceName("")
      .baseURL("http://localhost")
      .entityName("entity name")
      .username("username")
      .password("password")
      .selectOption("col1,col2,   \n  parent/col1,\r       col3     ");
    try {
      pluginConfig = pluginConfigBuilder.build();
      successFactorsSource = new SuccessFactorsSource(pluginConfig);
      Map<String, Object> plugins = new HashMap<>();
      MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(null, plugins);
      successFactorsSource.configurePipeline(mockPipelineConfigurer);
      Assert.fail("Exception is not thrown if valid reference name is provided");
    } catch (ValidationException ve) {
      List<ValidationFailure> failures = ve.getFailures();
      Assert.assertEquals(1, failures.size());
      Assert.assertEquals("Invalid reference name ''.", ve.getFailures().get(0).getMessage());
    }
  }

  private URL getUrl() throws MalformedURLException {
    return new URL("http://localhost:8000/odata/v2/entity-name");
  }

  private SuccessFactorsResponseContainer getResponseContainer() {
    return new SuccessFactorsResponseContainer(200, "ok",
                                               "2.0", new byte[]{50});
  }

  private Schema getPluginSchema() throws IOException {
    String schemaString = "{\"type\":\"record\",\"name\":\"SuccessFactorsColumnMetadata\",\"fields\":[{\"name\":" +
      "\"backgroundElementId\",\"type\":\"long\"},{\"name\":\"bgOrderPos\",\"type\":\"long\"},{\"name\":" +
      "\"description\",\"type\":[\"string\",\"null\"]},{\"name\":\"endDate\",\"type\":[{\"type\":\"long\"," +
      "\"logicalType\":\"timestamp-micros\"},\"null\"]},{\"name\":\"lastModifiedDate\",\"type\":" +
      "[{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"},\"null\"]},{\"name\":\"project\",\"type\":" +
      "\"string\"},{\"name\":\"startDate\",\"type\":[{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}," +
      "\"null\"]},{\"name\":\"userId\",\"type\":\"string\"}]}";

    return Schema.parseJson(schemaString);
  }

  @Test
  public void testConfigurePipelineWSchemaNotNull() throws SuccessFactorsServiceException, TransportException,
    IOException {
    pluginConfigBuilder = SuccessFactorsPluginConfig.builder()
      .referenceName("unit-test-ref-name")
      .baseURL("http://localhost")
      .entityName("entity-name")
      .username("username")
      .password("password");

    pluginConfig = pluginConfigBuilder.build();
    successFactorsTransporter = new SuccessFactorsTransporter(pluginConfig.getConnection());
    successFactorsUrlContainer = new SuccessFactorsUrlContainer(pluginConfig);
    successFactorsSchemaGenerator = new SuccessFactorsSchemaGenerator(new SuccessFactorsEntityProvider(edm));

    new Expectations(SuccessFactorsUrlContainer.class, SuccessFactorsTransporter.class,
                     SuccessFactorsSchemaGenerator.class) {
      {
        successFactorsUrlContainer.getTesterURL();
        result = getUrl();
        minTimes = 1;

        successFactorsUrlContainer.getMetadataURL();
        result = getUrl();
        minTimes = 1;

        successFactorsTransporter.callSuccessFactorsEntity(null, anyString, anyString);
        result = getResponseContainer();
        minTimes = 1;

        successFactorsSchemaGenerator.buildDefaultOutputSchema(anyString);
        result = getPluginSchema();
        minTimes = 1;
      }
    };

    successFactorsSource = new SuccessFactorsSource(pluginConfig);
    Map<String, Object> plugins = new HashMap<>();
    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(null, plugins);
    successFactorsSource.configurePipeline(pipelineConfigurer);
    schema = pipelineConfigurer.getOutputSchema();
    Assert.assertNotNull("Output Schema generated is not Null", schema);
    Assert.assertEquals(8, schema.getFields().size());
    Assert.assertEquals("{name: bgOrderPos, schema: \"long\"}", schema.getFields().get(1).toString());
  }

  @Test
  public void testConfigurePipelineSchemaNull() {
    SuccessFactorsPluginConfig pluginConfig = pluginConfigBuilder.build();
    new Expectations(SuccessFactorsPluginConfig.class) {
      {
        pluginConfig.isSchemaBuildRequired();
        result = false;
        minTimes = 1;
      }
    };
    Map<String, Object> plugins = new HashMap<>();
    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(null, plugins);
    successFactorsSource = new SuccessFactorsSource(pluginConfig);
    successFactorsSource.configurePipeline(mockPipelineConfigurer);
    Assert.assertNull(schema);
  }

  /**
   * ValidationException is expected as SuccessFactors services are not handled here.
   */
  @Test(expected = ValidationException.class)
  public void testConnectionException() {
    pluginConfig = pluginConfigBuilder.build();
    successFactorsSource = new SuccessFactorsSource(pluginConfig);
    Map<String, Object> plugins = new HashMap<>();
    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(null, plugins);
    successFactorsSource.configurePipeline(mockPipelineConfigurer);
  }

  /**
   * ValidationException is expected as SuccessFactors services are not handled here.
   * Connection will fail here and exception is thrown.
   */
  @Test
  public void testConfigurePipelineConnectionException() {
    pluginConfig = pluginConfigBuilder.build();
    new Expectations(SuccessFactorsPluginConfig.class, SuccessFactorsTransporter.class) {
      {
        pluginConfig.isSchemaBuildRequired();
        result = true;
        minTimes = 1;
      }
    };
    try {
      successFactorsSource = new SuccessFactorsSource(pluginConfig);
      Map<String, Object> plugins = new HashMap<>();
      MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(null, plugins);
      successFactorsSource.configurePipeline(mockPipelineConfigurer);
      Assert.fail("Connection exception is not thrown if valid connection is provided");
    } catch (ValidationException te) {
      List<ValidationFailure> failures = te.getFailures();
      Assert.assertEquals(1, failures.size());
    }
  }

  private List<SuccessFactorsInputSplit> getSplits() {
    List<SuccessFactorsInputSplit> splits = new ArrayList<>();

    splits.add(new SuccessFactorsInputSplit(101, 120, 20));
    splits.add(new SuccessFactorsInputSplit(121, 140, 20));
    splits.add(new SuccessFactorsInputSplit(141, 160, 20));
    splits.add(new SuccessFactorsInputSplit(161, 180, 20));
    splits.add(new SuccessFactorsInputSplit(181, 200, 20));

    return splits;
  }

  @Test
  public void testPrepareRun() throws Exception {
    successFactorsService = new SuccessFactorsService(pluginConfig, null);
    successFactorsPartitionBuilder = new SuccessFactorsPartitionBuilder();
    pluginConfigBuilder = SuccessFactorsPluginConfig.builder()
      .referenceName("unit-test-ref-name")
      .baseURL("http://localhost")
      .entityName("entity name")
      .username("username")
      .password("password")
      .paginationType("serverSide")
      .selectOption("col1,col2,   \n  parent/col1,\r       col3     ")
      .filterOption("$topeq2");

    pluginConfig = pluginConfigBuilder.build();
    successFactorsTransporter = new SuccessFactorsTransporter(pluginConfig.getConnection());
    new Expectations(SuccessFactorsService.class, SuccessFactorsTransporter.class) {
      {
        context.getOutputSchema();
        result = getPluginSchema();
        minTimes = 1;

        successFactorsTransporter.callSuccessFactors(null, anyString, anyString);
        result = getResponseContainer();
        minTimes = 1;

        successFactorsPartitionBuilder.buildSplits(50);

        result = getSplits();
        minTimes = 1;

        successFactorsService.getEncodedServiceMetadata();
        result = "encodeMetadataString";
        minTimes = 1;
      }

    };
    successFactorsSource = new SuccessFactorsSource(pluginConfig);
    successFactorsSource.prepareRun(context);
    Map<String, Object> plugins = new HashMap<>();
    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(null, plugins);
    schema = mockPipelineConfigurer.getOutputSchema();
    MockFailureCollector failureCollector = new MockFailureCollector();
    Assert.assertEquals(0, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testPrepareRunUnauthorizedError() throws Exception {
    successFactorsService = new SuccessFactorsService(pluginConfig, null);
    pluginConfigBuilder = SuccessFactorsPluginConfig.builder()
      .referenceName("")
      .baseURL("")
      .entityName("entity name")
      .username("username")
      .password("password");

    pluginConfig = pluginConfigBuilder.build();
    successFactorsSource = new SuccessFactorsSource(pluginConfig);

    new Expectations(SuccessFactorsService.class) {
      {
        context.getOutputSchema();
        result = null;
        minTimes = 1;

        successFactorsService.checkSuccessFactorsURL();
        result = new SuccessFactorsServiceException("Unauthorized Access", 401);
        minTimes = 1;
      }
    };

    try {
      successFactorsSource.prepareRun(context);
      Assert.fail("Exception is not thrown with authorized one");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(ResourceConstants.ERR_MACRO_INPUT.getMsgForKeyWithCode(), e.getMessage());
    }
  }

  @Test
  public void testPrepareRunForbiddenError() throws Exception {
    successFactorsService = new SuccessFactorsService(pluginConfig, null);
    pluginConfigBuilder = SuccessFactorsPluginConfig.builder()
      .referenceName("")
      .baseURL("")
      .entityName("entity name")
      .username("username")
      .password("password");

    pluginConfig = pluginConfigBuilder.build();
    successFactorsSource = new SuccessFactorsSource(pluginConfig);

    new Expectations(SuccessFactorsService.class) {
      {
        context.getOutputSchema();
        result = null;
        minTimes = 1;

        successFactorsService.checkSuccessFactorsURL();
        result = new SuccessFactorsServiceException("Forbidden Error", 403);
        minTimes = 1;
      }
    };

    try {
      successFactorsSource.prepareRun(context);
      Assert.fail("Exception is not thrown with authorized one");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(ResourceConstants.ERR_MACRO_INPUT.getMsgForKeyWithCode(), e.getMessage());
    }
  }

  @Test
  public void testPrepareRunNotFoundError() throws Exception {
    successFactorsService = new SuccessFactorsService(pluginConfig, null);
    pluginConfigBuilder = SuccessFactorsPluginConfig.builder()
      .referenceName("")
      .baseURL("")
      .entityName("entity name")
      .username("username")
      .password("password");

    pluginConfig = pluginConfigBuilder.build();
    successFactorsSource = new SuccessFactorsSource(pluginConfig);

    new Expectations(SuccessFactorsService.class) {
      {
        context.getOutputSchema();
        result = null;
        minTimes = 1;

        successFactorsService.checkSuccessFactorsURL();
        result = new SuccessFactorsServiceException("Not Found Error", 404);
        minTimes = 1;
      }
    };

    try {
      successFactorsSource.prepareRun(context);
      Assert.fail("Exception is not thrown with authorized one");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(ResourceConstants.ERR_MACRO_INPUT.getMsgForKeyWithCode(), e.getMessage());
    }
  }

  @Test
  public void testPrepareRunBadRequestError() throws Exception {
    successFactorsService = new SuccessFactorsService(pluginConfig, null);
    pluginConfigBuilder = SuccessFactorsPluginConfig.builder()
      .referenceName("")
      .baseURL("")
      .entityName("entity name")
      .username("username")
      .password("password");

    pluginConfig = pluginConfigBuilder.build();
    successFactorsSource = new SuccessFactorsSource(pluginConfig);

    new Expectations(SuccessFactorsService.class) {
      {
        context.getOutputSchema();
        result = null;
        minTimes = 1;

        successFactorsService.checkSuccessFactorsURL();
        result = new SuccessFactorsServiceException("Bad Request Error", 400);
        minTimes = 1;
      }
    };

    try {
      successFactorsSource.prepareRun(context);
      Assert.fail("Exception is not thrown with authorized one");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(ResourceConstants.ERR_MACRO_INPUT.getMsgForKeyWithCode(), e.getMessage());
    }
  }

  @Test
  public void testPrepareRunInvalidVersionError() throws Exception {
    successFactorsService = new SuccessFactorsService(pluginConfig, null);
    pluginConfigBuilder = SuccessFactorsPluginConfig.builder()
      .referenceName("")
      .baseURL("")
      .entityName("entity name")
      .username("username")
      .password("password");

    pluginConfig = pluginConfigBuilder.build();
    successFactorsSource = new SuccessFactorsSource(pluginConfig);

    new Expectations(SuccessFactorsService.class) {
      {
        context.getOutputSchema();
        result = null;
        minTimes = 1;

        successFactorsService.checkSuccessFactorsURL();
        result = new SuccessFactorsServiceException("Invalid Version Error", 1);
        minTimes = 1;
      }
    };

    try {
      successFactorsSource.prepareRun(context);
      Assert.fail("Exception is not thrown with correct version");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals(ResourceConstants.ERR_MACRO_INPUT.getMsgForKeyWithCode(), e.getMessage());
    }
  }
}

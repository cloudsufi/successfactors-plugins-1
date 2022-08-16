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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.PluginSpec;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.mock.common.MockConnectorConfigurer;
import io.cdap.cdap.etl.mock.common.MockConnectorContext;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.successfactors.common.exception.SuccessFactorsServiceException;
import io.cdap.plugin.successfactors.common.exception.TransportException;
import io.cdap.plugin.successfactors.source.SuccessFactorsSource;
import io.cdap.plugin.successfactors.source.config.SuccessFactorsPluginConfig;
import io.cdap.plugin.successfactors.source.metadata.SuccessFactorsEntityProvider;
import io.cdap.plugin.successfactors.source.metadata.SuccessFactorsSchemaGenerator;
import io.cdap.plugin.successfactors.source.transport.SuccessFactorsResponseContainer;
import io.cdap.plugin.successfactors.source.transport.SuccessFactorsTransporter;
import io.cdap.plugin.successfactors.source.transport.SuccessFactorsUrlContainer;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Tested;
import org.apache.olingo.odata2.api.edm.Edm;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.ep.EntityProviderException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SuccessFactorsConnectorTest {
  @Tested
  private SuccessFactorsPluginConfig.Builder pluginConfigBuilder;
  private SuccessFactorsPluginConfig pluginConfig;
  private SuccessFactorsTransporter successFactorsTransporter;
  private SuccessFactorsConnector successFactorsConnector;

  @Mocked
  private Edm edm;

  @Mocked
  private BatchSourceContext context;
  private SuccessFactorsSchemaGenerator successFactorsSchemaGenerator;

  @Before
  public void testConfiguration() throws TransportException, SuccessFactorsServiceException {
    pluginConfigBuilder = SuccessFactorsPluginConfig.builder()
      .referenceName("unit-test-ref-name")
      .baseURL("http://localhost")
      .entityName("entity-name")
      .username("username")
      .password("password");

    pluginConfig = pluginConfigBuilder.build();
  }

  @Test
  public void testValidateSuccessfulConnection() throws TransportException, SuccessFactorsServiceException {
    successFactorsTransporter = new SuccessFactorsTransporter(pluginConfig.getConnection().getUsername(),
                                                              pluginConfig.getConnection().getPassword());
    new Expectations(SuccessFactorsUrlContainer.class, SuccessFactorsTransporter.class,
                     SuccessFactorsSchemaGenerator.class) {
      {
        successFactorsTransporter.callSuccessFactorsEntity(null, anyString, anyString);
        result = getSuccessfulResponseContainer();
        minTimes = 1;
      }
    };
    pluginConfig.getConnection().validateConnection(context.getFailureCollector());
    Assert.assertEquals(0, context.getFailureCollector().getValidationFailures().size());
  }

  @Test
  public void testValidateUnauthorisedConnection() throws TransportException, SuccessFactorsServiceException {
    MockFailureCollector collector = new MockFailureCollector();
    ConnectorContext context = new MockConnectorContext(new MockConnectorConfigurer());
    successFactorsTransporter = new SuccessFactorsTransporter(pluginConfig.getConnection().getUsername(),
                                                              pluginConfig.getConnection().getPassword());
    new Expectations(SuccessFactorsUrlContainer.class, SuccessFactorsTransporter.class,
                     SuccessFactorsSchemaGenerator.class) {
      {
        successFactorsTransporter.callSuccessFactorsEntity(null, anyString, anyString);
        result = getUnauthorisedResponseContainer();
        minTimes = 1;
      }
    };
    pluginConfig.getConnection().validateConnection(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateNotFoundConnection() throws TransportException, SuccessFactorsServiceException {
    MockFailureCollector collector = new MockFailureCollector();
    successFactorsTransporter = new SuccessFactorsTransporter(pluginConfig.getConnection().getUsername(),
                                                              pluginConfig.getConnection().getPassword());
    new Expectations(SuccessFactorsUrlContainer.class, SuccessFactorsTransporter.class,
                     SuccessFactorsSchemaGenerator.class) {
      {
        successFactorsTransporter.callSuccessFactorsEntity(null, anyString, anyString);
        result = getNotFoundResponseContainer();
        minTimes = 1;
      }
    };
    pluginConfig.getConnection().validateConnection(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
  }

  private SuccessFactorsResponseContainer getSuccessfulResponseContainer() {
    return new SuccessFactorsResponseContainer(200, "ok",
                                               "2.0", new byte[]{50});
  }

  private SuccessFactorsResponseContainer getUnauthorisedResponseContainer() {
    return new SuccessFactorsResponseContainer(401, "",
                                               "2.0", new byte[]{50});
  }

  private SuccessFactorsResponseContainer getNotFoundResponseContainer() {
    return new SuccessFactorsResponseContainer(404, "",
                                               "2.0", new byte[]{50});
  }


  @Test(expected = IOException.class)
  public void testGenerateSpec() throws TransportException, IOException {
    ConnectorContext context = new MockConnectorContext(new MockConnectorConfigurer());
    MockFailureCollector collector = new MockFailureCollector();
    successFactorsTransporter = new SuccessFactorsTransporter(pluginConfig.getConnection().getUsername(),
                                                              pluginConfig.getConnection().getPassword());
    new Expectations(SuccessFactorsTransporter.class) {
      {
        successFactorsTransporter.callSuccessFactorsEntity(null, anyString, anyString);
        result = getSuccessfulResponseContainer();
        minTimes = 1;

      }
    };
    SuccessFactorsConnector successFactorsConnector = new SuccessFactorsConnector(pluginConfig.getConnection());
    successFactorsConnector.test(context);
    ConnectorSpec connectorSpec = successFactorsConnector.generateSpec(new MockConnectorContext
                                                                         (new MockConnectorConfigurer()),
                                                                       ConnectorSpecRequest.builder().setPath
                                                                           (pluginConfig.getEntityName())
                                                                         .setConnection("${conn(connection-id)}").
                                                                         build());

    Set<PluginSpec> relatedPlugins = connectorSpec.getRelatedPlugins();
    Assert.assertEquals(1, relatedPlugins.size());
    PluginSpec pluginSpec = relatedPlugins.iterator().next();
    Assert.assertEquals(SuccessFactorsSource.NAME, pluginSpec.getName());
    Assert.assertEquals(BatchSource.PLUGIN_TYPE, pluginSpec.getType());
    Map<String, String> properties = pluginSpec.getProperties();
    Assert.assertEquals("true", properties.get(ConfigUtil.NAME_USE_CONNECTION));
    Assert.assertEquals("${conn(connection-id)}", properties.get(ConfigUtil.NAME_CONNECTION));
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testGenerateSpecWithSchema() throws TransportException, IOException, EntityProviderException,
    SuccessFactorsServiceException {
    ConnectorContext context = new MockConnectorContext(new MockConnectorConfigurer());
    MockFailureCollector collector = new MockFailureCollector();
    successFactorsConnector = new SuccessFactorsConnector(pluginConfig.getConnection());
    successFactorsConnector.test(context);
    successFactorsTransporter = new SuccessFactorsTransporter(pluginConfig.getConnection().getUsername(),
                                                              pluginConfig.getConnection().getPassword());
    new Expectations(SuccessFactorsConnector.class) {
      {

        successFactorsConnector.getSchema(anyString);
        result = getPluginSchema();
        minTimes = 1;

      }
    };
    ConnectorSpec connectorSpec = successFactorsConnector.generateSpec(new MockConnectorContext
                                                                         (new MockConnectorConfigurer()),
                                                                       ConnectorSpecRequest.builder().setPath
                                                                           (pluginConfig.getEntityName())
                                                                         .setConnection("${conn(connection-id)}").
                                                                         build());

    Schema schema = connectorSpec.getSchema();
    for (Schema.Field field : schema.getFields()) {
      Assert.assertNotNull(field.getSchema());
    }
    Set<PluginSpec> relatedPlugins = connectorSpec.getRelatedPlugins();
    Assert.assertEquals(1, relatedPlugins.size());
    PluginSpec pluginSpec = relatedPlugins.iterator().next();
    Assert.assertEquals(SuccessFactorsSource.NAME, pluginSpec.getName());
    Assert.assertEquals(BatchSource.PLUGIN_TYPE, pluginSpec.getType());
    Map<String, String> properties = pluginSpec.getProperties();
    Assert.assertEquals("true", properties.get(ConfigUtil.NAME_USE_CONNECTION));
    Assert.assertEquals("${conn(connection-id)}", properties.get(ConfigUtil.NAME_CONNECTION));
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testBrowse() throws IOException, TransportException {
    ConnectorContext context = new MockConnectorContext(new MockConnectorConfigurer());
    List<String> entities = new ArrayList<>();
    entities.add("Achievement");
    successFactorsTransporter = new SuccessFactorsTransporter(pluginConfig.getConnection().getUsername(),
                                                              pluginConfig.getConnection().getPassword());
    successFactorsConnector = new SuccessFactorsConnector(pluginConfig.getConnection());

    new Expectations(SuccessFactorsTransporter.class, SuccessFactorsTransporter.class, SuccessFactorsConnector.class) {
      {
        successFactorsConnector.listEntities();
        result = entities;
        minTimes = 1;

        successFactorsTransporter.callSuccessFactorsEntity(null, anyString, anyString);
        result = getResponseContainer();
        minTimes = 1;
      }
    };
    successFactorsConnector = new SuccessFactorsConnector(pluginConfig.getConnection());
    successFactorsConnector.test(context);

    BrowseDetail detail = successFactorsConnector.browse(new MockConnectorContext(new MockConnectorConfigurer()),
                                                         BrowseRequest.builder("/").build());
    Assert.assertTrue(detail.getTotalCount() > 0);
    Assert.assertTrue(detail.getEntities().size() > 0);
    for (BrowseEntity entity : detail.getEntities()) {
      Assert.assertFalse(entity.canBrowse());
      Assert.assertTrue(entity.canSample());
    }
  }


  /**
   * This will return null as no call is made here to fetch the data.
   */
  @Test(expected = IOException.class)
  public void testSampleWithoutSampleData() throws IOException {
    ConnectorContext context = new MockConnectorContext(new MockConnectorConfigurer());
    successFactorsTransporter = new SuccessFactorsTransporter(pluginConfig.getConnection().getUsername(),
                                                              pluginConfig.getConnection().getPassword());
    String entityName = "entity";
    List<StructuredRecord> records = new ArrayList<>();
    StructuredRecord structuredRecord = Mockito.mock(StructuredRecord.class);
    records.add(structuredRecord);
    SuccessFactorsConnector connector = new SuccessFactorsConnector(pluginConfig.getConnection());
    successFactorsConnector = new SuccessFactorsConnector(pluginConfig.getConnection());
    successFactorsConnector.test(context);
    List<StructuredRecord> sample = connector.sample(new MockConnectorContext(new MockConnectorConfigurer()),
                                                     SampleRequest.builder(1).setPath(entityName).build());
    Assert.assertNull(sample);
  }

  @Test
  public void testSampleWithSampleData() throws IOException, TransportException, EntityProviderException,
    SuccessFactorsServiceException, EdmException {
    successFactorsTransporter = new SuccessFactorsTransporter(pluginConfig.getConnection().getUsername(),
                                                              pluginConfig.getConnection().getPassword());
    String entityName = "entity";
    List<StructuredRecord> records = new ArrayList<>();
    StructuredRecord structuredRecord = Mockito.mock(StructuredRecord.class);
    records.add(structuredRecord);
    successFactorsConnector = new SuccessFactorsConnector(pluginConfig.getConnection());
    new Expectations(SuccessFactorsConnector.class) {
      {
        successFactorsConnector.listEntityData(anyString, anyLong);
        result = records;
        minTimes = 1;

      }
    };

    List<StructuredRecord> sample = successFactorsConnector.sample(new MockConnectorContext
                                                                     (new MockConnectorConfigurer()), SampleRequest.
                                                                      builder(1).setPath(entityName).build());
    Assert.assertNotNull(sample);
  }

  @Test
  public void testGetNonNavigationalProperties() throws EntityProviderException, TransportException, EdmException {
    Edm edmMetadata = Mockito.mock(Edm.class);
    SuccessFactorsEntityProvider edmData = new SuccessFactorsEntityProvider(edmMetadata);
    successFactorsSchemaGenerator = new SuccessFactorsSchemaGenerator(new SuccessFactorsEntityProvider(edm));
    successFactorsConnector = new SuccessFactorsConnector(pluginConfig.getConnection());
    List<String> columnDetailList = new ArrayList<>();
    columnDetailList.add("name");
    new Expectations(SuccessFactorsConnector.class, SuccessFactorsSchemaGenerator.class) {
      {
        successFactorsConnector.fetchServiceMetadata(anyString);
        result = edmData;
        minTimes = 1;

        successFactorsSchemaGenerator.getNonNavigationalProperties(anyString);
        result = columnDetailList;
        minTimes = 1;
      }
    };
    Assert.assertEquals("name", successFactorsConnector.getNonNavigationalProperties("entity").get(0));
  }

  /**
   * exception is expected as entity is null.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testSampleWithEntityNull() throws IOException {
    String entityName = null;
    List<StructuredRecord> records = new ArrayList<>();
    StructuredRecord structuredRecord = Mockito.mock(StructuredRecord.class);
    records.add(structuredRecord);
    SuccessFactorsConnector connector = new SuccessFactorsConnector(pluginConfig.getConnection());
    connector.sample(new MockConnectorContext(new MockConnectorConfigurer()),
                     SampleRequest.builder(1).setPath(entityName).build());
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

  private SuccessFactorsResponseContainer getResponseContainer() {
    return new SuccessFactorsResponseContainer(200, "ok",
                                               "2.0", new byte[]{50});
  }
}

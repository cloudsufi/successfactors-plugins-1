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

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.successfactors.common.exception.SuccessFactorsServiceException;
import io.cdap.plugin.successfactors.common.util.ResourceConstants;
import io.cdap.plugin.successfactors.source.config.SuccessFactorsPluginConfig;
import io.cdap.plugin.successfactors.source.input.SuccessFactorsInputSplit;
import io.cdap.plugin.successfactors.source.input.SuccessFactorsPartitionBuilder;
import io.cdap.plugin.successfactors.source.metadata.SuccessFactorsEntityProvider;
import io.cdap.plugin.successfactors.source.metadata.TestSuccessFactorsUtil;
import io.cdap.plugin.successfactors.source.service.SuccessFactorsService;
import io.cdap.plugin.successfactors.source.transform.SuccessFactorsRecordReader;
import org.apache.olingo.odata2.api.edm.Edm;
import org.apache.olingo.odata2.api.edm.EdmEntitySet;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.ep.EntityProvider;
import org.apache.olingo.odata2.api.ep.EntityProviderException;
import org.apache.olingo.odata2.api.ep.EntityProviderReadProperties;
import org.apache.olingo.odata2.api.ep.entry.ODataEntry;
import org.apache.olingo.odata2.api.ep.feed.ODataFeed;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.core.MediaType;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;

/**
 * This {@code RuntimeFunctionalTest} represents the runtime functional behaviour.
 */
public class RuntimeFunctionalTest {

  @Rule
  public final ExpectedException exceptionRule = ExpectedException.none();
  @Rule
  public WireMockRule wireMockRule = new WireMockRule(WireMockConfiguration.wireMockConfig().dynamicPort());
  private SuccessFactorsPluginConfig.Builder pluginConfigBuilder;
  private Schema pluginSchema;
  private SuccessFactorsTransporter transporter;
  private SuccessFactorsService successFactorsService;
  private Edm edmData;
  private String encodedMetadataString;

  @Before
  public void setUp() throws Exception {
    pluginConfigBuilder = SuccessFactorsPluginConfig.builder()
      .baseURL("http://localhost:" + wireMockRule.port() + "/odata/v2")
      .entityName("Background_SpecialAssign")
      .username("test")
      .password("secret")
      .authType("basicAuth")
      .paginationType("serverSide");

    String metadataString = TestSuccessFactorsUtil.convertInputStreamToString(TestSuccessFactorsUtil.readResource
      ("successfactors-metadata2.xml"));
    encodedMetadataString = Base64.getEncoder().encodeToString(metadataString.getBytes(StandardCharsets.UTF_8));

    pluginSchema = getPluginSchema();
  }

  @Test
  public void runPipelineWithDefaultValues() throws Exception {
    SuccessFactorsPluginConfig pluginConfig = pluginConfigBuilder.build();
    long availableRowCount = 3;
    List<SuccessFactorsInputSplit> partitionList = new SuccessFactorsPartitionBuilder().buildSplits(availableRowCount);

    transporter = new SuccessFactorsTransporter(pluginConfig.getConnection());
    successFactorsService = new SuccessFactorsService(pluginConfig, transporter);
    prepareStubForMetadata(pluginConfig);
    edmData = successFactorsService.getSuccessFactorsServiceEdm(encodedMetadataString);
    successFactorsService.buildOutputSchema();
    successFactorsService.getEncodedServiceMetadata();
    for (SuccessFactorsInputSplit inputSplit : partitionList) {
      prepareStubForRun(pluginConfig);
      SuccessFactorsRecordReader successFactorsRecordReader =
        new SuccessFactorsRecordReader(successFactorsService, edmData, pluginSchema, inputSplit.getStart(),
                                       inputSplit.getEnd(),
                                       inputSplit.getBatchSize());
      successFactorsRecordReader.initialize(null, null);
      List<StructuredRecord> recordList = new ArrayList<>();
      while (successFactorsRecordReader.nextKeyValue()) {
        recordList.add(successFactorsRecordReader.getCurrentValue());
      }
      long expectedRecordsToPull = (inputSplit.getEnd() - inputSplit.getStart()) + 1;
      String msg = String.format("Total record count for split (start: %s & end: %s) is not matching",
                                 inputSplit.getStart(), inputSplit.getEnd());
      Assert.assertEquals(msg, expectedRecordsToPull, recordList.size());
      int expectedNetworkCallCount = (int) (expectedRecordsToPull / inputSplit.getBatchSize());
      verify(expectedNetworkCallCount, getRequestedFor(WireMock.urlEqualTo("/odata/v2/Background_SpecialAssign?" +
                                                                             "%24select=backgroundElementId%2C" +
                                                                             "bgOrderPos%2Cdescription%2CendDate%2" +
                                                                             "ClastModifiedDate%2Cproject%2C" +
                                                                             "startDate%2CuserId%2Cprice&%24top=3")));
    }
  }

  @Test
  public void verifyFailToDecodeMetadataString() throws SuccessFactorsServiceException {
    SuccessFactorsPluginConfig pluginConfig = pluginConfigBuilder.build();
    exceptionRule.expect(SuccessFactorsServiceException.class);
    exceptionRule
      .expectMessage(ResourceConstants.ERR_METADATA_DECODE.getMsgForKeyWithCode(pluginConfig.getEntityName()));
    transporter = new SuccessFactorsTransporter(pluginConfig.getConnection());
    successFactorsService = new SuccessFactorsService(pluginConfig, transporter);
    successFactorsService.getSuccessFactorsServiceEdm("encodedMetadataString");
  }

  @Test
  public void verifyDataCorrectness()
    throws IOException, InterruptedException, EdmException, EntityProviderException, SuccessFactorsServiceException {
    SuccessFactorsPluginConfig pluginConfig = pluginConfigBuilder.build();
    prepareStubForMetadata(pluginConfig);
    long availableRowCount = 3;
    List<SuccessFactorsInputSplit> partitionList = new SuccessFactorsPartitionBuilder().buildSplits(availableRowCount);
    transporter = new SuccessFactorsTransporter(pluginConfig.getConnection());
    successFactorsService = new SuccessFactorsService(pluginConfig, transporter);
    edmData = successFactorsService.getSuccessFactorsServiceEdm(encodedMetadataString);
    for (SuccessFactorsInputSplit inputSplit : partitionList) {
      prepareStubForRun(pluginConfig);
      SuccessFactorsRecordReader successFactorsRecordReader =
        new SuccessFactorsRecordReader(successFactorsService, edmData, pluginSchema, inputSplit.getStart(),
                                       inputSplit.getEnd(), inputSplit.getBatchSize());
      successFactorsRecordReader.initialize(null, null);
      List<StructuredRecord> recordList = new ArrayList<>();
      while (successFactorsRecordReader.nextKeyValue()) {
        recordList.add(successFactorsRecordReader.getCurrentValue());
      }
      long expectedRecordsToPull = (inputSplit.getEnd() - inputSplit.getStart()) + 1;
      String msg = String.format("Total record count for split (start: %s & end: %s) is not matching",
                                 inputSplit.getStart(), inputSplit.getEnd());
      Assert.assertEquals(msg, expectedRecordsToPull, recordList.size());
      ODataFeed oDataFeed = prepareODataFeed(pluginConfig);
      for (int i = 0; i < oDataFeed.getEntries().size(); i++) {
        ODataEntry oDataEntry = oDataFeed.getEntries().get(i);
        StructuredRecord structuredRecord = recordList.get(i);
        pluginSchema.getFields().forEach(field -> {
          String fieldName = field.getName();
          if (!field.getSchema().getType().isSimpleType() && field.getSchema().getUnionSchema(0).getLogicalType()
            != null && field.getSchema().getUnionSchema(0).getLogicalType() == Schema.LogicalType.DECIMAL) {
            Assert.assertArrayEquals(field.getName() + " value is not equal.",
                                     (byte[]) processSchemaTypeValue(field.getSchema(), oDataEntry.getProperties()
                                       .get(fieldName)), structuredRecord.get(field.getName()));
          } else {
            Assert.assertEquals(field.getName() + " value is not equal.",
                                processSchemaTypeValue(field.getSchema(), oDataEntry.getProperties().get(fieldName)),
                                structuredRecord.get(field.getName()));
          }

        });
      }
    }
  }


  private Object processSchemaTypeValue(Schema fieldSchema, Object fieldValue) {
    fieldSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;
    Schema.LogicalType logicalType = fieldSchema.getLogicalType();
    if (logicalType == Schema.LogicalType.DECIMAL) {
      // below change is done on the plugin level
      BigDecimal pluginValue = new BigDecimal(String.valueOf(fieldValue)).setScale(fieldSchema.getScale());
      // below update is done by the StructuredRecord.Builder#setDecimal
      return pluginValue.unscaledValue().toByteArray();
    } else if (logicalType == Schema.LogicalType.TIME_MICROS) {
      // below change is done on the plugin level
      LocalTime pluginValue = ((GregorianCalendar) fieldValue).toZonedDateTime().toLocalTime();
      // below update is done by the StructuredRecord.Builder#setTime
      long nanos = pluginValue.toNanoOfDay();
      return TimeUnit.NANOSECONDS.toMicros(nanos);
    } else if (logicalType == Schema.LogicalType.TIMESTAMP_MICROS) {
      // below change is done on the plugin level
      ZonedDateTime pluginValue = ((GregorianCalendar) fieldValue).toZonedDateTime();
      // below update is done by the StructuredRecord.Builder#setTimestamp
      Instant instant = pluginValue.toInstant();
      long micros = TimeUnit.SECONDS.toMicros(instant.getEpochSecond());
      return Math.addExact(micros, TimeUnit.NANOSECONDS.toMicros(instant.getNano()));
    } else if (logicalType == Schema.LogicalType.DATETIME) {
      LocalDateTime localDateTime = ((GregorianCalendar) fieldValue).toZonedDateTime().toLocalDateTime();
      return localDateTime.toString();
    }
    return fieldValue;
  }

  private ODataFeed prepareODataFeed(SuccessFactorsPluginConfig pluginConfig) throws EntityProviderException,
    EdmException {
    try (InputStream metadataStream = TestSuccessFactorsUtil.readResource("successfactors-metadata2.xml");
         InputStream responseStream = TestSuccessFactorsUtil.readResource("successfactors-data.json")) {
      SuccessFactorsEntityProvider serviceHelper =
        new SuccessFactorsEntityProvider(EntityProvider.readMetadata(metadataStream, false));
      EdmEntitySet entity = serviceHelper.getEntitySet(pluginConfig.getEntityName());
      return EntityProvider
        .readFeed(MediaType.APPLICATION_JSON, entity, responseStream, EntityProviderReadProperties.init().build());
    } catch (IOException e) {
      return null;
    }
  }

  private void prepareStubForRun(SuccessFactorsPluginConfig pluginConfig) {
    String expectedBody = TestSuccessFactorsUtil.convertInputStreamToString(TestSuccessFactorsUtil.readResource
      ("successfactors-data.json"));
    WireMock.stubFor(WireMock.get(WireMock.urlPathMatching("/odata/v2/Background_SpecialAssign"))
                       .withBasicAuth(pluginConfig.getConnection().getUsername(),
                                      pluginConfig.getConnection().getPassword())
                       .willReturn(WireMock.ok()
                                     .withHeader(SuccessFactorsTransporter.SERVICE_VERSION, "2.0")
                                     .withBody(expectedBody)));
  }

  private void prepareStubForMetadata(SuccessFactorsPluginConfig pluginConfig) {
    String expectedBody = TestSuccessFactorsUtil.convertInputStreamToString(TestSuccessFactorsUtil.readResource
      ("successfactors-metadata2.xml"));
    WireMock.stubFor(get(urlPathEqualTo("/odata/v2/Background_SpecialAssign/$metadata"))
                       .withHeader("Accept", equalTo("application/xml"))
                       .willReturn(aResponse()
                                     .withStatus(200)
                                     .withHeader("Content-Type", "text/xml")
                                     .withBody(expectedBody)));
  }

  private Schema getPluginSchema() throws IOException {
    String schemaString = "{\"type\":\"record\",\"name\":\"SuccessFactorsColumnMetadata\",\"fields\":[{\"name\":" +
      "\"backgroundElementId\",\"type\":\"long\"},{\"name\":\"bgOrderPos\",\"type\":\"long\"},{\"name\":" +
      "\"description\",\"type\":[\"string\",\"null\"]},{\"name\":\"endDate\",\"type\":[{\"type\":\"long\"," +
      "\"logicalType\":\"timestamp-micros\"},\"null\"]},{\"name\":\"lastModifiedDate\",\"type\":" +
      "[{\"type\":\"long\",\"logicalType\":\"datetime\"},\"null\"]},{\"name\":\"project\",\"type\":" +
      "\"string\"},{\"name\":\"startDate\",\"type\":[{\"type\":\"long\",\"logicalType\":\"time-micros\"}," +
      "\"null\"]},{\"name\":\"userId\",\"type\":\"string\"},{\"name\":\"price\",\"type\":[{\"type\":\"bytes\"," +
      "\"logicalType\":\"decimal\",\"precision\":15,\"scale\":2},\"null\"]},{\"name\":\"annualMaxPayComponent\"," +
      "\"type\":[\"string\",\"null\"]}]}";
    return Schema.parseJson(schemaString);
  }

}

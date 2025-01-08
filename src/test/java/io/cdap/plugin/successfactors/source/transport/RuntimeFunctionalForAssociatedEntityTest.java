/*
 * Copyright © 2023 Cask Data, Inc.
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
import io.cdap.plugin.successfactors.source.config.SuccessFactorsPluginConfig;
import io.cdap.plugin.successfactors.source.input.SuccessFactorsInputSplit;
import io.cdap.plugin.successfactors.source.input.SuccessFactorsPartitionBuilder;
import io.cdap.plugin.successfactors.source.metadata.TestSuccessFactorsUtil;
import io.cdap.plugin.successfactors.source.service.SuccessFactorsService;
import io.cdap.plugin.successfactors.source.transform.SuccessFactorsRecordReader;
import org.apache.olingo.odata2.api.edm.Edm;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;

/**
 * This {@code RuntimeFunctionalTest} represents the runtime functional behaviour.
 */
public class RuntimeFunctionalForAssociatedEntityTest {

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
      .entityName("Picklist")
      .associateEntityName("PicklistOption,PicklistLabel")
      .expandOption("picklistOptions/picklistLabels")
      .filterOption("picklistId eq 'hrRanking'")
      .username("test")
      .password("secret")
      .authType("basicAuth")
      .paginationType("serverSide");

    String metadataString = TestSuccessFactorsUtil.convertInputStreamToString(TestSuccessFactorsUtil.readResource
      ("successfactors-metadata3.xml"));
    encodedMetadataString = Base64.getEncoder().encodeToString(metadataString.getBytes(StandardCharsets.UTF_8));

    pluginSchema = getPluginSchema();
  }

  @Test
  public void testRecordReaderForAssociatedEntity() throws Exception {
    SuccessFactorsPluginConfig pluginConfig = pluginConfigBuilder.build();
    long availableRowCount = 1;
    List<SuccessFactorsInputSplit> partitionList = new SuccessFactorsPartitionBuilder().buildSplits(availableRowCount);

    transporter = new SuccessFactorsTransporter(pluginConfig.getConnection());
    successFactorsService = new SuccessFactorsService(pluginConfig, transporter);
    prepareStubForMetadata();
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
      verify(expectedNetworkCallCount, getRequestedFor(WireMock.urlEqualTo("/odata/v2/Picklist?%24filter=" +
                                                                             "picklistId%20eq%20%27hrRanking%27&%24" +
                                                                             "expand=picklistOptions%2FpicklistLabels" +
                                                                             "&%24top=1")));
    }
  }

  private void prepareStubForRun(SuccessFactorsPluginConfig pluginConfig) {
    String expectedBody = TestSuccessFactorsUtil.convertInputStreamToString(TestSuccessFactorsUtil.readResource
      ("successfactors-data1.json"));
    WireMock.stubFor(WireMock.get(WireMock.urlPathMatching("/odata/v2/Picklist"))
                       .withBasicAuth(pluginConfig.getConnection().getUsername(),
                                      pluginConfig.getConnection().getPassword())
                       .willReturn(WireMock.ok()
                                     .withHeader(SuccessFactorsTransporter.SERVICE_VERSION, "2.0")
                                     .withBody(expectedBody)));
  }

  private void prepareStubForMetadata() {
    String expectedBody = TestSuccessFactorsUtil.convertInputStreamToString(TestSuccessFactorsUtil.readResource
      ("successfactors-metadata3.xml"));
    WireMock.stubFor(get(urlPathEqualTo("/odata/v2/Picklist,PicklistOption,PicklistLabel/$metadata"))
                       .withHeader("Accept", equalTo("application/xml"))
                       .willReturn(aResponse()
                                     .withStatus(200)
                                     .withHeader("Content-Type", "text/xml")
                                     .withBody(expectedBody)));
  }

  private Schema getPluginSchema() throws IOException {
    String schemaString = "{" +
      "  \"type\": \"record\"," +
      "  \"name\": \"SuccessFactorsColumnMetadata\"," +
      "  \"fields\": [" +
      "    {" +
      "      \"name\": \"picklistId\"," +
      "      \"type\": \"string\"" +
      "    }," +
      "    {" +
      "      \"name\": \"picklistOptions\"," +
      "      \"type\": {" +
      "        \"type\": \"array\"," +
      "        \"items\": {" +
      "          \"type\": \"record\"," +
      "          \"name\": \"picklistOptions_6906dcf8_ea26_454e_a07d_2613ca8c518c\"," +
      "          \"fields\": [" +
      "            {" +
      "              \"name\": \"externalCode\"," +
      "              \"type\": [" +
      "                \"string\"," +
      "                \"null\"" +
      "              ]" +
      "            }," +
      "            {" +
      "              \"name\": \"id\"," +
      "              \"type\": \"long\"" +
      "            }," +
      "            {" +
      "              \"name\": \"localeLabel\"," +
      "              \"type\": [" +
      "                \"string\"," +
      "                \"null\"" +
      "              ]" +
      "            }," +
      "            {" +
      "              \"name\": \"maxValue\"," +
      "              \"type\": [" +
      "                \"double\"," +
      "                \"null\"" +
      "              ]" +
      "            }," +
      "            {" +
      "              \"name\": \"mdfExternalCode\"," +
      "              \"type\": [" +
      "                \"string\"," +
      "                \"null\"" +
      "              ]" +
      "            }," +
      "            {" +
      "              \"name\": \"minValue\"," +
      "              \"type\": [" +
      "                \"double\"," +
      "                \"null\"" +
      "              ]" +
      "            }," +
      "            {" +
      "              \"name\": \"optionValue\"," +
      "              \"type\": [" +
      "                \"double\"," +
      "                \"null\"" +
      "              ]" +
      "            }," +
      "            {" +
      "              \"name\": \"sortOrder\"," +
      "              \"type\": [" +
      "                \"int\"," +
      "                \"null\"" +
      "              ]" +
      "            }," +
      "            {" +
      "              \"name\": \"status\"," +
      "              \"type\": [" +
      "                \"string\"," +
      "                \"null\"" +
      "              ]" +
      "            }," +
      "            {" +
      "              \"name\": \"picklistLabels\"," +
      "              \"type\": {" +
      "                \"type\": \"array\"," +
      "                \"items\": {" +
      "                  \"type\": \"record\"," +
      "                  \"name\": \"picklistLabels_d2e680e7_a241_45da_815f_28b0daacbff3\"," +
      "                  \"fields\": [" +
      "                    {" +
      "                      \"name\": \"id\"," +
      "                      \"type\": [" +
      "                        \"long\"," +
      "                        \"null\"" +
      "                      ]" +
      "                    }," +
      "                    {" +
      "                      \"name\": \"label\"," +
      "                      \"type\": [" +
      "                        \"string\"," +
      "                        \"null\"" +
      "                      ]" +
      "                    }," +
      "                    {" +
      "                      \"name\": \"locale\"," +
      "                      \"type\": \"string\"" +
      "                    }," +
      "                    {" +
      "                      \"name\": \"optionId\"," +
      "                      \"type\": \"long\"" +
      "                    }" +
      "                  ]" +
      "                }" +
      "              }" +
      "            }" +
      "          ]" +
      "        }" +
      "      }" +
      "    }" +
      "  ]" +
      "}";
    return Schema.parseJson(schemaString);
  }

}

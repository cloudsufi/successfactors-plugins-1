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
package io.cdap.plugin.successfactors.source.input;

import io.cdap.plugin.successfactors.source.SuccessFactorsSource;
import io.cdap.plugin.successfactors.source.config.SuccessFactorsPluginConfig;
import io.cdap.plugin.successfactors.source.metadata.TestSuccessFactorsUtil;
import io.cdap.plugin.successfactors.source.service.SuccessFactorsService;
import io.cdap.plugin.successfactors.source.transport.SuccessFactorsTransporter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.olingo.odata2.api.edm.Edm;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

@RunWith(PowerMockRunner.class)
@PrepareForTest(SuccessFactorsTransporter.class)
public class SuccessFactorsInputFormatTest {
  public static final String SUCCESSFACTORS_PLUGIN_PROPERTIES = "successFactorsPluginProperties";
  public static final String ENCODED_ENTITY_METADATA_STRING = "encodedMetadataString";
  public SuccessFactorsPluginConfig pluginConfig;

  @Before
  public void initializeTests() {
    pluginConfig = Mockito.spy(new SuccessFactorsPluginConfig("referenceName",
                                                              "baseURL",
                                                              "entityName",
                                                              null,
                                                              "username",
                                                              "password",
                                                              "filterOption",
                                                              "selectOption",
                                                              "expandOption",
                                                              null,
                                                              null,
                                                              null,
                                                              null));
  }

  @Test
  public void testCreateRecordReader() throws Exception {
    String schemaString = "{\"type\":\"record\",\"name\":\"SuccessFactorsColumnMetadata\",\"fields\":[{\"name\":" +
      "\"backgroundElementId\",\"type\":\"long\"},{\"name\":\"bgOrderPos\",\"type\":\"long\"},{\"name\":" +
      "\"description\",\"type\":[\"string\",\"null\"]},{\"name\":\"endDate\",\"type\":[{\"type\":\"long\"," +
      "\"logicalType\":\"timestamp-micros\"},\"null\"]},{\"name\":\"lastModifiedDate\",\"type\":" +
      "[{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"},\"null\"]},{\"name\":\"project\",\"type\":" +
      "\"string\"},{\"name\":\"startDate\",\"type\":[{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}," +
      "\"null\"]},{\"name\":\"userId\",\"type\":\"string\"}]}";
    Configuration configuration = Mockito.mock(Configuration.class);
    SuccessFactorsTransporter transporter = PowerMockito.mock(SuccessFactorsTransporter.class);
    PowerMockito.whenNew(SuccessFactorsTransporter.class).withArguments(pluginConfig.getUsername(),
                                                                        pluginConfig.getPassword()).
      thenReturn(transporter);
    SuccessFactorsInputSplit split = Mockito.mock(SuccessFactorsInputSplit.class);
    TaskAttemptContext taskAttemptContext = Mockito.mock(TaskAttemptContext.class);
    Mockito.when(taskAttemptContext.getConfiguration()).thenReturn(configuration);
    Mockito.when(taskAttemptContext.getConfiguration().get(SuccessFactorsSource.OUTPUT_SCHEMA)).
      thenReturn(schemaString);
    Mockito.when(taskAttemptContext.getConfiguration().get(SUCCESSFACTORS_PLUGIN_PROPERTIES)).thenReturn(schemaString);
    String metadataString = TestSuccessFactorsUtil.convertInputStreamToString(TestSuccessFactorsUtil.readResource
      ("successfactors-metadata2.xml"));
    String encodedMetaData = Base64.getEncoder().encodeToString(metadataString.getBytes(StandardCharsets.UTF_8));
    Mockito.when(taskAttemptContext.getConfiguration().get(ENCODED_ENTITY_METADATA_STRING)).thenReturn(encodedMetaData);
    Edm edm = Mockito.mock(Edm.class);
    SuccessFactorsService successFactorsService = Mockito.mock(SuccessFactorsService.class);
    Mockito.when(successFactorsService.getSuccessFactorsServiceEdm("encodedMetadataString")).thenReturn(edm);
    SuccessFactorsInputFormat successFactorsInputFormat = new SuccessFactorsInputFormat();
    Assert.assertNotNull(successFactorsInputFormat.createRecordReader(split, taskAttemptContext));
  }
}

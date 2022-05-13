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

import io.cdap.plugin.successfactors.source.config.SuccessFactorsPluginConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;

public class SuccessFactorsUrlContainerTest {
  public SuccessFactorsPluginConfig pluginConfig;
  @Before
  public void initializeTests() {
    pluginConfig = Mockito.spy(new SuccessFactorsPluginConfig("referenceName",
                                                              "https://baseUrl",
                                                              "entityName",
                                                              "associatedEntity",
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
  public void testGetTesterURL() {
    SuccessFactorsUrlContainer urlContainer = new SuccessFactorsUrlContainer(pluginConfig);
    String expectedUrl = "https://baseurl/entityName?%24filter=filterOption&%24select=selectOption&%24" +
      "expand=expandOption&%24top=1";
    URL actualUrl = urlContainer.getTesterURL();
    Assert.assertEquals(actualUrl.toString(), expectedUrl);
  }

  @Test
  public void testGetMetaDataUrlWithAssociatedProperty() {
    SuccessFactorsUrlContainer urlContainer = new SuccessFactorsUrlContainer(pluginConfig);
    String expectedUrl = "https://baseurl/entityName,associatedEntity/$metadata";
    URL actualUrl = urlContainer.getMetadataURL();
    Assert.assertEquals(actualUrl.toString(), expectedUrl);
  }

  @Test
  public void testGetTotalRecordCountURL() {
    SuccessFactorsUrlContainer urlContainer = new SuccessFactorsUrlContainer(pluginConfig);
    String expectedUrl = "https://baseurl/entityName/$count?%24filter=filterOption";
    URL actualUrl = urlContainer.getTotalRecordCountURL();
    Assert.assertEquals(actualUrl.toString(), expectedUrl);
  }
}

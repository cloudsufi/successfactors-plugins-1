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

import io.cdap.plugin.successfactors.common.exception.SuccessFactorsServiceException;
import io.cdap.plugin.successfactors.common.exception.TransportException;
import io.cdap.plugin.successfactors.common.util.ResourceConstants;
import io.cdap.plugin.successfactors.common.util.SuccessFactorsUtil;
import io.cdap.plugin.successfactors.source.config.SuccessFactorsPluginConfig;
import io.cdap.plugin.successfactors.source.service.SuccessFactorsService;
import okhttp3.HttpUrl;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.URL;

/**
 * This {@code SuccessFactorsUrlContainer} contains the implementation of different SuccessFactors url:
 * * Test url
 * * Metadata url
 * * Available record count url
 * * Data url
 */
public class SuccessFactorsUrlContainer {

  private static final Logger LOG = LoggerFactory.getLogger(SuccessFactorsUrlContainer.class);
  private static final String TOP_OPTION = "$top";
  private static final String SKIP_OPTION = "$skip";
  private static final String PROPERTY_SEPARATOR = ",";
  private final SuccessFactorsPluginConfig pluginConfig;

  public SuccessFactorsUrlContainer(SuccessFactorsPluginConfig pluginConfig) {
    this.pluginConfig = pluginConfig;
  }

  /**
   * Construct tester URL.
   *
   * @return tester URL.
   */
  public URL getTesterURL() {
    HttpUrl.Builder builder = HttpUrl.parse(pluginConfig.getBaseURL())
      .newBuilder()
      .addPathSegment(pluginConfig.getEntityName());

    URL testerURL = buildQueryOptions(builder, Boolean.FALSE)
      .addQueryParameter(TOP_OPTION, "1")
      .build()
      .url();

    LOG.debug(ResourceConstants.DEBUG_TEST_ENDPOINT.getMsgForKey(testerURL));

    return testerURL;
  }

  /**
   * Constructs metadata URL.
   *
   * @return metadata URL.
   */
  public URL getMetadataURL() {
    URL metadataURL = HttpUrl.parse(pluginConfig.getBaseURL())
      .newBuilder()
      .addPathSegments(pluginConfig.getEntityName())
      .addPathSegment("$metadata")
      .build()
      .url();
    
    if (SuccessFactorsUtil.isNotNullOrEmpty(pluginConfig.getAssociatedEntityName())) {
      metadataURL = HttpUrl.parse(pluginConfig.getBaseURL())
        .newBuilder()
        .addPathSegments(pluginConfig.getEntityName()
        .concat(PROPERTY_SEPARATOR)
        .concat(pluginConfig.getAssociatedEntityName()))
        .addPathSegment("$metadata")
        .build()
        .url();
    }

    LOG.debug(ResourceConstants.DEBUG_METADATA_ENDPOINT.getMsgForKey(metadataURL));

    return metadataURL;
  }

  /**
   * Adds Query option parameters in {@code HttpUrl.Builder} as per the given sequence.
   * Sequence:
   * 1. $filter
   * 2. $select
   * 3. $expand
   *
   * @param urlBuilder builds the final url
   * @return initialize the passed {@code HttpUrl.Builder} with the provided query options
   * in {@code SuccessFactorsPluginConfig} and return it.
   */
  private HttpUrl.Builder buildQueryOptions(HttpUrl.Builder urlBuilder, Boolean isDataFetch) {
    if (SuccessFactorsUtil.isNotNullOrEmpty(pluginConfig.getFilterOption())) {
      urlBuilder.addQueryParameter("$filter", pluginConfig.getFilterOption());
    }
    if (SuccessFactorsUtil.isNotNullOrEmpty(pluginConfig.getSelectOption())) {
      urlBuilder.addQueryParameter("$select", pluginConfig.getSelectOption());
    } else if (isDataFetch) {
      SuccessFactorsTransporter transporter = new SuccessFactorsTransporter(pluginConfig.getUsername(),
                                                                            pluginConfig.getPassword());
      SuccessFactorsService successFactorsService = new SuccessFactorsService(pluginConfig, transporter);
      try {
        StringBuilder selectFieldValue = new StringBuilder(String.join(PROPERTY_SEPARATOR, successFactorsService.
          getNonNavigationalProperties()));
        if (SuccessFactorsUtil.isNotNullOrEmpty(pluginConfig.getExpandOption())) {
          selectFieldValue.append(PROPERTY_SEPARATOR).append(pluginConfig.getExpandOption());
        }
        urlBuilder.addQueryParameter("$select", selectFieldValue.toString());

      } catch (TransportException | SuccessFactorsServiceException | EdmException e) {
        e.printStackTrace();
      }
    }
    if (SuccessFactorsUtil.isNotNullOrEmpty(pluginConfig.getExpandOption())) {
      urlBuilder.addQueryParameter("$expand", pluginConfig.getExpandOption());
    }

    return urlBuilder;
  }

  /**
   * Constructs total available record count URL.
   *
   * @return total available record count URL.
   */
  public URL getTotalRecordCountURL() {
    HttpUrl.Builder builder = HttpUrl.parse(pluginConfig.getBaseURL())
      .newBuilder()
      .addPathSegment(pluginConfig.getEntityName())
      .addPathSegment("$count");

    if (SuccessFactorsUtil.isNotNullOrEmpty(pluginConfig.getFilterOption())) {
      builder.addQueryParameter("$filter", pluginConfig.getFilterOption());
    }
    URL recordCountURL = builder.build().url();

    LOG.debug(ResourceConstants.DEBUG_DATA_COUNT_ENDPOINT.getMsgForKey(recordCountURL));

    return recordCountURL;
  }

  /**
   * Constructs data URL with provided '$skip' and '$top' parameters.
   *
   * @param skip records to skip.
   * @param top  records to fetch.
   * @return data URL with provided '$skip' and '$top' parameters.
   */
  public URL getDataFetchURL(Long skip, Long top) {
    HttpUrl.Builder builder = HttpUrl.parse(pluginConfig.getBaseURL())
      .newBuilder()
      .addPathSegment(pluginConfig.getEntityName());

    buildQueryOptions(builder, Boolean.TRUE);
    if (skip != null && skip != 0) {
      builder.addQueryParameter(SKIP_OPTION, String.valueOf(skip));
    }
    if (top != null) {
      builder.addQueryParameter(TOP_OPTION, String.valueOf(top));
    }
    URL dataURL = builder.build().url();

    LOG.debug(ResourceConstants.DEBUG_DATA_ENDPOINT.getMsgForKey(dataURL));

    return dataURL;
  }
}

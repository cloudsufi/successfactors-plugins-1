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
import io.cdap.plugin.successfactors.common.util.SuccessFactorsUtil;
import io.cdap.plugin.successfactors.source.config.SuccessFactorsPluginConfig;
import io.cdap.plugin.successfactors.source.service.SuccessFactorsService;
import okhttp3.HttpUrl;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Arrays;
import javax.annotation.Nullable;

/**
 * This {@code SuccessFactorsUrlContainer} contains the implementation of different SuccessFactors url:
 * * Test url
 * * Metadata url
 * * Available record count url
 * * Data url
 */
public class SuccessFactorsUrlContainer {

  public static final String PROPERTY_SEPARATOR = ",";
  public static final String NAV_PROPERTY_SEPARATOR = "/";
  private static final Logger LOG = LoggerFactory.getLogger(SuccessFactorsUrlContainer.class);
  private static final String TOP_OPTION = "$top";
  private static final String SKIP_OPTION = "$skip";
  private static final String FILTER_OPTION = "$filter";
  private static final String SELECT_OPTION = "$select";
  private static final String EXPAND_OPTION = "$expand";
  private static final String COUNT = "$count";
  private static final String PAGING = "paging";
  private static final String SNAPSHOT = "snapshot";
  private static final String METADATA = "$metadata";
  private static final String TESTURL_TOP_VALUE = "1";

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
    HttpUrl.Builder builder = HttpUrl.parse(pluginConfig.getConnection().getBaseURL())
      .newBuilder()
      .addPathSegment(pluginConfig.getEntityName());

    URL testerURL = buildQueryOptions(builder, Boolean.FALSE)
      .addQueryParameter(TOP_OPTION, TESTURL_TOP_VALUE)
      .build()
      .url();

    return testerURL;
  }

  /**
   * Constructs metadata URL.
   *
   * @return metadata URL.
   */
  public URL getMetadataURL() {
    URL metadataURL = HttpUrl.parse(pluginConfig.getConnection().getBaseURL())
      .newBuilder()
      .addPathSegments(pluginConfig.getEntityName())
      .addPathSegment(METADATA)
      .build()
      .url();

    if (SuccessFactorsUtil.isNotNullOrEmpty(pluginConfig.getAssociatedEntityName())) {
      metadataURL = HttpUrl.parse(pluginConfig.getConnection().getBaseURL())
        .newBuilder()
        .addPathSegments(pluginConfig.getEntityName()
        .concat(PROPERTY_SEPARATOR)
        .concat(pluginConfig.getAssociatedEntityName()))
        .addPathSegment(METADATA)
        .build()
        .url();
    }

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
   * @param isDataFetch flag to determine whether to fetch non-navigational properties or not
   * @return initialize the passed {@code HttpUrl.Builder} with the provided query options
   * in {@code SuccessFactorsPluginConfig} and return it.
   */
  private HttpUrl.Builder buildQueryOptions(HttpUrl.Builder urlBuilder, Boolean isDataFetch) {
    if (SuccessFactorsUtil.isNotNullOrEmpty(pluginConfig.getFilterOption())) {
      urlBuilder.addQueryParameter(FILTER_OPTION, pluginConfig.getFilterOption());
    }
    
    if (SuccessFactorsUtil.isNotNullOrEmpty(pluginConfig.getSelectOption())) {
      if (SuccessFactorsUtil.isNotNullOrEmpty(pluginConfig.getExpandOption())) {
        String selectFieldValue = pluginConfig.getSelectOption().concat(PROPERTY_SEPARATOR)
          .concat(pluginConfig.getExpandOption());
        urlBuilder.addQueryParameter(SELECT_OPTION, selectFieldValue);
      } else {
        urlBuilder.addQueryParameter(SELECT_OPTION, pluginConfig.getSelectOption());
      }
    } else if (getExpandLevel() <= 1  && isDataFetch) {
      SuccessFactorsService successFactorsService = SuccessFactorsUtil.getSuccessFactorsService(pluginConfig);
      try {
        StringBuilder selectNonNav = new StringBuilder(String.join(PROPERTY_SEPARATOR, successFactorsService.
          getNonNavigationalProperties()));
        if (SuccessFactorsUtil.isNotNullOrEmpty(pluginConfig.getExpandOption())) {
          selectNonNav.append(PROPERTY_SEPARATOR).append(pluginConfig.getExpandOption());
        }
        urlBuilder.addQueryParameter(SELECT_OPTION, selectNonNav.toString());

      } catch (TransportException | SuccessFactorsServiceException | EdmException e) {
        LOG.error("Exception occurred while getting non-navigational properties for building query options {}", e);
      }
    }

    if (SuccessFactorsUtil.isNotNullOrEmpty(pluginConfig.getExpandOption())) {
      urlBuilder.addQueryParameter(EXPAND_OPTION, pluginConfig.getExpandOption());
    }

    return urlBuilder;
  }

  /**
   * Constructs total available record count URL.
   *
   * @return total available record count URL.
   */
  public URL getTotalRecordCountURL() {
    HttpUrl.Builder builder = HttpUrl.parse(pluginConfig.getConnection().getBaseURL())
      .newBuilder()
      .addPathSegment(pluginConfig.getEntityName())
      .addPathSegment(COUNT);

    if (SuccessFactorsUtil.isNotNullOrEmpty(pluginConfig.getFilterOption())) {
      builder.addQueryParameter(FILTER_OPTION, pluginConfig.getFilterOption());
    }
    URL recordCountURL = builder.build().url();

    return recordCountURL;
  }

  /**
   * Constructs data URL with provided '$skip' and '$top' parameters.
   *
   * @param skip records to skip.
   * @param top  records to fetch.
   * @return data URL with provided '$skip' and '$top' parameters.
   */
  public URL getDataFetchURL(@Nullable Long skip, @Nullable Long top) {
    HttpUrl.Builder builder = HttpUrl.parse(pluginConfig.getConnection().getBaseURL())
      .newBuilder()
      .addPathSegment(pluginConfig.getEntityName());

    buildQueryOptions(builder, Boolean.TRUE);
    if (skip != null && skip != 0) {
      builder.addQueryParameter(SKIP_OPTION, String.valueOf(skip));
    }
    if (top != null) {
      builder.addQueryParameter(TOP_OPTION, String.valueOf(top));
    }
    if (skip == null && top == null) {
      builder.addQueryParameter(PAGING, SNAPSHOT);
    }
    URL dataURL = builder.build().url();

    return dataURL;
  }

  /*
   * Get the level up to which the entity has been expanded.
   */
  private int getExpandLevel() {
    int expandLevel = 0;
    if (SuccessFactorsUtil.isNotNullOrEmpty(pluginConfig.getExpandOption())) {
      expandLevel = Arrays.asList(
          pluginConfig.getExpandOption().split(SuccessFactorsUrlContainer.NAV_PROPERTY_SEPARATOR))
        .size();
    }
    return expandLevel;
  }
}

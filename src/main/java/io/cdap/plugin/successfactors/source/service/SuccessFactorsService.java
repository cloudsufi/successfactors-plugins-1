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

package io.cdap.plugin.successfactors.source.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.successfactors.common.exception.SuccessFactorsServiceException;
import io.cdap.plugin.successfactors.common.exception.TransportException;
import io.cdap.plugin.successfactors.common.util.ExceptionParser;
import io.cdap.plugin.successfactors.common.util.ResourceConstants;
import io.cdap.plugin.successfactors.common.util.SuccessFactorsUtil;
import io.cdap.plugin.successfactors.source.config.SuccessFactorsPluginConfig;
import io.cdap.plugin.successfactors.source.metadata.SuccessFactorsEntityProvider;
import io.cdap.plugin.successfactors.source.metadata.SuccessFactorsSchemaGenerator;
import io.cdap.plugin.successfactors.source.transport.SuccessFactorsResponseContainer;
import io.cdap.plugin.successfactors.source.transport.SuccessFactorsTransporter;
import io.cdap.plugin.successfactors.source.transport.SuccessFactorsUrlContainer;
import okhttp3.HttpUrl;
import org.apache.olingo.odata2.api.edm.Edm;
import org.apache.olingo.odata2.api.edm.EdmEntitySet;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.ep.EntityProvider;
import org.apache.olingo.odata2.api.ep.EntityProviderException;
import org.apache.olingo.odata2.api.ep.EntityProviderReadProperties;
import org.apache.olingo.odata2.api.ep.feed.ODataFeed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.ws.rs.core.MediaType;

/**
 * This {@code SuccessFactorsService} contains all the SAP SuccessFactors relevant service call implementations
 * - check the correctness of the formed SuccessFactors URL
 * - builds the Output Schema
 * - fetch total number of available record count
 * - builds the base64 encoded SAP SuccessFactors entity metadata string
 */
public class SuccessFactorsService {

  public static final String TEST = "TEST";
  public static final String METADATA = "METADATA";
  public static final String DATA = "DATA";
  public static final String COUNT = "COUNT";
  public static final String SERVER_SIDE = "serverSide";
  private static final String ODATA_ROOT_ELEMENT = "d";
  private static final String ODATA_RESULT_ELEMENT = "results";
  private static final Logger LOG = LoggerFactory.getLogger(SuccessFactorsService.class);
  private final SuccessFactorsPluginConfig pluginConfig;
  private final SuccessFactorsTransporter successFactorsHttpClient;
  private final SuccessFactorsUrlContainer urlContainer;
  private String nextUrl;

  public SuccessFactorsService(SuccessFactorsPluginConfig pluginConfig,
                               SuccessFactorsTransporter successFactorsHttpClient) {
    this.pluginConfig = pluginConfig;
    this.successFactorsHttpClient = successFactorsHttpClient;
    urlContainer = new SuccessFactorsUrlContainer(pluginConfig);
  }

  /**
   * Calls to check the Successfactors URL correctness.
   *
   * @throws TransportException             any http client exceptions are wrapped under it.
   * @throws SuccessFactorsServiceException any SuccessFactors service based exception is wrapped under it.
   */
  public void checkSuccessFactorsURL() throws TransportException, SuccessFactorsServiceException {

    SuccessFactorsResponseContainer responseContainer =
      successFactorsHttpClient.callSuccessFactorsEntity(urlContainer.getTesterURL(), MediaType.APPLICATION_JSON, TEST);

    ExceptionParser.checkAndThrowException(ResourceConstants.ERR_FAILED_ENTITY_VALIDATION.getMsgForKey(),
                                           responseContainer);
  }

  /**
   * Prepares output schema based on the provided plugin config parameters.
   * e.g.
   * - builds schema with given selective properties
   * - builds schema with default and given expanded navigation properties
   * - builds schema with non-navigation default properties
   * <p>
   * For more detail please refer {@code SuccessFactorsSchemaGenerator}
   *
   * @return {@code Schema}
   * @throws TransportException             any http client exceptions are wrapped under it.
   * @throws SuccessFactorsServiceException any SuccessFactors service based exception is wrapped under it.
   */
  public Schema buildOutputSchema() throws SuccessFactorsServiceException, TransportException {

    SuccessFactorsEntityProvider edmData = fetchServiceMetadata(callEntityMetadata());
    SuccessFactorsSchemaGenerator successFactorsSchemaGenerator = new SuccessFactorsSchemaGenerator(edmData);

    if (SuccessFactorsUtil.isNotNullOrEmpty(pluginConfig.getSelectOption())) {
      return successFactorsSchemaGenerator.buildSelectOutputSchema(pluginConfig.getEntityName(), pluginConfig.
        getSelectOption());
    } else if (SuccessFactorsUtil.isNotNullOrEmpty(pluginConfig.getExpandOption())) {
      return successFactorsSchemaGenerator.buildExpandOutputSchema(pluginConfig.getEntityName(), pluginConfig.
        getExpandOption());
    } else {
      return successFactorsSchemaGenerator.buildDefaultOutputSchema(pluginConfig.getEntityName());
    }
  }

  /**
   * Calls the SAP SuccessFactors Service and returns the {@code Edm} instance.
   *
   * @return {@code SuccessFactorsEntityProvider}
   * @throws SuccessFactorsServiceException any SuccessFactors service based exception is wrapped under it.
   */
  private SuccessFactorsEntityProvider fetchServiceMetadata(InputStream metadataStream)
    throws SuccessFactorsServiceException {
    try (InputStream stream = metadataStream) {
      Edm metadata = EntityProvider.readMetadata(stream, false);
      return new SuccessFactorsEntityProvider(metadata);
    } catch (EntityProviderException | IOException e) {
      String errMsg = ResourceConstants.ERR_READING_METADATA.getMsgForKey(pluginConfig.getEntityName());
      throw new SuccessFactorsServiceException(errMsg, e);
    }
  }

  /**
   * Calls the SAP SuccessFactors catalog entity to fetch Entity metadata
   *
   * @return {@code InputStream}
   * @throws TransportException any http client exceptions are wrapped under it.
   */
  private InputStream callEntityMetadata() throws TransportException {
    SuccessFactorsResponseContainer responseContainer = successFactorsHttpClient
      .callSuccessFactorsEntity(urlContainer.getMetadataURL(), MediaType.APPLICATION_XML, METADATA);
    return responseContainer.getResponseStream();
  }

  /**
   * Fetch the total available record count from the SAP OData service
   *
   * @return count of total available records
   * @throws TransportException             any http client exceptions are wrapped under it.
   * @throws SuccessFactorsServiceException any OData service based exception is wrapped under it.
   * @throws IOException
   */
  public long getTotalAvailableRowCount() throws TransportException, SuccessFactorsServiceException, IOException {
    try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
      callEntityDataCount(), StandardCharsets.UTF_8))) {
      String raw = bufferedReader.lines().collect(Collectors.joining(""));
      return Long.parseLong(raw);
    }
  }

  /**
   * Calls the SAP SuccessFactors service entity to fetch the total number of available records
   *
   * @return
   * @throws TransportException             any http client exceptions are wrapped under it.
   * @throws SuccessFactorsServiceException any OData service based exception is wrapped under it.
   */
  private InputStream callEntityDataCount() throws SuccessFactorsServiceException, TransportException {
    SuccessFactorsResponseContainer responseContainer = successFactorsHttpClient
      .callSuccessFactors(urlContainer.getTotalRecordCountURL(), MediaType.TEXT_PLAIN, COUNT);

    String errMsg = ResourceConstants.ERR_FETCH_RECORD_COUNT.getMsgForKeyWithCode(pluginConfig.getEntityName());
    ExceptionParser.checkAndThrowException(errMsg, responseContainer);
    return responseContainer.getResponseStream();
  }

  /**
   * @throws TransportException             any http client exceptions are wrapped under it.
   * @throws SuccessFactorsServiceException any OData service based exception is wrapped under it.
   */
  public String getEncodedServiceMetadata() throws TransportException, SuccessFactorsServiceException {
    byte[] buffer = new byte[1024];
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    int numRead = 0;
    try (InputStream metaDataStream = callEntityMetadata()) {
      while ((numRead = metaDataStream.read(buffer)) > -1) {
        output.write(buffer, 0, numRead);
      }

      return Base64.getEncoder().encodeToString(output.toByteArray());
    } catch (IOException ioe) {
      throw new SuccessFactorsServiceException(ResourceConstants.ERR_METADATA_ENCODED_STRING
                                                 .getMsgForKeyWithCode(pluginConfig.getEntityName()), ioe);

    }
  }

  /**
   * Converts the base64 encoded SuccessFactors entity metadata string to actual 'Edm' type.
   * This method will be used in the runtime.
   *
   * @param encodedMetadata base64 encoded SuccessFactors entity metadata string
   * @return {@code Edm}
   * @throws SuccessFactorsServiceException any SuccessFactors based exception is wrapped under it.
   */
  public Edm getSuccessFactorsServiceEdm(String encodedMetadata) throws SuccessFactorsServiceException {
    try {
      byte[] bytes = Base64.getDecoder().decode(encodedMetadata);
      try (ByteArrayInputStream metadataStream = new ByteArrayInputStream(bytes)) {
        return fetchServiceMetadata(metadataStream).getEdmMetadata();
      }
    } catch (IOException | IllegalArgumentException e) {
      throw new SuccessFactorsServiceException(
        ResourceConstants.ERR_METADATA_DECODE.getMsgForKeyWithCode(pluginConfig.getEntityName()), e);
    }
  }

  /**
   * Calls the SAP SuccessFactors service to fetch records and convert it into list of {@code ODataEntry}.
   *
   * @param edm  SuccessFactors service entity metadata
   * @param skip number of rows to skip
   * @param top  number of rows to fetch
   * @return {@code ODataFeed}
   * @throws TransportException             any http client exceptions are wrapped under it.
   * @throws SuccessFactorsServiceException any OData service based exception is wrapped under it.
   */
  public ODataFeed readServiceEntityData(Edm edm, Long skip, Long top)
    throws SuccessFactorsServiceException, TransportException {

    SuccessFactorsEntityProvider serviceHelper = new SuccessFactorsEntityProvider(edm);
    try (InputStream dataStream = callEntityData(skip, top)) {

      EdmEntitySet entity = serviceHelper.getEntitySet(pluginConfig.getEntityName());
      // compile raw data to ODataFeed type
      ODataFeed dataFeed;
      if (pluginConfig.getExpandOption() != null) {
        dataFeed = EntityProvider
          .readFeed(MediaType.APPLICATION_JSON, entity, filterExpandedEntityData(dataStream),
                    EntityProviderReadProperties.init().build());
      } else {
        dataFeed = EntityProvider
          .readFeed(MediaType.APPLICATION_JSON, entity, dataStream, EntityProviderReadProperties
            .init().build());
      }

      if (dataFeed != null) {
        if (pluginConfig.getPaginationType().equals(SERVER_SIDE)) {
          String nextLink = dataFeed.getFeedMetadata().getNextLink();
          if (nextLink != null) {
            nextUrl = nextLink;
            LOG.info("Next page url: {}", nextLink);
          }
        }
        return dataFeed;
      }

    } catch (EdmException | EntityProviderException | IOException ex) {
      String errMsg = ResourceConstants.ERR_RECORD_PROCESSING.getMsgForKeyWithCode(pluginConfig.getEntityName());
      throw new SuccessFactorsServiceException(errMsg, ex);
    } catch (TransportException te) {
      String errMsg = ResourceConstants.ERR_RECORD_PULL.getMsgForKeyWithCode(pluginConfig.getEntityName());
      errMsg += ExceptionParser.buildTransportError(te);
      throw new TransportException(errMsg, te);
    } catch (SuccessFactorsServiceException ose) {
      String errMsg = ResourceConstants.ERR_RECORD_PULL.getMsgForKeyWithCode(pluginConfig.getEntityName());
      errMsg += ExceptionParser.buildSuccessFactorsServiceError(ose);
      throw new SuccessFactorsServiceException(errMsg, ose);
    }
    return null;
  }

  /**
   * Calls the SAP SuccessFactors service entity to fetch the data from the given range
   *
   * @param skip number to rows to skip
   * @param top  number to rows to fetch
   * @return {@code InputStream}
   * @throws TransportException             any http client exceptions are wrapped under it.
   * @throws SuccessFactorsServiceException any OData service based exception is wrapped under it.
   */
  private InputStream callEntityData(Long skip, Long top)
    throws SuccessFactorsServiceException, TransportException, IOException {
    URL dataURL;
    if (nextUrl != null) {
      dataURL = Objects.requireNonNull(HttpUrl.parse(nextUrl)).newBuilder().build().url();
    } else {
      dataURL = urlContainer.getDataFetchURL(skip, top);
    }
    SuccessFactorsResponseContainer responseContainer = successFactorsHttpClient.callSuccessFactorsWithRetry(dataURL);

    ExceptionParser.checkAndThrowException("", responseContainer);
    return responseContainer.getResponseStream();
  }

  public List<String> getNonNavigationalProperties() throws TransportException, SuccessFactorsServiceException,
    EdmException {
    SuccessFactorsEntityProvider edmData = fetchServiceMetadata(callEntityMetadata());
    SuccessFactorsSchemaGenerator successFactorsSchemaGenerator = new SuccessFactorsSchemaGenerator(edmData);
    List<String> columnDetailList = successFactorsSchemaGenerator.getNonNavigationalProperties
      (pluginConfig.getEntityName());
    return columnDetailList;
  }

  /**
   * @param dataStream
   * @return filteredDataStream Filtered Data Stream after removing expanded entity data
   * @throws IOException
   */
  private InputStream filterExpandedEntityData(InputStream dataStream) throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    List<String> expandFieldList = new ArrayList<>();
    JsonNode root = objectMapper.readTree(dataStream);
    JsonNode arrayNode = root.get(ODATA_ROOT_ELEMENT).get(ODATA_RESULT_ELEMENT);
    for (JsonNode objectNode : arrayNode) {
      String expandOption = pluginConfig.getExpandOption();
      if (expandOption.contains(SuccessFactorsUrlContainer.PROPERTY_SEPARATOR)) {
        expandFieldList = Arrays.asList(pluginConfig.getExpandOption()
                                          .split(SuccessFactorsUrlContainer.PROPERTY_SEPARATOR));
      } else {
        expandFieldList.add(pluginConfig.getExpandOption());
      }
      for (String expandField : expandFieldList) {
        JsonNode expandedNode = objectNode.get(expandField);
        Iterator<Map.Entry<String, JsonNode>> iterator = expandedNode.fields();
        while (iterator.hasNext()) {
          if (iterator.next().getValue().isContainerNode()) {
            iterator.remove();
          }
        }

      }
    }
    InputStream filteredDataStream = new ByteArrayInputStream(objectMapper.writeValueAsBytes(root));
    return filteredDataStream;
  }
}

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

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.DirectConnector;
import io.cdap.cdap.etl.api.connector.PluginSpec;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.ReferenceNames;
import io.cdap.plugin.successfactors.common.exception.SuccessFactorsServiceException;
import io.cdap.plugin.successfactors.common.exception.TransportException;
import io.cdap.plugin.successfactors.common.util.ExceptionParser;
import io.cdap.plugin.successfactors.source.SuccessFactorsSource;
import io.cdap.plugin.successfactors.source.config.SuccessFactorsPluginConfig;
import io.cdap.plugin.successfactors.source.metadata.SuccessFactorsEntityProvider;
import io.cdap.plugin.successfactors.source.metadata.SuccessFactorsSchemaGenerator;
import io.cdap.plugin.successfactors.source.transform.SuccessFactorsTransformer;
import io.cdap.plugin.successfactors.source.transport.SuccessFactorsResponseContainer;
import io.cdap.plugin.successfactors.source.transport.SuccessFactorsTransporter;
import okhttp3.HttpUrl;

import org.apache.olingo.odata2.api.edm.Edm;
import org.apache.olingo.odata2.api.edm.EdmEntitySet;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.ep.EntityProvider;
import org.apache.olingo.odata2.api.ep.EntityProviderException;
import org.apache.olingo.odata2.api.ep.EntityProviderReadProperties;
import org.apache.olingo.odata2.api.ep.entry.ODataEntry;
import org.apache.olingo.odata2.api.ep.feed.ODataFeed;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.ws.rs.core.MediaType;

/**
 * SuccessFactorsConnector Class
 */
@Plugin(type = Connector.PLUGIN_TYPE)
@Name(SuccessFactorsConnector.NAME)
@Description("Connection to access data in SuccessFactors.")
public class SuccessFactorsConnector implements DirectConnector {
  public static final String NAME = "SuccessFactors";
  public static final String METADATA = "METADATA";
  public static final String PROPERTY_SEPARATOR = ",";
  private static final String ENTITY_TYPE_ENTITY = "entity";
  private static final String METADATACALL = "$metadata";
  private static final String TOP_OPTION = "$top";
  private static final String SELECT_OPTION = "$select";
  private static final String ENTITY_SETS = "EntitySets";
  private final SuccessFactorsConnectorConfig config;

  public SuccessFactorsConnector(SuccessFactorsConnectorConfig config) {
    this.config = config;
  }

  @Override
  public void test(ConnectorContext connectorContext) throws ValidationException {
    FailureCollector collector = connectorContext.getFailureCollector();
    config.validateBasicCredentials(collector);
    config.validateConnection(collector);
  }

  @Override
  public BrowseDetail browse(ConnectorContext connectorContext, BrowseRequest browseRequest) throws IOException {
    BrowseDetail.Builder browseDetailBuilder = BrowseDetail.builder();
    int count = 0;
    List<String> entities = null;
    try {
      entities = listEntities();
    } catch (TransportException e) {
      throw new IOException("Error in communicating SuccessFactors", e);
    }
    for (int i = 0; i < entities.size(); i++) {
      String name = entities.get(i);
      BrowseEntity.Builder entity = (BrowseEntity.builder(name, name, ENTITY_TYPE_ENTITY).
        canBrowse(false).canSample(true));
      browseDetailBuilder.addEntity(entity.build());
      count++;
    }
    return browseDetailBuilder.setTotalCount(count).build();
  }

  @Override
  public ConnectorSpec generateSpec(ConnectorContext connectorContext, ConnectorSpecRequest connectorSpecRequest)
    throws IOException {
    ConnectorSpec.Builder specBuilder = ConnectorSpec.builder();
    Map<String, String> properties = new HashMap<>();
    properties.put(io.cdap.plugin.common.ConfigUtil.NAME_USE_CONNECTION, "true");
    properties.put(ConfigUtil.NAME_CONNECTION, connectorSpecRequest.getConnectionWithMacro());
    String entity = connectorSpecRequest.getPath();
    if (entity != null) {
      properties.put(SuccessFactorsPluginConfig.ENTITY_NAME, entity);
      properties.put(Constants.Reference.REFERENCE_NAME, ReferenceNames.cleanseReferenceName(entity));
      try {
        Schema schema = getSchema(entity);
        specBuilder.setSchema(schema);
      } catch (SuccessFactorsServiceException | TransportException | EntityProviderException e) {
        throw new IOException("Unable to create Schema", e);
      }
    }
    return specBuilder.addRelatedPlugin(new PluginSpec(SuccessFactorsSource.NAME, BatchSource.PLUGIN_TYPE,
      properties)).build();
  }

  List<String> listEntities() throws TransportException, IOException {
    URL dataURL = HttpUrl.parse(config.getBaseURL()).newBuilder().build().url();
    SuccessFactorsTransporter successFactorsHttpClient = new SuccessFactorsTransporter(config);
    SuccessFactorsResponseContainer responseContainer = successFactorsHttpClient.callSuccessFactorsEntity
      (dataURL, MediaType.APPLICATION_JSON, METADATA);
    try (InputStream inputStream = responseContainer.getResponseStream()) {
      BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
      String result = reader.lines().collect(Collectors.joining(""));
      Gson gson = new Gson();
      JsonObject jsonObject = gson.fromJson(result, JsonObject.class);
      JsonArray jsonArray = jsonObject.getAsJsonArray(ENTITY_SETS);

      Type type = new TypeToken<List<String>>() {
      }.getType();
      return gson.fromJson(jsonArray, type);
    }
  }

  Schema getSchema(String entityName) throws TransportException, EntityProviderException,
    SuccessFactorsServiceException, IOException {
    try (InputStream inputStream = getMetaDataStream(entityName)) {
      Edm metadata = EntityProvider.readMetadata(inputStream, false);
      SuccessFactorsEntityProvider edmData = new SuccessFactorsEntityProvider(metadata);
      SuccessFactorsSchemaGenerator successFactorsSchemaGenerator = new SuccessFactorsSchemaGenerator(edmData);
      return successFactorsSchemaGenerator.buildDefaultOutputSchema(entityName);
    }
  }

  @Override
  public List<StructuredRecord> sample(ConnectorContext connectorContext, SampleRequest sampleRequest)
    throws IOException {
    String entity = sampleRequest.getPath();
    if (entity == null) {
      throw new IllegalArgumentException("Path should contain entity name.");
    }
    try {
      return listEntityData(entity, sampleRequest.getLimit());
    } catch (EntityProviderException | SuccessFactorsServiceException | TransportException | EdmException e) {
      throw new IOException("Unable to fetch data.", e);
    }
  }

  /**
   * @return returns the list of the data for the selected entity.
   */
  List<StructuredRecord> listEntityData(String entity, long top)
    throws EdmException, TransportException, EntityProviderException, SuccessFactorsServiceException, IOException {
    try (InputStream inputStream = getMetaDataStream(entity)) {
      Edm edm = EntityProvider.readMetadata(inputStream, false);
      SuccessFactorsEntityProvider serviceHelper = new SuccessFactorsEntityProvider(edm);
      EdmEntitySet edmEntitySet = serviceHelper.getEntitySet(entity);
      try (InputStream dataStream = callEntityData(top, entity)) {
        ODataFeed dataFeed = EntityProvider.readFeed(MediaType.APPLICATION_JSON, edmEntitySet,
          dataStream, EntityProviderReadProperties.init().build());
        SuccessFactorsTransformer valueConverter = new SuccessFactorsTransformer(getSchema(entity));
        List<ODataEntry> oDataEntryList;
        oDataEntryList = dataFeed != null ? dataFeed.getEntries() : Collections.emptyList();
        List<StructuredRecord> data = new ArrayList<>();
        for (int i = 0; i < oDataEntryList.size(); i++) {
          StructuredRecord dataRecord = valueConverter.buildCurrentRecord(oDataEntryList.get(i));
          data.add(dataRecord);
        }
        return data;
      }
    }
  }

  /**
   * @return returns the responseStream for the data of the selected entity.
   */
  private InputStream callEntityData(long top, String entityName)
    throws SuccessFactorsServiceException, TransportException, IOException, EntityProviderException, EdmException {
    StringBuilder selectFields = new StringBuilder(String.join(PROPERTY_SEPARATOR,
      getNonNavigationalProperties(entityName)));
    URL dataURL = HttpUrl.parse(config.getBaseURL()).newBuilder().addPathSegment(entityName).
      addQueryParameter(TOP_OPTION, String.valueOf(top)).addQueryParameter(SELECT_OPTION, selectFields.toString())
      .build().url();
    SuccessFactorsTransporter successFactorsHttpClient = new SuccessFactorsTransporter(config);
    SuccessFactorsResponseContainer responseContainer = successFactorsHttpClient.callSuccessFactorsWithRetry(dataURL);

    ExceptionParser.checkAndThrowException("", responseContainer);
    return responseContainer.getResponseStream();
  }

  List<String> getNonNavigationalProperties(String entity) throws TransportException, EdmException,
    EntityProviderException, IOException {
    SuccessFactorsEntityProvider edmData = fetchServiceMetadata(entity);
    SuccessFactorsSchemaGenerator successFactorsSchemaGenerator = new SuccessFactorsSchemaGenerator(edmData);
    List<String> columnDetailList = successFactorsSchemaGenerator.getNonNavigationalProperties
      (entity);
    return columnDetailList;
  }

  SuccessFactorsEntityProvider fetchServiceMetadata(String entity) throws TransportException,
    EntityProviderException, IOException {
    try (InputStream metadataStream = getMetaDataStream(entity)) {
      Edm metadata = EntityProvider.readMetadata(metadataStream, false);
      return new SuccessFactorsEntityProvider(metadata);
    }
  }
  /**
   * @return returns the responseStream for metadata call.
   */
  private InputStream getMetaDataStream(String entity) throws TransportException, IOException {
    URL metadataURL = HttpUrl.parse(config.getBaseURL()).newBuilder().addPathSegments(entity)
      .addPathSegment(METADATACALL).build().url();
    SuccessFactorsTransporter successFactorsHttpClient = new SuccessFactorsTransporter(config);
    SuccessFactorsResponseContainer responseContainer = successFactorsHttpClient
      .callSuccessFactorsEntity(metadataURL, MediaType.APPLICATION_XML, METADATA);
    return responseContainer.getResponseStream();
    }
  }


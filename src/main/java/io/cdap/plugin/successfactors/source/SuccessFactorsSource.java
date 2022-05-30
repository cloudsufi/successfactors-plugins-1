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

import com.google.gson.Gson;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.common.batch.JobUtils;
import io.cdap.plugin.successfactors.common.exception.SuccessFactorsServiceException;
import io.cdap.plugin.successfactors.common.exception.TransportException;
import io.cdap.plugin.successfactors.common.util.ExceptionParser;
import io.cdap.plugin.successfactors.common.util.ResourceConstants;
import io.cdap.plugin.successfactors.source.config.SuccessFactorsPluginConfig;
import io.cdap.plugin.successfactors.source.input.SuccessFactorsInputFormat;
import io.cdap.plugin.successfactors.source.input.SuccessFactorsInputSplit;
import io.cdap.plugin.successfactors.source.input.SuccessFactorsPartitionBuilder;
import io.cdap.plugin.successfactors.source.service.SuccessFactorsService;
import io.cdap.plugin.successfactors.source.transport.SuccessFactorsTransporter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Plugin returns records from SuccessFactors using entity name provided by user.
 * Reads data in batches, every batch is processed as a separate split by mapreduce.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(SuccessFactorsSource.NAME)
@Description("Reads the SuccessFactors data which is exposed as OData services from SAP.")
public class SuccessFactorsSource extends BatchSource<LongWritable, StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(SuccessFactorsSource.class);
  public static final String NAME = "SuccessFactors";
  private final SuccessFactorsPluginConfig config;
  public static final String OUTPUT_SCHEMA = "outputSchema";

  public SuccessFactorsSource(SuccessFactorsPluginConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector failureCollector = stageConfigurer.getFailureCollector();
    config.validatePluginParameters(failureCollector);

    if (config.isSchemaBuildRequired()) {
      Schema schema = config.getSchema(failureCollector);
      if (schema != null) {
        stageConfigurer.setOutputSchema(schema);
      } else {
        stageConfigurer.setOutputSchema(getOutputSchema(failureCollector));
      }
    } else {
      stageConfigurer.setOutputSchema(null);
    }
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    Schema outputSchema = context.getOutputSchema();
    if (outputSchema == null) {
      outputSchema = getOutputSchema(context.getFailureCollector());
    }

    if (outputSchema == null) {
      throw new IllegalArgumentException(ResourceConstants.ERR_MACRO_INPUT.getMsgForKeyWithCode());
    }

    FailureCollector collector = context.getFailureCollector();

    configureJob(context, outputSchema);

    collector.getOrThrowException();

  }

  /**
   * Gets the appropriate Schema based on the provided plugin parameters and also
   * sets the appropriate error messages in case any error is identified while preparing the Schema.
   *
   * @param failureCollector {@code FailureCollector}
   * @param isRuntimeError   this flag is used to attach error codes to any inline UI error message during runtime.
   *                         As there could be cases where macro values are not correct which only get processed at the
   *                         runtime, so this flag acts as an indicator to the plugin to add the error code to
   *                         any UI inline errors such as invalid SAP credentials and so on.
   * @return {@code Schema}
   */
  @Nullable
  private Schema getOutputSchema(FailureCollector failureCollector) {
    SuccessFactorsTransporter transporter = new SuccessFactorsTransporter(config.getUsername(), config.getPassword());
    SuccessFactorsService successFactorsServices = new SuccessFactorsService(config, transporter);
    try {
      //validate if the given parameters form a valid SuccessFactors URL.
      successFactorsServices.checkSuccessFactorsURL();
      return successFactorsServices.buildOutputSchema();
    } catch (TransportException te) {
      String errorMsg = ExceptionParser.buildTransportError(te);
      errorMsg = ResourceConstants.ERR_ODATA_SERVICE_CALL.getMsgForKeyWithCode(errorMsg);
      failureCollector.addFailure(errorMsg, null).withConfigProperty(SuccessFactorsPluginConfig.BASE_URL);
    } catch (SuccessFactorsServiceException ose) {
      attachFieldWithError(ose, failureCollector);
    }
    failureCollector.getOrThrowException();
    return null;
  }

  /**
   * Checks and attaches the UI fields with its relevant error message.
   *
   * @param ose              {@code SuccessFactorsServiceException}
   * @param failureCollector {@code FailureCollector}
   */
  private void attachFieldWithError(SuccessFactorsServiceException ose, FailureCollector failureCollector) {

    String errMsg = ExceptionParser.buildSuccessFactorsServiceError(ose);
    errMsg = ResourceConstants.ERR_ODATA_ENTITY_FAILURE.getMsgForKeyWithCode(errMsg);
    switch (ose.getErrorCode()) {
      case HttpURLConnection.HTTP_UNAUTHORIZED:
        failureCollector.addFailure(errMsg, null).withConfigProperty(SuccessFactorsPluginConfig.UNAME);
        failureCollector.addFailure(errMsg, null).withConfigProperty(SuccessFactorsPluginConfig.PASSWORD);
        break;
      case HttpURLConnection.HTTP_FORBIDDEN:
      case ExceptionParser.NO_VERSION_FOUND:
      case ExceptionParser.INVALID_VERSION_FOUND:
        failureCollector.addFailure(errMsg, null).withConfigProperty(SuccessFactorsPluginConfig.ENTITY_NAME);
        break;
      case HttpURLConnection.HTTP_NOT_FOUND:
        failureCollector.addFailure(errMsg, ResourceConstants.ERR_NOT_FOUND.getMsgForKey());
        break;
      case HttpURLConnection.HTTP_BAD_REQUEST:
        failureCollector.addFailure(errMsg, ResourceConstants.ERR_CHECK_ADVANCED_PARAM.getMsgForKey());
        break;

      default:
        failureCollector.addFailure(errMsg, null);
    }
  }

  /**
   * Configures the Hadoop job for data extraction.
   *
   * @param context
   * @param outputSchema
   * @throws TransportException    any http client exceptions are wrapped under it.
   * @throws SuccessFactorsServiceException any SuccessFactors service based exception is wrapped under it.
   * @throws IOException           any IO exception occurs during the Hadoop Job instance creation.
   */
  private void configureJob(BatchSourceContext context, Schema outputSchema)
    throws TransportException, SuccessFactorsServiceException, IOException {


    SuccessFactorsTransporter transporter = new SuccessFactorsTransporter(config.getUsername(), config.getPassword());

    SuccessFactorsService successFactorsService = new SuccessFactorsService(config, transporter);
    
    long fetchRowCount = 0;

    long availableRowCount = successFactorsService.getTotalAvailableRowCount();

    if (availableRowCount <= 0) {
      LOG.warn(ResourceConstants.ERR_NO_RECORD_FOUND.getMsgForKeyWithCode(config.getEntityName()));
    }

    if (context.isPreviewEnabled()) {
      long previewRecordCount = context.getMaxPreviewRecords();
      if (previewRecordCount <= 0) {
        fetchRowCount = 100L;
      }
      fetchRowCount = Math.min(previewRecordCount, Math.min(fetchRowCount, availableRowCount));
    }

    SuccessFactorsPartitionBuilder partitionBuilder = new SuccessFactorsPartitionBuilder();
    List<SuccessFactorsInputSplit> partitions;
    if (config.getPaginationType().equals("serverSide")) {
      partitions = new ArrayList<>();
      partitions.add(new SuccessFactorsInputSplit());
    } else {
      partitions = partitionBuilder.buildSplit(availableRowCount, fetchRowCount);
    }

    setJobForDataRead(context, outputSchema, partitions, successFactorsService);
  }

  /**
   * Sets the Hadoop Job runtime configuration parameters.
   *
   * @param context
   * @param outputSchema
   * @param partitions
   * @param successFactorsService
   * @throws TransportException    any http client exceptions are wrapped under it.
   * @throws SuccessFactorsServiceException any OData service based exception is wrapped under it.
   * @throws IOException           any IO exception occurs during the Hadoop Job instance creation.
   */
  private void setJobForDataRead(BatchSourceContext context, Schema outputSchema, List<SuccessFactorsInputSplit>
    partitions,
                                 SuccessFactorsService successFactorsService)
    throws TransportException, SuccessFactorsServiceException, IOException {

    Configuration jobConfiguration;
    Job job = JobUtils.createInstance();
    jobConfiguration = job.getConfiguration();
    Gson gson = new Gson();

    // Set plugin properties in Hadoop Job's configuration
    jobConfiguration.set(SuccessFactorsInputFormat.SUCCESSFACTORS_PLUGIN_PROPERTIES, gson.toJson(config));

    // Serialize the list of partitions to save in Hadoop Configuration
    jobConfiguration.set(SuccessFactorsInputFormat.PARTITIONS_PROPERTY, gson.toJson(partitions));

    // Setting plugin output schema
    jobConfiguration.set(OUTPUT_SCHEMA, outputSchema.toString());

    // Serialize the SuccessFactors metadata to save in Hadoop Configuration
    String metadataString = successFactorsService.getEncodedServiceMetadata();
    jobConfiguration.set(SuccessFactorsInputFormat.ENCODED_ENTITY_METADATA_STRING, metadataString);

    emitLineage(context, outputSchema, config.getEntityName());

    SourceInputFormatProvider inputFormat = new SourceInputFormatProvider(SuccessFactorsInputFormat.class,
                                                                          jobConfiguration);
    context.setInput(Input.of(config.getReferenceName(), inputFormat));
  }

  private void emitLineage(BatchSourceContext context, Schema schema, String entity) {

    LineageRecorder lineageRecorder = new LineageRecorder(context, config.getReferenceName());
    lineageRecorder.createExternalDataset(schema);

    if (schema.getFields() != null) {
      String operationDesc = String.format("Read '%s' from SAP SuccessFactors", entity);

      lineageRecorder.recordRead("Read", operationDesc,
                                 schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }
  }
}

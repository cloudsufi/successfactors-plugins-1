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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.successfactors.common.exception.SuccessFactorsServiceException;
import io.cdap.plugin.successfactors.source.SuccessFactorsSource;
import io.cdap.plugin.successfactors.source.config.SuccessFactorsPluginConfig;
import io.cdap.plugin.successfactors.source.service.SuccessFactorsService;
import io.cdap.plugin.successfactors.source.transform.SuccessFactorsRecordReader;
import io.cdap.plugin.successfactors.source.transport.SuccessFactorsTransporter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.olingo.odata2.api.edm.Edm;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * This {@code SuccessFactorsInputFormat} defines the InputFormat implementation for SuccessFactors.
 */
public class SuccessFactorsInputFormat extends InputFormat<LongWritable, StructuredRecord> {

  public static final String SUCCESSFACTORS_PLUGIN_PROPERTIES = "successFactorsPluginProperties";
  public static final String PARTITIONS_PROPERTY = "partitionProperty";
  public static final String ENCODED_ENTITY_METADATA_STRING = "encodedMetadataString";
  private static final Gson GSON = new Gson();
  private static final Type INPUT_SPLIT_TYPE = new TypeToken<List<SuccessFactorsInputSplit>>() {

  }.getType();

  @Override
  public List<InputSplit> getSplits(JobContext jContext) throws IOException, InterruptedException {

    Configuration configuration = jContext.getConfiguration();
    List<InputSplit> splits = new ArrayList<>();

    // Deserialize partitions from Hadoop Configuration
    List<SuccessFactorsInputSplit> partitions = GSON.fromJson(configuration.get(PARTITIONS_PROPERTY),
                                                              INPUT_SPLIT_TYPE);

    splits.addAll(partitions);

    return splits;
  }


  @Override
  public RecordReader<LongWritable, StructuredRecord> createRecordReader(InputSplit split,
                                                                         TaskAttemptContext taContext)
    throws IOException, InterruptedException {

    SuccessFactorsInputSplit inputSplit = (SuccessFactorsInputSplit) split;

    SuccessFactorsPluginConfig pluginConfig = GSON.fromJson(taContext.getConfiguration()
      .get(SUCCESSFACTORS_PLUGIN_PROPERTIES), SuccessFactorsPluginConfig.class);

    Schema outputSchema = Schema.parseJson(taContext.getConfiguration().get(SuccessFactorsSource.OUTPUT_SCHEMA));

    String encodedMetadataString = taContext.getConfiguration().get(ENCODED_ENTITY_METADATA_STRING);

    SuccessFactorsTransporter transporter = new SuccessFactorsTransporter(pluginConfig.getUsername(),
                                                                          pluginConfig.getPassword());

    SuccessFactorsService successFactorsService = new SuccessFactorsService(pluginConfig, transporter);

    try {
      Edm edmData = successFactorsService.getSuccessFactorsServiceEdm(encodedMetadataString);
      if (!Objects.equals(pluginConfig.getPaginationType(), "serverSide")) {
        return new SuccessFactorsRecordReader(successFactorsService, edmData, outputSchema, inputSplit.getStart(),
                                              inputSplit.getEnd(), inputSplit.getPackageSize());
      } else {
        return new SuccessFactorsRecordReader(successFactorsService, edmData, outputSchema, null, null,
                                              null);
      }
    } catch (SuccessFactorsServiceException e) {
      throw new IOException(e.getMessage(), e);
    }
  }
}

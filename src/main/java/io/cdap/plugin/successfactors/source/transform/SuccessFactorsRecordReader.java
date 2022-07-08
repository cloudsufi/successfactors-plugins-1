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

package io.cdap.plugin.successfactors.source.transform;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.successfactors.common.exception.SuccessFactorsServiceException;
import io.cdap.plugin.successfactors.common.exception.TransportException;
import io.cdap.plugin.successfactors.source.service.SuccessFactorsService;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.olingo.odata2.api.edm.Edm;
import org.apache.olingo.odata2.api.ep.entry.ODataEntry;
import org.apache.olingo.odata2.api.ep.feed.ODataFeed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;

/**
 * This {@code SuccessFactorsRecordReader} contains Hadoop Job RecordReader implementation
 */
public class SuccessFactorsRecordReader extends RecordReader<LongWritable, StructuredRecord> {

  private final SuccessFactorsService successFactorsService;
  private final Edm edmData;
  private final SuccessFactorsTransformer valueConverter;

  @Nullable
  private final Long start;

  @Nullable
  private final Long end;

  @Nullable
  private final Long packageSize;
  private Long skipCount;
  private Long fetchCount;
  private long numRowsProcessed;
  private LongWritable key;
  private List<ODataEntry> oDataEntryList;
  private ODataFeed oDataFeed;
  private Iterator<ODataEntry> dataEntryIterator;
  private StructuredRecord dataRecord;

  public SuccessFactorsRecordReader(SuccessFactorsService successFactorsService, Edm edmData, Schema pluginSchema,
                                    @Nullable Long start, @Nullable Long end, @Nullable Long packageSize) {

    this.successFactorsService = successFactorsService;
    this.edmData = edmData;
    this.start = start;
    this.end = end;
    this.packageSize = packageSize;

    valueConverter = new SuccessFactorsTransformer(pluginSchema);
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext taContext) throws IOException {
    key = new LongWritable();
    oDataEntryList = new ArrayList<>();
    dataEntryIterator = oDataEntryList.listIterator();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (!dataEntryIterator.hasNext()) {
      if (!isCallRequired()) {
        return false;
      }
      // This condition will be true in case of client side pagination
      if (start != null && end != null && packageSize != null) {
        calculateSkipAndFetchCount();
      }

      try {
        // Pulls the data from the SuccessFactors entity for the given range via 'rows to skip' and 'rows to fetch'.
        oDataFeed = successFactorsService.readServiceEntityData(edmData, skipCount, fetchCount);
        oDataEntryList = oDataFeed != null ? oDataFeed.getEntries() : Collections.emptyList();

        if (oDataEntryList.isEmpty()) {
          return false;
        }
        dataEntryIterator = oDataEntryList.listIterator();

      } catch (SuccessFactorsServiceException | TransportException e) {
        throw new IOException(e.getMessage(), e);
      }
    }

    dataRecord = valueConverter.buildCurrentRecord(dataEntryIterator.next());
    numRowsProcessed++;
    key.set(numRowsProcessed);
    return true;
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public StructuredRecord getCurrentValue() throws IOException, InterruptedException {
    return dataRecord;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return numRowsProcessed / (float) getLength();
  }

  @Override
  public void close() throws IOException {
    // No-op
  }

  private boolean isCallRequired() {
    if (start == null && end == null && packageSize == null) {
      return oDataFeed == null || oDataFeed.getFeedMetadata().getNextLink() != null;
    } else {
      return getLength() - numRowsProcessed > 0;
    }
  }

  private void calculateSkipAndFetchCount() {
    skipCount = start + numRowsProcessed - 1;
    long remain = getLength() - numRowsProcessed;
    fetchCount = Math.min(remain, packageSize);
  }

  private long getLength() {
    return end - start + 1;
  }
}

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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This {@code SuccessFactorsInputSplit} holds a wrapper for partition details like start and end indices of
 * the records and static information like runtime Metadata which remains same
 * for all splits generated in this instance of InputFormat.
 */
public class SuccessFactorsInputSplit extends InputSplit implements Writable {

  private long start;
  private long end;
  // Standard package size. A split may need multiple SAP network calls (batches),
  // and last batch may have lesser number of records than this packageSize
  private long batchSize;

  public SuccessFactorsInputSplit() {
  }

  public SuccessFactorsInputSplit(long start, long end, long batchSize) {
    this.start = start;
    this.end = end;
    this.batchSize = batchSize;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return end - start + 1;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return new String[]{};
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(start);
    out.writeLong(end);
    out.writeLong(batchSize);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.start = in.readLong();
    this.end = in.readLong();
    this.batchSize = in.readLong();
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  public long getBatchSize() {
    return batchSize;
  }
}

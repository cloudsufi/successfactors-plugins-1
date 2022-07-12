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

import java.util.ArrayList;
import java.util.List;

/**
 * This {@code SuccessFactorsPartitionBuilder} will prepare the list of optimized splits containing start & end indices
 * for each split including the optimized batch size.
 * <p>
 * Max allowed Batch size is 1000
 * Max records in Split is 10000
 * <p>
 * If the total available record count is less than equal to 10000 then only 1 split will be created.
 * <p>
 */
public class SuccessFactorsPartitionBuilder {
  public static final long MAX_ALLOWED_BATCH_SIZE = 1000L;
  public static final long MAX_RECORDS_IN_SPLIT = 10000L;

  /**
   * Builds the list of {@code SuccessFactorsInputSplit}
   *
   * @param availableRecordCount available row count
   * @return list of {@code SuccessFactorsInputSplit}
   */
  public List<SuccessFactorsInputSplit> buildSplits(long availableRecordCount) {

    List<SuccessFactorsInputSplit> list = new ArrayList<>();
    long start = 1;
    // setting up the optimal split size and count values
    long batchSize = Math.min(availableRecordCount, MAX_ALLOWED_BATCH_SIZE);
    long optimalLoadOnSplit = Math.min(availableRecordCount, MAX_RECORDS_IN_SPLIT);
    long optimalSplitCount = availableRecordCount / optimalLoadOnSplit +
      (availableRecordCount % optimalLoadOnSplit != 0 ? 1 : 0);

    for (int split = 1; split <= optimalSplitCount; split++) {
      long end = (start - 1) + optimalLoadOnSplit;

      // prepare the split list
      list.add(new SuccessFactorsInputSplit(start, end, batchSize));
      start = end + 1;
    }
    return list;
  }
}

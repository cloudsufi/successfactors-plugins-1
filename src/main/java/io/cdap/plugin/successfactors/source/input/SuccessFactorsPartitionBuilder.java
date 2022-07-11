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
 * for each split including the optimized package size.
 * <p>
 * Max allowed Package size is 1000
 * Default Split size is 10000
 * <p>
 * If the total fetch record count is less then equal to 2500 then only 1 split will be created.
 * <p>
 * Note: DEFAULT & MAX on split count and package size is for the Customer Preview.
 */
public class SuccessFactorsPartitionBuilder {
  public static final long MAX_ALLOWED_PACKAGE_SIZE = 1000L;
  public static final long SPLIT_SIZE = 10000L;

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
    long packageSize = Math.min(availableRecordCount, MAX_ALLOWED_PACKAGE_SIZE);
    long optimalLoadOnSplit = Math.min(availableRecordCount, SPLIT_SIZE);
    long optimalSplitCount = availableRecordCount / optimalLoadOnSplit +
      (availableRecordCount % optimalLoadOnSplit != 0 ? 1 : 0);
    long leftoverLoadCount = availableRecordCount % optimalSplitCount;

    for (int split = 1; split <= optimalSplitCount; split++) {
      long extra = split < leftoverLoadCount ? 1 : 0;
      long end = (start - 1) + optimalLoadOnSplit + extra;

      // prepare the split list
      list.add(new SuccessFactorsInputSplit(start, end, packageSize + extra));
      start = end + 1;
    }
    return list;
  }
}

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;

/**
 * This {@code SuccessFactorsPartitionBuilder} will prepare the list of optimized splits containing start & end indices
 * for each split including the optimized package size.
 *
 * Default Split count is 8
 * Max allowed Split count is 10
 * Max allowed Package size is 1000
 *
 * If the total fetch record count is less then equal to 2500 then only 1 split will be created.
 *
 * Note: DEFAULT & MAX on split count and package size is for the Customer Preview.
 */
public class SuccessFactorsPartitionBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(SuccessFactorsPartitionBuilder.class);

  public static final int DEFAULT_SPLIT_COUNT = 8;
  public static final long MAX_ALLOWED_PACKAGE_SIZE = 1000L;

  /**
   * builds the list of {@code SuccessFactorsInputSplit}
   *
   * @param availableRecordCount available row count
   * @param fetchRowCount        plugin property, number of rows to extract. 0 (zero) means the total record to fetch
   *                             will be same as total number of available records.
   * @return list of {@code SuccessFactorsInputSplit}
   */
  public List<SuccessFactorsInputSplit> buildSplits(long availableRecordCount, long fetchRowCount) {

    List<SuccessFactorsInputSplit> list = new ArrayList<>();

    long recordReadStartIndex = 1;
    long actualRecordToExtract = (fetchRowCount == 0 ? availableRecordCount : fetchRowCount);
    long recordReadEndIndex = Math.min(actualRecordToExtract, availableRecordCount);
    if (actualRecordToExtract > availableRecordCount) {
      actualRecordToExtract = availableRecordCount;
      if (actualRecordToExtract <= 0) {
        String msg = "As per the provided configuration no records were found for extraction.";
        throw new IllegalArgumentException(msg);
      }
    }
    
    // setting up the optimal package size values
    long packageSize = Math.min(actualRecordToExtract, MAX_ALLOWED_PACKAGE_SIZE);

    int splitCount = DEFAULT_SPLIT_COUNT;

    // in case total fetch record count is less than the DEFAULT_PACKAGE_SIZE then Split count will be defaulted to 1.
    if (actualRecordToExtract <= MAX_ALLOWED_PACKAGE_SIZE) {
      splitCount = 1;
    }

    long optimalLoadOnSplit =
      (splitCount == 1 ? actualRecordToExtract : (actualRecordToExtract / splitCount));
    long optimalPackageSize = Math.min(optimalLoadOnSplit, packageSize);

    long leftoverLoadCount = actualRecordToExtract % splitCount;

    LOG.info("Total number to available record: {}", availableRecordCount);
    LOG.info("Total number of record to extract: {}", actualRecordToExtract);
    LOG.info("Record extraction to begin at index: {}", recordReadStartIndex);
    LOG.info("Record extraction to end at index: {}", recordReadEndIndex);
    LOG.info("Calculated number of splits: {}", splitCount);
    LOG.info("Optimal record count to extract on the each splits: {}", optimalLoadOnSplit);
    LOG.info("Optimal package size in each splits: {}", optimalPackageSize);

    long start = recordReadStartIndex;

    for (int i = 0; i < splitCount; i++) {
      long extra = i < leftoverLoadCount ? 1 : 0;
      long end = (start - 1) + optimalLoadOnSplit + extra;

      // prepare the split list
      list.add(new SuccessFactorsInputSplit(start, end, optimalPackageSize + extra));

      start = end + 1;
    }

    return list;
  }
}

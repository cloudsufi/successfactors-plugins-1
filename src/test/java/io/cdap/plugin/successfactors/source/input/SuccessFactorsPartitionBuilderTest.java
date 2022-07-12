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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Class to test the building of splits.
 * <p>
 * Optimal Split count is calculated by dividing the total number of available records by the optimal load on split
 * plus 1, if the remainder of the total number of available records and optimal load on split is not equal to zero
 * optimalSplitCount = availableRecordCount / optimalLoadOnSplit +
 * (availableRecordCount % optimalLoadOnSplit != 0 ? 1 : 0);
 * Default Split Size is 10000L. Optimal load on split is the minimum of (total number of available records and the
 * split size)
 * Maximum batch size is 1000L.
 */
public class SuccessFactorsPartitionBuilderTest {

  private SuccessFactorsPartitionBuilder partitionBuilder;

  @Before
  public void setUp() {
    partitionBuilder = new SuccessFactorsPartitionBuilder();
  }

  @Test
  public void testWithDefaultValue() {
    long availableRowCount = 100;

    List<SuccessFactorsInputSplit> partitionList = partitionBuilder.buildSplits(availableRowCount);

    Assert.assertEquals("Start is not same", 1, partitionList.get(0).getStart());
    Assert.assertEquals("End is not same", 100, partitionList.get(0).getEnd());
    Assert.assertEquals("Batch size is not same", 100, partitionList.get(0).getBatchSize());
  }

  /**
   * optimalSplitCount = 378403 / 10000 + (378403 % 10000 != 0 ? 1 : 0) = 37 + 1 = 38
   */
  @Test
  public void testWithExtraLoadOnSplitWithMaxBatchSize() {
    long availableRowCount = 378403;

    List<SuccessFactorsInputSplit> partitionList = partitionBuilder.buildSplits(availableRowCount);

    long expectedExtraLoadCount = availableRowCount % partitionList.size() - 1;
    long optimalLoad = (availableRowCount / partitionList.size());
    //For rounding off
    optimalLoad = (optimalLoad + 500) / 1000 * 1000;
    Assert
      .assertEquals("Split size is not same", 38,
                    partitionList.size());
    Assert.assertEquals("Start is not same", 1, partitionList.get(0).getStart());
    Assert.assertEquals("End is not same", (optimalLoad), partitionList.get(0).getEnd());
    Assert.assertEquals("Batch size is not same", (SuccessFactorsPartitionBuilder.MAX_ALLOWED_BATCH_SIZE),
      partitionList.get(0).getBatchSize());
  }

  /**
   * optimalSplitCount = 378403 / 10000 + (378403 % 10000 != 0 ? 1 : 0) = 37 + 1 = 38
   */
  @Test
  public void testWithExtraLoadOnSplit() {
    long availableRowCount = 378403;
    long batchSize = 1000;

    List<SuccessFactorsInputSplit> partitionList = partitionBuilder.buildSplits(availableRowCount);

    long expectedExtraLoadCount = availableRowCount % partitionList.size() - 1;
    long optimalLoad = availableRowCount / partitionList.size();
    //For rounding off
    optimalLoad = (optimalLoad + 500) / 1000 * 1000;

    Assert
      .assertEquals("Split size is not same", 38, partitionList.size());
    Assert.assertEquals("Start is not same", 1, partitionList.get(0).getStart());
    Assert.assertEquals("End is not same", (optimalLoad), partitionList.get(0).getEnd());
    Assert.assertEquals("Batch size is not same", (batchSize), partitionList.get(0).getBatchSize());
  }

  @Test
  public void testUpdatedFetchRowCount() {
    long availableRowCount = 123;

    List<SuccessFactorsInputSplit> partitionList = partitionBuilder.buildSplits(availableRowCount);

    long actualFetchSize =
      partitionList.stream().collect(Collectors.summarizingLong(SuccessFactorsInputSplit::getBatchSize)).getSum();
    Assert.assertEquals("Total record extraction count is not same", availableRowCount, actualFetchSize);
  }

  @Test
  public void testBatchSizeOptimizationBasedOnSplitCount() {
    long availableRowCount = 123;

    List<SuccessFactorsInputSplit> partitionList = partitionBuilder.buildSplits(availableRowCount);

    Assert.assertEquals("Batch size is not optimized", availableRowCount,
                        partitionList.get(partitionList.size() - 1).getBatchSize());
  }

  @Test
  public void testMaxBatchSizeOptimization() {
    long availableRowCount = 2000;
    int splitCount = 1;

    List<SuccessFactorsInputSplit> partitionList = partitionBuilder.buildSplits(availableRowCount);

    Assert.assertTrue("Batch size is beyond the allowed optimized size.",
      partitionList.stream()
        .filter(successFactorsInputSplit -> successFactorsInputSplit.getBatchSize() == SuccessFactorsPartitionBuilder.
          MAX_ALLOWED_BATCH_SIZE)
        .count() == splitCount);
  }

  @Test
  public void testStartEndAndBatchSizeCompare() {
    long availableRowCount = 9;

    List<SuccessFactorsInputSplit> partitionList = partitionBuilder.buildSplits(availableRowCount);

    Assert.assertEquals("Start is not same for split 1", 1, partitionList.get(0).getStart());
    Assert.assertEquals("End is not same for split 1", 9, partitionList.get(0).getEnd());
    Assert.assertEquals("Batch size is not same for split 1", 9, partitionList.get(0).getBatchSize());
  }

  /**
   * optimalSplitCount = 9000 / 10000 + (9000 % 10000 != 0 ? 1 : 0) = 0 + 1 = 1
   */
  @Test
  public void testBatchSize() {
    long availableRowCount = 9000;

    List<SuccessFactorsInputSplit> partitionList = partitionBuilder.buildSplits(availableRowCount);
    Assert.assertEquals("Split count is not same", 1, partitionList.size());
    Assert.assertEquals("Batch size is not same", SuccessFactorsPartitionBuilder.MAX_ALLOWED_BATCH_SIZE,
                        partitionList.get(0).getBatchSize());
  }

  /**
   * optimalSplitCount = 9000 / 10000 + (9000 % 10000 != 0 ? 1 : 0) = 0 + 1 = 1
   */
  @Test
  public void testSplitCountOnBelowDefaultSplitSize() {
    long availableRowCount = 9000;

    List<SuccessFactorsInputSplit> partitionList = partitionBuilder.buildSplits(availableRowCount);
    Assert.assertEquals("Split count is not same", 1,
                        partitionList.size());
  }

  /**
   * optimalSplitCount = 190000 / 10000 + (190000 % 10000 != 0 ? 1 : 0) = 19 + 0 = 19
   */
  @Test
  public void testMaxAllowedSplitCountAndBatchSize() {
    long availableRowCount = 190000;

    List<SuccessFactorsInputSplit> partitionList = partitionBuilder.buildSplits(availableRowCount);
    Assert
      .assertEquals("Split count is not same", 19,
                    partitionList.size());
    Assert.assertEquals("Batch size is not same", SuccessFactorsPartitionBuilder.MAX_ALLOWED_BATCH_SIZE,
      partitionList.get(0).getBatchSize());
  }
}

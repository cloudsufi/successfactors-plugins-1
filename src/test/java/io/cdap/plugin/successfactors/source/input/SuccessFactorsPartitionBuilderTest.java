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

public class SuccessFactorsPartitionBuilderTest {

  private SuccessFactorsPartitionBuilder partitionBuilder;

  @Before
  public void setUp() {
    partitionBuilder = new SuccessFactorsPartitionBuilder();
  }

  @Test
  public void testWithDefaultValue() {
    long availableRowCount = 100;
    long fetchRowCount = 0;

    List<SuccessFactorsInputSplit> partitionList = partitionBuilder.buildSplits(availableRowCount, fetchRowCount);

    Assert.assertEquals("Start is not same", 1, partitionList.get(0).getStart());
    Assert.assertEquals("End is not same", 100, partitionList.get(0).getEnd());
    Assert.assertEquals("Package size is not same", 100, partitionList.get(0).getBatchSize());
  }

  @Test
  public void testWithExtraLoadOnSplitWithMaxPackageSize() {
    long availableRowCount = 378403;
    long fetchRowCount = 0;

    List<SuccessFactorsInputSplit> partitionList = partitionBuilder.buildSplits(availableRowCount, fetchRowCount);

    long expectedExtraLoadCount = availableRowCount % partitionList.size();
    long optimalLoad = (availableRowCount / partitionList.size());

    Assert
      .assertEquals("Split size is not same", SuccessFactorsPartitionBuilder.DEFAULT_SPLIT_COUNT,
                    partitionList.size());
    Assert.assertEquals("Start is not same", 1, partitionList.get(0).getStart());
    Assert.assertEquals("End is not same", (optimalLoad + 1), partitionList.get(0).getEnd());
    Assert.assertEquals("Package size is not same", (SuccessFactorsPartitionBuilder.MAX_ALLOWED_PACKAGE_SIZE + 1),
      partitionList.get(0).getBatchSize());

    long distributedExtraLoadCount =
      partitionList.stream().filter(sapInputSplit ->
        sapInputSplit.getBatchSize() == (SuccessFactorsPartitionBuilder.MAX_ALLOWED_PACKAGE_SIZE + 1))
        .count();

    Assert.assertEquals("extra load distribution count is not same",
      expectedExtraLoadCount, distributedExtraLoadCount);

    long distributedLoadCount =
      partitionList.stream().filter(sapInputSplit ->
        sapInputSplit.getBatchSize() == SuccessFactorsPartitionBuilder.MAX_ALLOWED_PACKAGE_SIZE).count();

    Assert.assertEquals("Optimal load distribution count is not same",
      (partitionList.size() - expectedExtraLoadCount), distributedLoadCount);
  }

  @Test
  public void testWithExtraLoadOnSplit() {
    long availableRowCount = 378403;
    long fetchRowCount = 0;
    long packageSize = 1000;

    List<SuccessFactorsInputSplit> partitionList = partitionBuilder.buildSplits(availableRowCount, fetchRowCount);

    long expectedExtraLoadCount = availableRowCount % partitionList.size();
    long optimalLoad = (availableRowCount / partitionList.size());

    Assert
      .assertEquals("Split size is not same", SuccessFactorsPartitionBuilder.DEFAULT_SPLIT_COUNT,
                    partitionList.size());
    Assert.assertEquals("Start is not same", 1, partitionList.get(0).getStart());
    Assert.assertEquals("End is not same", (optimalLoad + 1), partitionList.get(0).getEnd());
    Assert.assertEquals("Package size is not same", (packageSize + 1), partitionList.get(0).getBatchSize());

    long distributedExtraLoadCount =
      partitionList.stream().filter(sapInputSplit -> sapInputSplit.getBatchSize() == (packageSize + 1)).count();
    Assert.assertEquals("extra load distribution count is not same",
      expectedExtraLoadCount, distributedExtraLoadCount);

    long distributedLoadCount =
      partitionList.stream().filter(sapInputSplit -> sapInputSplit.getBatchSize() == packageSize).count();
    Assert.assertEquals("Optimal load distribution count is not same",
      (partitionList.size() - expectedExtraLoadCount), distributedLoadCount);
  }

  @Test
  public void testUpdatedFetchRowCount() {
    long availableRowCount = 123;
    long fetchRowCount = 100;

    List<SuccessFactorsInputSplit> partitionList = partitionBuilder.buildSplits(availableRowCount, fetchRowCount);

    long expectedFetchSize = fetchRowCount;
    long actualFetchSize =
      partitionList.stream().collect(Collectors.summarizingLong(SuccessFactorsInputSplit::getBatchSize)).getSum();
    Assert.assertEquals("Total record extraction count is not same", expectedFetchSize, actualFetchSize);
  }

  @Test
  public void testPackageSizeOptimizationBasedOnSplitCount() {
    long availableRowCount = 123;
    long fetchRowCount = 100;

    List<SuccessFactorsInputSplit> partitionList = partitionBuilder.buildSplits(availableRowCount, fetchRowCount);

    long expectedPackageSize = fetchRowCount;
    Assert.assertEquals("Package size is not optimized", expectedPackageSize,
      partitionList.get(partitionList.size() - 1).getBatchSize());
  }

  @Test
  public void testMaxPackageSizeOptimization() {
    long availableRowCount = 2000;
    long fetchRowCount = 1000;
    int splitCount = 1;

    List<SuccessFactorsInputSplit> partitionList = partitionBuilder.buildSplits(availableRowCount, fetchRowCount);

    Assert.assertTrue("Package size is beyond the allowed optimized size.",
      partitionList.stream()
        .filter(successFactorsInputSplit -> successFactorsInputSplit.getBatchSize() == 1000)
        .count() == splitCount);
  }

  @Test
  public void testStartEndAndPackageSizeCompare() {
    long availableRowCount = 9;
    long fetchRowCount = 0;

    List<SuccessFactorsInputSplit> partitionList = partitionBuilder.buildSplits(availableRowCount, fetchRowCount);

    Assert.assertEquals("Start is not same for split 1", 1, partitionList.get(0).getStart());
    Assert.assertEquals("End is not same for split 1", 9, partitionList.get(0).getEnd());
    Assert.assertEquals("Package size is not same for split 1", 9, partitionList.get(0).getBatchSize());
  }

  @Test
  public void testBatchSize() {
    long availableRowCount = 9000;
    long fetchRowCount = 0;

    List<SuccessFactorsInputSplit> partitionList = partitionBuilder.buildSplits(availableRowCount, fetchRowCount);
    Assert.assertEquals("Split count is not same", SuccessFactorsPartitionBuilder.DEFAULT_SPLIT_COUNT,
                        partitionList.size());
    Assert.assertEquals("Package size is not same", SuccessFactorsPartitionBuilder.MAX_ALLOWED_PACKAGE_SIZE,
                        partitionList.get(0).getBatchSize());
  }

  @Test
  public void testSplitCountOnBelowDefaultPackageSize() {
    long availableRowCount = 9000;
    long fetchRowCount = 1500;

    /**
     * Package size is computed by dividing the number of records to extract by the number of splits.
     * packageSize = actualRecordToExtract / splitCount i.e. 5000/8 = 188
     * Default Split count is 8
     */
    List<SuccessFactorsInputSplit> partitionList = partitionBuilder.buildSplits(availableRowCount, fetchRowCount);
    Assert.assertEquals("Split count is not same", 8, partitionList.size());
    Assert.assertEquals("Package size is not same", 188, partitionList.get(0).getBatchSize());
  }

  @Test
  public void testMaxAllowedSplitCountAndBatchSize() {
    long availableRowCount = 190000;
    long fetchRowCount = 0;

    List<SuccessFactorsInputSplit> partitionList = partitionBuilder.buildSplits(availableRowCount, fetchRowCount);
    Assert
      .assertEquals("Split count is not same", SuccessFactorsPartitionBuilder.DEFAULT_SPLIT_COUNT,
                    partitionList.size());
    Assert.assertEquals("Package size is not same", SuccessFactorsPartitionBuilder.MAX_ALLOWED_PACKAGE_SIZE,
      partitionList.get(0).getBatchSize());
  }
}

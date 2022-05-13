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
    long skipRowCount = 0;
    int splitCount = 0;
    long packageSize = 0;

    List<SuccessFactorsInputSplit> partitionList = partitionBuilder.buildSplit(availableRowCount, fetchRowCount,
                                                                               skipRowCount,
                                                                               splitCount, packageSize);
    Assert.assertEquals(1, partitionList.get(0).getStart());
    Assert.assertEquals(100, partitionList.get(0).getEnd());
    Assert.assertEquals(100, partitionList.get(0).getPackageSize());
  }

  @Test
  public void testWithSkipRowCount() {
    long availableRowCount = 100;
    long fetchRowCount = 0;
    long skipRowCount = 10;
    int splitCount = 1;
    long packageSize = 90;
    List<SuccessFactorsInputSplit> partitionList = partitionBuilder.buildSplit(availableRowCount,
                                                                               fetchRowCount, skipRowCount, splitCount,
                                                                               packageSize);
    Assert.assertEquals(11, partitionList.get(0).getStart());
    Assert.assertEquals(100, partitionList.get(0).getEnd());
    Assert.assertEquals(90, partitionList.get(0).getPackageSize());
  }

  @Test
  public void testWithExtraLoadOnSplitWithDefaultPackageSize() {
    long availableRowCount = 378403;
    long fetchRowCount = 0;
    long skipRowCount = 0;
    int splitCount = 43;
    long packageSize = 0;
    List<SuccessFactorsInputSplit> partitionList = partitionBuilder.buildSplit(availableRowCount,
                                                                               fetchRowCount, skipRowCount, splitCount,
                                                                               packageSize);
    long expectedExtraLoadCount = availableRowCount % partitionList.size();
    long optimalLoad = (availableRowCount / partitionList.size());
    Assert
      .assertEquals(SuccessFactorsPartitionBuilder.MAX_ALLOWED_SPLIT_COUNT,
                    partitionList.size());
    Assert.assertEquals(1, partitionList.get(0).getStart());
    Assert.assertEquals((optimalLoad + 1), partitionList.get(0).getEnd());
    Assert.assertEquals((SuccessFactorsPartitionBuilder.DEFAULT_PACKAGE_SIZE + 1),
                        partitionList.get(0).getPackageSize());
    long distributedExtraLoadCount =
      partitionList.stream().filter(sapInputSplit ->
                                      sapInputSplit.getPackageSize() == (SuccessFactorsPartitionBuilder
                                        .DEFAULT_PACKAGE_SIZE + 1))
        .count();
    Assert.assertEquals(expectedExtraLoadCount, distributedExtraLoadCount);
    long distributedLoadCount =
      partitionList.stream().filter(sapInputSplit ->
                                      sapInputSplit.getPackageSize() == SuccessFactorsPartitionBuilder
                                        .DEFAULT_PACKAGE_SIZE).count();

    Assert.assertEquals((partitionList.size() - expectedExtraLoadCount), distributedLoadCount);
  }

  @Test
  public void testWithExtraLoadOnSplit() {
    long availableRowCount = 378403;
    long fetchRowCount = 0;
    long skipRowCount = 0;
    int splitCount = 43;
    long packageSize = 4538;
    List<SuccessFactorsInputSplit> partitionList = partitionBuilder.buildSplit(availableRowCount,
                                                                               fetchRowCount, skipRowCount, splitCount,
                                                                               packageSize);
    long expectedExtraLoadCount = availableRowCount % partitionList.size();
    long optimalLoad = (availableRowCount / partitionList.size());
    Assert
      .assertEquals(SuccessFactorsPartitionBuilder.MAX_ALLOWED_SPLIT_COUNT,
                    partitionList.size());
    Assert.assertEquals(1, partitionList.get(0).getStart());
    Assert.assertEquals((optimalLoad + 1), partitionList.get(0).getEnd());
    Assert.assertEquals((packageSize + 1), partitionList.get(0).getPackageSize());
    long distributedExtraLoadCount =
      partitionList.stream().filter(sapInputSplit -> sapInputSplit.getPackageSize() == (packageSize + 1)).count();
    Assert.assertEquals(expectedExtraLoadCount, distributedExtraLoadCount);
    long distributedLoadCount =
      partitionList.stream().filter(sapInputSplit -> sapInputSplit.getPackageSize() == packageSize).count();
    Assert.assertEquals((partitionList.size() - expectedExtraLoadCount), distributedLoadCount);
  }

  @Test
  public void testUpdatedFetchRowCount() {
    long availableRowCount = 123;
    long fetchRowCount = 100;
    long skipRowCount = 40;
    int splitCount = 9;
    long packageSize = 0;

    List<SuccessFactorsInputSplit> partitionList = partitionBuilder.buildSplit(availableRowCount,
                                                                               fetchRowCount, skipRowCount, splitCount,
                                                                               packageSize);

    long expectedFetchSize = availableRowCount - skipRowCount;
    long actualFetchSize =
      partitionList.stream().collect(Collectors.summarizingLong(SuccessFactorsInputSplit::getPackageSize)).getSum();
    Assert.assertEquals(expectedFetchSize, actualFetchSize);
  }

  @Test
  public void testPackageSizeOptimizationBasedOnSplitCount() {
    long availableRowCount = 123;
    long fetchRowCount = 100;
    long skipRowCount = 19;
    int splitCount = 7;
    long packageSize = 30;

    List<SuccessFactorsInputSplit> partitionList = partitionBuilder.buildSplit(availableRowCount,
                                                                               fetchRowCount, skipRowCount, splitCount,
                                                                               packageSize);

    long expectedPackageSize = Math.min(packageSize, (fetchRowCount / partitionList.size()));
    Assert.assertEquals(expectedPackageSize,
                        partitionList.get(partitionList.size() - 1).getPackageSize());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoRecordFoundForExtraction() {
    long availableRowCount = 123;
    long fetchRowCount = 1000;
    long skipRowCount = 190;
    int splitCount = 3;
    long packageSize = 30;

    partitionBuilder.buildSplit(availableRowCount, fetchRowCount, skipRowCount, splitCount, packageSize);
  }

  @Test
  public void testMaxPackageSizeOptimization() {
    long availableRowCount = 20000;
    long fetchRowCount = 18000;
    long skipRowCount = 0;
    int splitCount = 2;
    long packageSize = 6000;

    List<SuccessFactorsInputSplit> partitionList = partitionBuilder.buildSplit(availableRowCount, fetchRowCount,
                                                                               skipRowCount,
                                                                               splitCount, packageSize);
    Assert.assertTrue(partitionList.stream()
                        .filter(successFactorsInputSplit -> successFactorsInputSplit.getPackageSize() == 5000)
                        .count() == splitCount);
  }

  @Test
  public void testStartEndAndPackageSizeCompare() {
    long availableRowCount = 9;
    long fetchRowCount = 0;
    long skipRowCount = 0;
    int splitCount = 3;
    long packageSize = 2;

    List<SuccessFactorsInputSplit> partitionList = partitionBuilder.buildSplit(availableRowCount, fetchRowCount,
                                                                               skipRowCount,
                                                                               splitCount, packageSize);
    Assert.assertEquals(1, partitionList.get(0).getStart());
    Assert.assertEquals(9, partitionList.get(0).getEnd());
    Assert.assertEquals(2, partitionList.get(0).getPackageSize());
  }

  @Test
  public void testDefaultSplitCount() {
    long availableRowCount = 9000;
    long fetchRowCount = 0;
    long skipRowCount = 0;
    int splitCount = 0;
    long packageSize = 3000;

    List<SuccessFactorsInputSplit> partitionList = partitionBuilder.buildSplit(availableRowCount, fetchRowCount,
                                                                               skipRowCount,
                                                                               splitCount, packageSize);
    Assert.assertEquals(SuccessFactorsPartitionBuilder.DEFAULT_SPLIT_COUNT,
                        partitionList.size());
  }

  @Test
  public void testDefaultBatchSize() {
    long availableRowCount = 9000;
    long fetchRowCount = 0;
    long skipRowCount = 0;
    int splitCount = 2;
    long packageSize = 0;

    List<SuccessFactorsInputSplit> partitionList = partitionBuilder.buildSplit(availableRowCount, fetchRowCount,
                                                                               skipRowCount,
                                                                               splitCount, packageSize);
    Assert.assertEquals(2, partitionList.size());
    Assert.assertEquals(SuccessFactorsPartitionBuilder.DEFAULT_PACKAGE_SIZE,
                        partitionList.get(0).getPackageSize());
  }

  @Test
  public void testSplitCountOnBelowDefaultPackageSize() {
    long availableRowCount = 9000;
    long fetchRowCount = 1500;
    long skipRowCount = 0;
    int splitCount = 2;
    long packageSize = 0;

    List<SuccessFactorsInputSplit> partitionList = partitionBuilder.buildSplit(availableRowCount, fetchRowCount,
                                                                               skipRowCount,
                                                                               splitCount, packageSize);
    Assert.assertEquals(1, partitionList.size());
    Assert.assertEquals(1500, partitionList.get(0).getPackageSize());
  }

  @Test
  public void testMaxAllowedSplitCountAndBatchSize() {
    long availableRowCount = 190000;
    long fetchRowCount = 0;
    long skipRowCount = 0;
    int splitCount = 14;
    long packageSize = 8000;

    List<SuccessFactorsInputSplit> partitionList = partitionBuilder.buildSplit(availableRowCount, fetchRowCount,
                                                                               skipRowCount,
                                                                               splitCount, packageSize);
    Assert.assertEquals(SuccessFactorsPartitionBuilder.MAX_ALLOWED_SPLIT_COUNT,
                    partitionList.size());
    Assert.assertEquals(SuccessFactorsPartitionBuilder.MAX_ALLOWED_BATCH_SIZE,
                        partitionList.get(0).getPackageSize());
  }
}

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
import org.junit.Test;
import org.mockito.Mockito;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;

public class SuccessFactorsInputSplitTest {

  @Test
  public void testGetLocations() throws IOException, InterruptedException {
    Assert.assertEquals(String[].class, new SuccessFactorsInputSplit(1L, 2L, 20L).getLocations().
      getClass());
    Assert.assertEquals(0, (new SuccessFactorsInputSplit(1L, 2L, 20L)).getLocations().length);
  }

  @Test
  public void testGetLength() throws IOException, InterruptedException {
    Assert.assertEquals(String[].class, new SuccessFactorsInputSplit(1L, 2L, 20L).getLocations().
      getClass());
    Assert.assertEquals(2, (new SuccessFactorsInputSplit(1L, 2L, 20L)).getLength());
  }

  @Test
  public void testReadFields() throws IOException {
    SuccessFactorsInputSplit successFactorsInputSplit = new SuccessFactorsInputSplit(1L, 2L, 20L);
    ObjectInputStream objectInputStream = Mockito.mock(ObjectInputStream.class);
    Mockito.when(objectInputStream.readLong()).thenReturn(0L);
    successFactorsInputSplit.readFields(objectInputStream);
    Assert.assertEquals(0L, successFactorsInputSplit.getStart());
    Assert.assertEquals(0L, successFactorsInputSplit.getEnd());
    Assert.assertEquals(0L, successFactorsInputSplit.getBatchSize());
  }

  @Test
  public void testWrite() throws IOException {
    SuccessFactorsInputSplit successFactorsInputSplit = Mockito.spy(new SuccessFactorsInputSplit
                                                                     (1L, 2L, 20L));
    DataOutput dataOutput = Mockito.mock(DataOutput.class);
    successFactorsInputSplit.write(dataOutput);
    Mockito.verify(successFactorsInputSplit, Mockito.times(1)).write(dataOutput);
  }

  @Test(expected = NullPointerException.class)
  public void testWriteWithNullData() throws IOException {
    DataOutput dataOutput = null;
    Long start = null;
    Long end = null;
    Long packageSize = null;
    SuccessFactorsInputSplit successFactorsInputSplit = new SuccessFactorsInputSplit(start, end, packageSize);
    successFactorsInputSplit.write(dataOutput);
  }

  @Test(expected = NullPointerException.class)
  public void testRead() throws IOException {
    DataInput dataInput = null;
    Long start = null;
    Long end = null;
    Long packageSize = null;
    SuccessFactorsInputSplit successFactorsInputSplit = new SuccessFactorsInputSplit(start, end, packageSize);
    successFactorsInputSplit.readFields(dataInput);
  }
}

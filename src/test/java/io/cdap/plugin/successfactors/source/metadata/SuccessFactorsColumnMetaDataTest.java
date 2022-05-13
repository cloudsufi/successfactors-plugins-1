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
package io.cdap.plugin.successfactors.source.metadata;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import java.util.Arrays;
import java.util.List;

public class SuccessFactorsColumnMetaDataTest {
  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  private static final String FIELD_NAME = "fieldName";

  private SuccessFactorsColumnMetadata successFactorsColumnMetadata;

  private enum SapSuccessFactorsColMetadata {
    SBYTE(FIELD_NAME + 1, "SByte", false, null),
    BYTE(FIELD_NAME + 2, "Byte", false, null),
    INT16(FIELD_NAME + 3, "Int16", false, null),
    INT32(FIELD_NAME + 4, "Int32", false, null),
    INT64(FIELD_NAME + 5, "Int64", false, null),
    SINGLE(FIELD_NAME + 6, "Single", false, null),
    DOUBLE(FIELD_NAME + 7, "Double", false, null),
    DECIMAL(FIELD_NAME + 8, "Decimal", false, null),
    GUID(FIELD_NAME + 9, "Guid", false, null),
    STRING(FIELD_NAME + 10, "String", false, null),
    BINARY(FIELD_NAME + 11, "Binary", false, null),
    BOOLEAN(FIELD_NAME + 12, "Boolean", false, null),
    DATETIME(FIELD_NAME + 13, "DateTime", false, null),
    TIME(FIELD_NAME + 14, "Time", false, null),
    DATETIMEOFFSET(FIELD_NAME + 15, "DateTimeOffset", false, null);

    private String name;
    private String type;
    private boolean isNullable;
    private List<SuccessFactorsColumnMetadata> childList;

    SapSuccessFactorsColMetadata(String name, String type, boolean isNullable,
                        List<SuccessFactorsColumnMetadata> childList) {
      this.name = name;
      this.type = type;
      this.isNullable = isNullable;
    }

    public String getName() {
      return name;
    }

    public String getType() {
      return type;
    }

    public boolean isNullable() {
      return isNullable;
    }
  }

  @Test
  public void checkBuildForDefaultInitialization() {
    successFactorsColumnMetadata = SuccessFactorsColumnMetadata.builder().build();

    Assert.assertTrue(successFactorsColumnMetadata.isNullable());
    Assert.assertFalse(successFactorsColumnMetadata.containsChild());
  }

  @Test
  public void checkBasicInitialization() {
    successFactorsColumnMetadata = SuccessFactorsColumnMetadata.builder()
      .name(SapSuccessFactorsColMetadata.STRING.getName())
      .type(SapSuccessFactorsColMetadata.STRING.getType())
      .isNullable(SapSuccessFactorsColMetadata.STRING.isNullable())
      .build();

    Assert.assertEquals(SapSuccessFactorsColMetadata.STRING.getName(), successFactorsColumnMetadata.getName());
    Assert.assertEquals(SapSuccessFactorsColMetadata.STRING.getType(), successFactorsColumnMetadata.getType());
    Assert.assertFalse(successFactorsColumnMetadata.isNullable());
    Assert.assertFalse(successFactorsColumnMetadata.containsChild());
  }

  @Test
  public void checkBuildForChildInitialization() {
    SuccessFactorsColumnMetadata child1 = SuccessFactorsColumnMetadata.builder()
      .name(SapSuccessFactorsColMetadata.GUID.getName())
      .type(SapSuccessFactorsColMetadata.GUID.getType())
      .build();

    SuccessFactorsColumnMetadata child2 = SuccessFactorsColumnMetadata.builder()
      .name(SapSuccessFactorsColMetadata.DATETIME.getName())
      .type(SapSuccessFactorsColMetadata.DATETIME.getType())
      .build();

    successFactorsColumnMetadata = SuccessFactorsColumnMetadata.builder()
      .name(SapSuccessFactorsColMetadata.STRING.getName())
      .type(SapSuccessFactorsColMetadata.STRING.getType())
      .childList(Arrays.asList(child1, child2))
      .build();

    Assert.assertTrue(successFactorsColumnMetadata.isNullable());
    Assert.assertTrue(successFactorsColumnMetadata.containsChild());

    Assert.assertEquals(2, successFactorsColumnMetadata.getChildList().size());
  }

  @Test
  public void checkAppendChildren() {
    successFactorsColumnMetadata = SuccessFactorsColumnMetadata.builder()
      .name(SapSuccessFactorsColMetadata.INT16.getName())
      .type(SapSuccessFactorsColMetadata.INT16.getType())
      .build();

    SuccessFactorsColumnMetadata child1 = SuccessFactorsColumnMetadata.builder()
      .name(SapSuccessFactorsColMetadata.BYTE.getName())
      .type(SapSuccessFactorsColMetadata.BYTE.getType())
      .build();

    successFactorsColumnMetadata.appendChild(child1);

    Assert.assertTrue(successFactorsColumnMetadata.containsChild());

    Assert.assertEquals(1, successFactorsColumnMetadata.getChildList().size());
  }

  @Test
  public void checkAppendChildrenAfterFinalize() {
    successFactorsColumnMetadata = SuccessFactorsColumnMetadata.builder()
      .name(SapSuccessFactorsColMetadata.INT16.getName())
      .type(SapSuccessFactorsColMetadata.INT16.getType())
      .build();

    successFactorsColumnMetadata.finalizeChildren();

    SuccessFactorsColumnMetadata child1 = SuccessFactorsColumnMetadata.builder()
      .name(SapSuccessFactorsColMetadata.BYTE.getName())
      .type(SapSuccessFactorsColMetadata.BYTE.getType())
      .build();

    exceptionRule.expect(IllegalStateException.class);
    exceptionRule.expectMessage("No more children can be added for the current object, check if the " +
                                  "'finalizeChildren' is called before adding the children.");

    successFactorsColumnMetadata.appendChild(child1);
  }

  @Test
  public void checkAppendingNestedChildrenAfterParentFinalize() {
    SuccessFactorsColumnMetadata child1 = SuccessFactorsColumnMetadata.builder()
      .name(SapSuccessFactorsColMetadata.GUID.getName())
      .type(SapSuccessFactorsColMetadata.GUID.getType())
      .build();

    SuccessFactorsColumnMetadata child2 = SuccessFactorsColumnMetadata.builder()
      .name(SapSuccessFactorsColMetadata.DATETIME.getName())
      .type(SapSuccessFactorsColMetadata.DATETIME.getType())
      .build();

    successFactorsColumnMetadata = SuccessFactorsColumnMetadata.builder()
      .name(SapSuccessFactorsColMetadata.STRING.getName())
      .type(SapSuccessFactorsColMetadata.STRING.getType())
      .childList(Arrays.asList(child1, child2))
      .build();

    SuccessFactorsColumnMetadata child3 = SuccessFactorsColumnMetadata.builder()
      .name(SapSuccessFactorsColMetadata.DATETIME.getName())
      .type(SapSuccessFactorsColMetadata.DATETIME.getType())
      .build();

    successFactorsColumnMetadata.getChildList().get(1).appendChild(child3);

    Assert.assertTrue(successFactorsColumnMetadata.containsChild());

    Assert.assertEquals(1, successFactorsColumnMetadata.getChildList().get(1).getChildList().size());

    successFactorsColumnMetadata.finalizeChildren();

    exceptionRule.expect(IllegalStateException.class);
    exceptionRule.expectMessage("No more children can be added for the current object, check if the " +
                                  "'finalizeChildren' is called before adding the children.");

    SuccessFactorsColumnMetadata child4 = SuccessFactorsColumnMetadata.builder()
      .name(SapSuccessFactorsColMetadata.DATETIME.getName())
      .type(SapSuccessFactorsColMetadata.DATETIME.getType())
      .build();

    // must throw IllegalStateException as finalizeChildren is already called
    successFactorsColumnMetadata.getChildList().get(1).appendChild(child4);
  }
}

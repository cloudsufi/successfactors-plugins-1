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
import javax.annotation.Nullable;

public class SuccessFactorsColumnMetaDataTest {
  private static final String FIELD_NAME = "fieldName";
  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();
  private SuccessFactorsColumnMetadata successFactorsColumnMetadata;

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
      .isVisible(SapSuccessFactorsColMetadata.STRING.isVisible)
      .maxLength(SapSuccessFactorsColMetadata.STRING.getMaxLength())
      .build();
    Assert.assertEquals(SapSuccessFactorsColMetadata.STRING.getName(), successFactorsColumnMetadata.getName());
    Assert.assertEquals(SapSuccessFactorsColMetadata.STRING.getType(), successFactorsColumnMetadata.getType());
    Assert.assertEquals(SapSuccessFactorsColMetadata.STRING.getMaxLength(),
                        successFactorsColumnMetadata.getMaxLength());
    Assert.assertTrue(successFactorsColumnMetadata.isVisible());
    Assert.assertFalse(successFactorsColumnMetadata.isNullable());
    Assert.assertFalse(successFactorsColumnMetadata.containsChild());
  }

  @Test
  public void checkBasicInitialization2() {
    successFactorsColumnMetadata = SuccessFactorsColumnMetadata.builder()
      .isUnicode(SapSuccessFactorsColMetadata.DECIMAL.isUnicode())
      .isFixedLength(SapSuccessFactorsColMetadata.DECIMAL.isFixedLength())
      .requiredInFilter(SapSuccessFactorsColMetadata.DECIMAL.getRequiredInFilter())
      .build();
    Assert.assertFalse(successFactorsColumnMetadata.isUnicode());
    Assert.assertTrue(successFactorsColumnMetadata.isFixedLength());
    Assert.assertTrue(successFactorsColumnMetadata.getRequiredInFilter());

  }

  @Test
  public void checkSapAttributesInitialization() {
    successFactorsColumnMetadata = SuccessFactorsColumnMetadata.builder()
      .collation(SapSuccessFactorsColMetadata.BYTE.getCollation())
      .concurrencyModeName(SapSuccessFactorsColMetadata.BYTE.getConcurrencyModeName())
      .filterRestrictions(SapSuccessFactorsColMetadata.BYTE.getFilterRestrictions())
      .displayFormat(SapSuccessFactorsColMetadata.BYTE.getDisplayFormat())
      .kindName(SapSuccessFactorsColMetadata.BYTE.getKindName())
      .defaultValue(SapSuccessFactorsColMetadata.BYTE.getDefaultValue())
      .label(SapSuccessFactorsColMetadata.BYTE.getLabel())
      .build();

    Assert.assertEquals(SapSuccessFactorsColMetadata.BYTE.getFilterRestrictions(),
                        successFactorsColumnMetadata.getFilterRestrictions());
    Assert.assertEquals(SapSuccessFactorsColMetadata.BYTE.getCollation(), successFactorsColumnMetadata.getCollation());
    Assert.assertEquals(SapSuccessFactorsColMetadata.BYTE.getConcurrencyModeName(),
                        successFactorsColumnMetadata.getConcurrencyModeName());
    Assert.assertEquals(SapSuccessFactorsColMetadata.BYTE.getDisplayFormat(),
                        successFactorsColumnMetadata.getDisplayFormat());
    Assert.assertEquals(SapSuccessFactorsColMetadata.BYTE.getLabel(),
                        successFactorsColumnMetadata.getLabel());
    Assert.assertEquals(SapSuccessFactorsColMetadata.BYTE.getDefaultValue(),
                        successFactorsColumnMetadata.getDefaultValue());
    Assert.assertEquals(SapSuccessFactorsColMetadata.BYTE.getKindName(),
                        successFactorsColumnMetadata.getKindName());
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

  private enum SapSuccessFactorsColMetadata {
    BYTE(FIELD_NAME, "Byte", false, null, 2, Boolean.TRUE,
         Boolean.TRUE, Boolean.TRUE, Boolean.TRUE, "displayFormat", "filterRestrictions",
         "collation", "concurrencyModeName", "defaultValue", "kindName",
         "label"),
    INT16(FIELD_NAME, "Int16", false, null, 2, Boolean.TRUE,
          Boolean.TRUE, Boolean.TRUE, Boolean.TRUE, "displayFormat", "filterRestrictions",
          "collation", "concurrencyModeName", "defaultValue", "kindName",
          "label"),
    DECIMAL(FIELD_NAME, "Decimal", false, null, 2, Boolean.TRUE,
            Boolean.TRUE, Boolean.FALSE, Boolean.TRUE, "displayFormat", "filterRestrictions",
            "collation", "concurrencyModeName", "defaultValue", "kindName",
            "label"),
    GUID(FIELD_NAME, "Guid", false, null, 2, Boolean.TRUE,
         Boolean.TRUE, Boolean.TRUE, Boolean.TRUE, "displayFormat", "filterRestrictions",
         "collation", "concurrencyModeName", "defaultValue", "kindName",
         "label"),
    STRING(FIELD_NAME, "String", false, null, 2, Boolean.TRUE,
           Boolean.TRUE, Boolean.TRUE, Boolean.TRUE, "displayFormat", "filterRestrictions",
           "collation", "concurrencyModeName", "defaultValue", "kindName",
           "label"),
    DATETIME(FIELD_NAME, "DateTime", false, null, 2, Boolean.TRUE,
             Boolean.TRUE, Boolean.FALSE, Boolean.TRUE, "displayFormat", "filterRestrictions",
             "collation", "concurrencyModeName", "defaultValue", "kindName",
             "label");

    private final String name;
    private final String type;
    private final boolean isNullable;
    private final Integer maxLength;
    private final Boolean isVisible;
    private final Boolean isFixedLength;
    private final Boolean isUnicode;
    private final Boolean getRequiredInFilter;
    private final String displayFormat;
    private final String filterRestrictions;
    private final String collation;
    private final String concurrencyModeName;
    private final String kindName;
    private final String label;
    private final String defaultValue;
    private List<SuccessFactorsColumnMetadata> childList;


    SapSuccessFactorsColMetadata(String name, String type, boolean isNullable,
                                 List<SuccessFactorsColumnMetadata> childList, Integer maxLength, Boolean isVisible,
                                 Boolean isFixedLength, Boolean isUnicode, Boolean getRequiredInFilter,
                                 String displayFormat, String filterRestrictions, String collation,
                                 String concurrencyModeName, String label, String kindName, String defaultValue) {
      this.name = name;
      this.type = type;
      this.isNullable = isNullable;
      this.maxLength = maxLength;
      this.isVisible = isVisible;
      this.isUnicode = isUnicode;
      this.isFixedLength = isFixedLength;
      this.getRequiredInFilter = getRequiredInFilter;
      this.filterRestrictions = filterRestrictions;
      this.collation = collation;
      this.displayFormat = displayFormat;
      this.concurrencyModeName = concurrencyModeName;
      this.kindName = kindName;
      this.defaultValue = defaultValue;
      this.label = label;

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

    public boolean isVisible() {
      return isVisible;
    }

    public Integer getMaxLength() {
      return maxLength;
    }

    public boolean isFixedLength() {
      return isFixedLength;
    }

    public boolean isUnicode() {
      return isUnicode;
    }

    public boolean getRequiredInFilter() {
      return getRequiredInFilter;
    }

    public String getCollation() {
      return collation;
    }

    public String getConcurrencyModeName() {
      return concurrencyModeName;
    }

    public String getDisplayFormat() {
      return displayFormat;
    }

    public String getFilterRestrictions() {
      return filterRestrictions;
    }

    public String getKindName() {
      return kindName;
    }

    public String getDefaultValue() {
      return defaultValue;
    }

    public String getLabel() {
      return label;
    }
  }
}

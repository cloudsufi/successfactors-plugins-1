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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.successfactors.common.exception.SuccessFactorsServiceException;
import org.apache.olingo.odata2.api.edm.Edm;
import org.apache.olingo.odata2.api.ep.EntityProvider;
import org.apache.olingo.odata2.api.ep.EntityProviderException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

public class SuccessFactorsSchemaGeneratorTest {

  private static Edm edm;
  @Rule
  public final ExpectedException exception = ExpectedException.none();
  private SuccessFactorsEntityProvider serviceHelper;
  private SuccessFactorsSchemaGenerator generator;

  @Before
  public void setup() throws EntityProviderException {
    edm = EntityProvider.readMetadata(TestSuccessFactorsUtil.readResource("successfactors-metadata.xml"), false);
    serviceHelper = new SuccessFactorsEntityProvider(edm);
    generator = new SuccessFactorsSchemaGenerator(serviceHelper);
  }

  @Test
  public void testBuildSelectOutputSchema() throws SuccessFactorsServiceException {
    Schema outputSchema = generator.buildSelectOutputSchema("C_GLAccountHierarchyNode",
                                                            "GLAccountHierarchy,HierarchyNode," +
                                                              "HierarchyVersion,SemanticTag");

    int lastIndex = outputSchema.getFields().size() - 1;
    Assert.assertEquals("Schema field size is not same.", 4, outputSchema.getFields().size());
    Assert.assertEquals("Schema first field name is not same.", "GLAccountHierarchy",
                        outputSchema.getFields().get(0).getName());
    Assert.assertEquals("Schema last field name is not same.",
                        "SemanticTag",
                        outputSchema.getFields().get(lastIndex).getName());
  }

  @Test
  public void testSelectWithExpandNames() throws SuccessFactorsServiceException {
    Schema outputSchema = generator.buildSelectOutputSchema("C_GLAccountHierarchyNode",
                                                            "to_GLAccountInChartOfAccounts/GLAccount," +
                                                              "to_GLAccountInChartOfAccounts/" +
                                                              "GLAccount_Text,HierarchyNode");

    Assert.assertEquals("Schema field size is not same.",
                        2,
                        outputSchema.getFields().size());

    Assert.assertEquals("to_GLAccountInChartOfAccounts field is not of Schema.Type.RECORD.",
                        Schema.Type.RECORD,
                        getFieldSchema(outputSchema.getFields(), "to_GLAccountInChartOfAccounts").getType());

    Assert.assertEquals("to_GLAccountInChartOfAccounts field does not contains 2 child.",
                        2,
                        getFieldSchema(outputSchema.getFields(), "to_GLAccountInChartOfAccounts").
                          getFields().size());

  }

  @Test
  public void testBuildExpandOutputSchema() throws SuccessFactorsServiceException {
    Schema outputSchema = generator.buildExpandOutputSchema("C_GLAccountHierarchyNode",
                                                            "to_GLAccountInChartOfAccounts");

    int lastIndex = outputSchema.getFields().size() - 1;
    Assert.assertEquals("Schema field size is not same.",
                        14,
                        outputSchema.getFields().size());

    Assert.assertEquals("Schema last field name is not same.",
                        "to_GLAccountInChartOfAccounts",
                        outputSchema.getFields().get(lastIndex).getName());

    Assert.assertFalse("Schema last field is not a nested field.",
                       outputSchema.getFields().get(lastIndex).getSchema().getType().isSimpleType());

    Assert.assertEquals("Schema last nested field child count is not same.",
                        22,
                        outputSchema.getFields().get(lastIndex).getSchema().getNonNullable().getFields().size());
  }

  @Test
  public void testBuildDefaultOutputSchema() throws SuccessFactorsServiceException {
    Schema outputSchema = generator.buildDefaultOutputSchema("C_GLAccountHierarchyNode");

    Assert.assertEquals("Schema field size is not same.",
                        13,
                        outputSchema.getFields().size());

    Assert.assertEquals("Schema 1st field name is not same.",
                        "GLAccountHierarchy",
                        outputSchema.getFields().get(0).getName());
  }

  @Test
  public void testSchemaTypeMapping() throws EntityProviderException, SuccessFactorsServiceException {
    edm = EntityProvider.readMetadata(TestSuccessFactorsUtil
      .readResource("successfactors-supported-datatype.xml"), false);
    serviceHelper = new SuccessFactorsEntityProvider(edm);
    generator = new SuccessFactorsSchemaGenerator(serviceHelper);

    Schema outputSchema = generator.buildDefaultOutputSchema("ZDATA_TYPESet");

    List<Schema.Field> fieldList = outputSchema.getFields();
    Assert.assertEquals("Edm.String to Schema.Type.STRING.",
                        Schema.Type.STRING,
                        getFieldSchema(fieldList, "Zchar").getType());

    Assert.assertEquals("Edm.Byte to Schema.Type.BYTES.",
                        Schema.Type.BYTES,
                        getFieldSchema(fieldList, "Zint1").getType());

    Assert.assertEquals("Edm.Int16 to Schema.Type.INT.",
                        Schema.Type.INT,
                        getFieldSchema(fieldList, "Zint2").getType());

    Assert.assertEquals("Edm.Double to Schema.Type.DOUBLE.",
                        Schema.Type.DOUBLE,
                        getFieldSchema(fieldList, "Zdec16").getType());

    Assert.assertEquals("Edm.Decimal to Schema.LogicalType.DECIMAL.",
                        Schema.LogicalType.DECIMAL,
                        getFieldSchema(fieldList, "Zdec34").getLogicalType());

    Assert.assertEquals("Edm.Time to Schema.Type.DOUBLE.",
                        Schema.LogicalType.TIME_MICROS,
                        getFieldSchema(fieldList, "Ztime").getLogicalType());

    Assert.assertEquals("Edm.DateTime to Schema.LogicalType.DATETIME.",
                        Schema.LogicalType.DATETIME,
                        getFieldSchema(fieldList, "Zdate").getLogicalType());

    Assert.assertEquals("Edm.Binary to Schema.Type.BYTES.",
                        Schema.Type.BYTES,
                        getFieldSchema(fieldList, "Zraw").getType());
  }

  @Test
  public void testInvalidEntityName() throws SuccessFactorsServiceException {
    exception.expectMessage("'Default property' not found in the 'INVALID-ENTITY-NAME' entity.");
    generator.buildDefaultOutputSchema("INVALID-ENTITY-NAME");
  }

  @Test
  public void testInvalidExpandName() throws SuccessFactorsServiceException {
    exception.expectMessage("'INVALID-NAVIGATION-NAME' not found in the 'C_GLAccountHierarchyNode' entity.");
    generator.buildExpandOutputSchema("C_GLAccountHierarchyNode", "INVALID-NAVIGATION-NAME");
  }

  private Schema getFieldSchema(List<Schema.Field> fieldList, String fieldName) {
    Schema schema = fieldList.stream().filter(field -> field.getName().equals(fieldName))
      .findFirst()
      .get()
      .getSchema();
    if (schema.isNullable()) {
      return schema.getNonNullable();
    }
    return schema;
  }
}

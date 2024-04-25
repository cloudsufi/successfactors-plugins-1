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
import io.cdap.plugin.successfactors.source.config.SuccessFactorsPluginConfig;

import org.apache.olingo.odata2.api.edm.Edm;
import org.apache.olingo.odata2.api.ep.EntityProvider;
import org.apache.olingo.odata2.api.ep.EntityProviderException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

import javax.annotation.Nullable;

public class SuccessFactorsSchemaGeneratorTest {

  private static Edm edm;
  @Rule
  public final ExpectedException exception = ExpectedException.none();
  private SuccessFactorsEntityProvider serviceHelper;
  private SuccessFactorsSchemaGenerator generator;
  private SuccessFactorsPluginConfig pluginConfig;

  @Before
  public void setup() throws EntityProviderException {
    edm = EntityProvider.readMetadata(TestSuccessFactorsUtil.readResource("successfactors-metadata.xml"), false);
    serviceHelper = new SuccessFactorsEntityProvider(edm);
    generator = new SuccessFactorsSchemaGenerator(serviceHelper);
    pluginConfig = new SuccessFactorsPluginConfig("referenceName", "baseUR",
      "entityName", "associateEntityName", "username", "password",
      null, null, null, "filterOption", "selectOption",
      "expandOption", "additionalQueryParameters", "paginationType",
                                                  null, null, null, null);
  }

  @Test
  public void testBuildSelectOutputSchema() throws SuccessFactorsServiceException {
    Schema outputSchema = generator.buildSelectOutputSchema("Benefit",
                                                            "ageOfRetirement,annualMaxContributionAmount," +
                                                              "annualMaxPayComponent,annualMinContributionAmount");

    int lastIndex = outputSchema.getFields().size() - 1;
    Assert.assertEquals("Schema field size is same.", 4, outputSchema.getFields().size());
    Assert.assertEquals("Schema first field name is same.", "ageOfRetirement",
                        outputSchema.getFields().get(0).getName());
    Assert.assertEquals("Schema last field name is same.",
                        "annualMinContributionAmount",
                        outputSchema.getFields().get(lastIndex).getName());
  }

  @Test
  public void testSelectWithExpandNames() throws SuccessFactorsServiceException {
    Schema outputSchema = generator.buildSelectOutputSchema("Benefit",
                                                            "eligibleBenefits/benefitId," +
                                                              "eligibleBenefits/" +
                                                              "benefitSchedule,mdfSystemRecordStatus");
    Assert.assertEquals("Schema field size is same.",
                        2,
                        outputSchema.getFields().size());
    Assert.assertEquals("eligibleBenefits field is of Schema.Type.ARRAY.",
                        Schema.Type.ARRAY,
                        getFieldSchema(outputSchema.getFields(), "eligibleBenefits").getType());
  }

  @Test
  public void testBuildExpandOutputSchema() throws SuccessFactorsServiceException {
    Schema outputSchema = generator.buildExpandOutputSchema("Benefit",
                                                            "eligibleBenefits", "associatedEntity", pluginConfig);
    int lastIndex = outputSchema.getFields().size() - 1;
    Assert.assertEquals("Schema field size is same.",
                        96,
                        outputSchema.getFields().size());
    Assert.assertEquals("Schema last field name is same.",
                        "eligibleBenefits",
                        outputSchema.getFields().get(lastIndex).getName());
    Assert.assertFalse("Schema last field is not of simple type.",
                       outputSchema.getFields().get(lastIndex).getSchema().getType().isSimpleType());
  }

  @Test
  public void testBuildDefaultOutputSchema() throws SuccessFactorsServiceException {
    Schema outputSchema = generator.buildDefaultOutputSchema("Benefit");
    Assert.assertEquals("Schema field size is same.",
                        95,
                        outputSchema.getFields().size());
    Assert.assertEquals("Schema 1st field name is same.",
                        "ageOfRetirement",
                        outputSchema.getFields().get(0).getName());
  }

  @Test
  public void testSchemaTypeMapping() throws EntityProviderException, SuccessFactorsServiceException {
    edm = EntityProvider.readMetadata(TestSuccessFactorsUtil
                                        .readResource("successfactors-supported-datatype.xml"), false);
    serviceHelper = new SuccessFactorsEntityProvider(edm);
    generator = new SuccessFactorsSchemaGenerator(serviceHelper);
    Schema outputSchema = generator.buildDefaultOutputSchema("EmployeePayrollRunResultsItems");
    List<Schema.Field> fieldList = outputSchema.getFields();
    Assert.assertEquals("Edm.String to Schema.Type.STRING.",
                        Schema.Type.STRING,
                        getFieldSchema(fieldList, "EmployeePayrollRunResults_externalCode").getType());

    Assert.assertEquals("Edm.Int64 to Schema.Type.LONG.",
                        Schema.Type.LONG,
                        getFieldSchema(fieldList, "mdfSystemTransactionSequence").getType());

    Assert.assertEquals("Edm.Decimal to Schema.LogicalType.DECIMAL",
                        Schema.LogicalType.DECIMAL,
                        getFieldSchema(fieldList, "amount").getLogicalType());

    Assert.assertEquals("Edm.Decimal to Schema.Type.BYTES",
                        Schema.Type.BYTES,
                        getFieldSchema(fieldList, "quantity").getType());

    Assert.assertEquals("Edm.DateTime to Schema.LogicalType.TIMESTAMP_MICROS",
                        Schema.LogicalType.DATETIME,
                        getFieldSchema(fieldList, "createdDate").getLogicalType());


    Assert.assertEquals("Edm.Decimal to Schema.LogicalType.DECIMAL",
                        Schema.LogicalType.DECIMAL,
                        getFieldSchema(fieldList, "quantity").getLogicalType());

    Assert.assertEquals("Edm.String to Schema.Type.STRING.",
                        Schema.Type.STRING,
                        getFieldSchema(fieldList, "mdfSystemRecordStatus").getType());
  }

  @Test
  public void testInvalidEntityName() throws SuccessFactorsServiceException {
    exception.expectMessage("'Default property' not found in the 'INVALID-ENTITY-NAME' entity.");
    generator.buildDefaultOutputSchema("INVALID-ENTITY-NAME");
  }

  @Test
  public void testInvalidExpandName() throws SuccessFactorsServiceException {
    exception.expectMessage("'assEntity' not found in the 'Benefit' entity.");
    generator.buildExpandOutputSchema("Benefit", "INVALID-NAVIGATION-NAME",
      "assEntity", pluginConfig);
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

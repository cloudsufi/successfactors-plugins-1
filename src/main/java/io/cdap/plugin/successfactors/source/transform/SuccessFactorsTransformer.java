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

package io.cdap.plugin.successfactors.source.transform;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.successfactors.source.service.SuccessFactorsService;
import org.apache.olingo.odata2.api.ep.entry.ODataEntry;
import org.apache.olingo.odata2.api.ep.feed.ODataDeltaFeed;
import org.apache.olingo.odata2.core.ep.entry.ODataEntryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This {@code SuccessFactorsTransformer} contains the logic to convert SuccessFactors entity record to
 * {@code StructuredRecord}
 */
public class SuccessFactorsTransformer {

  private static final Logger LOG = LoggerFactory.getLogger(SuccessFactorsTransformer.class);
  private final Schema recordSchema;

  public SuccessFactorsTransformer(Schema recordSchema) {
    this.recordSchema = recordSchema;
  }

  /**
   * Builds and returns the StructuredRecord from SuccessFactors entity record, which contains all the navigation
   * records details.
   *
   * @return {@code StructuredRecord}
   */
  public StructuredRecord buildCurrentRecord(ODataEntry oDataEntry) {
    return buildStructureRecord(recordSchema, oDataEntry);
  }

  /**
   * Builds a single record which may contain nested records if the provided 'SuccessFactorsEntry` instance hold any
   * navigation entity as an child.
   *
   * @param recordSchema plugin schema
   * @param oDataEntry   SAP SuccessFactors entity data
   * @return {@code StructuredRecord}
   */
  private StructuredRecord buildStructureRecord(Schema recordSchema, ODataEntry oDataEntry) {

    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(recordSchema);
    List<Schema.Field> cdfFields = recordSchema.getFields();

    cdfFields.forEach(field -> {
      String fieldName = field.getName();
      Object value = oDataEntry.getProperties().get(fieldName);
      Schema childSchema = field.getSchema();

      /* any schema of type 'ARRAY' means the value is holding a navigation entity of 1 to * multiplicity.
       e.g.
       {
         "Suppliers":[{
           "ID":0,
           "Name":"Z Suppliers"
           },{
           "ID":0,
           "Name":"PAN Suppliers"
           }]
       }*/
      if (childSchema.getType().equals(Schema.Type.ARRAY)) {
        if (value == null) {
          value = Collections.emptyList();
        } else if (value instanceof ODataEntryImpl) {
          value = readInternalODataEntry(childSchema, (ODataEntryImpl) value);
        } else if (value instanceof ODataDeltaFeed) {
          value = readInternalDeltaFeed(childSchema, (ODataDeltaFeed) value);
        }
      /* any schema type which is not simple means the value is holding a navigation entity of 0 to 1 multiplicity.
       eg.
        {
          "Category":{
            "ID":0,
            "Name":"Food"
          }
        }*/
      } else if (!childSchema.getType().isSimpleType()) {
        if (childSchema.getUnionSchema(0).getType().equals(Schema.Type.RECORD)) {
          if (value != null) {
            value = buildStructureRecord(childSchema.getUnionSchema(0), (ODataEntry) value);
          }
        }
      }

      // any thing non-null other then the above two class type means the value holds the actual data
      // e.g.
      //   "Name":"Z Suppliers"
      if (value != null) {
        processSchemaTypeValue(childSchema, recordBuilder, fieldName, value);
      }
    });

    return recordBuilder.build();
  }

  /**
   * Builds the navigation entity of 1 to * multiplicity.
   * Sample ODataDeltaFeed data:
   * {
   * "Suppliers":[{
   * "ID":0,
   * "Name":"Z Suppliers"
   * },{
   * "ID":0,
   * "Name":"PAN Suppliers"
   * }]
   * }
   *
   * @param recordSchema   plugin schema
   * @param oDataEntryList SAP SuccessFactors navigation entity data
   * @return list of {@code StructuredRecord}
   */
  private List<StructuredRecord> readInternalDeltaFeed(Schema recordSchema, ODataDeltaFeed oDataEntryList) {
    List<ODataEntry> entryList = oDataEntryList.getEntries();

    return entryList.stream().map(oDataEntry -> buildStructureRecord(recordSchema.getComponentSchema(), oDataEntry))
      .collect(Collectors.toList());
  }

  /**
   * Builds the navigation entity of 1 to * multiplicity.
   * Sample ODataDeltaFeed data:
   * {
   * "Suppliers":[{
   * "ID":0,
   * "Name":"Z Suppliers"
   * },{
   * "ID":0,
   * "Name":"PAN Suppliers"
   * }]
   * }
   *
   * @param recordSchema   plugin schema
   * @param oDataEntryImpl SAP SuccessFactors navigation entity data
   * @return list of {@code StructuredRecord}
   */
  private List<StructuredRecord> readInternalODataEntry(Schema recordSchema, ODataEntryImpl oDataEntryImpl) {
    return Arrays.asList(buildStructureRecord(recordSchema.getComponentSchema(), oDataEntryImpl));
  }

  /**
   * Process the value for field which is mapped to a {@code Schema.LogicalType}
   * and set into the {@code StructuredRecord.Builder}.
   *
   * @param fieldSchema   non nullable field schema
   * @param recordBuilder structured record builder
   * @param fieldName     field name
   * @param fieldValue    field value
   */
  private void processSchemaTypeValue(Schema fieldSchema, StructuredRecord.Builder recordBuilder,
                                      String fieldName, Object fieldValue) {

    fieldSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;
    Schema.LogicalType logicalType = fieldSchema.getLogicalType();
    if (logicalType == null) {
      recordBuilder.set(fieldName, fieldValue);

    } else if (logicalType == Schema.LogicalType.DECIMAL) {
      recordBuilder
        .setDecimal(fieldName, new BigDecimal(String.valueOf(fieldValue)).setScale(fieldSchema.getScale(),
                                                                                   BigDecimal.ROUND_HALF_UP));

    } else if (logicalType == Schema.LogicalType.DATETIME) {
      LocalDateTime localDateTime = ((GregorianCalendar) fieldValue).toZonedDateTime().toLocalDateTime();
      recordBuilder.setDateTime(fieldName, localDateTime);

    } else if (logicalType == Schema.LogicalType.TIME_MICROS) {
      LocalTime localTime = ((GregorianCalendar) fieldValue).toZonedDateTime().toLocalTime();
      recordBuilder.setTime(fieldName, localTime);

    } else if (logicalType == Schema.LogicalType.TIMESTAMP_MICROS) {
      ZonedDateTime zonedDateTime = ((GregorianCalendar) fieldValue).toZonedDateTime();
      recordBuilder.setTimestamp(fieldName, zonedDateTime);
    }
  }
}

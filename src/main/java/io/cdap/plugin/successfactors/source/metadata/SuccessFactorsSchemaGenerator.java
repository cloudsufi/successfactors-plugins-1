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

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.successfactors.common.exception.SuccessFactorsServiceException;
import io.cdap.plugin.successfactors.common.util.ResourceConstants;
import io.cdap.plugin.successfactors.common.util.SapAttributes;
import io.cdap.plugin.successfactors.common.util.SuccessFactorsDataTypes;
import io.cdap.plugin.successfactors.common.util.SuccessFactorsUtil;
import org.apache.olingo.odata2.api.edm.EdmAnnotationAttribute;
import org.apache.olingo.odata2.api.edm.EdmComplexType;
import org.apache.olingo.odata2.api.edm.EdmEntityType;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.edm.EdmFacets;
import org.apache.olingo.odata2.api.edm.EdmProperty;
import org.apache.olingo.odata2.api.edm.EdmTypeKind;
import org.apache.olingo.odata2.api.edm.EdmTyped;
import org.apache.olingo.odata2.core.edm.provider.EdmNavigationPropertyImplProv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * This {@code SuccessFactorsSchemaGenerator} contains all the logic to generate the different set of schemas.
 * e.g.
 * - schema with default (non-navigation) properties
 * - schema with default and given expanded navigation properties
 * - schema with given selective properties
 * <p>
 *   
 * Note:
 *  - Default Property: A statically declared Property on an Entity. The value of a default property is a primitive or
 *                      complex type.
 *  - Navigation Property: A property of an Entry that represents a Link from the Entry to one or more related Entries.
 */
public class SuccessFactorsSchemaGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(SuccessFactorsSchemaGenerator.class);

  // Mapping of Successfactors type as key and its corresponding Schema type as value
  private static final Map<String, Schema> SCHEMA_TYPE_MAPPING;
  private static final Integer DEFAULT_PRECISION = 15;
  private static final Integer DEFAULT_SCALE = 2;
  private static final String DEFAULT_PROPERTY = "Default property";
  private static final String NAV_PROPERTY_SEPARATOR = "/";
  private static final String PROPERTY_SEPARATOR = ",";

  static {
    Map<String, Schema> dataTypeMap = new HashMap<>();
    dataTypeMap.put(SuccessFactorsDataTypes.SBYTE, Schema.of(Schema.Type.INT));
    dataTypeMap.put(SuccessFactorsDataTypes.BYTE, Schema.of(Schema.Type.BYTES));
    dataTypeMap.put(SuccessFactorsDataTypes.INT16, Schema.of(Schema.Type.INT));
    dataTypeMap.put(SuccessFactorsDataTypes.INT32, Schema.of(Schema.Type.INT));
    dataTypeMap.put(SuccessFactorsDataTypes.INT64, Schema.of(Schema.Type.LONG));
    dataTypeMap.put(SuccessFactorsDataTypes.DOUBLE, Schema.of(Schema.Type.DOUBLE));
    dataTypeMap.put(SuccessFactorsDataTypes.FLOAT, Schema.of(Schema.Type.FLOAT));
    // These are default values for precision and scale - these values are used if precision, scale are not provided.
    dataTypeMap.put(SuccessFactorsDataTypes.DECIMAL, Schema.decimalOf(DEFAULT_PRECISION, DEFAULT_SCALE));

    dataTypeMap.put(SuccessFactorsDataTypes.STRING, Schema.of(Schema.Type.STRING));

    dataTypeMap.put(SuccessFactorsDataTypes.BINARY, Schema.of(Schema.Type.BYTES));
    
    dataTypeMap.put(SuccessFactorsDataTypes.BOOLEAN, Schema.of(Schema.Type.BOOLEAN));

    dataTypeMap.put(SuccessFactorsDataTypes.DATETIME, Schema.of(Schema.LogicalType.DATETIME));
    dataTypeMap.put(SuccessFactorsDataTypes.TIME, Schema.of(Schema.LogicalType.TIME_MICROS));
    dataTypeMap.put(SuccessFactorsDataTypes.DATETIMEOFFSET, Schema.of(Schema.LogicalType.TIMESTAMP_MICROS));

    SCHEMA_TYPE_MAPPING = Collections.unmodifiableMap(dataTypeMap);
  }
  
  private final SuccessFactorsEntityProvider successFactorsServiceHelper;

  public SuccessFactorsSchemaGenerator(SuccessFactorsEntityProvider successFactorsServiceHelper) {
    this.successFactorsServiceHelper = successFactorsServiceHelper;
  }

  /**
   * Build schema with all the default (non-navigation) properties for the given entity name.
   *
   * @param entityName   service entity name
   * @return {@code Schema}
   * @throws SuccessFactorsServiceException throws in following two cases
   *        1. if no default property were found in the given entity name,
   *        2. if fails at apache olingo processing.
   */
  public Schema buildDefaultOutputSchema(String entityName) throws SuccessFactorsServiceException {

    try {
      List<SuccessFactorsColumnMetadata> columnDetailList = buildDefaultColumns(entityName);
      if (columnDetailList.isEmpty()) {
        throw new SuccessFactorsServiceException(
          ResourceConstants.ERR_NO_COLUMN_FOUND.getMsgForKey(DEFAULT_PROPERTY, entityName));
      }

      freezeSuccessFactorsColumnMetadata(columnDetailList);

      return buildSchema(columnDetailList);
    } catch (EdmException ee) {
      throw new SuccessFactorsServiceException(
        ResourceConstants.ERR_BUILDING_COLUMNS.getMsgForKey(DEFAULT_PROPERTY, entityName), ee);
    }
  }

  /**
   * Builds all the non-navigational property for the given entity name.
   *
   * @param entityName service entity name
   * @return list of {@code SuccessFactorsColumnMetadata} or empty list in case of invalid entity name.
   * @throws EdmException any apache olingo processing exception
   */
  public List<SuccessFactorsColumnMetadata> buildDefaultColumns(String entityName) throws EdmException {

    EdmEntityType entityType = successFactorsServiceHelper.getEntityType(entityName);
    List<String> propList = successFactorsServiceHelper.getEntityPropertyList(entityType);

    if (propList != null) {
      return buildSuccessFactorsColumns(entityType, propList);
    }

    LOG.debug(ResourceConstants.DEBUG_ENTITY_NOT_FOUND.getMsgForKey(entityName));
    return Collections.emptyList();
  }

  /**
   * Prepares list of {@code SuccessFactorsColumnMetadata} for both navigation as well as non-navigation property from
   * the provided 'propList'.
   *
   * @param entityType service entity type
   * @param propList   list of required property name for the given entity type.
   * @return  list of {@code SuccessFactorsColumnMetadata} or empty list in case of invalid entity name.
   * @throws EdmException any apache olingo processing exception
   */
  private List<SuccessFactorsColumnMetadata> buildSuccessFactorsColumns(EdmEntityType entityType, List<String> propList)
    throws EdmException {

    List<SuccessFactorsColumnMetadata> successFactorsColumnDetailList = new ArrayList<>();
    for (String prop : propList) {
      String namespace = entityType.getNamespace();
      EdmTyped type = entityType.getProperty(prop);

      // There are many implementation of the 'EdmTyped' such as EdmProperty, EdmNavigationPropertyImplProv,
      // EdmComplexType, EdmEntityType, EdmParameterImplProv, EdmElementImplProv and so on however to
      // generate the plugin nested structure schema only three types are required EdmProperty,
      // EdmNavigationPropertyImplProv & EdmComplexType (used in other 'buildComplexTypes' method)

      // check for non-navigation property
      if (type instanceof EdmProperty) {
        EdmProperty edmProperty = (EdmProperty) type;
        successFactorsColumnDetailList.add(buildSuccessFactorsColumnMetadata(namespace, edmProperty));
        
      } else if (type instanceof EdmNavigationPropertyImplProv) {     // check for navigation property

        EdmNavigationPropertyImplProv navProperty = (EdmNavigationPropertyImplProv) type;
        EdmEntityType navEntityType = successFactorsServiceHelper.extractEntitySetFromNavigationProperty(navProperty);
        if (navEntityType != null) {

          List<SuccessFactorsColumnMetadata> navChild = buildSuccessFactorsColumns(navEntityType,
                                                                             navEntityType.getPropertyNames());

          SuccessFactorsColumnMetadata navigationColumn = SuccessFactorsColumnMetadata.builder()
            .name(prop)
            .type(navProperty.getType().getName())
            .multiplicityOrdinal(navProperty.getMultiplicity().ordinal())
            .childList(navChild)
            .build();

          successFactorsColumnDetailList.add(navigationColumn);
        }
      }
    }

    return successFactorsColumnDetailList;
  }

  /**
   * @param entityName SAP SuccessFactors entity Name to fetch the non-navigational properties
   * @return list of non-navigational properties
   * @throws EdmException any apache olingo processing exception
   */
  public List<String> getNonNavigationalProperties(String entityName) throws EdmException {
    EdmEntityType entityType = successFactorsServiceHelper.getEntityType(entityName);
    List<String> propList = successFactorsServiceHelper.getEntityPropertyList(entityType);
    List<String> successFactorsColumnDetailList = new ArrayList<>();
    for (String prop : propList) {
      EdmTyped type = entityType.getProperty(prop);
      if (type instanceof EdmProperty) {
        EdmProperty edmProperty = (EdmProperty) type;
        List<EdmAnnotationAttribute> edmAnnotationAttributes = edmProperty.getAnnotations().getAnnotationAttributes();
        for (EdmAnnotationAttribute edmAnnotationAttribute : edmAnnotationAttributes) {
          if (edmAnnotationAttribute.getName().equals("visible") && edmAnnotationAttribute.getText().equals("true")) {
            successFactorsColumnDetailList.add(prop);
          }
        }
      }
    }
    return successFactorsColumnDetailList;
  }

  /**
   * Builds the {@code SuccessFactorsColumnMetadata} from the given {@code EdmProperty}.
   * Also builds the COMPLEX property.
   *
   * @param namespace   SAP SuccessFactors entity namespace. Used to build the COMPLEX properties.
   * @param edmProperty {@code EdmProperty} of the SuccessFactors service entity.
   * @return {@code SuccessFactorsColumnMetadata}
   * @throws EdmException any apache olingo processing exception
   */
  private SuccessFactorsColumnMetadata buildSuccessFactorsColumnMetadata(String namespace, EdmProperty edmProperty)
    throws EdmException {

    SuccessFactorsColumnMetadata.Builder successFactorsColumnDetailBuilder = SuccessFactorsColumnMetadata.builder()
      .name(edmProperty.getName())
      .kindName(edmProperty.getType().getKind().name())
      .type(edmProperty.getType().getName())
      .multiplicityOrdinal(edmProperty.getMultiplicity().ordinal());

    if (edmProperty.getFacets() != null) {
      EdmFacets facets = edmProperty.getFacets();
      successFactorsColumnDetailBuilder
        .collation(facets.getCollation())
        .defaultValue(facets.getDefaultValue())
        .maxLength(facets.getMaxLength())
        .precision(facets.getPrecision())
        .scale(facets.getScale())
        .isFixedLength(facets.isFixedLength())
        .isUnicode(facets.isUnicode())
        .isNullable(facets.isNullable());
    }

    //setting SAP specific details.
    List<EdmAnnotationAttribute> edmAnnotationAttributes = edmProperty.getAnnotations().getAnnotationAttributes();
    if (edmAnnotationAttributes != null) {
      edmAnnotationAttributes.forEach(sapAttribute -> {
        String sapAttributeText = sapAttribute.getText();
        switch (sapAttribute.getName()) {
          case SapAttributes.DISPLAY_FORMAT:
            successFactorsColumnDetailBuilder.displayFormat(sapAttributeText);
            break;
          case SapAttributes.VISIBLE:
            successFactorsColumnDetailBuilder.isVisible(Boolean.parseBoolean(sapAttributeText));
            break;
          case SapAttributes.FILTER_RESTRICTION:
            successFactorsColumnDetailBuilder.filterRestrictions(sapAttributeText);
            break;
          case SapAttributes.REQUIRED_IN_FILTER:
            successFactorsColumnDetailBuilder.requiredInFilter(Boolean.parseBoolean(sapAttributeText));
            break;
          case SapAttributes.LABEL:
            successFactorsColumnDetailBuilder.label(sapAttributeText);
            break;

        }
      });
    }

    if (!edmProperty.isSimple()) {
      List<SuccessFactorsColumnMetadata> complexChild = buildComplexTypes(namespace, edmProperty);
      if (!complexChild.isEmpty()) {
        successFactorsColumnDetailBuilder.childList(complexChild);
      }
    }

    return successFactorsColumnDetailBuilder.build();
  }


  /**
   * Build schema with the all the default (non-navigation) properties for given entity name along with
   * all the navigation property provided in the 'expandOption'.
   *
   * @param entityName   service entity name
   * @param expandOption all the selective expanded property names
   * @return {@code Schema}
   * @throws SuccessFactorsServiceException throws in following two cases
   *        1. if neither default nor expanded property were found in the given entity name,
   *        2. if fails at apache olingo processing.
   */
  public Schema buildExpandOutputSchema(String entityName, String expandOption) throws SuccessFactorsServiceException {
    try {
      List<SuccessFactorsColumnMetadata> columnDetailList = buildDefaultColumns(entityName);
      if (columnDetailList.isEmpty()) {
        throw new SuccessFactorsServiceException(ResourceConstants.ERR_NO_COLUMN_FOUND.getMsgForKey(DEFAULT_PROPERTY,
          entityName));
      }

      List<SuccessFactorsColumnMetadata> expandColumnDetailList = buildExpandedEntity(entityName, expandOption);
      if (expandColumnDetailList.isEmpty()) {
        throw new SuccessFactorsServiceException(ResourceConstants.ERR_NO_COLUMN_FOUND.getMsgForKey(expandOption,

          entityName));
      }

      columnDetailList.addAll(expandColumnDetailList);

      freezeSuccessFactorsColumnMetadata(columnDetailList);

      return buildSchema(columnDetailList);
    } catch (EdmException ee) {
      throw new SuccessFactorsServiceException(
        ResourceConstants.ERR_BUILDING_COLUMNS.getMsgForKey(expandOption, entityName), ee);
    }
  }

  /**
   * Finds and builds all the children under the given expanded navigation path.
   * Example:
   * Let say the metadata is as follows:
   *  Root
   *    - C1
   *    - C2
   *    - N1
   *      - N1C1
   *      - N1C2
   *        - NN1C1
   *        - NN1Root -- navigation attribute to Root
   *    - N2
   *      - N2C1
   *      - N2C2
   *
   * Let say user provided following expanded path as an input: N1, N2, N1/N1C2, N1/N1C2/Root
   * So in this case the final output should be
   *
   *  - N1
   *    - N1C1
   *    - N1C2
   *      - NN1C1
   *      - Root
   *        - C1
   *        - C2
   *  - N2
   *    - N2C1
   *    - N2C2
   *
   * Iteration wise output
   * ---------------------
   * Iteration: 1
   * input: N1
   * output:
   *
   *  - N1
   *    - N1C1
   *
   * Iteration: 2
   * input: N2
   * output:
   *
   *  - N1
   *    - N1C1
   *  - N2
   *    - N2C1
   *    - N2C2
   *
   * Iteration: 3
   * input: N1/N1C2
   * output:
   *
   *  - N1
   *    - N1C1
   *    - N1C2
   *      - NN1C1
   *  - N2
   *    - N2C1
   *    - N2C2
   *
   * Iteration: 4
   * input: N1/N1C2/Root
   * output:
   *
   *  - N1
   *    - N1C1
   *    - N1C2
   *      - NN1C1
   *      - Root
   *        - C1
   *        - C2
   *  - N2
   *    - N2C1
   *    - N2C2
   *
   * will return the list of all the navigation attributes containing it's relevant child i.e. [N1, N2].
   *
   * @param entityName  service entity name
   * @param expandEntry all the selective expanded property names
   * @return list of {@code SuccessFactorsColumnMetadata} or empty list in case of invalid expanded property name.
   * @throws EdmException any apache olingo processing exception.
   */
  private List<SuccessFactorsColumnMetadata> buildExpandedEntity(String entityName, String expandEntry)
    throws EdmException {

    // create a root metadata container which will hold all provided nested navigation property details as child
    SuccessFactorsColumnMetadata root = SuccessFactorsColumnMetadata.builder().build();

    // breaks the comma separated expanded path and forms a array
    // e.g. "supplier,supplier/products/category" --> ["supplier","supplier/products/category"]
    String[] expandedPathList = expandEntry.split(PROPERTY_SEPARATOR);

    // traverse each expanded navigation path
    for (String expandPath : expandedPathList) {
      SuccessFactorsColumnMetadata parent = root;
      SuccessFactorsColumnMetadata current;

      // used to store the navigation property sequence path in progressive way
      List<String> childSequence = new ArrayList<>();

      // breaks the '/' separated expanded navigation path and form an array for build the navigation sequence
      // e.g. "supplier/products/category" --> ["supplier","products","category"]
      String[] expandSequenceList = expandPath.split(NAV_PROPERTY_SEPARATOR);

      // traverse each navigation property in sequence
      for (String expandSequence : expandSequenceList) {

        // adding each navigation property in the list to prepare the sequence path
        childSequence.add(expandSequence);

        // childSequence is joined back to form the interim navigation path, which will be used to build the
        // navigation property by "buildNavigationColumns" method
        String childPath = String.join(NAV_PROPERTY_SEPARATOR, childSequence);

        // checking if the parent contains the child with name (expandSequence)
        current = parent.getChildList()
          .stream()
          .filter(s -> SuccessFactorsUtil.isNotNullOrEmpty(s.getName()) && s.getName().equals(expandSequence))
          .findFirst()
          .orElse(null);

        // if 'current' is not null then parent contains the required navigation property so no need to create that
        // property just assign the 'current' to 'parent' to check the existence of the next navigation property
        // present in the sequence
        // otherwise create the property and assign the 'new property' to 'parent' to check the existence of the
        // next navigation property present in the sequence
        // Note: as per the example above, for the 1st iteration in 'expandedPathList' it will be null as the 'supplier'
        // is not yet created but in the
        // 2nd iteration in 'expandPathList' as 'supplier' already exists so no need to create it again and it will
        // be used to create the next navigation property in the path i.e. "products" and so on.
        if (current != null) {
          parent = current;
        } else {
          SuccessFactorsColumnMetadata child = buildNavigationColumns(entityName, childPath);
          // appending child
          if (child != null) {
            parent.appendChild(child);
            parent = child;
          }
        }
      }
    }
    return root.getChildList();
  }

  /**
   * Builds the {@code SuccessFactorsColumnMetadata} with all the default properties of the last expanded property
   * present under the given navigation path.
   *
   * @param entityName service entity name
   * @param navPath    can have navigation path or navigation property name
   * @return {@code SuccessFactorsColumnMetadata} or null in case of invalid expanded property path.
   * @throws EdmException any apache olingo processing exception.
   */
  @Nullable
  private SuccessFactorsColumnMetadata buildNavigationColumns(String entityName, String navPath) throws EdmException {
    EdmNavigationPropertyImplProv association = successFactorsServiceHelper.getNavigationProperty(entityName, navPath);
    if (association == null) {
      LOG.debug(ResourceConstants.DEBUG_NAVIGATION_NOT_FOUND.getMsgForKey(navPath, entityName));
      return null;
    }

    EdmEntityType entitySet = successFactorsServiceHelper.extractEntitySetFromNavigationProperty(association);
    if (entitySet == null) {
      LOG.debug(ResourceConstants.DEBUG_NAV_PROP_NOT_FOUND.getMsgForKey(association.getName(), navPath));
      return null;
    }

    List<String> propList = entitySet.getPropertyNames();
    List<SuccessFactorsColumnMetadata> columns = buildSuccessFactorsColumns(entitySet, propList);

    return SuccessFactorsColumnMetadata.builder()
      .name(association.getName())
      .type(entitySet.getKind().name())
      .multiplicityOrdinal(association.getMultiplicity().ordinal())
      .childList(columns)
      .build();
  }


  /**
   * Build schema for all the given selective property under the 'selectOption'.
   *
   * @param entityName   service entity name
   * @param selectOption all the selective property names
   * @return {@code Schema}
   * @throws SuccessFactorsServiceException throws in following two cases
   *        1. if no selective property were found in the given entity name,
   *        2. if fails at apache olingo processing.
   */
  public Schema buildSelectOutputSchema(String entityName, String selectOption) throws SuccessFactorsServiceException {
    try {
      List<SuccessFactorsColumnMetadata> columnDetailList = buildSelectedColumns(entityName, selectOption);
      if (columnDetailList.isEmpty()) {
        throw new SuccessFactorsServiceException(ResourceConstants.ERR_NO_COLUMN_FOUND.getMsgForKey(selectOption,
                                                                                                    entityName));
      }

      freezeSuccessFactorsColumnMetadata(columnDetailList);

      return buildSchema(columnDetailList);
    } catch (EdmException ee) {
      throw new SuccessFactorsServiceException(
        ResourceConstants.ERR_BUILDING_COLUMNS.getMsgForKey(selectOption, entityName), ee);
    }
  }

  /**
   * Finds and builds all the selective (navigation & non-navigation) property provided under 'selectEntity'
   *
   * @param entityName  service entity name
   * @param selectEntry selective property name
   * @return list of {@code SuccessFactorsColumnMetadata} or empty list in case of invalid selective property name.
   * @throws EdmException any apache olingo processing exception.
   */
  private List<SuccessFactorsColumnMetadata> buildSelectedColumns(String entityName, String selectEntry)
    throws EdmException {

    //collect non-navigation properties from the $select option
    List<String> selectList = Arrays.stream(selectEntry.split(PROPERTY_SEPARATOR))
      .filter(s -> !s.contains(NAV_PROPERTY_SEPARATOR))
      .collect(Collectors.toList());

    EdmEntityType entityType = successFactorsServiceHelper.getEntityType(entityName);
    if (entityType == null) {
      return Collections.emptyList();
    }
    List<SuccessFactorsColumnMetadata> columnList = buildSuccessFactorsColumns(entityType, selectList);

    //collect navigation properties from the $select option
    List<String> expandSelectList = Arrays.stream(selectEntry.split(PROPERTY_SEPARATOR))
      .filter(s -> s.contains(NAV_PROPERTY_SEPARATOR))
      .collect(Collectors.toList());

    if (!expandSelectList.isEmpty()) {
      SuccessFactorsColumnMetadata expandColumns = buildSelectedNavigationColumns(entityName, expandSelectList);
      if (expandColumns.containsChild()) {
        columnList.addAll(expandColumns.getChildList());
      }
    }
    return columnList;
  }

  /**
   * Builds {@code SuccessFactorsColumnMetadata} for all the selected navigation path
   *
   * @param entityName     service entity name
   * @param expandPathList list of all the navigation path
   * @return {@code SuccessFactorsColumnMetadata}
   * @throws EdmException any apache olingo processing exception.
   */
  private SuccessFactorsColumnMetadata buildSelectedNavigationColumns(String entityName, List<String> expandPathList)
    throws EdmException {

    SuccessFactorsColumnMetadata root = SuccessFactorsColumnMetadata.builder().build();

    for (String expandPath : expandPathList) {
      SuccessFactorsColumnMetadata parent = root;
      SuccessFactorsColumnMetadata current;

      //used to store the expanded entity sequence path
      List<String> childSequence = new ArrayList<>();

      String[] expandSequenceList = expandPath.split(NAV_PROPERTY_SEPARATOR);
      for (String expandSequence : expandSequenceList) {

        childSequence.add(expandSequence);
        String childPath = String.join(NAV_PROPERTY_SEPARATOR, childSequence);

        //checking if the parent contains the child with name(expandSequence)
        current = parent.getChildList()
          .stream()
          .filter(s -> SuccessFactorsUtil.isNotNullOrEmpty(s.getName()) && s.getName().equals(expandSequence))
          .findFirst()
          .orElse(null);

        if (current != null) {
          parent = current;
        } else {
          SuccessFactorsColumnMetadata child;
          if (childPath.equals(expandPath)) {
            childSequence.remove(childSequence.size() - 1);
            String parentPath = String.join(NAV_PROPERTY_SEPARATOR, childSequence);

            EdmEntityType entityType = successFactorsServiceHelper.getNavigationPropertyEntityType(entityName,
                                                                                                   parentPath);
            if (entityType == null) {
              LOG.debug(ResourceConstants.DEBUG_NAV_PROP_NOT_FOUND.getMsgForKey(entityName, parentPath));
              return null;
            }

            // the last child could be a property or can be a navigation property so in both the case there will be only
            // one item in the returned list, so fetching the first index value from the list.
            child = buildSuccessFactorsColumns(entityType, ImmutableList.of(expandSequence)).get(0);
          } else {
            //building the parents as per the given path(childPath)
            child = buildNavigationColumn(entityName, childPath);
          }

          //appending child
          if (child != null) {
            parent.appendChild(child);
            parent = child;
          }
        }
      }
    }
    return root;
  }

  /**
   * Build {@code SuccessFactorsColumnMetadata} for the provided navigation property.
   *
   * @param entityName service entity name
   * @param navName can have navigation path
   * @return {@code SuccessFactorsColumnMetadata} or null in case of invalid navigation property name
   * @throws EdmException any apache olingo processing exception.
   */
  @Nullable
  private SuccessFactorsColumnMetadata buildNavigationColumn(String entityName, String navName) throws EdmException {
    EdmNavigationPropertyImplProv association = successFactorsServiceHelper.getNavigationProperty(entityName, navName);
    if (association == null) {
      LOG.debug(ResourceConstants.DEBUG_NAVIGATION_NOT_FOUND.getMsgForKey(navName, entityName));
      return null;
    }

    EdmEntityType entitySet = successFactorsServiceHelper.extractEntitySetFromNavigationProperty(association);
    if (entitySet == null) {
      LOG.debug(ResourceConstants.DEBUG_NAV_PROP_NOT_FOUND.getMsgForKey(association.getName(), navName));
      return null;
    }

    return SuccessFactorsColumnMetadata.builder()
      .name(association.getName())
      .type(entitySet.getKind().name())
      .multiplicityOrdinal(association.getMultiplicity().ordinal())
      .build();
  }

  /**
   * Builds the COMPLEX properties into list of {@code SuccessFactorsColumnMetadata}.
   *
   * @param namespace   SuccessFactors service entity namespace.
   * @param edmProperty SuccessFactors service entity complex {@code EdmProperty}
   * @return list of {@code SuccessFactorsColumnMetadata}
   * @throws EdmException any apache olingo processing exception.
   */
  private List<SuccessFactorsColumnMetadata> buildComplexTypes(String namespace, EdmProperty edmProperty)
    throws EdmException {

    EdmComplexType complexType = successFactorsServiceHelper.getComplexType(namespace, edmProperty.getName());
    if (complexType == null) {
      return Collections.emptyList();
    }

    List<SuccessFactorsColumnMetadata> columns = new ArrayList<>();

    List<String> propList = complexType.getPropertyNames();
    for (String prop : propList) {
      EdmProperty property = ((EdmProperty) complexType.getProperty(prop));
      columns.add(buildSuccessFactorsColumnMetadata(namespace, property));
    }

    return columns;
  }

  /**
   * Builds schema from the given list of {@code SuccessFactorsColumnMetadata}
   *
   * @param columnDetailList {@code SuccessFactorsColumnMetadata}
   * @return {@code Schema}
   */
  private Schema buildSchema(List<SuccessFactorsColumnMetadata> columnDetailList) {
    List<Schema.Field> outputSchema = columnDetailList.stream()
      .map(this::buildSchemaField)
      .collect(Collectors.toList());

    return Schema.recordOf("SuccessFactorsColumnMetadata", outputSchema);
  }

  /**
   * Builds Schema field from {@code SuccessFactorsColumnMetadata}
   *
   * @param successFactorsColumnDetail {@code SuccessFactorsColumnMetadata}
   * @return {@code Schema.Field}
   */
  private Schema.Field buildSchemaField(SuccessFactorsColumnMetadata successFactorsColumnDetail) {
    if (successFactorsColumnDetail.containsChild()) {
      List<Schema.Field> outputSchema = successFactorsColumnDetail.getChildList()
        .stream()
        .map(this::buildSchemaField)
        .collect(Collectors.toList());

      String typeName = buildNestedTypeUniqueName(successFactorsColumnDetail.getName());

      if (successFactorsColumnDetail.getType().equals(EdmTypeKind.ENTITY.name()) &&
        (successFactorsColumnDetail.getMultiplicityOrdinal() != null &&
          successFactorsColumnDetail.getMultiplicityOrdinal() > 0)) {
        // adding 1 to * multiplicity record to ARRAY type
        return Schema.Field.of(successFactorsColumnDetail.getName(),
                               Schema.arrayOf(Schema.recordOf(typeName, outputSchema)));
      }

      // adding 0 to 1 multiplicity record to NULLABLE
      return Schema.Field.of(successFactorsColumnDetail.getName(),
                             Schema.nullableOf(Schema.recordOf(typeName, outputSchema)));
    }

    return Schema.Field.of(successFactorsColumnDetail.getName(), buildRequiredSchemaType(successFactorsColumnDetail));
  }

  /**
   * Build and returns the appropriate schema type.
   *
   * @param successFactorsColumnDetail {@code SuccessFactorsColumnMetadata}
   * @return {@code Schema}
   */
  private Schema buildRequiredSchemaType(SuccessFactorsColumnMetadata successFactorsColumnDetail) {
    Schema schemaType = SCHEMA_TYPE_MAPPING.get(successFactorsColumnDetail.getType());
    
    if (!SCHEMA_TYPE_MAPPING.containsKey(successFactorsColumnDetail.getType())) {
      schemaType = Schema.of(Schema.Type.STRING);
    }

    if (schemaType != null && schemaType.getLogicalType() == Schema.LogicalType.DECIMAL
      && successFactorsColumnDetail.getPrecision() != null
      && successFactorsColumnDetail.getScale() != null) {

      schemaType = Schema.decimalOf(successFactorsColumnDetail.getPrecision(), successFactorsColumnDetail.getScale());
    }

    // this check ensure that any DATE or TIME related fields are always set to NULLABLE Schema types.
    // Reason: in SuccessFactors catalog service any DATE or TIME field which is mandatory can hold '00000000' in case
    // of null and SuccessFactors service returns 'null' on data extraction for such fields so, to accordance this
    // behaviour inside the plugin any DATE or TIME related Schema type are hardcoded to NULLABLE type.
    if (schemaType != null && schemaType.getLogicalType() == Schema.LogicalType.TIMESTAMP_MICROS ||
      schemaType.getLogicalType() == Schema.LogicalType.TIME_MICROS) {

      return Schema.nullableOf(schemaType);
    }

    return successFactorsColumnDetail.isNullable() ? Schema.nullableOf(schemaType) : schemaType;
  }

  /**
   * Prepares a unique name for the provide name. This is required in case to avoid the same type name referencing
   * issue at the runtime. Name format: <actualname>_<random name>
   * e.g. Supplier_5810e28b_c38d_41fe_8dc1_e24150c515d9
   *
   * @param actualName nested property name
   * @return unique name
   */
  private String buildNestedTypeUniqueName(String actualName) {

    // schema name with '-', fails at runtime because '-' is not supported. Only characters and '_' are supported
    String randomName = UUID.randomUUID().toString().replace("-", "_");
    return actualName.concat("_").concat(randomName);
  }

  /**
   * freezes all the {@code SuccessFactorsColumnMetadata} to stop accepting more childrens.
   *
   * @param successFactorsColumnMetadataList provides the list of Metadata
   */
  private void freezeSuccessFactorsColumnMetadata(List<SuccessFactorsColumnMetadata> successFactorsColumnMetadataList) {
    successFactorsColumnMetadataList.forEach(SuccessFactorsColumnMetadata::finalizeChildren);
  }
}

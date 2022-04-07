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

import io.cdap.plugin.successfactors.common.util.SuccessFactorsUtil;
import org.apache.olingo.odata2.api.edm.Edm;
import org.apache.olingo.odata2.api.edm.EdmComplexType;
import org.apache.olingo.odata2.api.edm.EdmEntitySet;
import org.apache.olingo.odata2.api.edm.EdmEntityType;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.core.edm.provider.EdmNavigationPropertyImplProv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import javax.annotation.Nullable;

/**
 * This {@code SuccessFactorsEntityProvider} contains reusable SAP Successfactors service metadata functions.
 * This holds the SAP SuccessFactors entity metadata details and basis that it returns the required details such as
 *    - get Entity instance for the given entity name
 *    - get property list for the given entity name
 *    - get navigation property and complex property for the given entity name
 */
public class SuccessFactorsEntityProvider {
  private static final Logger LOG = LoggerFactory.getLogger(SuccessFactorsEntityProvider.class);

  private final Edm edmMetadata;

  public SuccessFactorsEntityProvider(Edm edmMetadata) {
    this.edmMetadata = edmMetadata;
  }

  /**
   * Find and return the EdmEntitySet instance from the metadata.
   *
   * @param entityName SuccessFactors entity name
   * @return instance of EdmEntitySet class
   * @throws EdmException Expected exception
   */
  @Nullable
  public EdmEntitySet getEntitySet(String entityName) throws EdmException {
    if (SuccessFactorsUtil.isNotNullOrEmpty(entityName)) {
      for (EdmEntitySet edmEntitySet : edmMetadata.getEntitySets()) {
        if (edmEntitySet.getName().equals(entityName)) {
          return edmEntitySet;
        }
      }
    }

    return null;
  }

  @Nullable
  public EdmEntityType getEntityType(String entityName) throws EdmException {
    EdmEntitySet entitySet = getEntitySet(entityName);
    if (entitySet != null) {
      return entitySet.getEntityType();
    }

    LOG.debug("Could not find entity: {}", entityName);

    return null;
  }

  /**
   * Get list of all the default property name associated with the give 'entityName'
   *
   * @param entityType service entity type
   * @return list of default property name
   * @throws EdmException Expected exception
   */
  @Nullable
  public List<String> getEntityPropertyList(EdmEntityType entityType) throws EdmException {
    if (entityType != null) {
      return entityType.getPropertyNames();
    }
    return null;
  }

  /**
   * Returns the list of default entity from the metadata.
   *
   * @return list of default entity from the metadata
   * @throws EdmException Expected exception
   */
  public List<EdmEntitySet> getDefaultEntitySet() throws EdmException {
    return edmMetadata.getDefaultEntityContainer().getEntitySets();
  }

  /**
   * Find and return the last navigation property from the given navigation path.
   *
   * @param entityName service entity name
   * @param navPath    can have navigation path or navigation property name
   * @return returns the relevant value
   * @throws EdmException Expected exception
   */
  @Nullable
  public EdmNavigationPropertyImplProv getNavigationProperty(String entityName, String navPath)
    throws EdmException {

    if (SuccessFactorsUtil.isNotNullOrEmpty(entityName) && SuccessFactorsUtil.isNotNullOrEmpty(navPath)) {
      EdmEntitySet entitySet = getEntitySet(entityName);
      if (entitySet != null) {
        EdmEntityType entityType = entitySet.getEntityType();
        EdmNavigationPropertyImplProv association = null;

        String[] navNames = navPath.split("/");
        for (String name : navNames) {
          if (entityType.getNavigationPropertyNames().contains(name)) {
            EdmNavigationPropertyImplProv navProperty = (EdmNavigationPropertyImplProv) (entityType.getProperty(name));
            entityType = navProperty.getRelationship().getEnd(navProperty.getToRole()).getEntityType();
            association = navProperty;
          }
        }
        return association;
      }
    }

    String debugMsg = String.format("Entity name: '%s' and Expand path: '%s', navigation property is not found in the" +
      " given expand path. " +
      "Root cause: null / empty or invalid entity name or expand path was provided.", entityName, navPath);
    LOG.debug(debugMsg);

    return null;
  }

  /**
   * Find and return the EdmEntityType for the given navigation property.
   *
   * @param navProperty navigation property
   * @return returns the relevant value
   * @throws EdmException Expected exception
   */
  @Nullable
  public EdmEntityType extractEntitySetFromNavigationProperty(EdmNavigationPropertyImplProv navProperty)
    throws EdmException {

    if (navProperty != null) {
      String toRole = navProperty.getToRole();
      return navProperty.getRelationship().getEnd(toRole).getEntityType();
    }

    LOG.debug("Could not find the Entity type extraction from the given navigation property. " +
      "Root cause: null object passed in the parameter.");

    return null;
  }

  /**
   * Find and returns the EdmComplexType for the given parameters.
   *
   * @param namespace    of the parent entity
   * @param propertyName complex property name
   * @return returns the relevant value
   * @throws EdmException Expected exception
   */
  @Nullable
  public EdmComplexType getComplexType(String namespace, String propertyName) throws EdmException {
    if (SuccessFactorsUtil.isNotNullOrEmpty(namespace) && SuccessFactorsUtil.isNotNullOrEmpty(propertyName)) {
      return edmMetadata.getComplexType(namespace, propertyName);
    }

    String debugMsg = String.format("Namespace: '%s' and Complex property name: '%s', " +
      "no complex type property found in the given namespace. " +
      "Root cause: null / empty or invalid Namespace or Complex property name provided.", namespace, propertyName);
    LOG.debug(debugMsg);

    return null;
  }

  @Nullable
  public EdmEntityType getNavigationPropertyEntityType(String entityName, String navPath) throws EdmException {
    EdmNavigationPropertyImplProv navProp = getNavigationProperty(entityName, navPath);
    return extractEntitySetFromNavigationProperty(navProp);
  }

  public Edm getEdmMetadata() {
    return edmMetadata;
  }
}

# Copyright © 2022 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.


@SuccessFactorsSource
@Smoke
@Regression
Feature: SuccessFactors Source - Design time scenarios (macros)

  @BATCH-TS-SCFA-DSGN-MACRO-01
  Scenario:Verify user should be able to validate the plugin when Basic properties are configured with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "SAP SuccessFactors" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SAP SuccessFactors"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Click on the Macro button of Property: "baseURL" and set the value to: "baseURL"
    And Click on the Macro button of Property: "entityName" and set the value to: "entityName"
    And Enter input plugin property: "username" with value: "admin.username"
    And Enter input plugin property: "password" with value: "admin.password"
    Then Validate "SAP SuccessFactors" plugin properties

  @BATCH-TS-SCFA-DSGN-MACRO-02
  Scenario:Verify user should be able to validate the plugin when Credentials Section is configured with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "SAP SuccessFactors" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SAP SuccessFactors"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.baseurl"
    And Enter input plugin property: "entityName" with value: "BadgeTemplates"
    And Click on the Macro button of Property: "username" and set the value to: "username"
    And Click on the Macro button of Property: "password" and set the value to: "password"
    Then Validate "SAP SuccessFactors" plugin properties

  @BATCH-TS-SCFA-DSGN-MACRO-03
  Scenario:Verify user should be able to validate the plugin when Advanced properties are configured with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "SAP SuccessFactors" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SAP SuccessFactors"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.baseurl"
    And Enter input plugin property: "entityName" with value: "BadgeTemplates"
    And Enter input plugin property: "username" with value: "admin.username"
    And Enter input plugin property: "password" with value: "admin.password"
    And Click on the Macro button of Property: "filterOption" and set the value in textarea: "filterOption"
    And Click on the Macro button of Property: "selectOption" and set the value in textarea: "selectOption"
    And Click on the Macro button of Property: "expandOption" and set the value to: "expandOption"
    And Click on the Macro button of Property: "associatedEntityName" and set the value to: "associatedEntityName"
    And Click on the Macro button of Property: "paginationType" and set the value to: "paginationType"
    Then Validate "SAP SuccessFactors" plugin properties

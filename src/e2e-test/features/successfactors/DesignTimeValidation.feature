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
Feature: SuccessFactors Source - Design time validation scenarios

  @BATCH-TS-SCFA-DSGN-ERROR-01
  Scenario: Verify required fields missing validation for listed properties
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "SAP SuccessFactors" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SAP SuccessFactors"
    And Click on the Validate button
    Then Verify mandatory property error for below listed properties:
      | referenceName    |
      | baseURL          |
      | entityName       |
      | username         |
      | password         |


  @BATCH-TS-SCFA-DSGN-ERROR-02
  Scenario: Verify validation message when user provides an invalid BaseURL
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "SAP SuccessFactors" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SAP SuccessFactors"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "invalid.baseurl"
    And Enter input plugin property: "entityName" with value: "BadgeTemplates"
    And Enter input plugin property: "username" with value: "admin.username"
    And Enter input plugin property: "password" with value: "admin.password"
    Then Click on the Validate button
    And Verify that the Plugin Property: "baseURL" is displaying an in-line error message: "invalid.baseurl.message"

  @BATCH-TS-SCFA-DSGN-ERROR-03
  Scenario: Verify validation message when user provides an invalid Entity Name
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "SAP SuccessFactors" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SAP SuccessFactors"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.baseurl"
    And Enter input plugin property: "entityName" with value: "invalid.entityname"
    And Enter input plugin property: "username" with value: "admin.username"
    And Enter input plugin property: "password" with value: "admin.password"
    Then Click on the Validate button
    And Verify that the Plugin is displaying an error message: "invalid.entityname.message" on the header

  @BATCH-TS-SCFA-DSGN-ERROR-04
  Scenario: Verify validation message when user provides invalid Credential details
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "SAP SuccessFactors" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SAP SuccessFactors"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.baseurl"
    And Enter input plugin property: "entityName" with value: "BadgeTemplates"
    And Enter input plugin property: "username" with value: "invalid.username"
    And Enter input plugin property: "password" with value: "invalid.password"
    Then Click on the Validate button
    And Verify that the Plugin Property: "username" is displaying an in-line error message: "invalid.credential.message"
    And Verify that the Plugin Property: "password" is displaying an in-line error message: "invalid.credential.message"

  @BATCH-TS-SCFA-DSGN-ERROR-05
  Scenario: Verify validation message when user provides an Expand Field property for a non hierarchical Entity
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "SAP SuccessFactors" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SAP SuccessFactors"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.baseurl"
    And Enter input plugin property: "entityName" with value: "Advance"
    And Enter input plugin property: "username" with value: "admin.username"
    And Enter input plugin property: "password" with value: "admin.password"
    And Enter input plugin property: "expandOption" with value: "approver"
    And Click on the Validate button
    And Verify that the Plugin is displaying an error message: "invalid.expandfield.message" on the header

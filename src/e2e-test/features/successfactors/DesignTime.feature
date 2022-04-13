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
Feature: SuccessFactors Source - Design time scenarios

  @BATCH-TS-SCFA-DSGN-01
  Scenario Outline: Verify user should be able to get output schema for a valid Entity name
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "SAP SuccessFactors" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SAP SuccessFactors"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.baseurl"
    And Enter input plugin property: "entityName" with value: "<EntityName>"
    And Enter input plugin property: "username" with value: "admin.username"
    And Enter input plugin property: "password" with value: "admin.password"
    Then Validate output schema with expectedSchema "<ExpectedSchema>"
    Examples: 
    | EntityName             | ExpectedSchema                |
    | BadgeTemplates         | schema.badgetemplates         |
    | AdvancesAccumulation   | schema.advancesaccumulation   |
    | Advance                | schema.advance                |
    | AdvancesEligibility    | schema.advanceseligibility    |
    | EmpCompensation        | schema.empcompensation        |
    | Background_Certificates| schema.backgroundcertificates |

  @BATCH-TS-SCFA-DSGN-02
  Scenario Outline: Verify user should be able to validate the plugin with Filter options property
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "SAP SuccessFactors" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SAP SuccessFactors"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.baseurl"
    And Enter input plugin property: "entityName" with value: "Advance"
    And Enter input plugin property: "username" with value: "admin.username"
    And Enter input plugin property: "password" with value: "admin.password"
    And Enter textarea plugin property: "filterOption" with value: "<FilterOption>"
    Then Validate "SAP SuccessFactors" plugin properties
    Examples:
    | FilterOption                     |
    | filteoption.equal                |
    | filteoption.notequal             |
    | filteoption.greaterthan          |
    | filteoption.greaterthanorequal   |
    | filteoption.lessthan             |
    | filteoption.lessthanorequal      |
    | filteoption.and                  |
    | filteoption.or                   |
    | filteoption.not                  |
    | filter.add                       |
    | filter.sub                       |
    | filter.mul                       |
    | filter.div                       |
    | filter.mod                       |
    | filter.grouping                  |

  @BATCH-TS-SCFA-DSGN-03
  Scenario Outline: Verify user should be able to get output schema with Select Fields property
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "SAP SuccessFactors" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SAP SuccessFactors"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.baseurl"
    And Enter input plugin property: "entityName" with value: "<EntityName>"
    And Enter input plugin property: "username" with value: "admin.username"
    And Enter input plugin property: "password" with value: "admin.password"
    And Enter textarea plugin property: "selectOption" with value: "<SelectedFields>"
    And Click on the Validate button
    Then Validate output schema with expectedSchema "<ExpectedSchema>"
    Examples:
      | EntityName      | SelectedFields                  | ExpectedSchema        |
      | Advance         | filter.advance.selectedfields | filter.advance.schema |
      | EmpCompensation | filter.empcomp.selectedfields | filter.empcomp.schema |

  @BATCH-TS-SCFA-DSGN-04
  Scenario Outline: Verify user should be able to validate the plugin with Advanced properties
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "SAP SuccessFactors" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SAP SuccessFactors"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.baseurl"
    And Enter input plugin property: "entityName" with value: "Advance"
    And Enter input plugin property: "username" with value: "admin.username"
    And Enter input plugin property: "password" with value: "admin.password"
    And Enter textarea plugin property: "selectOption" with value: "eligibileAmount"
    And Enter input plugin property: "skipRowCount" with value: "<SkipRowCount>"
    And Enter input plugin property: "numRowsToFetch" with value: "<NumRowsToFetch>"
    And Enter input plugin property: "splitCount" with value: "<SplitCount>"
    And Enter input plugin property: "batchSize" with value: "<BatchSize>"
    Then Validate "SAP SuccessFactors" plugin properties
    Examples:
      | SkipRowCount | NumRowsToFetch | SplitCount | BatchSize   |
      | 2            | 5              | 5          | 500         |
      | 10           | 10             | 5          | 10000       |

  @BATCH-TS-SCFA-DSGN-05
  Scenario: Verify user should be able to validate the plugin with Expand Fields Property
    When Open Datafusion Project to configure pipeline
    And Select plugin: "SAP SuccessFactors" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SAP SuccessFactors"
    And Enter input plugin property: "referenceName" with value: "referenceName"
    And Enter input plugin property: "baseURL" with value: "admin.baseurl"
    And Enter input plugin property: "entityName" with value: "User"
    And Enter input plugin property: "username" with value: "admin.username"
    And Enter input plugin property: "password" with value: "admin.password"
    And Enter textarea plugin property: "selectOption" with value: "customManager,customReports"
    And Enter input plugin property: "expandOption" with value: "customManager,customReports"
    Then Verify the Output Schema matches the Expected Schema for listed Hierarchical fields:
    | customManager | filter.expand.schema |



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
Feature: SuccessFactors Source - Run time scenarios (macros)

  @BATCH-TS-SCFA-DSGN-RNTM-MACRO-01 @BQ_SINK_TEST
  Scenario: Verify user should be able to preview the pipeline when plugin is configured for Basic properties with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "SAP SuccessFactors" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SAP SuccessFactors"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Click on the Macro button of Property: "baseURL" and set the value to: "baseURL"
    And Click on the Macro button of Property: "entityName" and set the value to: "entityName"
    And Click on the Macro button of Property: "username" and set the value to: "username"
    And Click on the Macro button of Property: "password" and set the value to: "password"
    And Click on the Validate button
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    And Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "SAP-SuccessFactors" and sink as "BigQueryTable" to establish connection
    And Save the pipeline
    And Preview and run the pipeline
    And Enter runtime argument value "admin.baseurl" for key "baseURL"
    And Enter runtime argument value "valid.entityname" for key "entityName"
    And Enter runtime argument value "admin.username" for key "username"
    And Enter runtime argument value "admin.password" for key "password"
    And Run the preview of pipeline with runtime arguments
    And Verify the preview of pipeline is "successfully"

  @BATCH-TS-SCFA-RNTM-MACRO-02 @BQ_SINK_TEST
  Scenario: Verify user should be able to deploy and run the pipeline when plugin is configured for Basic properties with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "SAP SuccessFactors" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SAP SuccessFactors"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Click on the Macro button of Property: "baseURL" and set the value to: "baseURL"
    And Click on the Macro button of Property: "entityName" and set the value to: "entityName"
    And Click on the Macro button of Property: "username" and set the value to: "username"
    And Click on the Macro button of Property: "password" and set the value to: "password"
    And Validate "SAP SuccessFactors" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "SAP-SuccessFactors" and sink as "BigQueryTable" to establish connection
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value "admin.baseurl" for key "baseURL"
    And Enter runtime argument value "valid.entityname" for key "entityName"
    And Enter runtime argument value "admin.username" for key "username"
    And Enter runtime argument value "admin.password" for key "password"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait till pipeline is in running state
    And Verify the pipeline status is "Succeeded"
    Then Verify count of no of records transferred to the target BigQuery Table

  @BATCH-TS-SCFA-DSGN-RNTM-MACRO-03 @BQ_SINK_TEST
  Scenario: Verify user should be able to preview the pipeline when plugin is configured for Advanced properties with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "SAP SuccessFactors" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SAP SuccessFactors"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.baseurl"
    And Enter input plugin property: "entityName" with value: "Advance"
    And Enter input plugin property: "username" with value: "admin.username"
    And Enter input plugin property: "password" with value: "admin.password"
    And Click on the Macro button of Property: "filterOption" and set the value in textarea: "filterOption"
    And Click on the Macro button of Property: "selectOption" and set the value in textarea: "selectOption"
    And Click on the Macro button of Property: "splitCount" and set the value to: "splitCount"
    And Click on the Macro button of Property: "batchSize" and set the value to: "batchSize"
    And Validate "SAP SuccessFactors" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    And Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "SAP-SuccessFactors" and sink as "BigQueryTable" to establish connection
    And Save the pipeline
    And Preview and run the pipeline
    And Enter runtime argument value "filteoption.rntm.greaterthan" for key "filterOption"
    And Enter runtime argument value "filter.rntm.selectedfields" for key "selectOption"
    And Enter runtime argument value "2" for key "splitCount"
    And Enter runtime argument value "1000" for key "batchSize"
    And Run the preview of pipeline with runtime arguments
    And Verify the preview of pipeline is "successfully"

  @BATCH-TS-SCFA-RNTM-MACRO-04 @BQ_SINK_TEST
  Scenario: Verify user should be able to deploy and run the pipeline when plugin is configured for Advanced properties with macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "SAP SuccessFactors" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SAP SuccessFactors"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.baseurl"
    And Enter input plugin property: "entityName" with value: "Advance"
    And Enter input plugin property: "username" with value: "admin.username"
    And Enter input plugin property: "password" with value: "admin.password"
    And Click on the Macro button of Property: "filterOption" and set the value in textarea: "filterOption"
    And Click on the Macro button of Property: "selectOption" and set the value in textarea: "selectOption"
    And Click on the Macro button of Property: "splitCount" and set the value to: "splitCount"
    And Click on the Macro button of Property: "batchSize" and set the value to: "batchSize"
    And Validate "SAP SuccessFactors" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "SAP-SuccessFactors" and sink as "BigQueryTable" to establish connection
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value "filteoption.rntm.greaterthan" for key "filterOption"
    And Enter runtime argument value "filter.rntm.selectedfields" for key "selectOption"
    And Enter runtime argument value "2" for key "splitCount"
    And Enter runtime argument value "1000" for key "batchSize"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait till pipeline is in running state
    And Verify the pipeline status is "Succeeded"
    Then Verify count of no of records transferred to the target BigQuery Table

  @BATCH-TS-SCFA-RNTM-MACRO-05 @BQ_SINK_TEST
  Scenario: Verify pipeline failure message in logs when user provides invalid Entity Name with Macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "SAP SuccessFactors" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SAP SuccessFactors"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.baseurl"
    And Enter input plugin property: "username" with value: "admin.username"
    And Enter input plugin property: "password" with value: "admin.password"
    And Click on the Macro button of Property: "entityName" and set the value to: "entityName"
    And Validate "SAP SuccessFactors" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "SAP-SuccessFactors" and sink as "BigQueryTable" to establish connection
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value "invalid.entityname" for key "entityName"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait till pipeline is in running state
    And Verify the pipeline status is "Failed"
    Then Open Pipeline logs and verify Log entries having below listed Level and Message:
      | Level | Message                                   |
      | ERROR | invalid.entityname.logsmessage            |

  @BATCH-TS-SCFA-RNTM-MACRO-06 @BQ_SINK_TEST
  Scenario:Verify pipeline failure message in logs when user provides invalid Credentials with Macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "SAP SuccessFactors" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SAP SuccessFactors"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.baseurl"
    And Enter input plugin property: "entityName" with value: "Advance"
    And Click on the Macro button of Property: "username" and set the value to: "username"
    And Click on the Macro button of Property: "password" and set the value to: "password"
    And Validate "SAP SuccessFactors" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "SAP-SuccessFactors" and sink as "BigQueryTable" to establish connection
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value "invalid.username" for key "username"
    And Enter runtime argument value "invalid.password" for key "password"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait till pipeline is in running state
    And Verify the pipeline status is "Failed"
    Then Open Pipeline logs and verify Log entries having below listed Level and Message:
      | Level | Message                                   |
      | ERROR | invalid.credentials.logsmessage           |

  @BATCH-TS-SCFA-RNTM-MACRO-07 @BQ_SINK_TEST
  Scenario: Verify pipeline failure message in logs when user provides invalid Advanced Properties with Macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "SAP SuccessFactors" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SAP SuccessFactors"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.baseurl"
    And Enter input plugin property: "entityName" with value: "Advance"
    And Enter input plugin property: "username" with value: "admin.username"
    And Enter input plugin property: "password" with value: "admin.password"
    And Click on the Macro button of Property: "filterOption" and set the value in textarea: "filterOption"
    And Click on the Macro button of Property: "selectOption" and set the value in textarea: "selectOption"
    And Click on the Macro button of Property: "splitCount" and set the value to: "splitCount"
    And Click on the Macro button of Property: "batchSize" and set the value to: "batchSize"
    And Click on the Macro button of Property: "skipRowCount" and set the value to: "skipRowCount"
    And Click on the Macro button of Property: "numRowsToFetch" and set the value to: "numRowsToFetch"
    And Validate "SAP SuccessFactors" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "SAP-SuccessFactors" and sink as "BigQueryTable" to establish connection
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value "invalid.filteoption" for key "filterOption"
    And Enter runtime argument value "invalid.filter.selectedfields" for key "selectOption"
    And Enter runtime argument value "invalid.splitcount" for key "splitCount"
    And Enter runtime argument value "invalid.batchsize" for key "batchSize"
    And Enter runtime argument value "invalid.skiprowcount" for key "skipRowCount"
    And Enter runtime argument value "invalid.numrowstofetch" for key "numRowsToFetch"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait till pipeline is in running state
    And Verify the pipeline status is "Failed"
    Then Open Pipeline logs and verify Log entries having below listed Level and Message:
      | Level | Message                                   |
      | ERROR | invalid.filters.logsmessage               |

  @BATCH-TS-SCFA-RNTM-MACRO-08 @BQ_SINK_TEST
  Scenario: Verify pipeline failure message in logs when user provides invalid Expand Field properties with Macros
    When Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Batch"
    And Select plugin: "SAP SuccessFactors" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SAP SuccessFactors"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.baseurl"
    And Enter input plugin property: "username" with value: "admin.username"
    And Enter input plugin property: "password" with value: "admin.password"
    And Click on the Macro button of Property: "entityName" and set the value to: "entityName"
    And Click on the Macro button of Property: "expandOption" and set the value to: "expandOption"
    And Click on the Macro button of Property: "associatedEntityName" and set the value to: "associatedEntityName"
    And Validate "SAP SuccessFactors" plugin properties
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Configure BigQuery sink plugin for Dataset and Table
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "SAP-SuccessFactors" and sink as "BigQueryTable" to establish connection
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value "hierarchical.valid.entityname" for key "entityName"
    And Enter runtime argument value "hierarchical.invalid.expandfield" for key "expandOption"
    And Enter runtime argument value "hierarchical.invalid.associatedentityname" for key "associatedEntityName"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait till pipeline is in running state
    And Verify the pipeline status is "Failed"
    Then Open Pipeline logs and verify Log entries having below listed Level and Message:
      | Level | Message                                   |
      | ERROR | invalid.expandfield.logsmessage           |




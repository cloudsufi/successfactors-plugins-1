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
Feature: SuccessFactors Source - Run time scenarios

  @BATCH-TS-SCFA-RNTM-01 @BQ_SINK @FILE_PATH @BQ_SINK_CLEANUP
  Scenario: Verify user should be able to preview and deploy the pipeline when plugin is configured for a valid Entity name
    When Open Datafusion Project to configure pipeline
    And Select plugin: "SAP SuccessFactors" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SAP SuccessFactors"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.baseurl" for Credentials and Authorization related fields
    And Enter input plugin property: "entityName" with value: "EmpCostDistribution"
    And Enter input plugin property: "username" with value: "admin.username" for Credentials and Authorization related fields
    And Enter input plugin property: "password" with value: "admin.password" for Credentials and Authorization related fields
    And Validate "SAP SuccessFactors" plugin properties
    And Capture the generated Output Schema
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId"
    And Enter input plugin property: "dataset" with value: "dataset"
    And Enter input plugin property: "table" with value: "bqtarget.table"
    And Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "SAP-SuccessFactors" and sink as "BigQueryTable" to establish connection
    And Preview and run the pipeline
    And Wait till pipeline preview is in running state
    And Open and capture pipeline preview logs
    And Verify the preview run status of pipeline in the logs is "succeeded"
    And Close the pipeline logs
    And Close the preview
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    And Open and capture logs
    And Verify the pipeline status is "Succeeded"
    And Close the pipeline logs
    Then Validate record created in Sink application is equal to expected output file "TestOutputFile"

  @BATCH-TS-SCFA-RNTM-02 @BQ_SINK @FILE_PATH @BQ_SINK_CLEANUP
  Scenario: Verify user should be able to preview and deploy the pipeline when plugin is configured with Advanced properties
    When Open Datafusion Project to configure pipeline
    And Select plugin: "SAP SuccessFactors" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SAP SuccessFactors"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.baseurl" for Credentials and Authorization related fields
    And Enter input plugin property: "entityName" with value: "EmpCostDistribution"
    And Enter input plugin property: "username" with value: "admin.username" for Credentials and Authorization related fields
    And Enter input plugin property: "password" with value: "admin.password" for Credentials and Authorization related fields
    And Select radio button plugin property: "paginationType" with value: "paginationtype.client"
    And Enter textarea plugin property: "filterOption" with value: "filteoption.rntm.equal"
    And Validate "SAP SuccessFactors" plugin properties
    And Capture the generated Output Schema
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId"
    And Enter input plugin property: "dataset" with value: "dataset"
    And Enter input plugin property: "table" with value: "bqtarget.table"
    And Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "SAP-SuccessFactors" and sink as "BigQueryTable" to establish connection
    And Preview and run the pipeline
    And Wait till pipeline preview is in running state
    And Open and capture pipeline preview logs
    And Verify the preview run status of pipeline in the logs is "succeeded"
    And Close the pipeline logs
    And Close the preview
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    And Open and capture logs
    And Verify the pipeline status is "Succeeded"
    And Close the pipeline logs
    Then Validate record created in Sink application is equal to expected output file "TestOutputFileWithFilter"

  @BATCH-TS-SCFA-RNTM-03 @BQ_SINK @FILE_PATH @BQ_SINK_CLEANUP @Required
  Scenario: Verify user should be able to deploy and run the pipeline when plugin is configured with Server side Pagination Type
    When Open Datafusion Project to configure pipeline
    And Select plugin: "SAP SuccessFactors" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "SAP SuccessFactors"
    And Enter input plugin property: "referenceName" with value: "Referencename"
    And Enter input plugin property: "baseURL" with value: "admin.baseurl" for Credentials and Authorization related fields
    And Enter input plugin property: "entityName" with value: "EmpCostDistribution"
    And Enter input plugin property: "username" with value: "admin.username" for Credentials and Authorization related fields
    And Enter input plugin property: "password" with value: "admin.password" for Credentials and Authorization related fields
    And Select radio button plugin property: "paginationType" with value: "paginationtype.server"
    And Validate "SAP SuccessFactors" plugin properties
    And Capture the generated Output Schema
    And Close the Plugin Properties page
    And Select Sink plugin: "BigQueryTable" from the plugins list
    And Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId"
    And Enter input plugin property: "dataset" with value: "dataset"
    And Enter input plugin property: "table" with value: "bqtarget.table"
    And Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    And Connect source as "SAP-SuccessFactors" and sink as "BigQueryTable" to establish connection
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    And Verify the pipeline status is "Succeeded"
    Then Validate record created in Sink application is equal to expected output file "TestOutputFile"

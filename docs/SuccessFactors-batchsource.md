# SuccessFactors Batch Source
## Description
The SuccessFactors Batch Source plugin enables bulk data integration from SuccessFactors with the Cloud Data Fusion
platform. You can configure and execute bulk data transfers from SuccessFactors Entities without any coding.

## Properties
You can configure the following properties for the SAP SuccessFactors.

**Note**: The following indicators are used to define the fields:  
**M** - Indicates Macros are supported for the respective field  
**O** - Optional field

## Basic
**Reference Name:** Name used to uniquely identify this source for lineage,
annotating metadata, etc.  
**Entity Name (M)**: Name of the Entity to be extracted.

## Connection
**Use Connection:** Whether to use a connection. If a connection is used, you do not need to provide the credentials.  
**Connection:** Name of the connection to use. Entity Names information will be provided by the connection.
You also can use the macro function ${conn(connection-name)}.  
**SAP SuccessFactors Logon Username (M)**: SAP SuccessFactors Logon Username for user authentication.  
**SAP SuccessFactors Logon Password (M)**: SAP SuccessFactors Logon password for user authentication.  
**SAP SuccessFactors Base URL (M)**: SAP SuccessFactors Base URL.  

## Proxy Configuration
**Proxy URL:** Proxy URL. Must contain a protocol, address and port.  
**Username:** Proxy username.  
**Password:** Proxy password. 

## Advance Option:

**Filter Options (M, O)**: Filter condition to restrict the output data volume e.g. Price gt 200  
Supported operation are as follows:
<table border="1" cellspacing="0" cellpadding="0" aria-label="Filter Query Option Operators">
<tbody>
<tr>
<th>Operator</th>
<th>Description</th>
<th>Example</th>
</tr>
<tr>
<td colspan="3"><b>Logical Operators</b></td>
</tr>
<tr>
<td>Eq</td>
<td>Equal</td>
<td>/EmpGlobalAssignment?$filter=assignmentClass eq 'GA'</td>
</tr>
<tr>
<td>Ne</td>
<td>Not equal</td>
<td>/RecurringDeductionItem?$filter=amount ne 18</td>
</tr>
<tr>
<td>Gt</td>
<td>Greater than</td>
<td>/RecurringDeductionItem?$filter=amount gt 4</td>
</tr>
<tr>
<td>Ge</td>
<td>Greater than or equal</td>
<td>/RecurringDeductionItem?$filter=amount ge 18</td>
</tr>
<tr>
<td>Lt</td>
<td>Less than</td>
<td>/RecurringDeductionItem?$filter=amount lt 18</td>
</tr>
<tr>
<td>Le</td>
<td>Less than or equal</td>
<td>/RecurringDeductionItem?$filter=amount le 20</td>
</tr>
<tr>
<td>And</td>
<td>Logical and</td>
<td>/RecurringDeductionItem?$filter=amount le 20 and amount gt 4</td>
</tr>
<tr>
<td>Or</td>
<td>Logical or</td>
<td>/RecurringDeductionItem?$filter=amount le 20 or amount gt 4</td>
</tr>
<tr>
<td>Not</td>
<td>Logical negation</td>
<td>/RecurringDeductionItem?$filter=not endswith(payComponentType, 'SUPSPEE_US')</td>
</tr>
<tr>
<td colspan="3"><b>Arithmetic Operators</b></td>
</tr>
<tr>
<td>Add</td>
<td>Addition</td>
<td>/RecurringDeductionItem?$filter=amount add 5 gt 18</td>
</tr>
<tr>
<td>Sub</td>
<td>Subtraction</td>
<td>/RecurringDeductionItem?$filter=amount sub 5 gt 18</td>
</tr>
<tr>
<td>Mul</td>
<td>Multiplication</td>
<td>/RecurringDeductionItem?$filter=amount mul 2 gt 18</td>
</tr>
<tr>
<td>Div</td>
<td>Division</td>
<td>/RecurringDeductionItem?$filter=amount div 2 gt 18</td>
</tr>
<tr>
<td>Mod</td>
<td>Modulo</td>
<td>/RecurringDeductionItem?$filter=amount mod 2 eq 0</td>
</tr>
<tr>
<td colspan="3"><b>Grouping Operators</b></td>
</tr>
<tr>
<td>( )</td>
<td>Precedence grouping</td>
<td>/RecurringDeductionItem?$filter=(amount sub 5) gt 8</td>
</tr>
</tbody>
</table>   


**Select Fields (M, O)**: Fields to be preserved in the extracted data. e.g.: Category, Price, Name, Address. If the 
field is left blank, then all the non-navigation fields will be preserved in the extracted data.
All the fields must be comma (,) separated.

**Expand Fields (M, O)**: List of navigation fields to be expanded in the extracted output data. 
For example: customManager. If an entity has hierarchical records, the source outputs a record for each row in the 
entity it reads, with each record containing an additional field that holds the value from the navigational property 
specified in the Expand Fields.

**Additional Query Parameters (M, O)**: Additional Query Parameters that can be added with the OData url. 
e.g. Effective Dated queries.Multiple parameters can be added as separated by '&' sign. 
e.g. fromDate=2023-01-01&toDate=2023-01-31

**Associated Entity Name (M, O)**: Name of the Associated Entity to be extracted
e.g.: EmpCompensationCalculated

**Pagination Type (M, O)** : The type of pagination to be used. 
Server-side Pagination uses snapshot-based pagination. If snapshot-based pagination is attempted on an entity that 
doesn’t support the feature, the server automatically forces client-side pagination on the query. Default is 
Server-side Pagination. 
https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/2cd6a3c92f2547c99cfd612c6867582f.html


Data Type Mappings from SuccessFactors to CDAP
----------
The following table lists out different successFactors data types, as well as their corresponding CDAP data types

| SuccessFactors type | CDAP type |
|----------------|---------------|
| Binary         | Bytes         |
| Boolean        | Boolean       |
| Byte           | Bytes         |
| DateTime       | DateTime      |
| DateTimeOffset | Timestamp_Micros|
| Decimal        | Decimal       |
| Double         | Double        |
| Float          | Float         |
| Int16          | Integer       |
| Int32          | Integer       |
| Int64          | Long          |
| SByte          | Integer       |
| String         | String        |
| Time           | Time_Micros   |

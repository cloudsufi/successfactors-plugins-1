# SuccessFactors Batch Source
## Description
The SuccessFactors Batch Source plugin enables bulk data integration from SuccessFactors with the Cloud Data Fusion
platform. You can configure and execute bulk data transfers from SuccessFactors Entities without any coding.

## Properties
You can configure the following properties for the SuccessFactors.

**Note**: The following indicators are used to define the fields:  
**M** - Indicates Macros are supported for the respective field  
**O** - Optional field

## Basic
**Reference Name:** Name used to uniquely identify this source for lineage,
annotating metadata, etc.
**SAP SuccessFactors Base URL (M)**: SuccessFactors Base URL.
**Entity Name (M)**: Name of the Entity which is being extracted.

## Credentials

**SAP SuccessFactors Logon Username (M)**: SuccessFactors Logon user ID.  
**SAP SuccessFactors Logon Password (M)**: SuccessFactors Logon password for user authentication.

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

**Select Fields (M, O)**: Fields to be preserved in the extracted data. e.g.: Category, Price, Name, Address. In case of empty all the non-navigation fields will be preserved in the extracted data.
All the fields must be comma (,) separated.  
**Expand Fields (M, O)**: List of navigation fields to be expanded in the extracted output data
e.g.: customManager
**Associated Entity Name (M, O)**: Name of the Associated Entity which is being extracted
e.g.: EmpCompensationCalculated
**Number of Rows to Skip (M, O)**: Number of rows to skip e.g.: 10.  
**Number of Rows to Fetch (M, O)**: Total number of rows to be extracted (accounts for conditions specified
in Filter Options).     
**Number of Splits to Generate (M, O)**: The number of splits is used to partition the input data. If not specified at UI, by default it is 8 splits. Otherwise, the user-provided splits at UI prevail over
defaulted splits.    
**Batch Size (M, O)**: Number of rows to fetch in each network call to SAP SuccessFactors. Smaller size will cause frequent
network calls repeating the associated overhead. A large size may slow down data retrieval & cause
excessive resource usage in SAP SuccessFactors. If this value is set to 0, default value is set to 2500 and max limit on
rows to fetch in each batch is 5000.

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

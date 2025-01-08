# SAP SuccessFactors Connection

Description
-----------
Use this connection to access data in SAP SuccessFactors.

Properties
----------
**Name:** Name of the connection. Connection names must be unique in a namespace.

**Description:** Description of the connection.

**Authentication Type:** Authentication type used to submit request. Supported types are Basic & OAuth 2.0. Default is Basic Authentication.
* **Basic Authentication**  
  **SAP SuccessFactors Logon Username (M)**: SAP SuccessFactors Logon Username for user authentication.  
  **SAP SuccessFactors Logon Password (M)**: SAP SuccessFactors Logon password for user authentication.
* **OAuth 2.0**  
  **Client ID:** Client ID (API Key) required to generate the token.  
  **Company ID:** Company ID required to generate the token.  
  **Token URL:** Token URL to generate the assertion token.  
  **Assertion Token Type:** Assertion token can be entered or can be created using the required parameters.
* **Enter Token**  
  **Assertion Token:** Assertion token used to generate the access token.
* **Create Token**  
  **Private Key:** Private key required to generate the token.  
  **Expire Assertion Token In (Minutes):** Assertion Token will not be valid after the specified time. Default 1440 minutes (24 hours).  
  **User ID:** User ID required to generate the token.

**SAP SuccessFactors Base URL (M)**: SAP SuccessFactors Base URL.

**Proxy URL:** Proxy URL. Must contain a protocol, address and port.

**Username:** Proxy username.

**Password:** Proxy password.

Path of the connection
----------------------
To browse, get a sample from, or get the specification for this connection.  
/{entity} This path indicates a SuccessFactors entity. A entity is the only one that can be sampled.
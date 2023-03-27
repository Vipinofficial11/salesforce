# Salesforce Connection

Description
-----------
Use this connection to access data in Salesforce.

Properties
----------
**Name:** Name of the connection. Connection names must be unique in a namespace.

**Description:** Description of the connection.

**Username:** Salesforce username.

**Password:** Salesforce password.

**Security Token:** Salesforce security Token. If the password does not contain the security token, the plugin
will append the token before authenticating with Salesforce.

**Consumer Key:** Application Consumer Key. This is also known as the OAuth client id.
A Salesforce connected application must be created in order to get a consumer key.

**Consumer Secret:** Application Consumer Secret. This is also known as the OAuth client secret.
A Salesforce connected application must be created in order to get a client secret.

**Login Url:** Salesforce OAuth2 login url.

**Connect Timeout:** Maximum time in milliseconds to wait for connection initialization before time out.

Path of the connection
----------------------
To browse, get a sample from, or get the specification for this connection. (Not supported in Streaming source and 
batch Multi source).  
/{object} This path indicates a Salesforce object. An object is the only one that can be sampled.
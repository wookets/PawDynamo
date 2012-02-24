DynamoDB Storage Guide
----------------------

The storage of objects will as closely follow a rest like api as possible. The reason for this is that we can make each DynamoDB 'table' into a complete database for an application and save on space and money and prevent unnecessary overhead. The DynamoDao will implement this spec behind the scenes out of sight to the user. Because of this, the DynamoDao interface will follow more of a standard hibernate / object mapping pattern. It is a successful pattern, so... 

Hashkey Implementation 
----------------------

Objects are stored in the table using the following composed hashkey.

{tenantId}/{type}/{id}

e.g. actifi/roadmap/55
returns. {_id: "55", _type:"Roadmap", name: "My Roadmap Name"}

Relationships between a tenant and their owned objects are stored as the following. This is akin to doing a 'load-all'

{tenantId}/{type}

e.g. actifi/roadmap


Other relationships, in which an object is the parent of another object, are represented as a one-to-many within the confines of an object property on the parent holding the children. 


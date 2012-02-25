#PawDynamo Help Guide

This project is an example of a multi-tenant single table AmazonDB Data Access Object. 

##Why would we want to do this?

The reason this project is that a single table with 5 reads / writes allocated will cost about $7 a month. So, if you have a small application
and just need minimal read writes to get you started, you can just use one table which falls under the 'free' tier for aws and scale up from there
if you need or if your project becomes bigger, migrate to a multi-table format. DynamoDB scales endlessly, so going multi-table would really be 
more about personal organization than actual performance (at least that's according to the aws guys). 

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

It should be noted that you could just as easily call tenant 'application' and store many applications worth a data under one table. 
--1. what are different distribution keys?
Key distribution, Even distribution, as well as All distribution.

Key distribution distributes rows according to the values in one column.

Even distribution distributes rows across slices in a roundabout fashion. 
Good when a table doesn't participate 'joins.

All distribution distributes a copy of the entire table to every single node. 

--2. For transaction data what distribution key works well?
Key distribution would work well for transaction data, as we 
would need to frequently join tables on the distribution key.

--3. what is dimension data?
It is data that refers to the descriptive attributes/characteristics of the data in a database,
it provides meaning to the items being analyzed.

--4. what is type 2 dimension?
It is data that retains the history of changes by creating a 
new record for each change in the data, usually on slowly changing dimensions.

--5. What is slowly changing dimension?
Dimensions can change over time, given such it would be a slowly changing dimension.
--6. what is leader node?
The leader node is responsible for coordinating and managing the entire Redshift cluster
 as it acts as the interface between the client applications and the compute nodes.

--7. what are different node types in redshift?
DS2 nodes - optimized for large amounts of data storage.
DC2 nodes - optimized for high-performance processing and queries requiring low-latency access to data
RA3 nodes -  computational nodes with features like high-speed caching, 
managed storage to optimize the warehouse by scaling, and high bandwidth networking. Redshift
stores permanent data to s3 and used the local disk for caching. 
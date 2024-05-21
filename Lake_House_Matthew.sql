-- Lake House

CREATE OR REPLACE TEMPORARY VIEW fireCallsParquet
USING parquet
OPTIONS (
  path "/mnt/davis/fire-calls/fire-calls-clean.parquet/"
)
CREATE DATABASE IF NOT EXISTS Databricks;
USE Databricks;
DROP TABLE IF EXISTS fireCallsPartitioned;
CREATE OR REPLACE TABLE fireCallsPartitioned
using delta
partitioned by (Priority)
as 
 select * from fireCallsParquet 

--------------------

-- Question 1
-- dbfs:/user/hive/warehouse/databricks.db/firecallspartitioned
select count(*) from fireCallsGroupCleaned
-- 9

--------------------

-- Question 2

select *
from fireCallsPartitioned;
delete from fireCallsPartitioned where City is NULL;
select count(*) from fireCallsPartitioned

--------------------

-- Question 3

describe history fireCallsPartitioned

--------------------

-- Question 4

update fireCallsPartitioned set Call_Type_Group="Non Life-threatening" where Call_Type_Group is null
select count(*)
from fireCallsPartitioned
where Call_Type_Group="Non Life-threatening"

--------------------

-- Question 5

select count(*)
from fireCallsPartitioned
version as of 0
--------------------

-- Q1.
-- Apache Spark is a powerful framework for large-scale data processing, ideal for ETL, machine learning, and real-time streaming. Its core features, such as Spark SQL, DataFrames, and Datasets, enable efficient data manipulation and querying. Spark's in-memory processing ensures faster operations compared to traditional methods, making it suitable for applications like business intelligence and data analytics. Additionally, Spark's MLlib library supports various machine learning algorithms for developing predictive models, while the GraphX module facilitates efficient graph computation for network analysis and fraud detection. This versatility makes Spark a key component in modern data applications.

-- Q2.
-- Parallelism is crucial in data processing because it reduces computation time and enhances efficiency, essential for handling large-scale data and complex computations. Apache Spark achieves parallelism by distributing data and tasks across multiple cluster nodes using Resilient Distributed Datasets (RDDs) and dividing computations into stages that run concurrently. For example, Spark SQL parallelizes query execution by distributing data across the cluster, enabling faster query responses. In machine learning, Spark's MLlib library parallelizes the training process by distributing data, which speeds up model training on large datasets. Similarly, Spark Streaming processes real-time data by dividing streams into small batches and processing them in parallel for timely insights. GraphX parallelizes graph computations by partitioning graph data and running algorithms like PageRank across the cluster, efficiently handling large-scale graph datasets.

-- Q3.
-- A DataFrame in Spark is a distributed collection of data organized into named columns, similar to a SQL table but with enhanced capabilities for large-scale processing. Unlike SQL tables, DataFrames support distributed processing and in-memory computation. For example, a DataFrame can distribute a billion rows across multiple nodes for parallel computation and can cache intermediate results in memory to speed up iterative tasks, such as machine learning algorithms.In contrast, SQL tables have a static schema defined at creation and are generally limited by single-server hardware, leading to slower performance on large datasets. For instance, adding a column to a large SQL table or executing complex joins on millions of rows can be slow and resource-intensive.

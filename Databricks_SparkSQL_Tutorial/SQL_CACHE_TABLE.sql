-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Overview
-- MAGIC This notebooks contains complete SPARK SQL / DELTA LAKE SQL  Tutorial
-- MAGIC ###Details
-- MAGIC | Detail Tag | Information
-- MAGIC | - | - | - | - |
-- MAGIC | NOTEBOOK | SQL CACHE Concepts at SQL Level
-- MAGIC | Originally Created By | Raveendra  
-- MAGIC | Reference And Credits  | apache.spark.org  & databricks.com
-- MAGIC
-- MAGIC ###History
-- MAGIC |Date | Developed By | comments
-- MAGIC |----|-----|----
-- MAGIC |23/05/2021|Ravendra| Initial Version
-- MAGIC | Find more Videos | Youtube   | <a href="https://www.youtube.com/watch?v=FpxkiGPFyfM&list=PL50mYnndduIHRXI2G0yibvhd3Ck5lRuMn" target="_blank"> Youtube link </a>|

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CACHE TABLE
-- MAGIC * CACHE TABLE statement caches contents of a table or output of a query with the given storage level. 
-- MAGIC * If a query is cached, then a temp view will be created for this query. This reduces scanning of the original files in future queries.
-- MAGIC
-- MAGIC * __`Syntax`__
-- MAGIC
-- MAGIC ```
-- MAGIC CACHE [ LAZY ] TABLE table_identifier
-- MAGIC     [ OPTIONS ( 'storageLevel' [ = ] value ) ] [ [ AS ] query ]
-- MAGIC ```
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Parameters
-- MAGIC
-- MAGIC * __`LAZY`__
-- MAGIC
-- MAGIC * Only cache the table when it is first used, instead of immediately.
-- MAGIC
-- MAGIC * __`table_identifier`__
-- MAGIC
-- MAGIC * Specifies the table or view name to be cached. The table or view name may be optionally qualified with a database name.
-- MAGIC
-- MAGIC * __`Syntax:`__
-- MAGIC
-- MAGIC ```
-- MAGIC [ database_name. ] table_name`__
-- MAGIC
-- MAGIC OPTIONS ( ‘storageLevel’ [ = ] value )
-- MAGIC
-- MAGIC ```
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * OPTIONS clause with storageLevel key and value pair. A Warning is issued when a key other than storageLevel is used. The valid options for storageLevel are:
-- MAGIC
-- MAGIC * NONE
-- MAGIC * __`DISK_ONLY`__
-- MAGIC * __`DISK_ONLY_2`__
-- MAGIC * __`DISK_ONLY_3`__
-- MAGIC * __`MEMORY_ONLY`__
-- MAGIC * __`MEMORY_ONLY_2`__
-- MAGIC * __`MEMORY_ONLY_SER`__
-- MAGIC * __`MEMORY_ONLY_SER_2`__
-- MAGIC * __`MEMORY_AND_DISK`__
-- MAGIC * __`MEMORY_AND_DISK_2`__
-- MAGIC * __`MEMORY_AND_DISK_SER`__
-- MAGIC * __`MEMORY_AND_DISK_SER_2`__
-- MAGIC * __`OFF_HEAP`__
-- MAGIC
-- MAGIC * An Exception is thrown when an invalid value is set for storageLevel. 
-- MAGIC * If storageLevel is not explicitly set using OPTIONS clause, the default storageLevel is set to __`MEMORY_AND_DISK`__.
-- MAGIC
-- MAGIC * __`query`__
-- MAGIC
-- MAGIC * A query that produces the rows to be cached. It can be in one of following formats:
-- MAGIC
-- MAGIC * __`a SELECT statement`__
-- MAGIC * __`a TABLE statement`__
-- MAGIC * __`a FROM statement`__

-- COMMAND ----------



-- COMMAND ----------

CACHE TABLE emp_cache OPTIONS ('storageLevel' 'DISK_ONLY') SELECT * FROM emp;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC * `UNCACHE` table

-- COMMAND ----------

UNCACHE table emp_cache

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * CACHE Table 

-- COMMAND ----------

CACHE TABLE EMP OPTIONS ('storageLevel' 'MEMORY_AND_DISK')

-- COMMAND ----------

select * from EMP

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### UNCACHE Table
-- MAGIC * UNCACHE TABLE [ IF EXISTS ] table_identifier
-- MAGIC
-- MAGIC * Removes  table cached data from the in-memory cache using `UNCACHE TABLE table_name`
-- MAGIC * UNCACHE TABLE removes the entries and associated data from the in-memory and/or on-disk cache for a given table or view. 
-- MAGIC * The underlying entries should already have been brought to cache by previous CACHE TABLE operation. 
-- MAGIC * UNCACHE TABLE on a non-existent table throws an exception if IF EXISTS is not specified.

-- COMMAND ----------

UNCACHE TABLE EMP

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Removes all cached tables from the in-memory cache.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.catalog.clearCache()
-- MAGIC #sqlContext.clearCache()
-- MAGIC #spark.catalog.uncacheTable(tableName)
-- MAGIC #sqlContext.uncacheTable(tableName)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### REFRESH table using refresh method

-- COMMAND ----------

-- The cached entries of the table will be refreshed  
REFRESH TABLE emp;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CLEAR CACHE
-- MAGIC * __`CLEAR CACHE`__ removes the entries and associated data from the in-memory and/or on-disk cache for all cached tables and views.

-- COMMAND ----------

CLEAR CACHE

-- COMMAND ----------

-- The Path is resolved using the datasource's File Index.
CREATE TABLE test(ID INT) using parquet;
INSERT INTO test SELECT 1000;
CACHE TABLE test;
INSERT INTO test SELECT 100;
REFRESH "dbfs:/user/hive/warehouse/test/";

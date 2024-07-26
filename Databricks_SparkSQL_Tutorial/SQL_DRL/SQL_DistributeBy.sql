-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Overview
-- MAGIC This notebooks contains complete SPARK SQL / DELTA LAKE SQL  Tutorial
-- MAGIC ###Details
-- MAGIC | Detail Tag | Information
-- MAGIC |----|-----
-- MAGIC | Notebook | SQL DRL DISTRIBUTED BY details 
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
-- MAGIC #### DISTRIBUTE BY Clause
-- MAGIC * The DISTRIBUTE BY clause is used to repartition the data based on the input expressions. 
-- MAGIC * Unlike the CLUSTER BY clause, this does not sort the data within each partition.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Syntax
-- MAGIC * __`DISTRIBUTE BY { expression [ , ... ] }`__
-- MAGIC
-- MAGIC * Parameters
-- MAGIC
-- MAGIC * Specifies combination of one or more values, operators and SQL functions that results in a value.

-- COMMAND ----------

DROP TABLE IF EXISTS person;
CREATE TABLE IF NOT EXISTS person (name STRING, age INT);
INSERT INTO person 
    SELECT 'Raj Kumar', 37  UNION ALL
    SELECT 'Ravi Kumar', 35 UNION ALL
    SELECT 'Mahesh Kumar', 34 UNION ALL
    SELECT 'Prasad Reddy', 36 UNION ALL
    SELECT 'Sridhar P', 34 UNION ALL
    SELECT 'Anitha Reddy', 36 UNION ALL
    SELECT 'Sindhu K', 35  UNION ALL
    SELECT 'Srinivas N', 37;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC *  Reduce the number of shuffle partitions to 2 to illustrate the behavior of __`DISTRIBUTE BY`__
-- MAGIC * It's easier to see the clustering and sorting behavior with less number of partitions.

-- COMMAND ----------

SET spark.sql.shuffle.partitions = 2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Select the rows with no ordering. Please note that without any sort directive, the result  of the query is not deterministic. 
-- MAGIC * It's included here to just contrast it with the behavior of `DISTRIBUTE BY`. The query below produces rows where age columns are not clustered together.

-- COMMAND ----------

SELECT * FROM person 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Produces rows clustered by age. Persons with same age are clustered together.
-- MAGIC * Unlike __`CLUSTER BY`__ clause, the rows are not sorted within a partition.

-- COMMAND ----------


SELECT age, name FROM person distribute BY age;

-- COMMAND ----------



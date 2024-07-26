-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Overview
-- MAGIC This notebooks contains complete SPARK SQL / DELTA LAKE SQL  Tutorial
-- MAGIC ###Details
-- MAGIC | Detail Tag | Information
-- MAGIC |----|-----
-- MAGIC | Notebook | SQL DRL CLUSTERED BY details 
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
-- MAGIC #### CLUSTER BY Clause
-- MAGIC * The __`CLUSTER BY`__ clause is used to first repartition the data based on the input expressions and then sort the data within each partition. 
-- MAGIC * This is semantically equivalent to performing a __`DISTRIBUTE BY followed by a SORT BY`__. 
-- MAGIC * This clause only ensures that the resultant rows are `sorted within each partition` and does not guarantee a total order of output.

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
    SELECT 'Srinivas N', 37 UNION ALL
    SELECT 'Vikranth T', 5 UNION ALL
    SELECT 'Reshwanth TG',10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Reduce the number of shuffle partitions to 2 to illustrate the behavior of `CLUSTER BY`.
-- MAGIC * It's easier to see the clustering and sorting behavior with less number of partitions.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.get("spark.sql.shuffle.partitions")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.get("spark.sql.shuffle.partitions")
-- MAGIC spark.conf.set("spark.sql.shuffle.partitions","2")
-- MAGIC print(spark.conf.get("spark.sql.shuffle.partitions"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(spark.conf.get("spark.sql.shuffle.partitions"))

-- COMMAND ----------

SET spark.sql.shuffle.partitions = 2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Select the rows with no ordering. Please note that without any sort directive, the results of the query is not deterministic. 
-- MAGIC * It's included here to show the difference in behavior of a query when `CLUSTER BY` is not used vs when it's used. 
-- MAGIC * The query below produces rows where age column is not sorted.

-- COMMAND ----------

-- zorder by --
-- without zorder by, if we are looking for repartitioning and sorting data within partition -- Clustered by(repartitioning and sort by)

-- COMMAND ----------

-- repartitioning and sorting
SELECT /*+ repartition(2,age) */ age, name FROM person sort by age;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Produces rows clustered by age. Persons with same age are clustered together.
-- MAGIC * In the query below, persons with age 18 and 25 are in first partition and the
-- MAGIC * persons with age 16 are in the second partition. The rows are sorted based on age within each partition.

-- COMMAND ----------

-- MAGIC %fs ls /user/hive/warehouse/person

-- COMMAND ----------

select distinct age from person

-- COMMAND ----------

select /*+ repartition(age) */ age,name from person

-- COMMAND ----------

SELECT age, name FROM person DISTRIBUTE  BY age;

-- delta lake Zorder By 

-- COMMAND ----------

SELECT age, name FROM person CLUSTER BY age;

-- delta lake Zorder By 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * __`CLUSTERED BY `__ is equal to `df.repartition(2,"age").sortWithinPartitions("age").show()`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql('select * from person')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.repartition(2,"age").sortWithinPartitions("age").show()

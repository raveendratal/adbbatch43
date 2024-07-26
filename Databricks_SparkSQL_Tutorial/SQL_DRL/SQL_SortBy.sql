-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Overview
-- MAGIC This notebooks contains complete SPARK SQL / DELTA LAKE SQL  Tutorial
-- MAGIC ###Details
-- MAGIC | Detail Tag | Information
-- MAGIC |----|-----
-- MAGIC | Notebook | SQL DRL SORT BY Statement details 
-- MAGIC | Originally Created By | Raveendra  
-- MAGIC | Reference And Credits  | apache.spark.org  & databricks.com
-- MAGIC
-- MAGIC ###History
-- MAGIC |Date | Developed By | comments
-- MAGIC |----|-----|----
-- MAGIC |23/05/2021|Ravendra| Initial Version
-- MAGIC | Find more Videos | Youtube   | <a href="https://www.youtube.com/watch?v=FpxkiGPFyfM&list=PL50mYnndduIHRXI2G0yibvhd3Ck5lRuMn" target="_blank"> Youtube link </a>|

-- COMMAND ----------

--Prepare data for ignore nulls example
DROP TABLE IF EXISTS person;
CREATE TABLE person (zip_code INT, name STRING, age INT);
INSERT INTO person VALUES
    (560043, 'Ravi', NULL),
    (560043, 'Prasad', 36),
    (560043, 'Raj', 40),
    (560016, 'Sridhar', 34),
    (560016, 'Mahesh', 35),
    (560016, 'Anitha', 36),
    (560005, 'Sindhu', NULL),
    (560005, 'Vikranth', 10),
    (560005, 'Reshwanth', 05),
    (560005, 'Ram', 15),
    (560005, 'Ram', 25),
    (560005, 'Ranjith', 30),
    (560005, 'Ranjith', 12);

-- COMMAND ----------

--- order by will apply on entire data set.(global sorting in ascending or descening order)
-- sort by (if table does not have multiple partitions then its same as Order By. means if it is having single partition then it will do global sorting)
--- if table is having multiple partitions. then sort by will do partition wise sorting. and its not a global sorting. local sorting.(partition wise sorting)

-- COMMAND ----------

select * from person sort by age

-- COMMAND ----------

select * from person order by age

-- COMMAND ----------

 --- /*+. hint value */

-- COMMAND ----------

-- hints 
--- after SELECT. use /*+ hint name */ 
select /*+ REPARTITION(3,zip_code) */ * from person order by age desc

-- COMMAND ----------

-- hints 
--- after SELECT. use /*+ hint name */ 
select /*+ REPARTITION(3,zip_code) */ * from person sort by age desc

-- COMMAND ----------

select /*+ REPARTITION(3,zip_code) */ * from person order by age 

-- COMMAND ----------

select /*+ REPARTITION(3,zip_code) */ * from person sort by age

-- COMMAND ----------

select  /*+ REPARTITION(3,zip_code) */ * from person order by age

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Use `REPARTITION` hint to partition the data by `zip_code` to
-- MAGIC * examine the `SORT BY` behavior. This is used in rest of the
-- MAGIC
-- MAGIC * Sort rows by `name` within each partition in ascending manner

-- COMMAND ----------

SELECT  * FROM person ORDER BY age;

-- COMMAND ----------

SELECT  /*+ REPARTITION(zip_code) */ * FROM person SORT BY age nulls last;

-- COMMAND ----------


SELECT /*+ REPARTITION(zip_code) */ name, age, zip_code FROM person SORT BY age;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Sort rows within each partition using column position.

-- COMMAND ----------

SELECT /*+ REPARTITION(age) */ age,name, zip_code FROM person SORT BY 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC *  Sort rows within partition in ascending manner keeping null values to be last using __`NULL LAST`__
-- MAGIC

-- COMMAND ----------

SELECT /*+ REPARTITION(zip_code) */ age, name, zip_code FROM person SORT BY age NULLS LAST;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Sort rows by age within each partition in descending manner, which defaults to NULL LAST.
-- MAGIC

-- COMMAND ----------

SELECT /*+ REPARTITION(zip_code) */ age, name, zip_code FROM person SORT BY age DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC *  Sort rows by age within each partition in descending manner keeping null values to be first using __`NULLS FIRST`__
-- MAGIC

-- COMMAND ----------

SELECT /*+ REPARTITION(zip_code) */ age, name, zip_code FROM person SORT BY age NULLS FIRST;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Sort rows within each partition based on more than one column with each column having
-- MAGIC * different sort direction.

-- COMMAND ----------

SELECT /*+ REPARTITION(zip_code) */ name, age, zip_code FROM person
    SORT BY name ASC, age DESC;

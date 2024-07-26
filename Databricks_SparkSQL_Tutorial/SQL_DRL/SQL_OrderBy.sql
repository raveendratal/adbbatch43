-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Overview
-- MAGIC This notebooks contains complete SPARK SQL / DELTA LAKE SQL  Tutorial
-- MAGIC ###Details
-- MAGIC | Detail Tag | Information
-- MAGIC |----|-----
-- MAGIC | Notebook | SQL DRL Order By Statement details 
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
-- MAGIC #### ORDER BY clause
-- MAGIC * Returns the result rows in a sorted manner in the user specified order. Unlike the SORT BY clause, this clause guarantees a total order in the output.
-- MAGIC * Parameters
-- MAGIC ##### ORDER BY
-- MAGIC
-- MAGIC * A comma-separated list of expressions along with optional parameters sort_direction and nulls_sort_order which are used to sort the rows.
-- MAGIC
-- MAGIC ##### sort_direction
-- MAGIC
-- MAGIC * Optionally specifies whether to sort the rows in ascending or descending order. The valid values for the sort direction are ASC for ascending and DESC for descending. If sort direction is not explicitly specified, then by default rows are sorted ascending.
-- MAGIC
-- MAGIC ##### Syntax: [ ASC | DESC ]
-- MAGIC
-- MAGIC * nulls_sort_order
-- MAGIC
-- MAGIC * Optionally specifies whether NULL values are returned before/after non-NULL values. If null_sort_order is not specified, then NULLs sort first if sort order is ASC and NULLS sort last if sort order is DESC.
-- MAGIC
-- MAGIC * If NULLS FIRST is specified, then NULL values are returned first regardless of the sort order.
-- MAGIC * If NULLS LAST is specified, then NULL values are returned last regardless of the sort order.
-- MAGIC * Syntax: [ NULLS { FIRST | LAST } ]

-- COMMAND ----------

-- MAGIC %fs rm -r dbfs:/user/hive/warehouse/person

-- COMMAND ----------

--Prepare data for ignore nulls example
DROP TABLE IF EXISTS person;
CREATE TABLE person (id INT, name STRING, age INT);
INSERT INTO person VALUES
    (100, 'Ravi', NULL),
    (200, 'Prasad', 36),
    (300, 'Raj', 40),
    (400, 'Sridhar', 34),
    (500, 'Mahesh', 35);

-- COMMAND ----------

select * from person sort by name

-- COMMAND ----------

select * from person order by name 

-- COMMAND ----------

select * from person order by age --desc nulls first

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Sort rows by age. By default rows are sorted in ascending manner with NULL FIRST.
-- MAGIC

-- COMMAND ----------

SELECT name, age FROM person ORDER BY age;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Sort rows in ascending manner keeping null values to be last.
-- MAGIC

-- COMMAND ----------

SELECT name, age FROM person ORDER BY age NULLS LAST;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC *  Sort rows by age in descending manner, which defaults to __`NULL LAST`__.
-- MAGIC

-- COMMAND ----------

SELECT name, age FROM person ORDER BY age DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Sort rows in ascending manner keeping null values to be first __`NULLS FIRST`__
-- MAGIC

-- COMMAND ----------

SELECT name, age FROM person ORDER BY age DESC NULLS FIRST;

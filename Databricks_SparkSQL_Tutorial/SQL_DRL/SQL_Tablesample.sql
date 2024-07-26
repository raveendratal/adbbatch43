-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Overview
-- MAGIC This notebooks contains complete SPARK SQL / DELTA LAKE SQL  Tutorial
-- MAGIC ###Details
-- MAGIC | Detail Tag | Information
-- MAGIC |----|-----
-- MAGIC | Notebook | SQL DRL Tablesample Statement details 
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
-- MAGIC #### Sampling queries
-- MAGIC * The TABLESAMPLE statement is used to sample the table. It supports the following sampling methods:
-- MAGIC
-- MAGIC * __`TABLESAMPLE(x ROWS)`__: Sample the table down to the given number of rows.
-- MAGIC * __`TABLESAMPLE(x PERCENT)`__: Sample the table down to the given percentage. Note that percentages are defined as a number between 0 and 100.
-- MAGIC * __`TABLESAMPLE(BUCKET x OUT OF y)`__: Sample the table down to a x out of y fraction.
-- MAGIC * __`Syntax :`__`
-- MAGIC * TABLESAMPLE ({ integer_expression | decimal_expression } PERCENT)
-- MAGIC *     | TABLESAMPLE ( integer_expression ROWS )
-- MAGIC *    | TABLESAMPLE ( BUCKET integer_expression OUT OF integer_expression )

-- COMMAND ----------

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

SELECT * FROM person;

-- COMMAND ----------

== Physical Plan ==
*(1) Sample 0.0, 0.2, false, 515
+- *(1) ColumnarToRow
   +- FileScan parquet spark_catalog.default.person[zip_code#8765,name#8766,age#8767] Batched: true, DataFilters: [], Format: Parquet, Location: PreparedDeltaFileIndex(1 paths)[dbfs:/user/hive/warehouse/person], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<zip_code:int,name:string,age:int>


-- COMMAND ----------

explain SELECT * FROM person TABLESAMPLE (20 PERCENT);

-- COMMAND ----------

== Physical Plan ==
CollectLimit 10
+- *(1) ColumnarToRow
   +- FileScan parquet spark_catalog.default.person[zip_code#8730,name#8731,age#8732] Batched: true, DataFilters: [], Format: Parquet, Location: PreparedDeltaFileIndex(1 paths)[dbfs:/user/hive/warehouse/person], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<zip_code:int,name:string,age:int>


-- COMMAND ----------

explain SELECT * FROM person TABLESAMPLE (10 ROWS);  

-- COMMAND ----------

-- 4 buckets - means total 13 rows are divided into 4 buckets
-- 2 buckets  - means out of 13 rows 50% rows means 2 buckets
SELECT * FROM person TABLESAMPLE (BUCKET 1 OUT OF 5);

-- COMMAND ----------

select /*+ repartition(zip_code) */ * from person

-- COMMAND ----------

-- 10 buckets - means total 13 rows are divided into 10 buckets
-- 5 buckets  - means out of 13 rows 50% rows means 5 buckets
SELECT * FROM person TABLESAMPLE (BUCKET 3 OUT OF 4);

-- COMMAND ----------



-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Overview
-- MAGIC This notebooks contains complete SPARK SQL / DELTA LAKE SQL  Tutorial
-- MAGIC ###Details
-- MAGIC | Detail Tag | Information
-- MAGIC |----|-----
-- MAGIC | Notebook | SQL DRL LIMIT Statement details 
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
-- MAGIC #### LIMIT Clause
-- MAGIC * The __`LIMIT`__ clause is used to constrain the number of rows returned by the SELECT statement. 
-- MAGIC * In general, this clause is used in conjunction with __`ORDER BY`__ to ensure that the results are deterministic.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Parameters
-- MAGIC
-- MAGIC  * __`ALL`__
-- MAGIC
-- MAGIC * If specified, the query returns all the rows. In other words, no limit is applied if this option is specified.
-- MAGIC
-- MAGIC * __`integer_expression`__
-- MAGIC
-- MAGIC * Specifies a foldable expression that returns an integer.

-- COMMAND ----------

select * from emp limit 1000

-- COMMAND ----------

--Prepare data for ignore nulls example
DROP TABLE IF EXISTS person;
CREATE TABLE IF NOT EXISTS person (zip_code INT, name STRING, age INT);
INSERT INTO person VALUES
    (560043, 'Ravi', NULL),
    (560043, 'Prasad', 36),
    (560043, 'Raj', 40),
    (560016, 'Sridhar', 34),
    (560016, 'Mahesh', 35),
    (560016, 'Anitha', 36),
    (560005, 'Sindhu', NULL),
    (560005, 'Vikranth', 10),
    (560005, 'Reshwanth', 05);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Select the first two rows.
-- MAGIC

-- COMMAND ----------

SELECT name, age FROM person  where age>10 order by age LIMIT 3;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Specifying ALL option on LIMIT returns all the rows.

-- COMMAND ----------

SELECT name, age FROM person ORDER BY name LIMIT ALL;

-- COMMAND ----------

select length('SPARK')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * A function expression as an input to LIMIT.

-- COMMAND ----------


SELECT name, age FROM person ORDER BY name LIMIT length('SPARK');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC *  A non-foldable expression as an input to LIMIT is not allowed.
-- MAGIC * __`org.apache.spark.sql.AnalysisException: The limit expression must evaluate to a constant value ...`__

-- COMMAND ----------


SELECT name, age FROM person ORDER BY name LIMIT length(name);

-- COMMAND ----------



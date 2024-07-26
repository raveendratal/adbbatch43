-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Overview
-- MAGIC This notebooks contains complete SPARK SQL / DELTA LAKE SQL  Tutorial
-- MAGIC ###Details
-- MAGIC | Detail Tag | Information
-- MAGIC |----|-----
-- MAGIC | Notebook | SQL DRL CASE Statement details 
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
-- MAGIC #### CASE Clause
-- MAGIC * CASE clause uses a rule to return a specific result based on the specified condition, similar to if/else statements in other programming languages.
-- MAGIC
-- MAGIC * __`Syntax`__
-- MAGIC ```
-- MAGIC CASE [ expression ] { WHEN boolean_expression THEN then_expression } [ ... ]
-- MAGIC     [ ELSE else_expression ]
-- MAGIC END
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Parameters
-- MAGIC * __`boolean_expression`__
-- MAGIC
-- MAGIC * Specifies any expression that evaluates to a result type boolean. Two or more expressions may be combined together using the logical operators ( AND, OR ).
-- MAGIC
-- MAGIC * __`then_expression`__
-- MAGIC
-- MAGIC * Specifies the then expression based on the boolean_expression condition; then_expression and else_expression should all be same type or coercible to a common type.
-- MAGIC
-- MAGIC * __`else_expression`__
-- MAGIC
-- MAGIC * Specifies the default expression; then_expression and else_expression should all be same type or coercible to a common type.

-- COMMAND ----------

--Prepare data for ignore nulls example
DROP TABLE IF EXISTS person;
CREATE TABLE IF NOT EXISTS person (id INT, name STRING, age INT);
INSERT INTO person VALUES
    (100, 'Ravi', NULL),
    (200, 'Prasad', 36),
    (300, 'Raj', 40),
    (400, 'Sridhar', 34),
    (500, 'Mahesh', 35),
    (600, 'Anitha', 36),
    (700, 'Sindhu', NULL),
    (800, 'Vikranth', 10),
    (900, 'Reshwanth', 05);

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/person

-- COMMAND ----------

SELECT
  id,age,
  CASE
    WHEN age > 35 THEN 'bigger'
    when age is null then 'its null'
    ELSE 'small'
  END case_col
FROM
  person;

-- COMMAND ----------

SELECT
  id,
  CASE
    WHEN id=100 then '100'
    WHEN id=300 THEN '300'
    WHEN id=400 THEN '400'
    WHEN id>500 THEN 'above 500'
    ELSE 'Others'
  END case_col
FROM
  person;

-- COMMAND ----------

 SELECT * FROM person
    WHERE
        CASE 1 = 1
            WHEN 100 THEN 'big'
            WHEN 200 THEN 'bigger'
            WHEN 300 THEN 'biggest'
            ELSE 'small'
        END = 'small';

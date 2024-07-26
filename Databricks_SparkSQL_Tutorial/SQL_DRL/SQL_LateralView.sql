-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Overview
-- MAGIC This notebooks contains complete SPARK SQL / DELTA LAKE SQL  Tutorial
-- MAGIC ###Details
-- MAGIC | Detail Tag | Information
-- MAGIC |----|-----
-- MAGIC | Notebook | SQL DRL Lateral View Statement details 
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
-- MAGIC #### LATERAL VIEW clause
-- MAGIC * Used in conjunction with generator functions such as EXPLODE, which generates a virtual table containing one or more rows. LATERAL VIEW applies the rows to each original output row.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Parameters
-- MAGIC
-- MAGIC * If OUTER specified, returns null if an input array/map is empty or null.
-- MAGIC
-- MAGIC * __`generator_function`__
-- MAGIC
-- MAGIC * Specifies a generator function (EXPLODE, INLINE, etc.).
-- MAGIC
-- MAGIC * __`table_alias`__
-- MAGIC
-- MAGIC * The alias for generator_function, which is optional.
-- MAGIC
-- MAGIC * __`column_alias`__
-- MAGIC
-- MAGIC * Lists the column aliases of generator_function, which may be used in output rows. 
-- MAGIC * We may have multiple aliases if generator_function have multiple output columns.

-- COMMAND ----------

--LATERAL VIEW [ OUTER ] generator_function ( expression [ , ... ] ) [ table_alias ] AS column_alias [ , ... ]

-- COMMAND ----------

DROP TABLE IF EXISTS person;
CREATE TABLE IF NOT EXISTS person (id INT, name STRING, age INT, class INT, address STRING);
INSERT INTO person VALUES
    (100, 'John', 30, 1, 'Street 1'),
    (200, 'Mary', NULL, 1, 'Street 2'),
    (300, 'Mike', 80, 3, 'Street 3'),
    (400, 'Dan', 50, 4, 'Street 4');

-- COMMAND ----------

SELECT * FROM person
    LATERAL VIEW EXPLODE(ARRAY(30,60)) tableName AS c_age
    LATERAL VIEW EXPLODE(ARRAY(40,80)) AS d_age; 

-- COMMAND ----------

SELECT c_age, COUNT(1) FROM person
    LATERAL VIEW EXPLODE(ARRAY(30, 50,60)) AS c_age
    LATERAL VIEW EXPLODE(ARRAY(40, 80)) AS d_age
GROUP BY c_age;

-- COMMAND ----------

SELECT * FROM person
    LATERAL VIEW EXPLODE(ARRAY()) tabelName AS c_age; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC *  If __`OUTER`__ specified, returns null if an input array/map is empty or null.
-- MAGIC

-- COMMAND ----------

SELECT * FROM person
    LATERAL VIEW OUTER EXPLODE(ARRAY()) name AS c_age; 

-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Overview
-- MAGIC This notebooks contains complete SPARK SQL / DELTA LAKE SQL  Tutorial
-- MAGIC ###Details
-- MAGIC | Detail Tag | Information
-- MAGIC |----|-----
-- MAGIC | Notebook | SQL DRL HAVING Clause Statement details 
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
-- MAGIC #### HAVING clause
-- MAGIC * Filters the results produced by GROUP BY based on the specified condition. Often used in conjunction with a GROUP BY clause.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Syntax
-- MAGIC  `__HAVING boolean_expression`__
-- MAGIC
-- MAGIC * Parameters
-- MAGIC
-- MAGIC  __`boolean_expression`__
-- MAGIC
-- MAGIC * Specifies any expression that evaluates to a result type boolean. Two or more expressions may be combined together using the logical operators ( AND, OR ).
-- MAGIC
-- MAGIC  __`Note`__
-- MAGIC
-- MAGIC * The expressions specified in the HAVING clause can only refer to:
-- MAGIC * * Constants
-- MAGIC * * Expressions that appear in GROUP BY
-- MAGIC * * Aggregate functions

-- COMMAND ----------

DROP TABLE  IF EXISTS dealer;
CREATE TABLE dealer (id INT, city STRING, car_model STRING, quantity INT);
INSERT INTO dealer VALUES
    (100, 'Bangalore', 'Honda Civic', 10),
    (100, 'Bangalore', 'Honda Accord', 15),
    (100, 'Bangalore', 'Honda CRV', 7),
    (200, 'Chennai', 'Honda Civic', 20),
    (200, 'Chennai', 'Honda Accord', 10),
    (200, 'Chennai', 'Honda CRV', 3),
    (300, 'Hyderabad', 'Honda Civic', 5),
    (300, 'Hyderabad', 'Honda Accord', 8);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * `HAVING` clause referring to column in `GROUP BY`.

-- COMMAND ----------

select * from dealer

-- COMMAND ----------


SELECT city, sum(quantity) AS sum FROM dealer  GROUP BY city order by sum ;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * `HAVING` clause referring to aggregate function.
-- MAGIC

-- COMMAND ----------

SELECT city, sum(quantity) AS sum FROM dealer GROUP BY city HAVING sum(quantity) > 15;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * `HAVING` clause referring to aggregate function by its alias.
-- MAGIC

-- COMMAND ----------

SELECT city, sum(quantity) AS sum FROM dealer GROUP BY city HAVING sum > 15;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * `HAVING` clause referring to a different aggregate function than what is present in
-- MAGIC * `SELECT` list.

-- COMMAND ----------

SELECT city, sum(quantity) AS sum FROM dealer GROUP BY city HAVING max(quantity) > 15;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * `HAVING` clause referring to constant expression.

-- COMMAND ----------

SELECT city, sum(quantity) AS sum FROM dealer GROUP BY city HAVING 1 > 0 ORDER BY city;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * `HAVING` clause without a `GROUP BY` clause.
-- MAGIC * While using HAVING clause without group by, we need to use aggregate functions in expression

-- COMMAND ----------

SELECT id,sum(quantity) AS sum FROM dealer GROUP BY id HAVING  sum(quantity) > 15; 

-- COMMAND ----------

SELECT sum(quantity) AS sum FROM dealer HAVING  sum > 10; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC *  __`HAVING`__ Clause column should be in select list. otherwise it will throw exception
-- MAGIC * __`EXCEPTION`__ : cannot resolve '`quantity`' given input columns: [sum]; 
-- MAGIC

-- COMMAND ----------

SELECT sum(quantity) AS sum FROM dealer HAVING  sum > 10; 

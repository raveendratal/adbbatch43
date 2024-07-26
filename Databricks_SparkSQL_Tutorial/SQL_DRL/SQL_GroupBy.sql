-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Overview
-- MAGIC This notebooks contains complete SPARK SQL / DELTA LAKE SQL  Tutorial
-- MAGIC ###Details
-- MAGIC | Detail Tag | Information
-- MAGIC |----|-----
-- MAGIC | Notebook | SQL DRL GROUP BY Statement details 
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
-- MAGIC ### GROUP BY Clause
-- MAGIC * The GROUP BY clause is used to group the rows based on a set of specified grouping expressions and compute aggregations on the group of rows based on one or more specified aggregate functions. 
-- MAGIC * Spark also supports advanced aggregations to do multiple aggregations for the same input record set via __`GROUPING SETS, CUBE, ROLLUP`__ clauses. 
-- MAGIC * When a `FILTER` clause is attached to an aggregate function, only the matching rows are passed to that function.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Syntax
-- MAGIC
-- MAGIC ```
-- MAGIC GROUP BY group_expression [ , group_expression [ , ... ] ]
-- MAGIC     [ { WITH ROLLUP | WITH CUBE | GROUPING SETS (grouping_set [ , ...]) } ]
-- MAGIC
-- MAGIC GROUP BY GROUPING SETS (grouping_set [ , ...])
-- MAGIC ```

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

select job,sum(sal) as total_Salary from emp  group by 1 having sum(sal)>=5000

-- COMMAND ----------

create table emp location '/user/hive/warehouse/emp'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Sum of quantity per dealership. Group by `id`.
-- MAGIC

-- COMMAND ----------

select sum(quantity),city from dealer  group by city having max(quantity)>=10

-- COMMAND ----------

SELECT id, sum(quantity) FROM dealer GROUP BY id ORDER BY id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Use column position in GROUP by clause.

-- COMMAND ----------

SELECT id, sum(quantity) FROM dealer GROUP BY 1 ORDER BY 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### aggregate_name
-- MAGIC
-- MAGIC * Specifies an aggregate function name (MIN, MAX, COUNT, SUM, AVG, etc.).

-- COMMAND ----------

-- Multiple aggregations.
-- 1. Sum of quantity per dealership.
-- 2. Max quantity per dealership.
SELECT id, sum(quantity) AS sum, max(quantity) AS max ,min(quantity) as min FROM dealer GROUP BY id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DISTINCT
-- MAGIC
-- MAGIC * Removes duplicates in input rows before they are passed to aggregate functions.

-- COMMAND ----------

-- Count the number of distinct dealer cities per car_model.
SELECT car_model, count(DISTINCT city) AS count FROM dealer GROUP BY car_model;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### FILTER
-- MAGIC
-- MAGIC * Filters the input rows for which the boolean_expression in the WHERE clause evaluates to true are passed to the aggregate function; other rows are discarded.

-- COMMAND ----------

-- Sum of only 'Honda Civic' and 'Honda CRV' quantities per dealership.
SELECT id, sum(quantity) FILTER (
            WHERE car_model IN ('Honda Civic', 'Honda CRV')
        ) AS `sum(quantity)` FROM dealer
    GROUP BY id ORDER BY id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Aggregations using multiple sets of grouping columns in a single statement.
-- MAGIC * Following performs aggregations based on four sets of grouping columns.
-- MAGIC *  `1. city, car_model`
-- MAGIC *  `2. city`
-- MAGIC *  `3. car_model`
-- MAGIC *  `4. Empty grouping set.` Returns quantities for all city and car models.

-- COMMAND ----------

SELECT city, car_model, sum(quantity) AS sum FROM dealer
    GROUP BY GROUPING SETS ((city, car_model), (city), (car_model), ())
    ORDER BY city;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Alternate syntax for `GROUPING SETS` in which both `GROUP BY` and `GROUPING SETS`
-- MAGIC * specifications are present.

-- COMMAND ----------


SELECT city, car_model, sum(quantity) AS sum FROM dealer
    GROUP BY city, car_model GROUPING SETS ((city, car_model), (city), (car_model), ())
    ORDER BY city, car_model;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### ROLLUP
-- MAGIC
-- MAGIC * Specifies multiple levels of aggregations in a single statement. 
-- MAGIC * This clause is used to compute aggregations based on multiple grouping sets. ROLLUP is a shorthand for GROUPING SETS. 
-- MAGIC * For example, GROUP BY warehouse, product WITH ROLLUP is equivalent to GROUP BY GROUPING SETS ((warehouse, product), (warehouse), ()). 
-- MAGIC * The N elements of a ROLLUP specification results in N+1 GROUPING SETS.

-- COMMAND ----------

-- Group by processing with `ROLLUP` clause.
-- Equivalent GROUP BY GROUPING SETS ((city, car_model), (city), ())
SELECT city, car_model, sum(quantity) AS sum FROM dealer
    GROUP BY city, car_model WITH ROLLUP
    ORDER BY city, car_model;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CUBE
-- MAGIC
-- MAGIC * CUBE clause is used to perform aggregations based on combination of grouping columns specified in the GROUP BY clause. 
-- MAGIC * CUBE is a shorthand for GROUPING SETS. 
-- MAGIC * For example, GROUP BY warehouse, product WITH CUBE is equivalent to GROUP BY GROUPING SETS ((warehouse, product), (warehouse), (product), ()). 
-- MAGIC * The N elements of a CUBE specification results in 2^N GROUPING SETS.

-- COMMAND ----------

-- Group by processing with `CUBE` clause.
-- Equivalent GROUP BY GROUPING SETS ((city, car_model), (city), (car_model), ())
SELECT city, car_model, sum(quantity) AS sum FROM dealer
    GROUP BY city, car_model WITH CUBE
    ORDER BY city, car_model;

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

--Select the first row in cloumn age
SELECT FIRST(age) FROM person;

-- COMMAND ----------

--Get the first row in cloumn `age` ignore nulls,last row in column `id` and sum of cloumn `id`.
SELECT FIRST(age IGNORE NULLS), LAST(id), SUM(id) FROM person;

-- COMMAND ----------

select * from person

-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Overview
-- MAGIC This notebooks contains complete SPARK SQL / DELTA LAKE SQL  Tutorial
-- MAGIC ###Details
-- MAGIC | Detail Tag | Information
-- MAGIC |----|-----
-- MAGIC | Notebook | SQL DRL WHERE Statement details 
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
-- MAGIC ### WHERE clause
-- MAGIC * The WHERE clause is used to limit the results of the FROM clause of a query or a subquery based on the specified condition.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### `Syntax`
-- MAGIC * __`WHERE boolean_expression`__
-- MAGIC
-- MAGIC * __`Parameters`__
-- MAGIC
-- MAGIC * `boolean_expression`
-- MAGIC
-- MAGIC * Specifies any expression that evaluates to a result type boolean. Two or more expressions may be combined together using the logical operators ( AND, OR ).

-- COMMAND ----------

DROP TABLE IF EXISTS person;
CREATE TABLE person (id INT, name STRING, age INT);
INSERT INTO person VALUES
    (100, 'Ravi', NULL),
    (200, 'Prasad', 36),
    (300, 'Raj', 40),
    (400, 'Sridhar', 34),
    (500, 'Mahesh', 35);

-- COMMAND ----------

--- SELECT -- for projection columns/*
--- FROM - for identifying tables
--- JOIN. -- for joining more than one table 
--- WHERE -- for filtering data based on condition
--- GROUP BY --- grouping data 
--- ORDER BY --- sorting data in Ascending or Descending order 
--- HAVING  --- for aggregated filters
--- Window fuctions (ranking and analytical functions) using PARTITION BY and ORDER BY with Over()
---

-- COMMAND ----------

--create table emp location '/user/hive/warehouse/emp'
--show create table emp

CREATE TABLE spark_catalog.default.emp (
  EMPNO DECIMAL(4,0),
  ENAME VARCHAR(10),
  JOB VARCHAR(9),
  MGR DECIMAL(4,0),
  HIREDATE DATE,
  SAL DECIMAL(7,2),
  COMM DECIMAL(7,2),
  DEPTNO DECIMAL(2,0))
USING delta
LOCATION 'dbfs:/user/hive/warehouse/emp'
TBLPROPERTIES (
  'Type' = 'EXTERNAL',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2')

-- COMMAND ----------

SELECT
 job,sum(sal) as total_salary
 --count(*) as no_of_rows,sum(sal) as sum_Salary,min(sal) as min_salary,max(sal) as max_salary,avg(sal) as avg_salary
FROM
  emp 
where deptno in (10,20)
GROUP BY job
HAVING sum(sal)>5000
order by total_salary desc
  
-- single line comment
/*. comment */

-- COMMAND ----------

-- MAGIC %python
-- MAGIC emp= spark.read.csv("/FileStore/tables/emp.csv",header=True,inferSchema=True)
-- MAGIC emp.dropDuplicates().write.format("delta").saveAsTable("emp")

-- COMMAND ----------

SELECT sum(sal) as total_sal,deptno FROM emp  WHERE total_sal>1000 GROUP BY deptno HAVING total_sal>1000 ORDER BY total_sal desc

-- COMMAND ----------

select sum(sal) as sum_of_sal,job from emp  where sum_of_sal=1000 group by job having sum(sal)>5000 ORDER BY sum_of_sal asc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Logical Operators
-- MAGIC * AND
-- MAGIC * OR
-- MAGIC * NOT

-- COMMAND ----------

select * from person where name like '%r'

-- COMMAND ----------

-- Comparison operator in `WHERE` clause.
SELECT * FROM person WHERE id > 200 ORDER BY age;

-- COMMAND ----------

-- Comparison and logical operators in `WHERE` clause.
SELECT * FROM person WHERE id=200 or name='Ravi'  ORDER BY id;

-- COMMAND ----------

-- IS NULL expression in `WHERE` clause.
SELECT * FROM person WHERE id > 300 OR age IS NULL ORDER BY id;

-- COMMAND ----------

-- Function expression in `WHERE` clause.
SELECT * FROM person WHERE length(name) > 3 ORDER BY id;

-- COMMAND ----------

-- `BETWEEN` expression in `WHERE` clause.
SELECT * FROM person WHERE age BETWEEN 30 AND 38 ORDER BY id;

-- COMMAND ----------

-- Scalar Subquery in `WHERE` clause.
SELECT * FROM person WHERE age > (SELECT avg(age) FROM person);

-- COMMAND ----------

-- Correlated Subquery in `WHERE` clause.
SELECT * FROM person AS parent
    WHERE EXISTS (
        SELECT 1 FROM person AS child
        WHERE parent.id = child.id AND child.age IS NULL
    );

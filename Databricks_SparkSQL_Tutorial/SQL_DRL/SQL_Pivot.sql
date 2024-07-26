-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Overview
-- MAGIC This notebooks contains complete SPARK SQL / DELTA LAKE SQL  Tutorial
-- MAGIC ###Details
-- MAGIC | Detail Tag | Information
-- MAGIC |----|-----
-- MAGIC | Notebook | SQL DRL PIVOT Statement details 
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
-- MAGIC #### PIVOT Clause
-- MAGIC * The __`PIVOT`__ clause is used for data perspective. 
-- MAGIC * We can get the aggregated values based on specific column values, which will be turned to multiple columns used in SELECT clause. The PIVOT clause can be specified after the table name or subquery.
-- MAGIC
-- MAGIC * __`Syntax`__
-- MAGIC
-- MAGIC ```
-- MAGIC PIVOT ( { aggregate_expression [ AS aggregate_expression_alias ] } [ , ... ]
-- MAGIC     FOR column_list IN ( expression_list ) )
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Parameters
-- MAGIC
-- MAGIC * __`aggregate_expression`__
-- MAGIC
-- MAGIC * Specifies an aggregate expression (SUM(a), COUNT(DISTINCT b), etc.).
-- MAGIC
-- MAGIC * __`aggregate_expression_alias`__
-- MAGIC
-- MAGIC * Specifies an alias for the aggregate expression.
-- MAGIC
-- MAGIC * __`column_list`__
-- MAGIC
-- MAGIC * Contains columns in the FROM clause, which specifies the columns we want to replace with new columns. We can use brackets to surround the columns, such as (c1, c2).
-- MAGIC
-- MAGIC * __`expression_list`__
-- MAGIC
-- MAGIC * Specifies new columns, which are used to match values in column_list as the aggregating condition. We can also add aliases for them.

-- COMMAND ----------

DROP TABLE IF EXISTS high_temps;
CREATE TABLE IF NOT EXISTS high_temps(date date,temp int);
insert into high_temps
select '2018-12-22',86 UNION ALL
select '2018-12-22',90 UNION ALL
select '2018-11-23',90 UNION ALL
select '2018-11-23',85 UNION ALL
select '2018-10-24',91 UNION ALL
select '2018-10-24',87 UNION ALL
select '2018-09-22',86 UNION ALL
select '2018-09-22',90 UNION ALL
select '2018-08-23',90 UNION ALL
select '2018-07-24',91 UNION ALL
select '2018-07-22',86 UNION ALL
select '2018-06-23',90 UNION ALL
select '2018-05-24',91 UNION ALL
select '2018-04-25',92 UNION ALL
select '2018-03-26',92 UNION ALL
select '2018-02-27',88 UNION ALL
select '2018-01-28',85 UNION ALL
select '2017-08-29',94 UNION ALL
select '2017-07-22',86 UNION ALL
select '2017-06-23',90 UNION ALL
select '2017-05-24',91 UNION ALL
select '2017-04-25',92 UNION ALL
select '2017-03-26',92 UNION ALL
select '2017-02-27',88 UNION ALL
select '2019-01-28',85 UNION ALL
select '2019-08-29',94 UNION ALL
select '2019-07-22',86 UNION ALL
select '2019-06-23',90 UNION ALL
select '2019-05-24',91 UNION ALL
select '2019-04-25',92 UNION ALL
select '2019-03-26',92 UNION ALL
select '2019-02-27',88 UNION ALL
select '2020-01-28',85 UNION ALL
select '2020-08-29',94 UNION ALL
select '2020-09-30',89

-- COMMAND ----------

select * from high_temps

-- COMMAND ----------

SELECT * FROM (
  SELECT year(date) year, month(date) month, temp
  FROM high_temps
  WHERE date BETWEEN DATE '2015-01-01' AND DATE '2021-08-31'
)
PIVOT (
  CAST(avg(temp) AS DECIMAL(4, 1))
  FOR month in (
    1 JAN, 2 FEB, 3 MAR, 4 APR, 5 MAY, 6 JUN,
    7 JUL, 8 AUG, 9 SEP, 10 OCT, 11 NOV, 12 DEC
  )
)
ORDER BY year DESC

-- COMMAND ----------

SELECT * FROM (
  SELECT year(date) year, month(date) month, temp
  FROM high_temps
  WHERE date BETWEEN DATE '2015-01-01' AND DATE '2021-08-31'
)
PIVOT (
  CAST(avg(temp) AS DECIMAL(4, 1)) avg, max(temp) max
  FOR month in (6 JUN, 7 JUL, 8 AUG, 9 SEP)
)
ORDER BY year DESC

-- COMMAND ----------

--Prepare data for ignore nulls example
DROP TABLE IF EXISTS person;
CREATE TABLE person (id INT, name STRING, age INT, class INT, address STRING);
INSERT INTO person VALUES
    (100, 'Raj', 30, 1, 'Street 1'),
    (200, 'Mahesh', NULL, 1, 'Street 2'),
    (300, 'Sridhar', 30, 3, 'Street 3'),
    (400, 'Prasad', 35, 4, 'Street 4');

-- COMMAND ----------

SELECT * FROM person
    PIVOT (
        SUM(age) AS sum
        FOR (name, age) IN (('Sridhar', 30) AS c1, ('Prasad', 35) AS c2)
    );

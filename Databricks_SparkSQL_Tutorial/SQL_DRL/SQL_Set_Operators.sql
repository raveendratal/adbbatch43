-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Overview
-- MAGIC This notebooks contains complete SPARK SQL / DELTA LAKE SQL  Tutorial
-- MAGIC ###Details
-- MAGIC | Detail Tag | Information
-- MAGIC |----|-----
-- MAGIC | Notebook | SQL DRL SET Operators details 
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
-- MAGIC #### Set operators
-- MAGIC * Combines two input relations into a single one. Spark SQL supports three types of set operators:
-- MAGIC
-- MAGIC * __`UNION`__
-- MAGIC * __`UNION ALL`__
-- MAGIC * __`EXCEPT or MINUS`__
-- MAGIC * __`INTERSECT`__
-- MAGIC
-- MAGIC
-- MAGIC * Input relations must have the same number of columns and compatible data types for the respective columns.
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <img src="http://www1.udel.edu/evelyn/SQL-Class2/joins.jpg" />

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <img src="http://1.bp.blogspot.com/-u1oBip_rYvY/VEI3UMbP7II/AAAAAAAAMLk/YlZvadzvZQY/s1600/datasts.jpg" />

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### EXCEPT
-- MAGIC * EXCEPT and EXCEPT ALL return the rows that are found in one relation but not the other. EXCEPT (alternatively, EXCEPT DISTINCT) takes only distinct rows while EXCEPT ALL does not remove duplicates from the result rows. Note that MINUS is an alias for EXCEPT.
-- MAGIC * __`[ ( ] relation [ ) ] EXCEPT | MINUS [ ALL | DISTINCT ] [ ( ] relation [ ) ]`__

-- COMMAND ----------

-- MAGIC %fs rm -r /user/hive/warehouse/person1

-- COMMAND ----------

-- MAGIC %fs rm -r /user/hive/warehouse/person2

-- COMMAND ----------

DROP TABLE IF EXISTS person1;
CREATE TABLE IF NOT EXISTS person1 (id int , name string);
INSERT INTO person1
    SELECT 1,'Raj Kumar'  UNION ALL
    SELECT 1,'Raj Kumar'  UNION ALL
    SELECT 2,'Ravi Kumar' UNION ALL
    SELECT 3,'Mahesh Kumar' UNION ALL
    SELECT 4,'Prasad Reddy' UNION ALL
    SELECT 4,'Prasad Reddy';

-- COMMAND ----------

DROP TABLE IF EXISTS person2;
CREATE TABLE IF NOT EXISTS person2 (id int , name string);
INSERT INTO person2
    SELECT 4,'Prasad Reddy' UNION ALL
    SELECT 4,'Prasad Reddy' UNION ALL
    SELECT 5,'Sridhar P' UNION ALL
    SELECT 5,'Sridhar P' UNION ALL
    SELECT 6,'Anitha Reddy' UNION ALL
    SELECT 7,'Sindhu K'  UNION ALL
    SELECT 8,'Srinivas N';

-- COMMAND ----------

select * from person1

-- COMMAND ----------

select * from person2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####  Use person1 and person2 tables to demonstrate set operators in this page.
-- MAGIC

-- COMMAND ----------

select 'no name' as name,'no value' as id from person1
union 
select * from person2

-- COMMAND ----------

select * from person1
except 
select * from person2

-- COMMAND ----------

SELECT * FROM person1

-- COMMAND ----------

SELECT * FROM person2

-- COMMAND ----------

-- pyspark union and union all both are same
-- sql union will give unique data and union all will all the data with duplicates.
SELECT id FROM person1 
minus 
SELECT id,name FROM person2;

-- COMMAND ----------

-- pyspark union and union all both are same
-- sql union will give unique data and union all will all the data with duplicates.
SELECT * FROM person1 
except 
SELECT * FROM person2;

-- COMMAND ----------

SELECT * FROM person1
union all
SELECT * FROM person2;

-- COMMAND ----------

SELECT * FROM person1 
EXCEPT ALL 
SELECT * FROM person2;

-- COMMAND ----------

SELECT * FROM person1 
MINUS ALL 
SELECT * FROM person2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### INTERSECT
-- MAGIC * INTERSECT and INTERSECT ALL return the rows that are found in both relations. INTERSECT (alternatively, INTERSECT DISTINCT) takes only distinct rows while INTERSECT ALL does not remove duplicates from the result rows.
-- MAGIC * __`[ ( ] relation [ ) ] INTERSECT [ ALL | DISTINCT ] [ ( ] relation [ ) ]`__

-- COMMAND ----------

SELECT * FROM person1 
INTERSECT 
SELECT * FROM person2;

-- COMMAND ----------

SELECT * FROM person1 
INTERSECT  DISTINCT
SELECT * FROM person2;

-- COMMAND ----------

SELECT * FROM person1 
INTERSECT  ALL
SELECT * FROM person2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### UNION
-- MAGIC * UNION and UNION ALL return the rows that are found in either relation. UNION (alternatively, UNION DISTINCT) takes only distinct rows while UNION ALL does not remove duplicates from the result rows.
-- MAGIC * __`[ ( ] relation [ ) ] UNION [ ALL | DISTINCT ] [ ( ] relation [ ) ]`__

-- COMMAND ----------

SELECT * FROM person1 
UNION
SELECT * FROM person2;

-- COMMAND ----------

SELECT * FROM person1 
UNION DISTINCT
SELECT * FROM person2;

-- COMMAND ----------

SELECT * FROM person1 
UNION ALL
SELECT * FROM person2;

-- COMMAND ----------



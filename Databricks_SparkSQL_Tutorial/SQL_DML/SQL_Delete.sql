-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Overview
-- MAGIC This notebooks contains complete SPARK SQL / DELTA LAKE SQL  Tutorial
-- MAGIC ###Details
-- MAGIC | Detail Tag | Information
-- MAGIC |----|-----
-- MAGIC | Notebook | SQL DELETE Statement details 
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
-- MAGIC #### DELETE FROM (Delta Lake on Databricks)
-- MAGIC * Deletes the rows that match a predicate. When no predicate is provided, deletes all rows.
-- MAGIC
-- MAGIC ##### Parameters
-- MAGIC
-- MAGIC * __table_identifier__
-- MAGIC
-- MAGIC * [database_name.] table_name: A table name, optionally qualified with a database name.
-- MAGIC * delta.`<path-to-table>` : The location of an existing Delta table.
-- MAGIC * __AS alias__
-- MAGIC
-- MAGIC * Define a table alias.
-- MAGIC
-- MAGIC * __WHERE__
-- MAGIC
-- MAGIC * Filter rows by predicate.
-- MAGIC
-- MAGIC * The WHERE predicate supports subqueries, including IN, NOT IN, EXISTS, NOT EXISTS, and scalar subqueries. The following types of subqueries are not supported:
-- MAGIC
-- MAGIC * Nested subqueries, that is, an subquery inside another subquery
-- MAGIC * NOT IN subquery inside an OR, for example, a = 3 OR b NOT IN (SELECT c from t)
-- MAGIC * In most cases, you can rewrite NOT IN subqueries using NOT EXISTS. We recommend using NOT EXISTS whenever possible, as DELETE with NOT IN subqueries can be slow.

-- COMMAND ----------

-- MAGIC %fs ls /user/hive/warehouse/all_employees/

-- COMMAND ----------

drop table IF EXISTS all_employees;
create table IF NOT EXISTS all_employees (id int,name string,loc_id int);
insert into all_employees
select 1,'Ram',1 union all
select 2,'Krishna',1 union all
select 3,'Raj',2 union all
select 4,'Prasad',2 union all
select 5,'Mohan',3 union all
select 6,'Manju',4 union all
select 7,'Sindhu',4 union all
select 8,'Anitha',2 union all
select 9,'Sridhar',1 ;

-- COMMAND ----------

-- DRL (SELECT)
-- DML (insert,update,delete,merge)
-- DDL (CREATE DATABASE,CREATE TABLE,DROP DATABASE,DROP TABLE, RENAME TABLE,ALTER TABLE,TRUNCATE TABLE)

-- COMMAND ----------

truncate table  all_employees --where loc_id=1

-- COMMAND ----------

describe history all_employees

-- COMMAND ----------

select id from locations where name='Hyderabad'

-- COMMAND ----------

select * from all_employees --where loc_id in (select id from locations where name='Hyderabad')

-- COMMAND ----------

show tables 

-- COMMAND ----------

select * from locations

-- COMMAND ----------

drop table IF EXISTS locations;
create table IF NOT EXISTS locations (id int,name string);
insert into locations
select 1,'Bangalore' union all
select 2,'Hyderabad' union all
select 3,'Chennai' union all
select 4,'Pune' union all
select 5,'Mumbai' union all
select 6,'Delhi' union all
select 7,'Vijag' union all
select 8,'Kolkatta'

-- COMMAND ----------

SELECT id FROM locations where name='Chennai'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Deleting records using inline query (sub-query)

-- COMMAND ----------

select *   FROM all_employees
  --WHERE loc_id in (SELECT id FROM locations where name='Chennai')

-- COMMAND ----------

delete FROM all_employees
  WHERE loc_id in (SELECT id FROM locations where name='Chennai')

-- COMMAND ----------

DELETE FROM all_employees
  WHERE loc_id > (SELECT min(id) FROM locations)

-- COMMAND ----------

select * FROM all_employees a
  WHERE    EXISTS (SELECT 'x' FROM locations b WHERE a.loc_id=b.id)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * deleting records using __`NOT IN`__ clause

-- COMMAND ----------

select * FROM all_employees
  WHERE loc_id not IN (SELECT id FROM locations WHERE name='Bangalore')

-- COMMAND ----------



-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Overview
-- MAGIC This notebooks contains complete SPARK SQL / DELTA LAKE SQL  Tutorial
-- MAGIC ###Details
-- MAGIC | Detail Tag | Information
-- MAGIC |----|-----
-- MAGIC | Notebook | SQL Data Update command details
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
-- MAGIC #### UPDATE (Delta Lake on Databricks)
-- MAGIC
-- MAGIC * Updates the column values for the rows that match a predicate. When no predicate is provided, update the column values for all rows.
-- MAGIC
-- MAGIC * __`Syntax : UPDATE table_identifier [AS alias] SET col1 = value1 [, col2 = value2 ...] [WHERE predicate] `__

-- COMMAND ----------

drop table IF EXISTS all_employee;
create table IF NOT EXISTS all_employee (id int,name string,loc_id int);
insert into all_employee
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

select * from all_employee --version as of 3--where id=1

-- COMMAND ----------

update all_employee 
set loc_id=10
--where loc_id=1

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

select * from all_employee a join locations b WHERE a.loc_id = b.id and a.name = 'Raj'

-- COMMAND ----------

update all_employees
set loc_id=2
where id=5

-- COMMAND ----------

select * from   all_employees 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Update with SubQuery examples

-- COMMAND ----------

SELECT min(id) FROM locations

-- COMMAND ----------

select * from all_employees

-- COMMAND ----------

UPDATE all_employees
  SET loc_id = 1
  WHERE loc_id > (SELECT min(id) FROM locations)

-- COMMAND ----------

UPDATE all_employee as e
  SET loc_id = 1
  WHERE EXISTS (SELECT 'x' FROM locations as l where l.id = e.loc_id)

-- COMMAND ----------

UPDATE all_employee as e
  SET loc_id = 0
  WHERE NOT EXISTS (SELECT 'x' FROM locations as l where l.id = e.loc_id)

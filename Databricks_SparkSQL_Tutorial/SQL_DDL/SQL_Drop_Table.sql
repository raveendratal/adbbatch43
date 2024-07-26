-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Overview
-- MAGIC This notebooks contains complete SPARK SQL / DELTA LAKE SQL  Tutorial
-- MAGIC ###Details
-- MAGIC | Detail Tag | Information
-- MAGIC |----|-----
-- MAGIC | Notebook | SQL DDL Drop Table Details
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
-- MAGIC #### Drop Table
-- MAGIC  * Deletes the table and removes the directory associated with the table from the file system if the table is not EXTERNAL table. An exception is thrown if the table does not exist.
-- MAGIC
-- MAGIC * In case of an external table, only the associated metadata information is removed from the metastore database
-- MAGIC
-- MAGIC * __`DROP TABLE [ IF EXISTS ] table_identifier`__
-- MAGIC ##### Parameter
-- MAGIC
-- MAGIC * __`IF EXISTS`__ : If specified, no exception is thrown when the table does not exist.
-- MAGIC
-- MAGIC * table_identifier
-- MAGIC
-- MAGIC * __`[database_name.] table_name `__: A table name, optionally qualified with a database name.
-- MAGIC * __delta.`<path-to-table>`__ : The location of an existing Delta table.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS employeetable(id int,name string);
insert into employeetable
SELECT 1,'Ravi' UNION ALL
SELECT 2,'RAJ' UNION ALL
SELECT 3,'PRASAD' UNION ALL
SELECT 4,'MOHAN';

-- COMMAND ----------

select * from employeetable

-- COMMAND ----------

-- MAGIC %fs ls /user/hive/warehouse/employeetable

-- COMMAND ----------

-- Assumes a table named `employeetable` exists.
DROP TABLE IF EXISTS employeetable;


-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS MYDB;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS mydb.employeetable(id int,name string);
insert into mydb.employeetable
SELECT 1,'Ravi' UNION ALL
SELECT 2,'RAJ' UNION ALL
SELECT 3,'PRASAD' UNION ALL
SELECT 4,'MOHAN';

-- COMMAND ----------


-- Assumes a table named `employeetable` exists in the `userdb` database
DROP TABLE mydb.employeetable;

-- COMMAND ----------

-- Assumes a table named `employeetable` does not exist,Try with IF EXISTS
-- this time it will not throw exception
DROP TABLE IF EXISTS employeetable;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Error: org.apache.spark.sql.AnalysisException: Table or view not found: employeetable;
-- MAGIC (state=,code=0)

-- COMMAND ----------

-- Assumes a table named `employeetable` does not exist.
-- Throws exception
DROP TABLE employeetable;


-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Overview
-- MAGIC This notebooks contains complete SPARK SQL / DELTA LAKE SQL  Tutorial
-- MAGIC ###Details
-- MAGIC | Detail Tag | Information
-- MAGIC |----|-----
-- MAGIC | Notebook | SQL DDL Drop Database Details
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
-- MAGIC #### DROP DATABASE
-- MAGIC
-- MAGIC * Drop a database and delete the directory associated with the database from the file system.
-- MAGIC * An exception will be thrown if the database does not exist in the system.
-- MAGIC
-- MAGIC __`Syntax`__
-- MAGIC
-- MAGIC __`DROP { DATABASE | SCHEMA } [ IF EXISTS ] dbname [ RESTRICT | CASCADE ]`__
-- MAGIC
-- MAGIC
-- MAGIC * Parameters
-- MAGIC
-- MAGIC __`DATABASE | SCHEMA`__
-- MAGIC
-- MAGIC * DATABASE and SCHEMA mean the same thing, either of them can be used.
-- MAGIC
-- MAGIC __`IF EXISTS`__
-- MAGIC
-- MAGIC * If specified, no exception is thrown when the database does not exist.
-- MAGIC
-- MAGIC __`RESTRICT`__
-- MAGIC
-- MAGIC * If specified, will restrict dropping a non-empty database and is enabled by default.
-- MAGIC
-- MAGIC __`CASCADE`__
-- MAGIC
-- MAGIC * If specified, will drop all the associated tables and functions.
-- MAGIC

-- COMMAND ----------

drop database IF EXISTS customer_db CASCADE

-- COMMAND ----------

describe database finance_db

-- COMMAND ----------

-- Create `inventory_db` Database
DROP DATABASE IF EXISTS inventory_db;
CREATE DATABASE IF NOT EXISTS inventory_db COMMENT 'This database is used to maintain Inventory';

-- COMMAND ----------

DROP TABLE IF EXISTS inventory_db.warehouse;
CREATE TABLE IF NOT EXISTS inventory_db.warehouse(id int,location string);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC *  While dropping a database it will raise exception if database is having any tables.
-- MAGIC __`org.apache.hadoop.hive.ql.metadata.HiveException: InvalidOperationException(message:Database inventory_db is not empty. One or more tables exist.)`__

-- COMMAND ----------

-- Drop the database wihtout cascade. it will raise exception if this database contains any tables..
DROP DATABASE inventory_db;

-- COMMAND ----------

-- Drop the database and it's tables
DROP DATABASE inventory_db CASCADE;

-- COMMAND ----------


-- Drop the database using IF EXISTS
DROP DATABASE IF EXISTS inventory_db CASCADE;

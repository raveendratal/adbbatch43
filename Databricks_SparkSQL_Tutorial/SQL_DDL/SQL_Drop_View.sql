-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Overview
-- MAGIC This notebooks contains complete SPARK SQL / DELTA LAKE SQL  Tutorial
-- MAGIC ###Details
-- MAGIC | Detail Tag | Information
-- MAGIC |----|-----
-- MAGIC | Notebook | SQL DDL Drop view details
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
-- MAGIC #### Removes the metadata associated with a specified view from the catalog.
-- MAGIC
-- MAGIC * __`Syntax`__ 
-- MAGIC * __`DROP VIEW [ IF EXISTS ] view_identifier`__ 
-- MAGIC  
-- MAGIC * __`IF EXISTS`__ : If specified, no exception is thrown when the view does not exist.
-- MAGIC
-- MAGIC * __`view_identifier`__ :  The view name to be dropped. The view name may be optionally qualified with a database name.
-- MAGIC
-- MAGIC * __`Syntax: [database_name.] view_name `__

-- COMMAND ----------

select * from all_employee

-- COMMAND ----------

-- Assumes a view named `employeeView` exists.
DROP VIEW IF EXISTS v_employees;

-- COMMAND ----------

-- Assumes a view named `employeeView` exists in the `userdb` database
DROP VIEW userdb.employeeView;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Error: org.apache.spark.sql.AnalysisException: Table or view not found: employeeView;
-- MAGIC (state=,code=0)

-- COMMAND ----------

-- Assumes a view named `employeeView` does not exist.
-- Throws exception
DROP VIEW employeeView;


-- COMMAND ----------

-- Assumes a view named `employeeView` does not exist,Try with IF EXISTS
-- this time it will not throw exception
DROP VIEW IF EXISTS employeeView;

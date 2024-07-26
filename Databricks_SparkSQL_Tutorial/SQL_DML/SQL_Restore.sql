-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Cloud data warehouses (MPP)
-- MAGIC * AZure SQL DWH/Dedicated SQL Pool /Synapse dwh
-- MAGIC * aws Redshift
-- MAGIC * google big query 
-- MAGIC * snowflake. (DWH AS a Service)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Overview
-- MAGIC This notebooks contains complete SPARK SQL / DELTA LAKE SQL  Tutorial
-- MAGIC ###Details
-- MAGIC | Detail Tag | Information
-- MAGIC |----|-----
-- MAGIC | Notebook | SQL Data Restore Command in Delta Lake   
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
-- MAGIC #### RESTORE (Delta Lake on Databricks)
-- MAGIC
-- MAGIC * Restores a Delta table to an earlier state. Restoring to an earlier version number or a timestamp is supported.
-- MAGIC ```
-- MAGIC RESTORE TABLE db.target_table TO VERSION AS OF <version>
-- MAGIC RESTORE TABLE delta.`/data/target/` TO TIMESTAMP AS OF <timestamp>
-- MAGIC ```

-- COMMAND ----------

select * from all_employee

-- COMMAND ----------

DESCRIBE HISTORY all_employee 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("describe history all_employee")
-- MAGIC display(df)

-- COMMAND ----------

CREATE TABLE spark_catalog.default.all_employee (
  id INT,
  name STRING,
  loc_id INT)
USING delta
TBLPROPERTIES (
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2')

-- COMMAND ----------

select * from all_employee

-- COMMAND ----------

restore table all_employee to timestamp as of '2022-10-14T16:47:05.000+0000'

-- COMMAND ----------

restore all_employee VERSION AS OF 4

-- COMMAND ----------

RESTORE all_employee VERSION AS OF 1

-- COMMAND ----------

select * from all_employee

-- COMMAND ----------

RESTORE  Student_delta TIMESTAMP AS OF '2021-05-22T16:59:25.000+0000'

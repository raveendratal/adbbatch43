-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Overview
-- MAGIC This notebooks contains complete SPARK SQL / DELTA LAKE SQL  Tutorial
-- MAGIC ###Details
-- MAGIC | Detail Tag | Information
-- MAGIC | - | - | - | - |
-- MAGIC | Notebook | SQL DDL Alter table details 
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
-- MAGIC #### ALTER TABLE
-- MAGIC * Alters the schema or properties of a table.
-- MAGIC * Delta table schema options
-- MAGIC * In addition to the standard ALTER TABLE options, Delta tables support the options described in this section.
-- MAGIC
-- MAGIC * __`ADD COLUMNS`__
-- MAGIC * __`CHANGE COLUMN`__
-- MAGIC * __`CHANGE COLUMN (Hive syntax)`__
-- MAGIC * __`REPLACE COLUMNS`__
-- MAGIC * __`ADD CONSTRAINT`__
-- MAGIC * __`DROP CONSTRAINT`__

-- COMMAND ----------

-- MAGIC %fs ls /user/hive/warehouse/studentinfo

-- COMMAND ----------

--Create partitioned and bucketed table 

CREATE TABLE IF NOT EXISTS student (id INT, name STRING, age INT)
    USING csv
    PARTITIONED BY (age)
    CLUSTERED BY (Id) INTO 4 buckets;

-- COMMAND ----------

--Create partitioned and bucketed table
--Error in SQL statement: DeltaAnalysisException: Operation not allowed: `Bucketing` is not supported for Delta tables

CREATE TABLE IF NOT EXISTS student (id INT, name STRING, age INT)
    USING delta
    PARTITIONED BY (age) 

-- COMMAND ----------

-- RENAME table
DESC student;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.get("spark.databricks.delta.alterTable.rename.enabledOnAWS")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set("spark.databricks.delta.alterTable.rename.enabledOnAWS","True")
-- MAGIC spark.conf.get("spark.databricks.delta.alterTable.rename.enabledOnAWS")

-- COMMAND ----------

ALTER TABLE Student RENAME TO StudentInfo;

-- After Renaming the table
DESC StudentInfo;

-- COMMAND ----------

drop table StudentInfo

-- COMMAND ----------

-- RENAME partition

SHOW PARTITIONS StudentInfo;

-- COMMAND ----------

ALTER TABLE default.StudentInfo PARTITION (age='10') RENAME TO PARTITION (age='15');

-- After renaming Partition
SHOW PARTITIONS StudentInfo;

-- COMMAND ----------

-- Add new columns to a table
DESC StudentInfo;

-- COMMAND ----------

ALTER TABLE StudentInfo ADD columns (LastName string, DOB timestamp);

-- After Adding New columns to the table
DESC StudentInfo;

-- COMMAND ----------

-- Add a new partition to a table
SHOW PARTITIONS StudentInfo;

-- COMMAND ----------

ALTER TABLE StudentInfo ADD IF NOT EXISTS PARTITION (age=18);

-- After adding a new partition to the table
SHOW PARTITIONS StudentInfo;

-- COMMAND ----------

-- Drop a partition from the table
SHOW PARTITIONS StudentInfo;

-- COMMAND ----------

ALTER TABLE StudentInfo DROP IF EXISTS PARTITION (age=18);

-- After dropping the partition of the table
SHOW PARTITIONS StudentInfo;

-- COMMAND ----------

-- Adding multiple partitions to the table
SHOW PARTITIONS StudentInfo;

-- COMMAND ----------

ALTER TABLE StudentInfo ADD IF NOT EXISTS PARTITION (age=18) PARTITION (age=20);

-- After adding multiple partitions to the table
SHOW PARTITIONS StudentInfo;

-- COMMAND ----------

-- ALTER OR CHANGE COLUMNS
DESC StudentInfo;

-- COMMAND ----------

ALTER TABLE StudentInfo ALTER COLUMN name COMMENT "new comment";

--After ALTER or CHANGE COLUMNS
DESC StudentInfo;

-- COMMAND ----------

-- Change the fileformat
ALTER TABLE loc_orc SET fileformat orc;

-- COMMAND ----------

ALTER TABLE p1 partition (month=2, day=2) SET fileformat parquet;

-- COMMAND ----------

-- Change the file Location
ALTER TABLE dbx.tab1 PARTITION (a='1', b='2') SET LOCATION '/path/to/part/ways'

-- COMMAND ----------

-- SET SERDE/ SERDE Properties
ALTER TABLE test_tab SET SERDE 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe';

-- COMMAND ----------

ALTER TABLE dbx.tab1 SET SERDE 'org.apache.hadoop' WITH SERDEPROPERTIES ('k' = 'v', 'kay' = 'vee')

-- COMMAND ----------

-- SET TABLE PROPERTIES
ALTER TABLE dbx.tab1 SET TBLPROPERTIES ('winner' = 'loser');

-- COMMAND ----------

-- SET TABLE COMMENT Using SET PROPERTIES
ALTER TABLE dbx.tab1 SET TBLPROPERTIES ('comment' = 'A table comment.');

-- COMMAND ----------

-- DROP TABLE PROPERTIES
ALTER TABLE dbx.tab1 UNSET TBLPROPERTIES ('winner');

-- COMMAND ----------

-- Alter TABLE COMMENT Using SET PROPERTIES
ALTER TABLE dbx.tab1 SET TBLPROPERTIES ('comment' = 'This is a new comment.');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Alter View
-- MAGIC ##### RENAME
-- MAGIC * Renames the existing view. If the new view name already exists in the source database, a TableAlreadyExistsException is thrown. This operation does not support moving the views across databases.

-- COMMAND ----------

ALTER VIEW v_employees RENAME TO v_emp

-- COMMAND ----------

select * from v_emp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### SET
-- MAGIC * Sets one or more properties of an existing view. The properties are the key value pairs. If the properties’ keys exist, the values are replaced with the new values. If the properties’ keys do not exist, the key-value pairs are added to the properties.

-- COMMAND ----------

ALTER VIEW view_identifier SET TBLPROPERTIES ( property_key = property_val [ , ... ] )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### ALTER VIEW AS SELECT
-- MAGIC * Changes the definition of a view. The SELECT statement must be valid and the view_identifier must exist.

-- COMMAND ----------

ALTER VIEW v_emp AS select id from studentinfo

-- COMMAND ----------

select * from studentinfo

-- COMMAND ----------

-- Rename only changes the view name.
-- The source and target databases of the view have to be the same.
-- Use qualified or unqualified name for the source and target view.
ALTER VIEW tempdb1.v1 RENAME TO tempdb1.v2;

-- COMMAND ----------

-- Verify that the new view is created.
DESCRIBE TABLE EXTENDED tempdb1.v2; 

-- COMMAND ----------

-- Before ALTER VIEW SET TBLPROPERTIES
DESC TABLE EXTENDED tempdb1.v2; 

-- COMMAND ----------

-- Set properties in TBLPROPERTIES
ALTER VIEW tempdb1.v2 SET TBLPROPERTIES ('created.by.user' = "John", 'created.date' = '01-01-2001' );

-- COMMAND ----------

-- Use `DESCRIBE TABLE EXTENDED tempdb1.v2` to verify
DESC TABLE EXTENDED tempdb1.v2; 

-- COMMAND ----------

-- Remove the key `created.by.user` and `created.date` from `TBLPROPERTIES`
ALTER VIEW tempdb1.v2 UNSET TBLPROPERTIES ('created.by.user', 'created.date');

-- COMMAND ----------

--Use `DESC TABLE EXTENDED tempdb1.v2` to verify the changes
DESC TABLE EXTENDED tempdb1.v2; 

-- COMMAND ----------

-- Change the view definition
ALTER VIEW tempdb1.v2 AS SELECT * FROM tempdb1.v1;

-- COMMAND ----------

-- Use `DESC TABLE EXTENDED` to verify
DESC TABLE EXTENDED tempdb1.v2; 

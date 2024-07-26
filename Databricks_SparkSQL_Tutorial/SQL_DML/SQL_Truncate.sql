-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Overview
-- MAGIC This notebooks contains complete SPARK SQL / DELTA LAKE SQL  Tutorial
-- MAGIC ###Details
-- MAGIC | Detail Tag | Information
-- MAGIC |----|-----
-- MAGIC | Notebook | SQL Data Truncate command details
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
-- MAGIC #### TRUNCATE TABLE
-- MAGIC * Removes all the rows from a table or partition(s). The table must not be a view or an external or temporary table. In order to truncate multiple partitions at once, specify the partitions in partition_spec. If no partition_spec is specified, removes all partitions in the table.

-- COMMAND ----------

TRUNCATE TABLE table_identifier [ partition_spec ]

-- COMMAND ----------

DROP TABLE IF EXISTS Student_csv;
-- Create table Student with partition
CREATE TABLE IF NOT EXISTS Student_csv (name STRING, rollno INT) using csv  PARTITIONED BY (age INT) ;

-- COMMAND ----------

INSERT INTO Student_csv VALUES('Ravi',1,35);
INSERT INTO Student_csv VALUES('Raj',2,35);
INSERT INTO Student_csv VALUES('Prasad',3,36);
INSERT INTO Student_csv VALUES('Reshwanth',4,34);
INSERT INTO Student_csv VALUES('Mohan',5,35);

-- COMMAND ----------

select * from Student_csv --where age=35

-- COMMAND ----------

truncate table Student_csv partition(age=35)
--- update,delete and merge operations only supported in DELTA format. if you have a table with csv,json,xml,orc,avro,parquet... format u can use only insert/append

-- COMMAND ----------

describe history Student_delta 

-- COMMAND ----------

-- MAGIC %fs rm -r dbfs:/user/hive/warehouse/student_delta

-- COMMAND ----------

DROP TABLE IF EXISTS Student_delta;
-- Create table Student with partition
CREATE TABLE IF NOT EXISTS Student_delta (name STRING, rollno INT,age int) ;
INSERT INTO Student_delta VALUES('Ravi',1,35);
INSERT INTO Student_delta VALUES('Raj',2,35);
INSERT INTO Student_delta VALUES('Prasad',3,36);
INSERT INTO Student_delta VALUES('Reshwanth',4,34);
INSERT INTO Student_delta VALUES('Mohan',5,35);

-- COMMAND ----------

describe history Student_delta

-- COMMAND ----------

select * from Student_delta --to  version as of 7

-- COMMAND ----------

-- MAGIC %fs ls /user/hive/warehouse/student/age=34/

-- COMMAND ----------

-- delete entire table data - 1) delete and 2) truncate 
-- if want to delete individual records 1) delete
-- databricks sql/spark sql  -- DELTA supports DML operations (update,delete,merge)
select * from student -- us,uk 

-- COMMAND ----------

DROP TABLE IF EXISTS Student;
-- Create table Student with partition
CREATE TABLE IF NOT EXISTS Student (name STRING, rollno INT) USING CSV PARTITIONED BY (age INT) ;

-- COMMAND ----------

INSERT INTO Student VALUES('Ravi',1,35);
INSERT INTO Student VALUES('Raj',2,35);
INSERT INTO Student VALUES('Prasad',3,36);
INSERT INTO Student VALUES('Reshwanth',4,34);
INSERT INTO Student VALUES('Mohan',5,35);

-- COMMAND ----------

CREATE TABLE spark_catalog.default.student (
  name STRING,
  rollno INT,
  age INT)
USING CSV
PARTITIONED BY (age)

-- COMMAND ----------

select * from student;

-- COMMAND ----------

-- MAGIC %fs ls /user/hive/warehouse/student/

-- COMMAND ----------

select * from  student --partition(age=34)

-- COMMAND ----------

truncate table Student partition(age=35)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Inserting multiple values in partition table 
-- MAGIC * `UNION ALL` doesn't work for inserting into partition table

-- COMMAND ----------

INSERT INTO Student VALUES('Venki',1,35),('Lucky',2,35),('Vikranth',3,36),('Kalam',4,34),('NoName',5,35);

-- COMMAND ----------

-- MAGIC %fs ls /user/hive/warehouse/student/age=34/

-- COMMAND ----------

select * from Student

-- COMMAND ----------

-- MAGIC %md
-- MAGIC __`NOTE FOR DELTA TABLE`__
-- MAGIC * __`TRUNCATE TABLE on Delta tables does not support partition predicates; use DELETE to delete specific partitions`__

-- COMMAND ----------

-- Remove all rows from the table in the specified partition
TRUNCATE TABLE Student partition(age=35);

-- COMMAND ----------

-- After truncate execution, records belonging to partition age=10 are removed
select * FROM Student;

-- COMMAND ----------

DROP TABLE IF EXISTS Student_delta;
-- Create table Student with partition
CREATE TABLE IF NOT EXISTS Student_delta (name STRING, rollno INT) USING DELTA PARTITIONED BY (age INT) ;
INSERT INTO Student_delta VALUES('Ravi',1,35),('Raj',2,35),('Prasad',3,36),('Reshwanth',4,34),('Mohan',5,35);

-- COMMAND ----------

select * from Student_delta

-- COMMAND ----------

-- Remove all rows from the table from all partitions
TRUNCATE TABLE Student_delta;

-- COMMAND ----------

SELECT * FROM Student_delta;

-- COMMAND ----------

DESCRIBE  DETAIL Student_delta

-- COMMAND ----------

DESCRIBE HISTORY Student_delta 

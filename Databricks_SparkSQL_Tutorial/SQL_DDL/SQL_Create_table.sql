-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Overview
-- MAGIC This notebooks contains complete SPARK SQL / DELTA LAKE SQL  Tutorial
-- MAGIC ###Details
-- MAGIC | Detail Tag | Information
-- MAGIC |----|-----
-- MAGIC | Notebook | SQL DDL Create Table Details
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
-- MAGIC ## CREATE DATABASE TABLE
-- MAGIC * The CREATE TABLE statement defines a new table using a Data Source.
-- MAGIC
-- MAGIC * __`Syntax`__
-- MAGIC
-- MAGIC ```
-- MAGIC  CREATE TABLE [ IF NOT EXISTS ] table_identifier
-- MAGIC     [ ( col_name1 col_type1 [ COMMENT col_comment1 ], ... ) ]
-- MAGIC     USING data_source
-- MAGIC     [ OPTIONS ( key1=val1, key2=val2, ... ) ]
-- MAGIC     [ PARTITIONED BY ( col_name1, col_name2, ... ) ]
-- MAGIC     [ CLUSTERED BY ( col_name3, col_name4, ... ) 
-- MAGIC         [ SORTED BY ( col_name [ ASC | DESC ], ... ) ] 
-- MAGIC         INTO num_buckets BUCKETS ]
-- MAGIC     [ LOCATION path ]
-- MAGIC     [ COMMENT table_comment ]
-- MAGIC     [ TBLPROPERTIES ( key1=val1, key2=val2, ... ) ]
-- MAGIC     [ AS select_statement ] 
-- MAGIC ```
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Parameters
-- MAGIC
-- MAGIC __`Specifies a table name, which may be optionally qualified with a database name.`__
-- MAGIC
-- MAGIC __`Syntax: [ database_name. ] table_name`__
-- MAGIC
-- MAGIC __`USING data_source`__
-- MAGIC
-- MAGIC * Data Source is the input format used to create the table. Data source can be CSV, TXT, ORC, JDBC, PARQUET, etc.
-- MAGIC
-- MAGIC __`PARTITIONED BY`__
-- MAGIC
-- MAGIC * Partitions are created on the table, based on the columns specified.
-- MAGIC
-- MAGIC __`CLUSTERED BY`__
-- MAGIC
-- MAGIC * Partitions created on the table will be bucketed into fixed buckets based on the column specified for bucketing.
-- MAGIC
-- MAGIC * `NOTE: Bucketing is an optimization technique that uses buckets (and bucketing columns) to determine data partitioning and avoid data shuffle.`
-- MAGIC
-- MAGIC __`SORTED BY`__
-- MAGIC
-- MAGIC * Specifies an ordering of bucket columns. Optionally, one can use ASC for an ascending order or DESC for a descending order after any column names in the SORTED BY clause. If not specified, ASC is assumed by default.
-- MAGIC
-- MAGIC __`INTO num_buckets BUCKETS`__
-- MAGIC
-- MAGIC * Specifies buckets numbers, which is used in CLUSTERED BY clause.
-- MAGIC
-- MAGIC __`LOCATION`__
-- MAGIC
-- MAGIC * Path to the directory where table data is stored, which could be a path on distributed storage like HDFS, etc.
-- MAGIC
-- MAGIC __`COMMENT`__
-- MAGIC
-- MAGIC * A string literal to describe the table.
-- MAGIC
-- MAGIC __`TBLPROPERTIES`__
-- MAGIC
-- MAGIC * A list of key-value pairs that is used to tag the table definition.
-- MAGIC
-- MAGIC __`AS select_statement`__
-- MAGIC
-- MAGIC * The table is populated using the data from the select statement.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.csv("/FileStore/tables/emp.csv",header=True,inferSchema=True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.format("parquet").mode("overwrite").partitionBy("DEPTNO").save("/tmp/parquet")

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC df.write.format("delta").mode("overwrite").partitionBy("DEPTNO").saveAsTable("emp_delta_data")

-- COMMAND ----------

insert into employees_avro values(1,'ram',4000);
insert into employees_parquet values(1,'ram',5000);
insert into employees_orc values(1,'ram',6000);

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/employees_orc

-- COMMAND ----------

select * from employees
union all
select * from employees_csv
union all
select * from employees_json
union all
select * from employees_avro
union all
select * from employees_parquet
union all
select * from employees_orc

-- COMMAND ----------

--create table employees_csv(id int,name string,sal int) using csv
--create table employees_json(id int,name string,sal int) using json
create table employees_avro(id int,name string,sal int) using avro;
create table employees_parquet(id int,name string,sal int) using parquet;
create table employees_orc(id int,name string,sal int) using orc;

-- COMMAND ----------

select * from employees_csv

-- COMMAND ----------

--- default location tables will call it as internal/managed table

-- COMMAND ----------

-- MAGIC %fs ls /user/hive/warehouse/employees

-- COMMAND ----------

drop table emp_ext;
-- external table -- if we remove table it will remove only metadata from spark catalog. Data will be available on location.
-- external table 
drop table employees; 
--- managed/internal table - if we remove table it will remove metadata from spark catalog and data from storage.
-- community edition if we remove cluster it will remove entire spark catalog.
--

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/batch30/emp_ext

-- COMMAND ----------

create table emp_ext location 'dbfs:/batch30/emp_ext'

-- COMMAND ----------

select * from emp_ext

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/batch30/emp_ext

-- COMMAND ----------

--create table emp_ext(id int, name string) location '/batch30/emp_ext'
insert into emp_ext values(1,'ravi')

-- COMMAND ----------

-- MAGIC %fs ls /user/hive/warehouse/employees/

-- COMMAND ----------

-- YEAR/MONTH/WK

-- COMMAND ----------

select * from employees

-- COMMAND ----------

employeesdrop table if exists employees;
create table employees(id int,name string,sal int,loc string) partitioned by(loc); --using (type of table - default it will create delta)

-- COMMAND ----------

CREATE TABLE spark_catalog.default.employees (
  id INT,
  name STRING,
  sal INT)
USING delta
TBLPROPERTIES (
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2')

-- COMMAND ----------

show create table employees

-- COMMAND ----------

-- csv
-- json 
-- xml 
-- orc
-- parquet
-- avro
-- delta (default databricks format)


-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/customers

-- COMMAND ----------

select * from delta.`/user/hive/warehouse/customers`

-- COMMAND ----------

create table customers location 'dbfs:/user/hive/warehouse/customers'

-- COMMAND ----------

CREATE TABLE spark_catalog.default.customers (
  id INT,
  name STRING)
USING delta
LOCATION 'dbfs:/user/hive/warehouse/customers'
TBLPROPERTIES (
  'Type' = 'EXTERNAL',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2')

-- COMMAND ----------

-- Managed/internal table -- default location (/user/hive/warehouse/table_name)
--- if we drop managed table it will remove data and metadata.
-- External table -- external location
--- if we drop external table it will remove only metadata from spark_catalog. data will be available on that external location.

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/customers

-- COMMAND ----------

select * from customers

-- COMMAND ----------

-- MAGIC %fs ls /user/hive/warehouse/customer_dim

-- COMMAND ----------

select * from customer_dim

-- COMMAND ----------

insert into customer_dim values(1,'reshwanth','Chennai')

-- COMMAND ----------

-- MAGIC %fs ls /user/hive/warehouse/customer_dim

-- COMMAND ----------

CREATE TABLE spark_catalog.default.customer_dim (
  id INT,
  name STRING,
  loc STRING)
USING delta
TBLPROPERTIES (
  'Type' = 'MANAGED',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2')

-- COMMAND ----------

show create table customers

-- COMMAND ----------

show create table customer_dim

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.get("spark.sql.warehouse.dir")

-- COMMAND ----------

CREATE TABLE spark_catalog.default.customer_csv_ext (
  id INT,
  name STRING,
  loc STRING)
USING csv
LOCATION 'dbfs:/customers_ext/csv'

-- COMMAND ----------

--create table customers location '/user/hive/warehouse/customers'
create external table customer_csv_ext(id int NOT NULL,name string NOT NULL,loc string) using csv location '/customers_ext/csv/'
--insert into customer_csv_ext values(1,'Vikranth','Hyderabad') 
--drop table customer_csv_ext

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/customers_ext/csv/

-- COMMAND ----------

-- MAGIC %fs ls /tables/customer_data/

-- COMMAND ----------

-- MAGIC %fs rm -r dbfs:/tables/customer_data

-- COMMAND ----------

drop table customer_data_ext

-- COMMAND ----------

show create table customer_data_ext
CREATE TABLE default.customer_data_ext (
  id INT,
  name STRING,
  loc STRING)
USING CSV
LOCATION 'dbfs:/tables/customer_data'

-- COMMAND ----------

--drop table customer_data_ext
--create  external table customer_data_ext(id int,name string,loc string) USING CSV  location '/tables/customer_data/'
insert into customer_data_ext values(1,'Ravi','Bangalore')

-- COMMAND ----------

--drop table customer_data
--create table customer_data (id int,name string,loc string) USING CSV
--insert into customer_data values(1,'Ravi','Bangalore')

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/customer_data/

-- COMMAND ----------

insert into emp_data_external values (2,'Krishna')

-- COMMAND ----------

create table emp_new as select empno,ename,sal,deptno from emp_delta_data

-- COMMAND ----------

-- MAGIC %fs ls /tmp/emp_csv

-- COMMAND ----------

--create external table emp_data_external (id int,name string) location '/tmp/emp_csv'
drop table emp_data_external

-- COMMAND ----------

show tables

-- COMMAND ----------

CREATE TABLE `default`.`emp_delta_data` (
  `EMPNO` INT,
  `ENAME` STRING,
  `JOB` STRING,
  `MGR` STRING,
  `HIREDATE` STRING,
  `SAL` INT,
  `COMM` STRING,
  `DEPTNO` INT)
USING delta
PARTITIONED BY (DEPTNO)

-- COMMAND ----------

-- MAGIC %fs ls /user/hive/warehouse/emp_partition/DEPTNO=10/

-- COMMAND ----------

show create table emp_parquet_ctas

-- COMMAND ----------

CREATE TABLE emp_CSV USING CSV AS
select * from parquet.`/tmp/emp_parquet`

-- COMMAND ----------

show create table boxes

-- COMMAND ----------

drop table IF EXISTS boxes;

-- COMMAND ----------

DROP TABLE IF EXISTS boxes;
CREATE TABLE IF NOT EXISTS boxes (width INT, length INT, height INT) USING CSV;

-- COMMAND ----------

DROP TABLE IF EXISTS boxes;
CREATE TABLE IF NOT EXISTS boxes
  (width INT, length INT, height INT)
  USING PARQUET
  OPTIONS ('compression'='snappy');

-- COMMAND ----------

show create table boxes

-- COMMAND ----------

DROP TABLE IF EXISTS rectangles;
CREATE TABLE rectangles
  USING PARQUET
  PARTITIONED BY (width)
  CLUSTERED BY (length) INTO 16 buckets
  AS SELECT * FROM boxes;

-- COMMAND ----------

show create table rectangles

-- COMMAND ----------

-- CREATE a HIVE SerDe table using the CREATE TABLE USING syntax.
DROP TABLE IF EXISTS my_table;
CREATE TABLE my_table (name STRING, age INT, hair_color STRING)
  USING HIVE
  OPTIONS(
      INPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileInputFormat',
      OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat',
      SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe')
  PARTITIONED BY (hair_color)
  TBLPROPERTIES ('status'='staging');

-- COMMAND ----------

DROP TABLE IF EXISTS my_table;
CREATE TABLE my_table (name STRING, age INT);


-- COMMAND ----------

DROP TABLE IF EXISTS my_table;
CREATE TABLE my_table (name STRING, age INT)
  COMMENT 'This table is partitioned'
  PARTITIONED BY (hair_color STRING COMMENT 'This is a column comment')
  TBLPROPERTIES ('status'='staging' );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Creating Hive Table

-- COMMAND ----------

DROP TABLE IF EXISTS my_table;
CREATE TABLE my_table (name STRING, age INT)
  COMMENT 'This table specifies a custom SerDe'
  ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
  STORED AS
      INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
      OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';

-- COMMAND ----------

DROP TABLE IF EXISTS my_table;
CREATE TABLE my_table (name STRING, age INT)
  COMMENT 'This table uses the CSV format'
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
  STORED AS TEXTFILE;

-- COMMAND ----------

DROP TABLE IF EXISTS your_table;
CREATE TABLE your_table
  COMMENT 'This table is created with existing data'
  AS SELECT * FROM my_table;

-- COMMAND ----------

DROP TABLE IF EXISTS my_table;
CREATE EXTERNAL TABLE IF NOT EXISTS my_table (name STRING, age INT)
  COMMENT 'This table is created with existing data'
  LOCATION '/tmp/tables/';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Table Like
-- MAGIC * Delta Lake does not support `CREATE TABLE LIKE`. Instead use `CREATE TABLE AS `. See AS.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC __`
-- MAGIC CREATE TABLE [IF NOT EXISTS] [db_name.]table_name1 LIKE [db_name.]table_name2 [LOCATION path]
-- MAGIC `__

-- COMMAND ----------

--Use data source
DROP TABLE IF EXISTS student;
CREATE TABLE IF NOT EXISTS student (id INT, name STRING, age INT) USING CSV;

-- COMMAND ----------

--Use data source
DROP TABLE IF EXISTS student_delta;
CREATE TABLE IF NOT EXISTS student_delta (id INT, name STRING, age INT) USING DELTA;

-- COMMAND ----------

--Use data from another table
drop table if exists student_like;
CREATE TABLE IF NOT EXISTS student_like  like student_delta;

-- COMMAND ----------

show create table student_LIKE

-- COMMAND ----------

--Omit the USING clause, which uses the default data source (parquet by default)
CREATE TABLE IF NOT EXISTS student (id INT, name STRING, age INT);

-- COMMAND ----------

--Specify table comment and properties
CREATE TABLE IF NOT EXISTS student (id INT, name STRING, age INT) USING CSV
    COMMENT 'this is a comment'
    TBLPROPERTIES ('foo'='bar');

-- COMMAND ----------

--Specify table comment and properties with different clauses order
CREATE TABLE IF NOT EXISTS student (id INT, name STRING, age INT) USING CSV
    TBLPROPERTIES ('foo'='bar')
    COMMENT 'this is a comment';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Creating partitioning with Buckets

-- COMMAND ----------

--Create partitioned and bucketed table
CREATE TABLE IF NOT EXISTS student (id INT, name STRING, age INT)
    USING CSV
    PARTITIONED BY (age)
    CLUSTERED BY (Id) INTO 4 buckets;

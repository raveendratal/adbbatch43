-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Overview
-- MAGIC This notebooks contains complete SPARK SQL / DELTA LAKE SQL  Tutorial
-- MAGIC ###Details
-- MAGIC | Detail Tag | Information
-- MAGIC |----|-----
-- MAGIC | Notebook | SQL DDL Create View Details
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
-- MAGIC #### CREATE VIEW
-- MAGIC * Constructs a virtual table that has no physical data based on the result-set of a SQL query. ALTER VIEW and DROP VIEW only change metadata.
-- MAGIC ##### Parameters
-- MAGIC * __`OR REPLACE`__
-- MAGIC
-- MAGIC * If a view of same name already exists, it is replaced.
-- MAGIC
-- MAGIC * __`[ GLOBAL ] TEMPORARY`__
-- MAGIC
-- MAGIC * TEMPORARY views are session-scoped and is dropped when session ends because it skips persisting the definition in the underlying metastore, if any. GLOBAL TEMPORARY views are tied to a system preserved temporary database global_temp.
-- MAGIC
-- MAGIC * __`IF NOT EXISTS`__
-- MAGIC
-- MAGIC * Creates a view if it does not exist.
-- MAGIC
-- MAGIC * view_identifier
-- MAGIC
-- MAGIC * A view name, optionally qualified with a database name.
-- MAGIC
-- MAGIC * __`Syntax: [database_name.] view_name`__
-- MAGIC
-- MAGIC * create_view_clauses
-- MAGIC
-- MAGIC * These clauses are optional and order insensitive. It can be of following formats.
-- MAGIC
-- MAGIC * [ ( column_name [ COMMENT column_comment ], ... ) ] to specify column-level comments.
-- MAGIC * [ COMMENT view_comment ] to specify view-level comments.
-- MAGIC * [ TBLPROPERTIES ( property_name = property_value [ , ... ] ) ] to add metadata key-value pairs.

-- COMMAND ----------

-- MAGIC %fs rm -r /user/hive/warehouse/all_employee

-- COMMAND ----------

drop table IF EXISTS all_employee;
create table IF NOT EXISTS all_employee (id int,name string,working_years int);
insert into all_employee
select 1,'Ram',5 union all
select 2,'Krishna',7 union all
select 3,'Raj',10 union all
select 4,'Prasad',15 union all
select 5,'Mohan',12 union all
select 6,'Manju',4 union all
select 7,'Sindhu',2 union all
select 8,'Anitha',8 union all
select 9,'Sridhar',9 ;

-- COMMAND ----------

create or replace view v_emp as select * from all_employee; -- this will be available at spark catalog. and depending on table or query.
create or replace temporary view tv_emp as select * from all_employee; -- use session level
-- scope of this view is user session. if we purge user memory(clear state), then it will remove user memory and u cannot access this view.
create or replace global temporary view  gtv_emp as select * from all_employee; --- Spark Cluster Session level view
-- scope of this view is spark session and u can access via. global_temp schema.

-- COMMAND ----------

drop view v_emp

-- COMMAND ----------

select * from  global_temp.gtv_emp

-- COMMAND ----------

--create or replace view v_emp as select * from all_employee where working_years>7
select * from all_employee

-- COMMAND ----------

-- MAGIC %fs ls user/hive/warehouse/v_emp

-- COMMAND ----------

CREATE VIEW default.v_emp (
  id,
  name,
  working_years)
TBLPROPERTIES (
  'transient_lastDdlTime' = '1668134968')
AS select * from all_employee where working_years>7

-- COMMAND ----------

select * from v_emp

-- COMMAND ----------

-- MAGIC %python
-- MAGIC name='ravi'
-- MAGIC loc='Bangalore'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC name

-- COMMAND ----------

select * from tv_emp --global_temp.gtv_emp

-- COMMAND ----------

create or replace temporary view tv_emp as select * from all_employee where working_years>7;
create or replace global temporary view gtv_emp as select * from all_employee where working_years>7;

-- COMMAND ----------

create or replace view v_emp as select * from all_employee where working_years>7

-- COMMAND ----------

create or replace view v_emp as select * from all_employee where id>5;
create or replace view temporary tv_emp as select * from all_employee where id>5;
create or replace view global temporary view gtv_emp as select * from all_employee where id>5;

-- COMMAND ----------



-- COMMAND ----------

CREATE VIEW spark_catalog.default.v_employee (
  id,
  name,
  working_years)
TBLPROPERTIES (
  'transient_lastDdlTime' = '1665281475')
AS select * from all_employee where id>5

-- COMMAND ----------



-- COMMAND ----------

select * from v_employee

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### `Difference between View and Temporary View and Global Temporary view`
-- MAGIC * `View` scope is spark catalog. which can be accessed any user in any session , in any cluster.
-- MAGIC * `temporary view `  scope is User level. this view will be available only that user. other users cannot be accessed.
-- MAGIC * `Global Temporary view ` Scope is spark session (cluster). this view is available only at spark cluster level. any one can access within the cluster.

-- COMMAND ----------

select * from tv_emp

-- COMMAND ----------

select * from global_temp.gtv_emp

-- COMMAND ----------

create or replace global temporary view gtv_emp as select * from all_employee where id>5

-- COMMAND ----------

drop view emp_v

-- COMMAND ----------

select * from emp_v

-- COMMAND ----------

create or replace temporary view emp_tv as select * from all_employee where id<5;
create or replace global temporary view emp_gv as select * from all_employee where id<5;
-- all global temporary views will be available or registered under global_temp schema

-- COMMAND ----------

select * from global_temp.emp_gv

-- COMMAND ----------

select id,name from all_employee where working_years>5

-- COMMAND ----------



-- COMMAND ----------

show create table all_employee

-- COMMAND ----------

-- MAGIC %md
-- MAGIC *  Create or replace view will be stored in metadata and its available for all sessions and all users and all clusters.
-- MAGIC * create or replace temporary view will be available in only current session.
-- MAGIC * create or replace global temporary view will be available in other sessions 

-- COMMAND ----------

CREATE OR REPLACE VIEW V_employees as 
SELECT * FROM all_employee
        WHERE working_years > 5;

-- COMMAND ----------

select * from v_employees

-- COMMAND ----------

SELECT * FROM all_employee

-- COMMAND ----------

select * from V_employees

-- COMMAND ----------

-- Create or replace view for `experienced_employee` with comments.
CREATE  VIEW experienced_employee
    (ID COMMENT 'Unique identification number', Name)
    COMMENT 'View for experienced employees'
    AS SELECT id, name FROM all_employee
        WHERE working_years > 5;

-- COMMAND ----------

select * from experienced_employee

-- COMMAND ----------

show create table experienced_employee;

-- COMMAND ----------

select * from experienced_employee

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_v = spark.read.table("experienced_employee")
-- MAGIC display(df_v)

-- COMMAND ----------

-- Create or replace view for `tv_experienced_employee` with comments.
CREATE OR REPLACE TEMPORARY VIEW tv_experienced_employee
    (ID COMMENT 'Unique identification number', Name)
    COMMENT 'View for experienced employees'
    AS SELECT id, name FROM all_employee
        WHERE working_years > 5;

-- COMMAND ----------

select * from tv_experienced_employee

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_tv = spark.read.table("tv_experienced_employee")
-- MAGIC display(df_tv)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Global Temporary View
-- MAGIC * Temporary views in Spark SQL are session-scoped and will disappear if the session that creates it terminates.
-- MAGIC * If you want to have a `temporary view` that is shared among all sessions and keep alive until the Spark application terminates, 
-- MAGIC * when you can create a __`global temporary view`__. `Global temporary view` is tied to a system preserved database __`global_temp`__, 
-- MAGIC * and we must use the qualified name to refer it, e.g. __`SELECT * FROM global_temp.view1`__.
-- MAGIC

-- COMMAND ----------

-- Create or replace view for `gv_experienced_employee` with comments.
CREATE OR REPLACE  GLOBAL TEMPORARY VIEW tv_experienced_employee
    (ID COMMENT 'Unique identification number', Name)
    COMMENT 'View for experienced employees'
    AS SELECT id, name FROM all_employee
        WHERE working_years < 5;

-- COMMAND ----------

select * from global_temp.tv_experienced_employee

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.table("global_temp.gv_experienced_employee")
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df)
-- MAGIC df.createOrReplaceTempView("v_df")

-- COMMAND ----------

select * from v_df

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.createOrReplaceGlobalTempView("gv_df")

-- COMMAND ----------

select * from global_temp.gv_df

-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Overview
-- MAGIC This notebooks contains complete SPARK SQL / DELTA LAKE SQL  Tutorial
-- MAGIC ###Details
-- MAGIC | Detail Tag | Information
-- MAGIC |----|-----
-- MAGIC | Notebook | SQL MERGE Statement details  
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
-- MAGIC #### ` SCDs Type 0 - append /insert only `
-- MAGIC
-- MAGIC * SCD Type 0 scenario is only append. if source is going to send full data or new data only then we can choose this opetion 

-- COMMAND ----------

-- static tables
-- one time load
-- truncate load/ full load tables

-- COMMAND ----------

-- MAGIC %fs rm -r dbfs:/user/hive/warehouse/dept

-- COMMAND ----------

DROP TABLE IF EXISTS DEPT;

CREATE TABLE DEPT
(DEPTNO DECIMAL(2),
DNAME VARCHAR(14),
LOC VARCHAR(13) );

-- COMMAND ----------

TRUNCATE table dept;
INSERT INTO DEPT 
select 10 as deptno, 'ACCOUNTING' as dname, 'NEW YORK' as loc
union all  
select 20, 'RESEARCH', 'DALLAS'
union all
select 30, 'SALES', 'CHICAGO'
union all 
select  40, 'OPERATIONS', 'BOSTON';

-- COMMAND ----------

select * from DEPT
-- SCD Type 0.  append or insert only 

-- COMMAND ----------

-- MAGIC %fs rm -r /user/hive/warehouse/dept_source

-- COMMAND ----------

DROP TABLE IF EXISTS dept_source;

CREATE TABLE dept_source
(DEPTNO DECIMAL(2),
DNAME VARCHAR(14),
LOC VARCHAR(13) );

-- COMMAND ----------

INSERT INTO dept_source 
select 10 as deptno, 'ACCOUNTING' as dname, 'NEW YORK' as loc
union all  
select 40, 'OPERATIONS', 'BOSTON'
union all
select 50, 'IT', 'BANGALORE'
union all 
select  60, 'HRMS', 'HYDERABAD'

-- COMMAND ----------

select * from dept

-- COMMAND ----------

-- insert only
insert into dept 
select * from dept_source where deptno not in (Select deptno from dept)

-- COMMAND ----------

select * from dept_source

-- COMMAND ----------

select * from dept_source src anti join dept as tgt on src.deptno =tgt.deptno
--select * from dept_source where deptno not in (select deptno from dept)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### insert only un-matched data which is not available in target.

-- COMMAND ----------

insert into dept select * from dept_source where deptno not in (select deptno from dept)

-- COMMAND ----------

select * from dept

-- COMMAND ----------



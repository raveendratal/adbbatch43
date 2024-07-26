-- Databricks notebook source
-- MAGIC %fs ls /user/hive/warehouse/emp

-- COMMAND ----------

select * from emp

-- COMMAND ----------

DROP TABLE IF EXISTS EMP;

CREATE TABLE EMP
(EMPNO DECIMAL(4),
ENAME VARCHAR(10),
JOB VARCHAR(9),
MGR DECIMAL(4),
HIREDATE DATE,
SAL DECIMAL(7, 2),
COMM DECIMAL(7, 2),
DEPTNO DECIMAL(2));

-- COMMAND ----------

INSERT INTO emp VALUES ('7369','SMITH','CLERK','7902','1980-12-17','800.00',NULL,'20');
INSERT INTO emp VALUES ('7499','ALLEN','SALESMAN','7698','1981-02-20','1600.00','300.00','30');
INSERT INTO emp VALUES ('7521','WARD','SALESMAN','7698','1981-02-22','1250.00','500.00','30');
INSERT INTO emp VALUES ('7566','JONES','MANAGER','7839','1981-04-02','2975.00',NULL,'20');
INSERT INTO emp VALUES ('7654','MARTIN','SALESMAN','7698','1981-09-28','1250.00','1400.00','30');
INSERT INTO emp VALUES ('7698','BLAKE','MANAGER','7839','1981-05-01','2850.00',NULL,'30');
INSERT INTO emp VALUES ('7782','CLARK','MANAGER','7839','1981-06-09','2450.00',NULL,'10');
INSERT INTO emp VALUES ('7788','SCOTT','ANALYST','7566','1982-12-09','3000.00',NULL,'20');
INSERT INTO emp VALUES ('7839','KING','PRESIDENT',NULL,'1981-11-17','5000.00',NULL,'10');
INSERT INTO emp VALUES ('7844','TURNER','SALESMAN','7698','1981-09-08','1500.00','0.00','30');
INSERT INTO emp VALUES ('7876','ADAMS','CLERK','7788','1983-01-12','1100.00',NULL,'20');
INSERT INTO emp VALUES ('7900','JAMES','CLERK','7698','1981-12-03','950.00',NULL,'30');
INSERT INTO emp VALUES ('7902','FORD','ANALYST','7566','1981-12-03','3000.00',NULL,'20');
INSERT INTO emp VALUES ('7934','MILLER','CLERK','7782','1982-01-23','1300.00',NULL,'10');
INSERT INTO emp VALUES ('1234','RAVI','IT','7782','1985-06-10','1300.00',NULL,'50');

-- COMMAND ----------

-- MAGIC %fs ls  /user/hive/warehouse/dept

-- COMMAND ----------

DROP TABLE IF EXISTS DEPT;

CREATE TABLE DEPT
(DEPTNO DECIMAL(2),
DNAME VARCHAR(14),
LOC VARCHAR(13) );

-- COMMAND ----------


INSERT INTO DEPT 
select 10 as deptno, 'ACCOUNTING' as dname, 'NEW YORK' as loc
union all  
select 20, 'RESEARCH', 'DALLAS'
union all
select 30, 'SALES', 'CHICAGO'
union all 
select  40, 'OPERATIONS', 'BOSTON'

-- COMMAND ----------


INSERT INTO DEPT VALUES (10, 'ACCOUNTING', 'NEW YORK');
INSERT INTO DEPT VALUES (20, 'RESEARCH', 'DALLAS');
INSERT INTO DEPT VALUES (30, 'SALES', 'CHICAGO');
INSERT INTO DEPT VALUES (40, 'OPERATIONS', 'BOSTON');

-- COMMAND ----------

-- MAGIC %fs ls /user/hive/warehouse/emp_bkp

-- COMMAND ----------

-- CTAS
create table emp_bkp1 as select * from emp  
--create table emp_bkp as select * from emp where 1=2

-- COMMAND ----------

select * from emp_bkp1

-- COMMAND ----------

select * from emp_bkp

-- COMMAND ----------

insert into emp_bkp select * from emp

-- COMMAND ----------

select * from emp_bkp

-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %fs  head dbfs:/user/hive/warehouse/customers/part-00000-tid-1231465104377478337-bea2184b-6533-42e6-a958-8fb17a8303e0-267-1-c000.csv

-- COMMAND ----------

CREATE TABLE spark_catalog.default.customers (
  id INT,
  name STRING)
USING csv

-- COMMAND ----------

create table customers(id int,name string) using csv;

-- COMMAND ----------

insert into customers select 1,'ravi'

-- COMMAND ----------

select * from customers;

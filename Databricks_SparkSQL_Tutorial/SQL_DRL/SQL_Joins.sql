-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Overview
-- MAGIC This notebooks contains complete SPARK SQL / DELTA LAKE SQL  Tutorial
-- MAGIC ###Details
-- MAGIC | Detail Tag | Information
-- MAGIC |----|-----
-- MAGIC | Notebook | SQL DRL JOINS Statement details 
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
-- MAGIC #### JOINS
-- MAGIC * Parameters
-- MAGIC
-- MAGIC ##### relation
-- MAGIC
-- MAGIC * The relation to be joined.
-- MAGIC
-- MAGIC ##### join_type
-- MAGIC
-- MAGIC *  The join type.
-- MAGIC
-- MAGIC ##### Syntax:
-- MAGIC
-- MAGIC * __`[ INNER ] | CROSS | LEFT [ OUTER ] | [ LEFT ] SEMI | RIGHT [ OUTER ] | FULL [ OUTER ] | [ LEFT ] ANTI `__
-- MAGIC
-- MAGIC #### join_criteria
-- MAGIC
-- MAGIC * Specifies how the rows from one relation is combined with the rows of another relation.
-- MAGIC
-- MAGIC * __`Syntax: ON boolean_expression | USING ( column_name [ , ... ] )`__
-- MAGIC
-- MAGIC * boolean_expression
-- MAGIC
-- MAGIC * An expression with a return type of Boolean

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <img src="https://storage.googleapis.com/gweb-cloudblog-publish/images/joins_1.max-1600x1600.png" />

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Join Types
-- MAGIC Inner Join
-- MAGIC Returns rows that have matching values in both relations. The default join.
-- MAGIC
-- MAGIC Syntax:
-- MAGIC
-- MAGIC relation [ INNER ] JOIN relation [ join_criteria ]
-- MAGIC
-- MAGIC Left Join
-- MAGIC Returns all values from the left relation and the matched values from the right relation, or appends NULL if there is no match. It is also referred to as a left outer join.
-- MAGIC
-- MAGIC Syntax:
-- MAGIC
-- MAGIC relation LEFT [ OUTER ] JOIN relation [ join_criteria ]
-- MAGIC
-- MAGIC Right Join
-- MAGIC Returns all values from the right relation and the matched values from the left relation, or appends NULL if there is no match. It is also referred to as a right outer join.
-- MAGIC
-- MAGIC Syntax:
-- MAGIC
-- MAGIC relation RIGHT [ OUTER ] JOIN relation [ join_criteria ]
-- MAGIC
-- MAGIC Full Join
-- MAGIC Returns all values from both relations, appending NULL values on the side that does not have a match. It is also referred to as a full outer join.
-- MAGIC
-- MAGIC Syntax:
-- MAGIC
-- MAGIC relation FULL [ OUTER ] JOIN relation [ join_criteria ]
-- MAGIC
-- MAGIC Cross Join
-- MAGIC Returns the Cartesian product of two relations.
-- MAGIC
-- MAGIC Syntax:
-- MAGIC
-- MAGIC relation CROSS JOIN relation [ join_criteria ]
-- MAGIC
-- MAGIC Semi Join
-- MAGIC Returns values from the left side of the relation that has a match with the right. It is also referred to as a left semi join.
-- MAGIC
-- MAGIC Syntax:
-- MAGIC
-- MAGIC relation [ LEFT ] SEMI JOIN relation [ join_criteria ]
-- MAGIC
-- MAGIC Anti Join
-- MAGIC Returns values from the left relation that has no match with the right. It is also referred to as a left anti join.
-- MAGIC
-- MAGIC Syntax:
-- MAGIC
-- MAGIC relation [ LEFT ] ANTI JOIN relation [ join_criteria ]

-- COMMAND ----------

-- MAGIC %fs rm -r /user/hive/warehouse/

-- COMMAND ----------

DROP TABLE IF EXISTS emp;
DROP TABLE IF EXISTS dept;
CREATE TABLE IF NOT EXISTS `emp`(`EMPNO` INT,`ENAME` STRING,`JOB` STRING,`MGR` STRING,`HIREDATE` DATE,`SAL` INT,`COMM` STRING,`DEPTNO` INT) USING delta;
CREATE TABLE IF NOT EXISTS `dept`( `Deptno` INT, `Dname` STRING, `Loc` STRING) USING delta;
INSERT INTO emp VALUES (7369,'SMITH','CLERK',7902,'1980-10-12',800,NULL,20),
(7499,'ALLEN','SALESMAN',7698,'1981-10-02',1600,300,30),
(7521,'WARD','SALESMAN',7698,'1981-12-02',1250,500,30),
(7566,'JONES','MANAGER',7839,'1981-02-04',2975,NULL,20),
(7654,'MARTIN','SALESMAN',7698,'1981-08-09',1250,1400,30),
(7698,'SGR','MANAGER',7839,'1981-01-05',2850,NULL,30),
(7782,'RAVI','MANAGER',7839,'1981-09-06',2450,NULL,10),
(7788,'SCOTT','ANALYST',7566,'1987-09-04',3000,NULL,20),
(7839,'KING','PRESIDENT',NULL,'1981-07-11',5000,NULL,10),
(7844,'TURNER','SALESMAN',7698,'1981-08-09',1500,0,30),
(7876,'ADAMS','CLERK',7788,'1987-03-05',1100,NULL,20),
(7900,'JAMES','CLERK',7698,'1981-03-12',950,NULL,30),
(7902,'FORD','ANALYST',7566,'1981-03-12',3000,NULL,20),
(7934,'MILLER','CLERK',7782,'1982-03-01',1300,NULL,10),
(1234,'SEKHAR','doctor',7777,NULL,667,78,50);
INSERT INTO DEPT VALUES (10,'ACCOUNTING','NEW YORK'),
(20,'RESEARCH','DALLAS'),
(30,'SALES','CHICAGO'),
(40,'OPERATIONS','BOSTON');

-- COMMAND ----------

select * from emp

-- COMMAND ----------

select * from dept

-- COMMAND ----------

select * from emp as e left anti join dept as d on e.deptno=d.deptno

-- COMMAND ----------

select * from emp as e full outer join dept as d on e.deptno=d.deptno

-- COMMAND ----------

---select * from emp

-- COMMAND ----------

select * from emp as e where not exists (select 'x' from dept as d where e.deptno = d.deptno)

-- COMMAND ----------

select * from emp as e where deptno not in (select deptno from dept)

-- COMMAND ----------

-- left semi is same as exists / in
--- left anti is same as not exists / not in
select * from emp as e left anti join dept as d on e.deptno = d.deptno

-- COMMAND ----------

select * from dept

-- COMMAND ----------

select /*+ broadcast(e) */  * from emp as e inner join dept as d on e.deptno = d.deptno

-- COMMAND ----------

select * from emp

-- COMMAND ----------

select * from emp as e where not exists (Select 'x' from dept as d where d.deptno=e.deptno )  --- left semi join

-- COMMAND ----------

-- Use employee and department tables to demonstrate different type of joins.
SELECT * FROM emp  as e left anti join dept as d 
on e.deptno = d.deptno;

-- COMMAND ----------

SELECT * FROM dept;

-- COMMAND ----------

-- Use employee and department tables to demonstrate inner join.
SELECT empno, ename, emp.deptno, dept.dname
    FROM emp INNER JOIN dept ON emp.deptno = dept.deptno;

-- COMMAND ----------

-- Use employee and department tables to demonstrate left join.
SELECT empno, ename, emp.deptno, dept.dname
    FROM emp LEFT JOIN dept ON emp.deptno = dept.deptno;

-- COMMAND ----------

-- Use employee and department tables to demonstrate right join.
SELECT empno, ename, emp.deptno, dept.dname
    FROM emp RIGHT JOIN dept ON emp.deptno = dept.deptno;

-- COMMAND ----------

-- Use employee and department tables to demonstrate full join.
SELECT empno, ename, emp.deptno, dept.dname
    FROM emp FULL JOIN dept ON emp.deptno = dept.deptno;

-- COMMAND ----------

-- Use employee and department tables to demonstrate cross join.
SELECT empno, ename, emp.deptno, dept.dname FROM emp CROSS JOIN dept;

-- COMMAND ----------

-- Use employee and department tables to demonstrate semi join.
SELECT * FROM emp SEMI JOIN dept ON emp.deptno = dept.deptno;

-- COMMAND ----------

-- Use employee and department tables to demonstrate anti join.
SELECT * FROM emp ANTI JOIN dept ON emp.deptno = dept.deptno;

-- COMMAND ----------

create table customers(id int,name string);
insert into customers 
select 1,'Ram'
union all
select 2,'Ravi'
union all
select 3,'Reshwanth'
union all
select 4,'Vikranth'

-- COMMAND ----------

create table source_customers(id int,name string);
insert into source_customers
select 4,'Vikranth'
union all
select 5,'Sindhu'
union all
select 6,'Mahesh'
union all
select 7,'Prasad'

-- COMMAND ----------

select deptno from dept

-- COMMAND ----------

select * from emp as e left anti join dept as d on e.deptno=d.deptno 

-- COMMAND ----------

select * from emp as e left  join dept as d on e.deptno=d.deptno where d.deptno is null

-- COMMAND ----------

--  Left Semi join. /. IN operator.(where clause sub-query) / EXISTS operator (correlated sub-query)
--  Left Anti join. /. NOT IN operator.(where clause sub-query) / NOT EXISTS operator (correlated sub-query)

-- COMMAND ----------

select * from emp where deptno  not in (select deptno from dept)

-- COMMAND ----------

--select * from emp where deptno  in (select deptno from dept)
select * from emp as e where not exists (select 'x' from dept as d where e.deptno = d.deptno)

-- COMMAND ----------

select * from emp as e left anti join dept as d on e.deptno=d.deptno

-- COMMAND ----------

insert into customers select * from source_customers as s left anti join customers as t on s.id = t.id

-- COMMAND ----------

select * from customers

-- COMMAND ----------



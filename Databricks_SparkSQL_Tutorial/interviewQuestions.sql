-- Databricks notebook source
-- MAGIC %python
-- MAGIC for i in spark.catalog.listDatabases():
-- MAGIC   print(spark.catalog.listTables(i.name))

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.csv("/FileStore/tables/emp.csv",header=True,inferSchema=True).distinct()

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC df.write.format("delta").mode("overWrite").saveAsTable("emp")

-- COMMAND ----------

select * from emp

-- COMMAND ----------

select * from 
(select e.*, RANK() over (order by sal DESC) rownum  from emp e)

--where rownum=1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Find 5th highest salary 

-- COMMAND ----------

select * from 
         ( 
  select e.*, DENSE_RANK() over (order by sal DESC) rownum  from emp e
         )
where   rownum=3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Top 3 highest salaried employees from employee table

-- COMMAND ----------

select * from 
( select e.*,DENSE_RANK() over (order by sal DESC) rownum 
from emp e ) 
where rownum <=3 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Find list of employees earning same salary

-- COMMAND ----------

-- Using window functions
SELECT *
FROM
(
SELECT e.*, count(*) Over (Partition BY sal ORDER BY sal) rownum FROM emp e 
) 
WHERE rownum>=2;

-- COMMAND ----------

-- without window function
select e.* from emp e,emp d where e.empno<>d.empno and e.sal==d.sal

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Get more than 2 employees under manager

-- COMMAND ----------

select * from (SELECT e.*, count(mgr) over (partition by mgr) as count from emp e ) where count >= 2 


-- COMMAND ----------

select A.sal from emp A, emp B where A.sal > B.sal

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Find MAX salary employee without using function

-- COMMAND ----------

-- its row by row scanning costly operation. but this is just to satify the requirement.
select * from emp
where sal not in 
(select A.sal from emp A, emp B where A.sal < B.sal) 


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Find no of rows in a table without functions

-- COMMAND ----------

select count(*) from emp

-- COMMAND ----------

SELECT MAX(rownum) FROM
(
SELECT ROW_NUMBER() OVER(ORDER BY empno DESC) as rownum FROM emp
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Find even or odd row from a table.

-- COMMAND ----------

SELECT * FROM
(
SELECT e.*,ROW_NUMBER() OVER(ORDER BY empno DESC) as rownum FROM emp as e
) where rownum%2==1

-- we can go with with MOD function. mod (rownum, 2) <> 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### find list of employees is working more than 35 years

-- COMMAND ----------

select * from emp 
where hiredate < add_months(current_date(),-420);
-- 12*35 

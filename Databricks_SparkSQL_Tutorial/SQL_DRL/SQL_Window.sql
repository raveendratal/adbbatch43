-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Overview
-- MAGIC This notebooks contains complete SPARK SQL / DELTA LAKE SQL  Tutorial
-- MAGIC
-- MAGIC ###Details
-- MAGIC | Detail Tag | Information
-- MAGIC |----|-----
-- MAGIC | Notebook | SQL DRL WINDOW Function Statement details 
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
-- MAGIC ### Window Functions
-- MAGIC * __`Window`__ functions operate on a group of rows, referred to as a window, and calculate a return value for each row based on the group of rows. 
-- MAGIC * Window functions are useful for processing tasks such as calculating a moving average, computing a cumulative statistic, or accessing the value of rows given the relative position of the current row.
-- MAGIC
-- MAGIC * __`Syntax`__
-- MAGIC
-- MAGIC ```
-- MAGIC window_function OVER
-- MAGIC ( [  { PARTITION | DISTRIBUTE } BY partition_col_name = partition_col_val ( [ , ... ] ) ]
-- MAGIC   { ORDER | SORT } BY expression [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [ , ... ]
-- MAGIC   [ window_frame ] )
-- MAGIC   
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Parameters
-- MAGIC
-- MAGIC * __`window_function`__
-- MAGIC
-- MAGIC * __`Ranking Functions`__
-- MAGIC
-- MAGIC * Syntax: RANK | DENSE_RANK | PERCENT_RANK | NTILE | ROW_NUMBER
-- MAGIC
-- MAGIC * __`Analytic Functions`__
-- MAGIC
-- MAGIC * Syntax: CUME_DIST | LAG | LEAD
-- MAGIC
-- MAGIC * __`Aggregate Functions`__
-- MAGIC
-- MAGIC * Syntax: MAX | MIN | COUNT | SUM | AVG | ...
-- MAGIC
-- MAGIC * Please refer to the Built-in Aggregation Functions document for a complete list of Spark aggregate functions.
-- MAGIC
-- MAGIC * __`window_frame`__
-- MAGIC
-- MAGIC * Specifies which row to start the window on and where to end it.
-- MAGIC
-- MAGIC * __`Syntax:`__
-- MAGIC
-- MAGIC * __`{ RANGE | ROWS } { frame_start | BETWEEN frame_start AND frame_end }`__
-- MAGIC
-- MAGIC * frame_start and frame_end have the following syntax:
-- MAGIC
-- MAGIC * __`Syntax:`__
-- MAGIC
-- MAGIC * __`UNBOUNDED PRECEDING | offset PRECEDING | CURRENT ROW | offset FOLLOWING | UNBOUNDED FOLLOWING`__
-- MAGIC
-- MAGIC * offset: specifies the offset from the position of the current row.
-- MAGIC
-- MAGIC * Note: If frame_end is omitted it defaults to CURRENT ROW.

-- COMMAND ----------

-- MAGIC %fs rm -r /user/hive/warehouse/employees

-- COMMAND ----------

DROP TABLE IF EXISTS employees;
CREATE TABLE IF NOT EXISTS employees (name STRING, dept STRING, salary INT, age INT);

INSERT INTO employees VALUES ("Ram", "Sales", 10000, 35),
("Raj", "Sales", 32000, 38),
("Ravi", "Engineering", 21000, 28),
("Raghu", "Sales", 30000, 33),
("Mahesh", "Engineering", 23000, 33),
("Prasad", "Marketing", 29000, 28),
("Sridhar", "Marketing", 35000, 38),
("Reshwanth", "Engineering", 29000, 23),
("Vikranth", "Engineering", 23000, 25);

-- COMMAND ----------

-- MAGIC %fs rm -r /user/hive/warehouse/emp

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

select sum(sal) from emp;

-- COMMAND ----------

-- from clause sub-query or inline view
select * from ( select empno,ename,sal,rank() over(order by sal desc) as rank,
dense_rank() over(order by sal desc) as drank from emp)  where drank=3

-- COMMAND ----------

-- normal filter we can use where 
-- aggregated filter we can use having or from clause sub-query
-- window function filter we can use qualify or from clause sub-query

-- COMMAND ----------

select empno,ename,deptno,sal,
first_value(sal) over(partition by deptno) as first_value,
last_value(sal) over(partition by deptno) as last_value from emp --order by sal desc;

-- COMMAND ----------

select empno,ename,deptno,sal,lead(sal,2,0) over(order by sal desc) as next_salary,
lag(sal,2,0) over(order by sal desc) as prev_salary,
first_value(sal) over() as first_value,
last_value(sal) over() as last_value from emp order by sal desc;

-- COMMAND ----------

select empno,ename,deptno,sal,rank() over(partition by deptno order by sal desc) as rank,
dense_rank() over(partition by deptno order by sal desc) as drank,
row_number() over(partition by deptno order by sal desc) as rowid,
percent_rank() over(partition by deptno order by sal desc) as prank,
ntile(3) over(partition by deptno order by sal desc) as threegroups from emp --qualify rowid=7 --qualify drank=3

-- COMMAND ----------

select empno,ename,job,sal,deptno, sum(sal) over( order by empno) as running_total,
avg(sal) over( order by empno) as running_averages from emp --order by deptno,sal;

-- COMMAND ----------

select empno,ename,sal,deptno,sum(sal) over( order by deptno) as running_totals,
min(sal) over( order by deptno) as running_min,
max(sal) over(  order by deptno) as running_max,
avg(sal) over(  order by deptno) as running_avg,
count(sal) over(  order by deptno) as running_count
from emp

-- COMMAND ----------

count(*) as now_employees,
max(sal) as max_salary,
min(sal) as min_salary,
sum(sal) as total_salary,
avg(sal) as avg_salaryselect deptno,job,
count(*) as now_employees,
max(sal) as max_salary,
min(sal) as min_salary,
sum(sal) as total_salary,
avg(sal) as avg_salary
from emp group by 1

-- COMMAND ----------

select * from (select empno,ename,deptno,sal,
rank() over(order by sal desc) as sal_rank,
dense_rank() over(order by sal desc) as sal_denserank,
row_number() over(order by sal desc) as row_num
from emp) as new_table where row_num=7

--- rowid 

-- COMMAND ----------

select empno,ename,deptno,sal,
rank() over(order by sal desc) as sal_rank,
dense_rank() over(order by sal desc) as sal_denserank,
row_number() over(order by sal desc) as row_num,
percent_rank() over(order by sal desc) as percent,
ntile(4) over(order by sal desc) as ntilerank
from emp

-- COMMAND ----------

create table emp location '/user/hive/warehouse/emp'

-- COMMAND ----------

select empno,ename,job,deptno,sal,sum(sal) over(order by sal) as cum from emp  

-- COMMAND ----------

partition by deptnoselect empno,
ename,
sal,
job,
deptno,
rank() over(partition by deptno order by sal desc) as rank_sal, 
dense_rank() over(partition by deptno order by sal desc) as denserank_sal,
row_number() over(partition by deptno order by sal desc) as rowid,
ntile(2) over(partition by deptno order by sal desc) as ntile_rank,
percent_rank() over(partition by deptno order by sal desc) as percent_rank
from emp

-- COMMAND ----------

select * from 
(select empno,ename,sal,rank() over(order by sal desc) as rank_sal,dense_rank() over(order by sal desc) as denserank_sal from emp) where denserank_sal=3



-- COMMAND ----------

SELECT * FROM employees;


-- COMMAND ----------

SELECT name, dept,salary, RANK() OVER (PARTITION BY dept ORDER BY salary desc) AS rank,
DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary desc) AS dense_rank,
ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary desc) AS row_number FROM employees;


-- COMMAND ----------

select * from employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC * `currentRow` : to specify a current value in a row.
-- MAGIC * `Preceding` : previous row
-- MAGIC * `following` : next row
-- MAGIC * `unboundedPreceding` : This can be used to have an unbounded start for the window.
-- MAGIC * `unboundedFollowing` : This can be used to have an unbounded end for the window.
-- MAGIC * `RANGE` , `ROWS` , `RANGE BETWEEN`, and `ROWS BETWEEN` for window frame types
-- MAGIC * `UNBOUNDED PRECEDING`, `UNBOUNDED FOLLOWING`, `CURRENT ROW` for frame bounds.
-- MAGIC
-- MAGIC

-- COMMAND ----------

SELECT i,v,SUM(v) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as win
  FROM (VALUES(1,1),(2,2),(3,3),(4,4)) t(i,v)

-- COMMAND ----------

SELECT i,v,SUM(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) as win
  FROM (VALUES(1,1),(2,2),(3,5),(4,7),(5,NULL),(6,NULL)) t(i,v)

-- COMMAND ----------

SELECT i,v,SUM(v) OVER (ORDER BY i RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) as win
  FROM (VALUES(1,1),(2,2),(3,5),(4,7),(5,NULL),(6,NULL)) t(i,v)

-- COMMAND ----------

SELECT i,v,SUM(v) OVER (ORDER BY i RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as win
  FROM (VALUES(1,1),(2,2),(3,5),(4,7),(5,NULL),(6,NULL)) t(i,v)

-- COMMAND ----------

SELECT i,v,SUM(v) OVER (ORDER BY i RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as win
  FROM (VALUES(1,1),(2,2),(3,5),(4,7),(5,NULL),(6,NULL)) t(i,v)

-- COMMAND ----------

SELECT name, dept,salary, RANK() OVER (PARTITION BY dept ORDER BY salary UNBOUNDED PRECEDING AND CURRENT ROW ) AS dense_rank FROM employees;

-- COMMAND ----------

SELECT name, dept,salary, DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary ROWS BETWEEN
    UNBOUNDED PRECEDING AND CURRENT ROW) AS dense_rank FROM employees;

-- COMMAND ----------

SELECT name, dept, age, CUME_DIST() OVER (PARTITION BY dept ORDER BY age
    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cume_dist FROM employees;

-- COMMAND ----------


SELECT name, dept, salary, MIN(salary) OVER (PARTITION BY dept ORDER BY salary) AS min
    FROM employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * __`LAG`__ will get previous record
-- MAGIC * __`LEAD`__ will get next record

-- COMMAND ----------

select * from employees

-- COMMAND ----------

SELECT name, salary,dept,
    LAG(salary,1,0) OVER (PARTITION BY dept ORDER BY salary) AS lag,
    LEAD(salary,1, 0) OVER (PARTITION BY dept ORDER BY salary) AS lead
    FROM employees;
    

-- COMMAND ----------

DROP TABLE IF EXISTS product_revenue;
CREATE TABLE IF NOT EXISTS product_revenue (product STRING, category STRING, revenue INT);

INSERT INTO product_revenue VALUES ("apple", "mobile", 10000),
("apple", "laptop", 32000),
("lenevo", "mobile", 21000),
("apple", "tablet", 30000),
("apple", "laptop", 23000),
("dell", "mobile", 29000),
("apple", "tablet", 35000),
("dell", "mobile", 29000),
("dell", "laptop", 23000);

-- COMMAND ----------

select product,category,revenue,rank() over(partition by category order by revenue desc) as ranks from product_revenue

-- COMMAND ----------

select * from product_revenue --order by product,revenue

-- COMMAND ----------

SELECT
  product,
  category,
  revenue,
  rank
FROM (
  SELECT
    product,
    category,
    revenue,
    rank() OVER (PARTITION BY category ORDER BY revenue DESC) as rank
  FROM product_Revenue) tmp
WHERE
  rank = 2

-- COMMAND ----------

select product,category,revenue,rank() over(partition By product Order By revenue desc) as rank,
dense_rank() over(partition By product Order By revenue desc) as dense_rank,
ntile(2) over(partition By product Order By revenue desc) as ntile,
percent_rank() over(partition By product Order By revenue desc) as p_rank,
row_number() over(partition By product Order By revenue desc) as row_number
from product_Revenue

-- COMMAND ----------

select product,category,revenue,
First_value(revenue) over( Order By category desc) as first_value,
last_value(revenue) over(Order By category desc) as last_value
from product_Revenue

-- COMMAND ----------



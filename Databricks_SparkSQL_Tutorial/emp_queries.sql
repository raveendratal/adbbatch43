-- Databricks notebook source
DROP TABLE IF EXISTS dept;
DROP TABLE IF EXISTS salgrade;
DROP TABLE IF EXISTS emp;

-- COMMAND ----------

-- MAGIC %fs rm -r /user/hive/warehouse/salgrade

-- COMMAND ----------

CREATE TABLE salgrade(
grade int not null,
losal decimal(10,2),
hisal decimal(10,2));

-- COMMAND ----------

-- MAGIC %fs rm -r /user/hive/warehouse/dept

-- COMMAND ----------

CREATE TABLE dept(
deptno int not null  ,
dname varchar(50) not null,
location varchar(50) not null);

-- COMMAND ----------

-- MAGIC %fs rm -r /user/hive/warehouse/emp

-- COMMAND ----------

CREATE TABLE emp(
empno int not null  ,
ename varchar(50) not null,
job varchar(50) not null,
mgr int ,
hiredate date,
sal decimal(10,2),
comm decimal(10,2),
deptno int  not null);

-- COMMAND ----------

insert into dept values (10,'Accounting','New York');
insert into dept values (20,'Research','Dallas');
insert into dept values (30,'Sales','Chicago');
insert into dept values (40,'Operations','Boston');

-- COMMAND ----------


insert into emp values (7369,'SMITH','CLERK',7902,'1993-6-13',800,0.00,20);
insert into emp values (7499,'ALLEN','SALESMAN',7698,'1998-8-15',1600,300,30);
insert into emp values (7521,'WARD','SALESMAN',7698,'1996-3-26',1250,500,30);
insert into emp values (7566,'JONES','MANAGER',7839,'1995-10-31',2975,null,20);
insert into emp values (7698,'BLAKE','MANAGER',7839,'1992-6-11',2850,null,30);
insert into emp values (7782,'CLARK','MANAGER',7839,'1993-5-14',2450,null,10);
insert into emp values (7788,'SCOTT','ANALYST',7566,'1996-3-5',3000,null,20);
insert into emp values (7839,'KING','PRESIDENT',null,'1990-6-9',5000,0,10);
insert into emp values (7844,'TURNER','SALESMAN',7698,'1995-6-4',1500,0,30);
insert into emp values (7876,'ADAMS','CLERK',7788,'1999-6-4',1100,null,20);
insert into emp values (7900,'JAMES','CLERK',7698,'2000-6-23',950,null,30);
insert into emp values (7934,'MILLER','CLERK',7782,'2000-1-21',1300,null,10);
insert into emp values (7902,'FORD','ANALYST',7566,'1997-12-5',3000,null,20);
insert into emp values (7654,'MARTIN','SALESMAN',7698,'1998-12-5',1250,1400,30);


insert into salgrade values (1,700,1200);
insert into salgrade values (2,1201,1400);
insert into salgrade values (3,1401,2000);
insert into salgrade values (4,2001,3000);
insert into salgrade values (5,3001,99999);

-- COMMAND ----------

show tables;

-- COMMAND ----------

select * from dept

-- COMMAND ----------

select * from emp

-- COMMAND ----------

select * from salgrade

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Get Department name wise no of employees.

-- COMMAND ----------

select dname, count(*) count_of_employees 
from dept, emp 
where dept.deptno = emp.deptno 
group by DNAME 
order by 2 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### SQL query to select the employees getting salary greater than the average salary of the department that are working in

-- COMMAND ----------

SELECT *
FROM emp E1,
     (
       SELECT deptno, AVG(SAL) as avg_sal 
       FROM emp GROUP BY deptno
     )    E2
WHERE E1.deptno = E2.deptno
AND   E1.SAL > E2.avg_sal;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### same requirement with Correlated sub query

-- COMMAND ----------

SELECT EMPNO, ENAME, DEPTNO, SAL
  FROM emp e1
 WHERE SAL > (
              SELECT avg(SAL) 
              FROM emp e2 
              WHERE e2.deptno = e1.deptno
              );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### same requirement with window functioin

-- COMMAND ----------

SELECT e.*
FROM  (
        SELECT E1.*, 
               avg(SAL) over(partition by DEPTNO) as avgsalary
        FROM emp E1
       ) e
 where e.SAL > e.avgsalary;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Query to find the name of the top level manager emp table.

-- COMMAND ----------

SELECT *
  FROM emp
  WHERE EMPNO IN (SELECT MGR FROM EMP )
  AND MGR IS NULL 

-- COMMAND ----------

SELECT ENAME
  FROM emp E1
  WHERE EXISTS  (SELECT 1 FROM EMP E2 WHERE E1.EMPNO=E2.MGR )
  AND E1.MGR IS NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### List the employees who are joined in the year between 1990 to 1995

-- COMMAND ----------

-- extract year from hire date using extract function
select * from emp where extract(year from hiredate) between 1990 and 1995

-- COMMAND ----------

-- using date format function getting year from hiredate
select * from emp where date_format(hiredate,'yyyy') between 1990 and 1995

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Calculate the sum of all the employees salaries at each manager

-- COMMAND ----------


select mgr,sum(sal) as total_salary from emp group by mgr

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Find the average commission, counting only those employees who receive a commission.

-- COMMAND ----------

select avg(comm)  from emp where comm is not null

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Find the average commission, counting employees who do not receive a commission as if they received a commission of 0.

-- COMMAND ----------

select avg(nvl(comm,0))  from emp 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### What is the salary paid to the lowest-paid employee?

-- COMMAND ----------

select * from emp where sal=(select min(sal) from emp)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Find the total annual salary for all employees

-- COMMAND ----------

select empno,ename,12*(sal+nvl(comm,0)) as total_annual_salary from emp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### List all employees with no manager.

-- COMMAND ----------

select * from emp where mgr is null

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### List all employees who are not managers.

-- COMMAND ----------

select * from emp e
where not exists 
(select 1 from emp m where m.mgr =e.empno);

-- COMMAND ----------

select * from emp e left anti join emp m on e.empno = m.mgr

-- COMMAND ----------

SELECT E.empno,e.mgr,E.ename,e.sal,e.deptno FROM emp E WHERE empno not in 
   (SELECT mgr FROM emp where mgr is not null)

-- COMMAND ----------



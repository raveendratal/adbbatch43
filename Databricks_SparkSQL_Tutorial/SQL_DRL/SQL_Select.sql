-- Databricks notebook source
-- MAGIC %md
-- MAGIC ```
-- MAGIC SELECT [ hints ] [ ALL | DISTINCT ] { named_expression | star_clause } [, ...]
-- MAGIC   FROM from_item [, ...]
-- MAGIC   [ LATERAL VIEW clause ]
-- MAGIC   [ PIVOT clause ]
-- MAGIC   [ WHERE clause ]
-- MAGIC   [ GROUP BY clause ]
-- MAGIC   [ HAVING clause]
-- MAGIC   [ QUALIFY clause ]
-- MAGIC
-- MAGIC from_item
-- MAGIC { table_name [ TABLESAMPLE clause ] [ table_alias ] |
-- MAGIC   JOIN clause |
-- MAGIC   [ LATERAL ] table_valued_function [ table_alias ] |
-- MAGIC   VALUES clause |
-- MAGIC   [ LATERAL ] ( query ) [ TABLESAMPLE clause ] [ table_alias ] }
-- MAGIC
-- MAGIC named_expression
-- MAGIC    expression [ column_alias ]
-- MAGIC
-- MAGIC star_clause
-- MAGIC    [ { table_name | view_name } . ] * [ except_clause ]
-- MAGIC
-- MAGIC except_clause
-- MAGIC    EXCEPT ( { column_name | field_name } [, ...] )
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ```
-- MAGIC Parameters
-- MAGIC hints
-- MAGIC
-- MAGIC Hints help the Databricks SQL optimizer make better planning decisions. Databricks SQL supports hints that influence selection of join strategies and repartitioning of the data.
-- MAGIC
-- MAGIC ALL
-- MAGIC
-- MAGIC Select all matching rows from the relation. Enabled by default.
-- MAGIC
-- MAGIC DISTINCT
-- MAGIC
-- MAGIC Select all matching rows from the relation after removing duplicates in results.
-- MAGIC
-- MAGIC named_expression
-- MAGIC
-- MAGIC An expression with an optional assigned name.
-- MAGIC
-- MAGIC expression
-- MAGIC
-- MAGIC A combination of one or more values, operators, and SQL functions that evaluates to a value.
-- MAGIC
-- MAGIC column_alias
-- MAGIC
-- MAGIC An optional column identifier naming the expression result. If no column_alias is provided Databricks SQL derives one.
-- MAGIC
-- MAGIC star_clause
-- MAGIC
-- MAGIC A shorthand to name all the referencable columns in the FROM clause. The list of columns is ordered by the order of from_items and the order of columns within each from_item.
-- MAGIC
-- MAGIC The _metadata column is not included this list. You must reference it explicitly.
-- MAGIC
-- MAGIC table_name
-- MAGIC
-- MAGIC If present limits the columns to be named to those in the specified referencable table.
-- MAGIC
-- MAGIC view_name
-- MAGIC
-- MAGIC If specified limits the columns to be expanded to those in the specified referencable view.
-- MAGIC
-- MAGIC except_clause
-- MAGIC
-- MAGIC Optionally prunes columns or fields from the referencable set of columns identified in the select_star clause.
-- MAGIC
-- MAGIC column_name
-- MAGIC
-- MAGIC A column that is part of the set of columns that you can reference.
-- MAGIC
-- MAGIC field_name
-- MAGIC
-- MAGIC A reference to a field in a column of the set of columns that you can reference. If you exclude all fields from a STRUCT, the result is an empty STRUCT.
-- MAGIC
-- MAGIC Each name must reference a column included in the set of columns that you can reference or their fields. Otherwise, Databricks SQL raises a UNRESOLVED_COLUMN error. If names overlap or are not unique, Databricks SQL raises an EXCEPT_OVERLAPPING_COLUMNS error.
-- MAGIC
-- MAGIC from_item
-- MAGIC
-- MAGIC A source of input for the SELECT. One of the following:
-- MAGIC
-- MAGIC table_name
-- MAGIC
-- MAGIC Identifies a table that may contain a temporal specification. See Query an older snapshot of a table (time travel) for details.
-- MAGIC
-- MAGIC view_name
-- MAGIC
-- MAGIC Identifies a view.
-- MAGIC
-- MAGIC JOIN
-- MAGIC
-- MAGIC Combines two or more relations using a join.
-- MAGIC
-- MAGIC [LATERAL] table_valued_function
-- MAGIC
-- MAGIC Invokes a table function. To refer to columns exposed by a preceding from_item in the same FROM clause you must specify LATERAL.
-- MAGIC
-- MAGIC VALUES
-- MAGIC
-- MAGIC Defines an inline table.
-- MAGIC
-- MAGIC [LATERAL] ( query )
-- MAGIC
-- MAGIC Computes a relation using a query. A query prefixed by LATERAL may reference columns exposed by a preceding from_item in the same FROM clause. Such a construct is called a correlated or dependent query.
-- MAGIC
-- MAGIC TABLESAMPLE
-- MAGIC
-- MAGIC Optionally reduce the size of the result set by only sampling a fraction of the rows.
-- MAGIC
-- MAGIC table_alias
-- MAGIC
-- MAGIC Optionally specifies a label for the from_item. If the table_alias includes column_identifiers their number must match the number of columns in the from_item.
-- MAGIC
-- MAGIC PIVOT
-- MAGIC
-- MAGIC Used for data perspective; you can get the aggregated values based on specific column value.
-- MAGIC
-- MAGIC LATERAL VIEW
-- MAGIC
-- MAGIC Used in conjunction with generator functions such as EXPLODE, which generates a virtual table containing one or more rows. LATERAL VIEW applies the rows to each original output row.
-- MAGIC
-- MAGIC WHERE
-- MAGIC
-- MAGIC Filters the result of the FROM clause based on the supplied predicates.
-- MAGIC
-- MAGIC GROUP BY
-- MAGIC
-- MAGIC The expressions that are used to group the rows. This is used in conjunction with aggregate functions (MIN, MAX, COUNT, SUM, AVG) to group rows based on the grouping expressions and aggregate values in each group. When a FILTER clause is attached to an aggregate function, only the matching rows are passed to that function.
-- MAGIC
-- MAGIC HAVING
-- MAGIC
-- MAGIC The predicates by which the rows produced by GROUP BY are filtered. The HAVING clause is used to filter rows after the grouping is performed. If you specify HAVING without GROUP BY, it indicates a GROUP BY without grouping expressions (global aggregate).
-- MAGIC
-- MAGIC QUALIFY
-- MAGIC
-- MAGIC The predicates that are used to filter the results of window functions. To use QUALIFY, at least one window function is required to be present in the SELECT list or the QUALIFY clause.
-- MAGIC ```

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

select * from dept

-- COMMAND ----------

SELECT empno,ename,sal as salary,comm,nvl(comm,0) as new_comm, sal+coalesce(null,null,null,comm,0) as total_Salary 
FROM emp as e

-- how to convert null values to actual values in sql nvl(col1,col2) function , coalesce 

-- COMMAND ----------

show tables

-- COMMAND ----------

describe emp

-- COMMAND ----------

-- column alias names
-- NULL is an un-defined value-
--- conversion functions. null value actual value
-- NVL(arg1,arg2) if arg1 is null then return arg2. if arg1 is not null then return arg1.
-- COALESCE (arg1,arg2,arg3........) it will return first not null.
select
  empno as EMPLOYEE_NUMBER,
  ename as EMPLOYEE_NAME,
  job,
  sal as SALARY,
  comm,
  sal + nvl(comm, 0) as total_salary,
  nvl(comm, 0) as newcomm,
  coalesce(NULL, NULL, NULL, comm, 0) as new_comm1
from
  emp

-- COMMAND ----------

-- aggregations -- min,max,avg,count,stdev.....
-- SELECT -- for projection (displaying columns)
-- FROM -- for reading data from a tables
-- WHERE -- for filtering data based on condition 
-- ORDER BY -- for sorting data in Ascending or Descending order 
-- GROUP BY -- for group data (aggregated and non-aggregated data)
-- HAVING  -- for aggregation filters

-- COMMAND ----------

select count(*) as no_employees,deptno from emp group by deptno having count(*)>=5

-- COMMAND ----------

select * from emp  where min(sal)>5000

-- COMMAND ----------



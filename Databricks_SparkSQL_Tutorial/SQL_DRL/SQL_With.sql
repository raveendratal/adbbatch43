-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Overview
-- MAGIC This notebooks contains complete SPARK SQL / DELTA LAKE SQL  Tutorial
-- MAGIC ###Details
-- MAGIC | Detail Tag | Information
-- MAGIC |----|-----
-- MAGIC | Notebook | SQL DRL WITH Clause Statement details 
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
-- MAGIC #### Common table expression (CTE)
-- MAGIC * Defines a temporary result set that a you can reference possibly multiple times within the scope of a SQL statement. A CTE is used mainly in a SELECT statement.
-- MAGIC * __Syntax__
-- MAGIC * `WITH common_table_expression [ , ... ]`
-- MAGIC * `expression_name [ ( column_name [ , ... ] ) ] [ AS ] ( query )`

-- COMMAND ----------

-- CTE with multiple column aliases
WITH t(x, y) AS (SELECT 1, 2)
SELECT * FROM t WHERE x = 1 AND y = 2;

-- COMMAND ----------

-- CTE in CTE definition
WITH t AS (
    WITH t2 AS (SELECT 1)
    SELECT * FROM t2
)
SELECT * FROM t;

-- COMMAND ----------

with t_emp as (select empno,ename,sal,dense_rank() over(order by sal desc) as drank from emp)
select empno,sal,drank from t_emp where drank=5;

-- COMMAND ----------

select inlineview.drank,inlineview.empno from (select empno,ename,sal,dense_rank() over(order by sal desc) as drank from emp) as inlineview

-- COMMAND ----------

select max(id),min(id),avg(id) from (
with sample as  (SELECT 1 as id union all select 2  union all select 3) 
select * from sample)

-- COMMAND ----------

-- CTE in subquery
SELECT max(c),min(c),avg(c),count(c) FROM (
    WITH t(c) AS (SELECT 1 union all select 2 union all select 3)
    SELECT * FROM t
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC *  more than one row returned by a subquery used as an expression will raise exception

-- COMMAND ----------

-- CTE in subquery expression
SELECT (
    WITH t AS (SELECT 1 )
    SELECT * FROM t
);

-- COMMAND ----------

-- CTE in CREATE VIEW statement
CREATE OR REPLACE VIEW v AS
 WITH t(a, b, c, d) AS (SELECT 1, 2, 3, 4)
 select * from t;

-- COMMAND ----------

SELECT * FROM v;

-- COMMAND ----------

-- If name conflict is detected in nested CTE, then AnalysisException is thrown by default.
-- SET spark.sql.legacy.ctePrecedencePolicy = CORRECTED (which is recommended),
-- inner CTE definitions take precedence over outer definitions.
SET spark.sql.legacy.ctePrecedencePolicy = CORRECTED;
WITH
    t AS (SELECT 1),
    t2 AS (
        WITH t AS (SELECT 2)
        SELECT * FROM t
    )
SELECT * FROM t2;

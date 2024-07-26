-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Overview
-- MAGIC This notebooks contains complete SPARK SQL / DELTA LAKE SQL  Tutorial
-- MAGIC ###Details
-- MAGIC | Detail Tag | Information
-- MAGIC |----|-----
-- MAGIC | Notebook  | SQL ACCESS - DCL COMMANDS - GRANT And REVOKE 
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
-- MAGIC #### Enable table access control for a cluster
-- MAGIC * Table access control is available in two versions:
-- MAGIC ##### SQL-only table access control, which:
-- MAGIC * __Is generally available__.
-- MAGIC * Restricts cluster users to SQL commands. You are restricted to the Apache Spark SQL API, and therefore cannot use Python, Scala, R, RDD APIs, or clients that directly read the data from cloud storage, such as DBUtils.
-- MAGIC ##### Python and SQL table access control, which:
-- MAGIC * __Is in Public Preview__.
-- MAGIC * Allows users to run SQL, Python, and PySpark commands. You are restricted to the Spark SQL API and DataFrame API, and therefore cannot use Scala, R, RDD APIs, or clients that directly read the data from cloud storage, such as DBUtils.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Securable objects
-- MAGIC * The securable objects are:
-- MAGIC
-- MAGIC * __`CATALOG`__: controls access to the entire data catalog.
-- MAGIC * __`DATABASE`__: controls access to a database.
-- MAGIC * __`TABLE`__: controls access to a managed or external table.
-- MAGIC * __`VIEW`__: controls access to SQL views.
-- MAGIC * __`FUNCTION`__: controls access to a named function.
-- MAGIC * __`ANONYMOUS FUNCTION`__: controls access to anonymous or temporary functions.
-- MAGIC * __`ANY FILE`__: controls access to the underlying filesystem. Users granted access to ANY FILE can bypass the restrictions put on the catalog, databases, tables, and views by reading from the filesystem directly.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Privileges Type
-- MAGIC * __`SELECT`__ : gives read access to an object.
-- MAGIC * __`CREATE`__ : gives ability to create an object (for example, a table in a database).
-- MAGIC * __`MODIFY`__ : gives ability to add, delete, and modify data to or from an object.
-- MAGIC * __`USAGE`__ : does not give any abilities, but is an additional requirement to perform any action on a database object.
-- MAGIC * __`READ_METADATA`__ : gives ability to view an object and its metadata.
-- MAGIC * __`CREATE_NAMED_FUNCTION`__ : gives ability to create a named UDF in an existing catalog or database.
-- MAGIC * __`MODIFY_CLASSPATH`__ : gives ability to add files to the Spark class path.
-- MAGIC * __`ALL PRIVILEGES`__ : gives all privileges (is translated into all the above privileges).

-- COMMAND ----------

CREATE DATABASE accounting;
GRANT USAGE ON DATABASE accounting TO finance;
GRANT CREATE ON DATABASE accounting TO finance;

-- COMMAND ----------

ALTER DATABASE <database-name> OWNER TO `<user-name>@<user-domain>.com`
ALTER TABLE <table-name> OWNER TO `group_name`
ALTER VIEW <view-name> OWNER TO `<user-name>@<user-domain>.com`

-- COMMAND ----------

GRANT SELECT ON ANY FILE TO users

-- COMMAND ----------

GRANT SELECT ON DATABASE <database-name> TO `<user>@<domain-name>`
GRANT SELECT ON ANONYMOUS FUNCTION TO `<user>@<domain-name>`
GRANT SELECT ON ANY FILE TO `<user>@<domain-name>`

SHOW GRANT `<user>@<domain-name>` ON DATABASE <database-name>

DENY SELECT ON <table-name> TO `<user>@<domain-name>`

REVOKE ALL PRIVILEGES ON DATABASE default FROM `<user>@<domain-name>`
REVOKE SELECT ON <table-name> FROM `<user>@<domain-name>`

GRANT SELECT ON ANY FILE TO users

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SHOW GRANT
-- MAGIC * Displays all privileges (including inherited, denied, and granted) that affect the specified object.
-- MAGIC
-- MAGIC * To run this command you must be either: An administrator or the owner of the object. The user specified in [<user>].

-- COMMAND ----------

SHOW GRANT [<user>] ON [CATALOG | DATABASE <database-name> | TABLE <table-name> | VIEW <view-name> | FUNCTION <function-name> | ANONYMOUS FUNCTION | ANY FILE]

-- COMMAND ----------

SHOW GRANT `<user>@<domain-name>` ON DATABASE <database-name>

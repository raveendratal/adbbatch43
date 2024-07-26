-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Overview
-- MAGIC This notebooks contains complete SPARK SQL / DELTA LAKE SQL  Tutorial
-- MAGIC ###Details
-- MAGIC | Detail Tag | Information
-- MAGIC |----|-----
-- MAGIC | Notebook | SQL DDL Constraints details
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
-- MAGIC ### Constraints
-- MAGIC * Delta tables support standard SQL constraint management clauses that ensure that the quality and integrity of data added to a table is automatically verified. When a constraint is violated, Delta Lake throws an InvariantViolationException to signal that the new data can’t be added.
-- MAGIC
-- MAGIC * __`Two types of constraints are supported`__
-- MAGIC
-- MAGIC * __`NOT NULL`__ : indicates that values in specific columns cannot be null.
-- MAGIC * __`CHECK`__ : indicates that a specified Boolean expression must be true for each input row.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * __`NOT NULL constraint`__
-- MAGIC * You specify NOT NULL constraints in the schema when you create a table and drop `NOT NULL` constraints using the __`ALTER TABLE CHANGE COLUMN`__ command.

-- COMMAND ----------

-- MAGIC %fs rm -r /user/hive/warehouse/events

-- COMMAND ----------

DROP TABLE IF EXISTS events;
CREATE TABLE IF NOT EXISTS events(
  id LONG NOT NULL,
  date STRING NOT NULL,
  location STRING,
  description STRING
) USING DELTA;

-- COMMAND ----------

select NULL as name,2*5 as id,5*'' as id2

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC name=None
-- MAGIC print(name)

-- COMMAND ----------

ALTER TABLE events CHANGE COLUMN date DROP NOT NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * You can add NOT NULL constraints to an existing Delta table using the ALTER TABLE CHANGE COLUMN SET NOT NULL command.

-- COMMAND ----------

DROP TABLE IF EXISTS events;
CREATE TABLE IF NOT EXISTS events(
  id LONG,
  date STRING,
  location STRING,
  description STRING
) USING DELTA;



-- COMMAND ----------

delete from events;
ALTER TABLE events CHANGE COLUMN id SET NOT NULL;

-- COMMAND ----------

select * from events

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Validating after adding constraint and inserting `NULL` into `id` field.
-- MAGIC * __`Error Message `__ : `NOT NULL constraint violated for column: id`

-- COMMAND ----------

insert into events values (2,current_date(),NULL,NULL)

-- COMMAND ----------

select * from events

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CHECK constraint
-- MAGIC * __`Available in Databricks Runtime 7.4 and above.`__
-- MAGIC * You manage CHECK constraints using the __`ALTER TABLE ADD CONSTRAINT and ALTER TABLE DROP CONSTRAINT`__ commands. 
-- MAGIC * __`ALTER TABLE ADD CONSTRAINT`__ verifies that all existing rows satisfy the constraint before adding it to the table.

-- COMMAND ----------

DROP TABLE IF EXISTS events;
CREATE TABLE IF NOT EXISTS events(
  id LONG NOT NULL,
  date STRING,
  location STRING,
  description STRING
) USING DELTA;

-- COMMAND ----------

ALTER TABLE events ADD CONSTRAINT dateWithinRange CHECK (date > '1900-01-01');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Validating check constraint after adding in date field.
-- MAGIC * `InvariantViolationException: CHECK constraint datewithinrange (`date` > '1900-01-01') violated by row with values: - date : 1800-01-01`

-- COMMAND ----------

insert into events values (2000,'1820-01-01','Bangalore','validating check constraint')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Drop Check Constraint

-- COMMAND ----------

ALTER TABLE events DROP CONSTRAINT dateWithinRange;

-- COMMAND ----------

select *   from events

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Adding one more check constraint.
-- MAGIC * it will allow more than one constraint.

-- COMMAND ----------

ALTER TABLE events ADD CONSTRAINT validIds CHECK (id > 1000 and id < 999999);

-- COMMAND ----------

CREATE TABLE spark_catalog.default.events (
  id BIGINT NOT NULL,
  date STRING,
  location STRING,
  description STRING)
USING delta
TBLPROPERTIES (
  'Type' = 'MANAGED',
  'delta.constraints.validids' = 'id > 1000 and id < 999999',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '3')

-- COMMAND ----------

delete from events

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Verifying existing constraints  and its available in properties column

-- COMMAND ----------

DESCRIBE DETAIL events;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Primary key and Unique key is available on Unity Catalog
-- MAGIC * these are not available in spark catalog

-- COMMAND ----------

create catalog my_catalog;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### `PRIMARY KEY ( key_column [, …] ) [ constraint_option ] […]`
-- MAGIC
-- MAGIC * Adds a primary key constraint to the table. A table can have at most one primary key.
-- MAGIC
-- MAGIC * Primary key columns are implicitly defined as NOT NULL.
-- MAGIC
-- MAGIC * Primary key constraints are not supported for tables in the hive_metastore catalog.
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- Create a table with a primary key
CREATE TABLE persons(first_name STRING NOT NULL, last_name STRING NOT NULL, nickname STRING,
                       CONSTRAINT persons_pk PRIMARY KEY(first_name, last_name));

-- COMMAND ----------

-- Create a table with a single column primary key and system generated name
CREATE TABLE customers(customerid STRING NOT NULL PRIMARY KEY, name STRING);



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### `FOREIGN KEY (foreign_key_column [, ...] ) REFERENCES parent_table [ ( parent_column [, ...] ) ] foreign_key_option`
-- MAGIC
-- MAGIC * Adds a foreign key (referential integrity) constraint to the table.
-- MAGIC
-- MAGIC * Foreign key constraints are not supported for tables in the hive_metastore catalog.
-- MAGIC
-- MAGIC * Foreign key constraints which only differ in the permutation of the foreign key columns are not allowed.
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- create a table with a foreign key
CREATE TABLE pets(name STRING, owner_first_name STRING, owner_last_name STRING,
                    CONSTRAINT pets_persons_fk FOREIGN KEY (owner_first_name, owner_last_name) REFERENCES persons);



-- COMMAND ----------

-- Create a table with a names single column primary key and a named single column foreign key
CREATE TABLE orders(orderid BIGINT NOT NULL CONSTRAINT orders_pk PRIMARY KEY,
                      customerid STRING CONSTRAINT orders_customers_fk REFERENCES customers);

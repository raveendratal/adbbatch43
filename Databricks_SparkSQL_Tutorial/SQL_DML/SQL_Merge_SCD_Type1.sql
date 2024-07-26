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
-- MAGIC #### `Type 1 SCDs - Overwriting`
-- MAGIC
-- MAGIC * In a Type 1 SCD the new data overwrites the existing data. Thus the existing data is lost as it is not stored anywhere else. This is the default type of dimension you create. You do not need to specify any additional information to create a Type 1 SCD.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### MERGE INTO (Delta Lake on Databricks)
-- MAGIC * Merges a set of updates, insertions, and deletions based on a source table into a target Delta table.
-- MAGIC * __`Syntax`__ :
-- MAGIC * __`MERGE INTO target_table_identifier [AS target_alias]`__
-- MAGIC * __`USING source_table_identifier [<time_travel_version>] [AS source_alias]`__
-- MAGIC * __`ON <merge_condition>`__
-- MAGIC * __`[ WHEN MATCHED [ AND <condition> ] THEN <matched_action> ]`__
-- MAGIC * __`[ WHEN MATCHED [ AND <condition> ] THEN <matched_action> ]`__
-- MAGIC * __`[ WHEN NOT MATCHED [ AND <condition> ]  THEN <not_matched_action> ] `__
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ```
-- MAGIC <merge_condition> =
-- MAGIC   How the rows from one relation are combined with the rows of another relation. An expression with a return type of Boolean.
-- MAGIC
-- MAGIC <matched_action>  =
-- MAGIC   DELETE  |
-- MAGIC   UPDATE SET *  |
-- MAGIC   UPDATE SET column1 = value1 [, column2 = value2 ...]
-- MAGIC
-- MAGIC <not_matched_action>  =
-- MAGIC   INSERT *  |
-- MAGIC   INSERT (column1 [, column2 ...]) VALUES (value1 [, value2 ...])
-- MAGIC
-- MAGIC <time_travel_version>  =
-- MAGIC   TIMESTAMP AS OF timestamp_expression |
-- MAGIC   VERSION AS OF version
-- MAGIC   ```

-- COMMAND ----------

MERGE INTO TARGET_TABLE
Using Source_table/query
ON Condition
WHEN MATCHED THEN
UPDATE
WHEN MATCHED AND condition THEN
DELETE
WHEN NOT MATCHED THEN 
INSERT

-- COMMAND ----------

-- MAGIC %fs rm -r dbfs:/user/hive/warehouse/events

-- COMMAND ----------

DROP TABLE IF EXISTS events;
CREATE TABLE IF NOT EXISTS events(event_id int,event_date date,data string,delete boolean);

-- COMMAND ----------

insert into events values(1,'2021-01-01','sample event',0),
(2,'2021-02-02','sample event',0),
(3,'2021-03-03','sample event',0),
(4,'2021-04-04','sample event',0)

-- COMMAND ----------

select * from events
-- SCD Type 0.  append or insert only
-- SCD Type 1  UPSERT ( if data available then update ,if data is not available in target then insert)

-- COMMAND ----------

insert into events values(5,'2021-02-01','sample event',0),
(6,'2021-04-02','sample event',0),
(2,'2021-03-03','sample event',0),
(4,'2021-04-04','sample event',0)

-- COMMAND ----------

-- MAGIC %fs rm -r /user/hive/warehouse/customer_target

-- COMMAND ----------

create table customer_target(id int,name string,location string);
insert into customer_target values (1,'Ram','Chennai'),(2,'Reshwanth','Hyderabad'),(3,'Vikranth','Bangalore')

-- COMMAND ----------

select * from customer_target

-- COMMAND ----------

select * from customer_source

-- COMMAND ----------

-- MAGIC %fs rm -r /user/hive/warehouse/customer_source

-- COMMAND ----------

create table customer_source(id int,name string,location string);
insert into customer_source values (1,'Ram','Mumbai'),(4,'Raj','Hyderabad'),(5,'Prasad','Pune')

-- COMMAND ----------

select * from customer_source

-- COMMAND ----------

MERGE INTO customer_target as t
USING customer_source as s
ON t.id = s.id
WHEN MATCHED THEN 
UPDATE SET location=s.location
WHEN NOT MATCHED THEN 
INSERT *

-- COMMAND ----------

select * from customer_target

-- COMMAND ----------

MERGE INTO customer_target as t
USING customer_source as s
on t.id=s.id
WHEN MATCHED THEN 
Update set *
WHEN NOT MATCHED THEN
insert *

-- COMMAND ----------

select * from customer_source

-- COMMAND ----------

select * from customer_target

-- COMMAND ----------

select * from events

-- COMMAND ----------

-- MAGIC %fs rm -r /user/hive/warehouse/updates

-- COMMAND ----------

DROP TABLE IF EXISTS updates;
CREATE TABLE IF NOT EXISTS updates(event_id int,event_date date,data string,delete boolean);
insert into updates values(5,'2021-01-01','5th event',0),
(6,'2021-02-02','6th event',0),
(7,'2021-03-03','7th event',0),
(2,'2021-03-03','7th event',1),
(1,'2021-04-04','1st event',0)

-- COMMAND ----------

select * from updates

-- COMMAND ----------

select * from events

-- COMMAND ----------

update (0.5)
insert ( 1M 0.5 in,0.5 update) (0.5)

-- COMMAND ----------




full load.
target: (50M)
source: 50M 
(full load - trucate load(delete and insert ))

-- COMMAND ----------

MERGE INTO events
USING updates
ON events.event_id = updates.event_id
WHEN MATCHED AND  updates.delete==true THEN
  delete
WHEN MATCHED THEN
  UPDATE SET events.data = updates.data,events.event_date = updates.event_date
WHEN NOT MATCHED
  THEN INSERT (event_id, event_date, data,delete) VALUES (event_id,event_date, data,delete)

-- COMMAND ----------

select * from events

-- COMMAND ----------

select * from events

-- COMMAND ----------

insert query 
# df.write.format("delta").mode("append").saveAsTable("tablename")

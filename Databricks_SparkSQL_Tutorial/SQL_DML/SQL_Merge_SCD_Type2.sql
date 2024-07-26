-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Overview
-- MAGIC This notebooks contains complete SPARK SQL / DELTA LAKE SQL  Tutorial
-- MAGIC ###Details
-- MAGIC | Detail Tag | Information
-- MAGIC |----|-----
-- MAGIC | Notebook | SQL SCD Type 2 Details  
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
-- MAGIC #### `What is a Slowly Changing Dimension?`
-- MAGIC
-- MAGIC * A `Slowly Changing Dimension (SCD)` is a dimension that stores and manages both current and historical data over time in a data warehouse. It is considered and implemented as one of the most critical ETL tasks in tracking the history of dimension records.
-- MAGIC
-- MAGIC * There are three types of SCDs and you can use Warehouse Builder to define, deploy, and load all three types of SCDs.
-- MAGIC * `SCD Type 1`
-- MAGIC * `SCD Type 2`
-- MAGIC * `SCD Type 3`
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Customers Delta table
-- MAGIC
-- MAGIC * This is the slowly changing table that we want to update. For every customer, there many any number of addresses. But for each address, there is range of dates, effectiveDate to endDate, in which that address was effectively the current address. In addition, there is another field current which is true for the address that is currently valid for each customer. That is, there is only 1 address and 1 row for each customer where current is true, for every other row, it is false.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Merge statement to perform SCD Type 2
-- MAGIC
-- MAGIC * This merge statement simultaneously does both for each customer in the source table.
-- MAGIC
-- MAGIC * Inserts the new address with its current set to true, and
-- MAGIC * Updates the previous current row to set current to false, and update the endDate from null to the effectiveDate from the source.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Updated Customers table
-- MAGIC
-- MAGIC * For customer 1, previous address was update as current = false and new address was inserted as current = true.
-- MAGIC * For customer 2, there was no update.
-- MAGIC * For customer 3, the new address was same as previous address, so no update was made.
-- MAGIC * For customer 4, new address was inserted.

-- COMMAND ----------

-- MAGIC %fs rm -r /user/hive/warehouse/customers_dim

-- COMMAND ----------

DROP TABLE IF EXISTS customers_dim;
CREATE TABLE IF NOT EXISTS customers_dim(customerId int, name string,address string, current boolean, effectiveDate timestamp, endDate timestamp);
insert into customers_dim VALUES (1,'Mahesh','Bangalore',1,current_timestamp(),'9999-12-31'),
(2,'ram','Hyderabad',1,current_timestamp(),'9999-12-31'),
(3,'ravi','Chennai',1,current_timestamp(),'9999-12-31'),
(4,'raj','Pune',1,current_timestamp(),'9999-12-31')

-- COMMAND ----------

select * from customers_dim
-- status fields values -  1 or 0   Y or N   True or False  Active or inactive 

-- COMMAND ----------

select * from customers_dim
-- statu fields values -  1 or 0   Y or N   True or False  Active or inactive 

-- COMMAND ----------

select * from updates_source

-- COMMAND ----------

-- MAGIC %fs rm -r /user/hive/warehouse/updates_source

-- COMMAND ----------

DROP TABLE IF EXISTS updates_source;
CREATE TABLE IF NOT EXISTS updates_source(customerId int,name string, address string, effectiveDate timestamp);
insert into updates_source VALUES (5,'Sridhar','Delhi',current_timestamp()),
(6,'Prasad','Mumbai',current_timestamp()),
(2,'ram','Bangalore',current_timestamp()),
(1,'Mahesh','Hyderabad',current_timestamp())

-- COMMAND ----------

SELECT updates_source.customerId as mergeKey, updates_source.*
  FROM updates_source 

-- COMMAND ----------

SELECT NULL as mergeKey, updates_source.*
  FROM updates_source JOIN customers_dim
  ON updates_source.customerid = customers_dim.customerid 
  WHERE customers_dim.current = 1 AND customers_dim.address <> updates_source.address  

-- COMMAND ----------

SELECT updates_source.customerId as mergeKey, updates_source.*
  FROM updates_source  
  UNION ALL
  SELECT NULL as mergeKey, updates_source.*
  FROM updates_source JOIN customers_dim
  ON updates_source.customerid = customers_dim.customerid 
  WHERE customers_dim.current = 1 AND customers_dim.address <> updates_source.address  

-- COMMAND ----------


-- These rows will either UPDATE the current addresses of existing customers or INSERT the new addresses of new customers
 -- These rows will INSERT new addresses of existing customers 
  -- Setting the mergeKey to NULL forces these rows to NOT MATCH and be INSERTED.
MERGE INTO customers_dim as customers
USING (SELECT updates.customerId as mergeKey, updates.*
  FROM updates_source as updates
  UNION ALL
  SELECT NULL as mergeKey, updates.*
  FROM updates_source as updates JOIN customers_dim as customers
  ON updates.customerid = customers.customerid 
  WHERE customers.current = 1 AND updates.address <> customers.address  
) staged_updates
ON customers.customerId = mergeKey
WHEN MATCHED AND customers.current = 1 AND customers.address <> staged_updates.address THEN  
  UPDATE SET current = 0, endDate = staged_updates.effectiveDate   
WHEN NOT MATCHED THEN 
  INSERT(customerid, name,address, current, effectivedate, enddate) 
  VALUES(staged_updates.customerId, staged_updates.name, staged_updates.address, 1, staged_updates.effectiveDate, '9999-12-31') 
  
-- Set current to true along with the new address and its effective date.
 -- Set current to false and endDate to source's effective date.  !=

-- COMMAND ----------

select * from customers_dim order by customerid,current--where current=false

-- COMMAND ----------



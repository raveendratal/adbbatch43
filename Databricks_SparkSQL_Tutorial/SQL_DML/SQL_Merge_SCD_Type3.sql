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
-- MAGIC #### `Type 3 SCDs` - Creating a current value field
-- MAGIC
-- MAGIC * A Type 3 SCD stores two versions of values for certain selected level attributes. Each record stores the `previous value` and the `current value` of the selected attribute. When the value of any of the selected attributes changes, the current value is stored as the old value and the new value becomes the current value.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Merge statement to perform SCD Type 3
-- MAGIC
-- MAGIC * This merge statement simultaneously update prev_loc and curr_loc columns.
-- MAGIC
-- MAGIC * its should update curr_loc to prev_loc if source loc and curr_loc is not matching.
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %fs rm -r /user/hive/warehouse/customer_type3

-- COMMAND ----------

drop table if exists customer_type3;
create table customer_type3(id int,name string,curr_loc string,prev_loc string);
insert into customer_type3(id,name,curr_loc,prev_loc) 
select 1,'Ravi','Bangalore',null 
union all 
select 2,'Ram','Chennai',null
union all 
select 3,'Prasad','Hyderabad',null

-- COMMAND ----------

select * from customer_type3

-- COMMAND ----------

select * from customer_type3

-- COMMAND ----------

select * from customer_src

-- COMMAND ----------

-- MAGIC %fs rm -r /user/hive/warehouse/customer_src

-- COMMAND ----------

DROP TABLE IF EXISTS customer_src;
CREATE TABLE IF NOT EXISTS customer_src(id int, name string, loc string);
insert into customer_src VALUES (1,'Ravi','Chennai'),
(2,'Ram','Hyderabad'),
(4,'Mahesh','Bangalore'),
(5,'Sridhar','Hyderabad')

-- COMMAND ----------

select * from customer_type3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### ` Approach 1`
-- MAGIC *. without joining with target table. 

-- COMMAND ----------

merge into customer_type3 as tgt
using customer_src as src
on tgt.id = src.id
WHEN MATCHED and lower(tgt.curr_loc)<>lower(src.loc) THEN
 update set tgt.prev_loc = tgt.curr_loc ,tgt.curr_loc =src.loc
WHEN NOT MATCHED THEN
insert   (id,name,curr_loc) values(src.id,src.name,src.loc)


-- COMMAND ----------

select * from customer_type3

-- COMMAND ----------

select src.id as id,
src.name as name,
src.loc as curr_loc,
case when src.loc <> tgt.curr_loc then tgt.curr_loc
else  tgt.prev_loc end as prev_loc
from customer_src as src 
left join customer_type3 as tgt on src.id=tgt.id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### `approach 2`
-- MAGIC * joining with target table get the comparision

-- COMMAND ----------

MERGE into customer_type3 as tgt
using (select src.id as id,
src.name as name,
src.loc as curr_loc,
case when src.loc <> tgt.curr_loc then tgt.curr_loc
else  tgt.prev_loc end as prev_loc
from customer_src as src 
left join customer_type3 as tgt on src.id=tgt.id) src
on tgt.id = src.id
when matched then 
update set tgt.id = src.id,tgt.name=src.name,tgt.curr_loc = src.curr_loc,tgt.prev_loc = src.prev_loc
when not matched then 
insert  (id,name,curr_loc,prev_loc) values  (src.id,src.name,src.curr_loc,src.prev_loc)

-- COMMAND ----------

select * from customer_type3

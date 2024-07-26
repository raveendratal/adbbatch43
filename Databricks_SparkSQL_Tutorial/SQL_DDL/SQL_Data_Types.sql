-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Data Types 
-- MAGIC * Spark SQL and DataFrames support the following data types:
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### __`Numeric types`__
-- MAGIC
-- MAGIC * __`ByteType`__ : Represents 1-byte signed integer numbers. The range of numbers is from -128 to 127.
-- MAGIC * __`ShortType`__ : Represents 2-byte signed integer numbers. The range of numbers is from -32768 to 32767.
-- MAGIC * __`IntegerType`__ : Represents 4-byte signed integer numbers. The range of numbers is from -2147483648 to 2147483647.
-- MAGIC * __`LongType`__ : Represents 8-byte signed integer numbers. The range of numbers is from -9223372036854775808 to 9223372036854775807.
-- MAGIC * __`FloatType`__ : Represents 4-byte single-precision floating point numbers.
-- MAGIC * __`DoubleType`__ : Represents 8-byte double-precision floating point numbers.
-- MAGIC * __`DecimalType`__ : Represents arbitrary-precision signed decimal numbers. Backed internally by java.math.BigDecimal. A BigDecimal consists of an arbitrary precision integer unscaled value and a 32-bit integer scale.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### __`String type`__
-- MAGIC
-- MAGIC * __`StringType`__ : Represents character string values.
-- MAGIC * __`VarcharType(length)`__ : A variant of StringType which has a length limitation. Data writing will fail if the input string exceeds the length limitation. Note: this type can only be used in table schema, not functions/operators.
-- MAGIC * __`CharType(length)`__ : A variant of VarcharType(length) which is fixed length. Reading column of type CharType(n) always returns string values of length n. Char type column comparison will pad the short one to the longer length.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### __`Other Types `__
-- MAGIC * __`Binary type`__ 
-- MAGIC * BinaryType: Represents byte sequence values.
-- MAGIC * __`Boolean type`__
-- MAGIC
-- MAGIC * BooleanType: Represents boolean values.
-- MAGIC
-- MAGIC * __`Datetime type`__
-- MAGIC * __`TimestampType`__: Represents values comprising values of fields year, month, day, hour, minute, and second, with the session local time-zone. 
-- MAGIC * The timestamp value represents an absolute point in time.
-- MAGIC
-- MAGIC * __`DateType`__: Represents values comprising values of fields year, month and day, without a time-zone.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Complex types
-- MAGIC * __`ArrayType(elementType, containsNull)`__: Represents values comprising a sequence of elements with the type of elementType. containsNull is used to indicate if elements in a ArrayType value can have null values.
-- MAGIC
-- MAGIC * __`MapType(keyType, valueType, valueContainsNull)`__ : Represents values comprising a set of key-value pairs. The data type of keys is described by keyType and the data type of values is described by valueType. For a MapType value, keys are not allowed to have null values. valueContainsNull is used to indicate if values of a MapType value can have null values.
-- MAGIC * __`StructType(fields)`__ : Represents values with the structure described by a sequence of StructFields (fields).
-- MAGIC
-- MAGIC * __`StructField(name, dataType, nullable)`__ : Represents a field in a StructType. The name of a field is indicated by name. The data type of a field is indicated by dataType. nullable is used to indicate if values of these fields can have null values

-- COMMAND ----------

-- MAGIC %md
-- MAGIC | Pyspark Data type | SQL Data type
-- MAGIC | - | - | - | - |
-- MAGIC | BooleanType | BOOLEAN 
-- MAGIC | ByteType | BYTE, TINYINT  
-- MAGIC | ShortType  | SHORT, SMALLINT
-- MAGIC | IntegerType  | INT, INTEGER
-- MAGIC | LongType  | LONG, BIGINT
-- MAGIC | FloatType | FLOAT, REAL
-- MAGIC | DoubleType  | DOUBLE
-- MAGIC | DecimalType  | DECIMAL, DEC, NUMERIC	
-- MAGIC | DateType  | DATE
-- MAGIC | TimestampType  | TIMESTAMP
-- MAGIC | StringType  | STRING
-- MAGIC | BinaryType  | BINARY	
-- MAGIC | CalendarIntervalType  | INTERVAL
-- MAGIC | ArrayType  | ARRAY<element_type>
-- MAGIC | StructType  | STRUCT<field1_name: field1_type, field2_name: field2_type, …>
-- MAGIC | MapType  | MAP<key_type, value_type>	

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SQL Functions
-- MAGIC * The behavior of some SQL functions can be different under ANSI mode (spark.sql.ansi.enabled=true).
-- MAGIC
-- MAGIC * __`size`__ : This function returns null for null input.
-- MAGIC * __`element_at`__ : 
-- MAGIC * * This function throws ArrayIndexOutOfBoundsException if using invalid indices.
-- MAGIC * * This function throws NoSuchElementException if key does not exist in map.
-- MAGIC * __`elt`__: This function throws ArrayIndexOutOfBoundsException if using invalid indices.
-- MAGIC
-- MAGIC * __`parse_url`__ : This function throws IllegalArgumentException if an input string is not a valid url.
-- MAGIC * __`to_date`__ : This function should fail with an exception if the input string can’t be parsed, or the pattern string is invalid.
-- MAGIC * __`to_timestamp`__: This function should fail with an exception if the input string can’t be parsed, or the pattern string is invalid.
-- MAGIC * __`unix_timestamp`__ : This function should fail with an exception if the input string can’t be parsed, or the pattern string is invalid.
-- MAGIC * __`to_unix_timestamp`__ : This function should fail with an exception if the input string can’t be parsed, or the pattern string is invalid.
-- MAGIC * __`make_date`__: This function should fail with an exception if the result date is invalid.
-- MAGIC * __`make_timestamp`__: This function should fail with an exception if the result timestamp is invalid.
-- MAGIC * __`make_interval`__: This function should fail with an exception if the result interval is invalid.

-- COMMAND ----------

DROP TABLE IF EXISTS datatypes;
create table IF NOT EXISTS datatypes(
  id int,
  id1 smallint,
  id2 long,
  id3 date,
  id4 timestamp,
  id5 boolean,
  id6 float
) USING DELTA ;

-- COMMAND ----------

--databricks runtime before 8x (hive table with orc format)

-- COMMAND ----------

show create table datatypes

-- COMMAND ----------

select current_timestamp(),current_Date()

-- COMMAND ----------

insert into datatypes values (1,2,3,current_Date(),current_timestamp(),1,1.5)
-- boolean values  1 or 0 / True or False  (1==True and 0==False)

-- COMMAND ----------

select * from datatypes

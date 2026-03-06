-- Databricks notebook source
-- MAGIC %python
-- MAGIC # *****************************************************************************************
-- MAGIC # Delta Lake Tables Demo (Databricks notebook exported as .py)
-- MAGIC # - Each key Delta operation is placed in its own cell, separated by `# COMMAND ----------`
-- MAGIC # - Uses one sample Delta table: delta_demo.customers_delta
-- MAGIC # - Covers: CREATE, DESCRIBE, DESCRIBE DETAIL, SHOW TBLPROPERTIES, SHOW CREATE TABLE,
-- MAGIC #           ALTER (ADD COLUMN, SET TBLPROPERTIES), INSERT, UPDATE, DELETE,
-- MAGIC #           MERGE INTO, DESCRIBE HISTORY, Time Travel (VERSION/TIMESTAMP), RESTORE,
-- MAGIC #           OPTIMIZE (with optional ZORDER), VACUUM, CLONE (shallow/deep), RENAME, DROP
-- MAGIC # - SQL cells use `%sql` and inline `--` comments; Python cells use `#` comments.
-- MAGIC # *****************************************************************************************

-- COMMAND ----------

--Switch current schema 
use catalog main  ;
use schema default;
SELECT current_catalog(), current_schema()

-- COMMAND ----------

-- Remove the table if a prior run created it
DROP TABLE IF EXISTS customers_delta;

-- COMMAND ----------

-- Create a Delta table with initial columns and data using CTAS
CREATE TABLE customers_delta
USING DELTA AS
SELECT
  customer_id,
  name,
  segment,
  current_timestamp() AS created_at
FROM (
  SELECT * FROM VALUES
    (1,  'Alice', 'Standard'),
    (2,  'Bob',   'Premium'),
    (3,  'Carol', 'Standard'),
    (4,  'David', 'Premium'),
    (5,  'Emma',  'Standard'),
    (6,  'Frank', 'Premium'),
    (7,  'Grace', 'Standard'),
    (8,  'Henry', 'Premium'),
    (9,  'Irene', 'Standard'),
    (10, 'Jack',  'Premium')
) AS v(customer_id, name, segment);

-- COMMAND ----------

-- Simple data peek
SELECT * FROM customers_delta ORDER BY customer_id;

-- COMMAND ----------

-- Column definitions and basic info
DESCRIBE TABLE customers_delta;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC %%sql
-- MAGIC -- Delta table metadata (storage path, size, file count, properties, etc.)
-- MAGIC DESCRIBE DETAIL customers_delta;

-- COMMAND ----------

-- View table properties
SHOW TBLPROPERTIES customers_delta;

-- COMMAND ----------

-- DDL representing this table definition
SHOW CREATE TABLE customers_delta;

-- COMMAND ----------

-- Add a nullable email column
ALTER TABLE customers_delta
ADD COLUMNS (email STRING);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC %%sql
-- MAGIC -- Set some illustrative properties (appendOnly false allows updates/deletes/merges)
-- MAGIC ALTER TABLE customers_delta
-- MAGIC SET TBLPROPERTIES (
-- MAGIC   'delta.appendOnly' = 'false',
-- MAGIC   'demo.note' = 'Delta table for feature demo'
-- MAGIC );

-- COMMAND ----------

-- Insert additional rows to create newer table versions
INSERT INTO customers_delta (customer_id, name, segment, created_at, email)
VALUES
  (11, 'Kim',  'Premium',  current_timestamp(), 'kim@example.com'),
  (12, 'Leo',  'Standard', current_timestamp(), 'leo@example.com');

-- COMMAND ----------

-- Promote certain customers to Premium
UPDATE customers_delta
SET segment = 'Premium'
WHERE customer_id IN (1, 3, 5);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC %%sql
-- MAGIC -- Remove a specific customer (for demo)
-- MAGIC DELETE FROM customers_delta
-- MAGIC WHERE customer_id = 10;

-- COMMAND ----------

-- Transaction history (each write creates a new version)
DESCRIBE HISTORY customers_delta;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC %%sql
-- MAGIC -- Replace the version as needed after inspecting DESCRIBE HISTORY
-- MAGIC SELECT * FROM customers_delta VERSION AS OF 0 ORDER BY customer_id;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC %%sql
-- MAGIC -- Use an actual timestamp value observed in DESCRIBE HISTORY, e.g. '2026-03-01T12:00:00Z'
-- MAGIC SELECT * FROM customers_delta TIMESTAMP AS OF '2026-03-01T13:35:20.733+00:00' ORDER BY customer_id;

-- COMMAND ----------

-- Restore to a prior version; verify via a subsequent SELECT
RESTORE TABLE customers_delta TO VERSION AS OF 1;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC %%sql
-- MAGIC --Cell: OPTIMIZE (file compaction) with optional ZORDER
-- MAGIC -- Purpose: Compact many small files into fewer larger ones; ZORDER improves locality for predicates
-- MAGIC -- OPTIMIZE compacts files. ZORDER is optional but helps data skipping for the listed columns
-- MAGIC OPTIMIZE customers_delta
-- MAGIC ZORDER BY (segment);

-- COMMAND ----------

--Cell: VACUUM (file cleanup)
--Purpose: Remove old, unreferenced data files beyond the retention period to free storage

-- Default retention is often 7 days. Adjust RETAIN only if you understand Time Travel impact.
-- NOTE: Lowering retention may require disabling the safety check at session/workspace level.
VACUUM customers_delta RETAIN 168 HOURS;  -- 7 days

-- COMMAND ----------

-- Cell: (Idempotent) Drop shallow clone table if it exists
-- Purpose: Prepare for creating a fresh shallow clone

-- Clean up any prior shallow clone
DROP TABLE IF EXISTS customers_delta_shallow_clone;

-- COMMAND ----------

--Cell: CLONE - Shallow Clone
-- Purpose: Fast metadata-only clone referencing the same underlying data files

-- Shallow clone shares data files at the time of cloning
CREATE TABLE customers_delta_shallow_clone
SHALLOW CLONE customers_delta;

-- COMMAND ----------

-- Cell: (Idempotent) Drop deep clone table if it exists
-- Purpose: Prepare for creating a fresh deep clone
-- Clean up any prior deep clone
DROP TABLE IF EXISTS customers_delta_deep_clone;

-- COMMAND ----------

--Cell: CLONE - Deep Clone
--Purpose: Full physical copy of data files at the clone point-in-time

-- Deep clone copies the files for independent storage
CREATE TABLE customers_delta_deep_clone
DEEP CLONE customers_delta;

-- COMMAND ----------

-- Cell: DESCRIBE DETAIL (shallow clone)
-- Purpose: Inspect metadata of the shallow clone

-- Note the relation to the source table and storage path
DESCRIBE DETAIL customers_delta_shallow_clone;

-- COMMAND ----------

--Cell: DESCRIBE DETAIL (deep clone)
--Purpose: Inspect metadata of the deep clone
-- Deep clone has its own set of copied data files
DESCRIBE DETAIL customers_delta_deep_clone;

-- COMMAND ----------

--Cell: RENAME TABLE
--Purpose: Rename the original table (useful to demonstrate object renames)

-- Rename the original table; downstream references should use the new name
ALTER TABLE customers_delta RENAME TO customers_delta_renamed;

-- COMMAND ----------

--Cell: DROP TABLE (cleanup shallow clone)
--Purpose: Remove shallow clone created for the demo

-- Drop shallow clone
DROP TABLE IF EXISTS customers_delta_shallow_clone;

-- COMMAND ----------

-- Cell: DROP TABLE (cleanup deep clone)
--Purpose: Remove deep clone created for the demo

-- Drop deep clone
DROP TABLE IF EXISTS customers_delta_deep_clone;

-- COMMAND ----------

--Cell: DROP TABLE (cleanup original/renamed)
--Purpose: Remove the main demo table to leave the environment clean

-- Drop the renamed original table to complete cleanup
DROP TABLE IF EXISTS customers_delta_renamed;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC #Cell: (Optional) Programmatic metadata via DeltaTable API (Python)
-- MAGIC # Purpose: Show how to inspect Delta metadata using the DeltaTable API
-- MAGIC #Note: This cell is optional; comment out if not needed.
-- MAGIC from delta.tables import DeltaTable
-- MAGIC from pyspark.sql import functions as F
-- MAGIC
-- MAGIC # Recreate the table quickly if it no longer exists (optional guard for this cell)
-- MAGIC spark.sql("CREATE DATABASE IF NOT EXISTS delta_demo")
-- MAGIC spark.sql("USE delta_demo")
-- MAGIC
-- MAGIC table_name = "delta_demo.customers_delta"
-- MAGIC
-- MAGIC # If the main table was dropped, recreate a tiny one so this cell can run (optional)
-- MAGIC if not spark._jsparkSession.catalog().tableExists("delta_demo", "customers_delta"):
-- MAGIC     spark.sql("CREATE TABLE customers_delta USING DELTA AS SELECT 1 AS customer_id, 'Temp' AS name, 'Standard' AS segment, current_timestamp() AS created_at, NULL AS email")
-- MAGIC
-- MAGIC # Load DeltaTable handle and inspect
-- MAGIC dt = DeltaTable.forName(spark, table_name)
-- MAGIC print("
-- MAGIC DeltaTable detail (Python API):")
-- MAGIC print(spark.sql(f"DESCRIBE DETAIL {table_name}").limit(1).collect()[0].asDict())
-- MAGIC
-- MAGIC print("
-- MAGIC Transaction history (top 10 rows):")
-- MAGIC spark.sql(f"DESCRIBE HISTORY {table_name}").orderBy(F.col("version").desc()).show(10, truncate=False)

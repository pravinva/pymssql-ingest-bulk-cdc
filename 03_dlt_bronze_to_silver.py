# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Pipeline: Bronze → Silver (Direct Delta)
# MAGIC
# MAGIC Simplified DLT pipeline that:
# MAGIC - Reads from Bronze Delta tables (created by Notebook 02)
# MAGIC - Creates Silver streaming tables (cleaned + deduplicated)
# MAGIC - Runs on Serverless DLT
# MAGIC - Aligned with Lakeflow Connect managed connector future state
# MAGIC
# MAGIC **Architecture:**
# MAGIC - Source: Bronze Delta tables (main.fh_bronze.*)
# MAGIC - Target: Silver Delta tables (main.fh_silver.*)
# MAGIC - No AutoLoader needed (direct Delta-to-Delta)

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer: Bronze → Silver Transformations

# COMMAND ----------

@dlt.table(
    name="Assets_silver",
    comment="Silver table for Assets - cleaned and deduplicated from Bronze",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "AssetID"
    }
)
@dlt.expect_or_drop("valid_asset_id", "AssetID IS NOT NULL")
@dlt.expect_or_drop("valid_asset_number", "AssetNumber IS NOT NULL")
def assets_silver():
    """
    Silver table for Assets
    - Reads from Bronze Delta table
    - Deduplicates by AssetID (keep latest)
    - Data quality: drops nulls
    """
    # Read from Bronze Delta table
    df = spark.readStream.table("main.fh_bronze.dbo_Assets")

    # Deduplication: Keep latest record by AssetID
    window_spec = Window.partitionBy("AssetID").orderBy(col("_ingestion_timestamp").desc())

    return (df
        .withColumn("_row_num", row_number().over(window_spec))
        .filter(col("_row_num") == 1)
        .drop("_row_num", "_source_table")  # Clean up metadata columns
        .select(
            "AssetID",
            "AssetNumber",
            "AssetName",
            "AssetType",
            "Location",
            "Status",
            "ModifiedDate",
            "_ingestion_timestamp"
        ))

# COMMAND ----------

@dlt.table(
    name="WorkOrders_silver",
    comment="Silver table for WorkOrders - cleaned and deduplicated from Bronze",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "WorkOrderID"
    }
)
@dlt.expect_or_drop("valid_workorder_id", "WorkOrderID IS NOT NULL")
@dlt.expect_or_drop("valid_workorder_number", "WorkOrderNumber IS NOT NULL")
def workorders_silver():
    """
    Silver table for WorkOrders
    - Reads from Bronze Delta table
    - Deduplicates by WorkOrderID (keep latest)
    - Data quality: drops nulls
    """
    # Read from Bronze Delta table
    df = spark.readStream.table("main.fh_bronze.dbo_WorkOrders")

    # Deduplication: Keep latest record by WorkOrderID
    window_spec = Window.partitionBy("WorkOrderID").orderBy(col("_ingestion_timestamp").desc())

    return (df
        .withColumn("_row_num", row_number().over(window_spec))
        .filter(col("_row_num") == 1)
        .drop("_row_num", "_source_table")  # Clean up metadata columns
        .select(
            "WorkOrderID",
            "WorkOrderNumber",
            "Description",
            "Status",
            "Priority",
            "AssetID",
            "CreatedDate",
            "ModifiedDate",
            "_ingestion_timestamp"
        ))

# COMMAND ----------

@dlt.table(
    name="MaintenanceRecords_silver",
    comment="Silver table for MaintenanceRecords - cleaned and deduplicated from Bronze",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "MaintenanceID"
    }
)
@dlt.expect_or_drop("valid_maintenance_id", "MaintenanceID IS NOT NULL")
@dlt.expect_or_drop("valid_cost", "Cost >= 0")
def maintenancerecords_silver():
    """
    Silver table for MaintenanceRecords
    - Reads from Bronze Delta table
    - Deduplicates by MaintenanceID (keep latest)
    - Data quality: drops nulls, validates cost >= 0
    """
    # Read from Bronze Delta table
    df = spark.readStream.table("main.fh_bronze.dbo_MaintenanceRecords")

    # Deduplication: Keep latest record by MaintenanceID
    window_spec = Window.partitionBy("MaintenanceID").orderBy(col("_ingestion_timestamp").desc())

    return (df
        .withColumn("_row_num", row_number().over(window_spec))
        .filter(col("_row_num") == 1)
        .drop("_row_num", "_source_table")  # Clean up metadata columns
        .select(
            "MaintenanceID",
            "AssetID",
            "WorkOrderID",
            "MaintenanceType",
            "MaintenanceDate",
            "Cost",
            "Duration",
            "ModifiedDate",
            "_ingestion_timestamp"
        ))

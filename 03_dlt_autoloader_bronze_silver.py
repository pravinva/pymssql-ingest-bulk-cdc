# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Pipeline: AutoLoader → Bronze → Silver
# MAGIC
# MAGIC Standard DLT pipeline that:
# MAGIC - Uses AutoLoader (file notification) to read from UC Volume
# MAGIC - Creates Bronze streaming tables (raw data + audit columns)
# MAGIC - Creates Silver streaming tables (cleaned + deduplicated)
# MAGIC - Runs on Serverless DLT

# COMMAND ----------

import dlt
from pyspark.sql.functions import current_timestamp, col, to_date

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# UC Volume path (Hive partitioned CDC records)
UC_VOLUME_PATH = "/Volumes/main/fh_ingestion/landing"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer: AutoLoader → Streaming Tables

# COMMAND ----------

@dlt.table(
    name="dbo_Assets_bronze",
    comment="Bronze streaming table for dbo_Assets - ingested via AutoLoader from UC Volume",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "_ingestion_timestamp",
        "delta.enableChangeDataFeed": "true"
    }
)
def dbo_assets_bronze():
    """
    Bronze table for Assets with AutoLoader
    """
    source_path = f"{UC_VOLUME_PATH}/dbo_Assets"
    schema_location = f"{UC_VOLUME_PATH}/_schemas/dbo_Assets"

    df = (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", schema_location)
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.useNotifications", "false")
            .option("cloudFiles.validateOptions", "true")
            .load(source_path)
            .withColumn("_ingestion_timestamp", current_timestamp())
            .withColumn("_source_file", col("_metadata.file_path"))
    )

    # Cast timestampNtz columns to regular timestamp for UC compatibility
    return df.withColumn("ModifiedDate", col("ModifiedDate").cast("timestamp"))

# COMMAND ----------

@dlt.table(
    name="dbo_WorkOrders_bronze",
    comment="Bronze streaming table for dbo_WorkOrders - ingested via AutoLoader from UC Volume",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "_ingestion_timestamp",
        "delta.enableChangeDataFeed": "true"
    }
)
def dbo_workorders_bronze():
    """
    Bronze table for WorkOrders with AutoLoader
    """
    source_path = f"{UC_VOLUME_PATH}/dbo_WorkOrders"
    schema_location = f"{UC_VOLUME_PATH}/_schemas/dbo_WorkOrders"

    df = (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", schema_location)
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.useNotifications", "false")
            .option("cloudFiles.validateOptions", "true")
            .load(source_path)
            .withColumn("_ingestion_timestamp", current_timestamp())
            .withColumn("_source_file", col("_metadata.file_path"))
    )

    # Cast timestampNtz columns to regular timestamp for UC compatibility
    return (df
        .withColumn("CreatedDate", col("CreatedDate").cast("timestamp"))
        .withColumn("ModifiedDate", col("ModifiedDate").cast("timestamp"))
    )

# COMMAND ----------

@dlt.table(
    name="dbo_MaintenanceRecords_bronze",
    comment="Bronze streaming table for dbo_MaintenanceRecords - ingested via AutoLoader from UC Volume",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "_ingestion_timestamp",
        "delta.enableChangeDataFeed": "true"
    }
)
def dbo_maintenancerecords_bronze():
    """
    Bronze table for MaintenanceRecords with AutoLoader
    """
    source_path = f"{UC_VOLUME_PATH}/dbo_MaintenanceRecords"
    schema_location = f"{UC_VOLUME_PATH}/_schemas/dbo_MaintenanceRecords"

    df = (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", schema_location)
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.useNotifications", "false")
            .option("cloudFiles.validateOptions", "true")
            .load(source_path)
            .withColumn("_ingestion_timestamp", current_timestamp())
            .withColumn("_source_file", col("_metadata.file_path"))
    )

    # Cast timestampNtz columns to regular timestamp for UC compatibility
    return (df
        .withColumn("MaintenanceDate", col("MaintenanceDate").cast("timestamp"))
        .withColumn("ModifiedDate", col("ModifiedDate").cast("timestamp"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer: Cleaned and Deduplicated

# COMMAND ----------

@dlt.table(
    name="dbo_Assets_silver",
    comment="Silver table - cleaned and deduplicated Assets"
)
@dlt.expect_or_drop("valid_asset_id", "AssetID IS NOT NULL")
@dlt.expect_or_drop("valid_asset_number", "AssetNumber IS NOT NULL")
def assets_silver():
    """
    Silver transformation for Assets:
    - Deduplication by AssetID
    - Data quality checks
    - Type casting and standardization
    """
    return (
        dlt.read_stream("dbo_Assets_bronze")
            .dropDuplicates(["AssetID"])
            .select(
                col("AssetID").cast("bigint"),
                col("AssetNumber"),
                col("AssetName"),
                col("AssetType"),
                col("Location"),
                col("Status"),
                col("ModifiedDate").cast("timestamp"),
                col("_ingestion_timestamp")
            )
    )


@dlt.table(
    name="dbo_WorkOrders_silver",
    comment="Silver table - cleaned and deduplicated WorkOrders"
)
@dlt.expect_or_drop("valid_work_order_id", "WorkOrderID IS NOT NULL")
@dlt.expect_or_drop("valid_created_date", "CreatedDate IS NOT NULL")
def work_orders_silver():
    """
    Silver transformation for WorkOrders:
    - Deduplication by WorkOrderID
    - Data quality checks
    - Join-ready format
    """
    return (
        dlt.read_stream("dbo_WorkOrders_bronze")
            .dropDuplicates(["WorkOrderID"])
            .select(
                col("WorkOrderID").cast("bigint"),
                col("WorkOrderNumber"),
                col("Description"),
                col("Status"),
                col("Priority"),
                col("AssetID").cast("bigint"),
                col("CreatedDate").cast("timestamp"),
                col("ModifiedDate").cast("timestamp"),
                col("_ingestion_timestamp")
            )
    )


@dlt.table(
    name="dbo_MaintenanceRecords_silver",
    comment="Silver table - cleaned and deduplicated MaintenanceRecords"
)
@dlt.expect_or_drop("valid_maintenance_id", "MaintenanceID IS NOT NULL")
@dlt.expect_or_drop("valid_cost", "Cost >= 0")
@dlt.expect_or_drop("valid_duration", "Duration >= 0")
def maintenance_records_silver():
    """
    Silver transformation for MaintenanceRecords:
    - Deduplication by MaintenanceID
    - Data quality checks (cost, duration must be positive)
    - Metrics-ready format
    """
    return (
        dlt.read_stream("dbo_MaintenanceRecords_bronze")
            .dropDuplicates(["MaintenanceID"])
            .select(
                col("MaintenanceID").cast("bigint"),
                col("AssetID").cast("bigint"),
                col("WorkOrderID").cast("bigint"),
                col("MaintenanceType"),
                col("MaintenanceDate").cast("timestamp"),
                col("Cost").cast("decimal(10,2)"),
                col("Duration").cast("int"),
                col("ModifiedDate").cast("timestamp"),
                col("_ingestion_timestamp")
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: Gold Aggregations

# COMMAND ----------

@dlt.table(
    name="work_orders_daily_summary",
    comment="Gold table - daily work order statistics"
)
def work_orders_daily_summary():
    """
    Example Gold aggregation:
    - Daily rollup of work orders by status
    - Materialized for fast dashboarding
    """
    from pyspark.sql.functions import date_trunc, count, countDistinct

    return (
        dlt.read("dbo_WorkOrders_silver")
            .groupBy(
                date_trunc("day", col("CreatedDate")).alias("date"),
                col("Status")
            )
            .agg(
                count("*").alias("total_work_orders"),
                countDistinct("WorkOrderID").alias("unique_work_orders")
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Configuration
# MAGIC
# MAGIC **Create DLT Pipeline with:**
# MAGIC - **Compute**: Serverless
# MAGIC - **Target**: main.fh_bronze (Bronze tables)
# MAGIC - **Target**: main.fh_silver (Silver tables)
# MAGIC - **Continuous**: Yes (for real-time CDC)
# MAGIC - **Channel**: Current

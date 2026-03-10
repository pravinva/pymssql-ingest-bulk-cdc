# Databricks notebook source
# MAGIC %md
# MAGIC # Incremental CDC Extraction
# MAGIC
# MAGIC Extracts only changed rows (WHERE ModifiedDate > last checkpoint)

# COMMAND ----------

%pip install pymssql --quiet
dbutils.library.restartPython()

# COMMAND ----------

import pymssql
from datetime import datetime, date
import time
from pyspark.sql.functions import to_date

# Source: Azure SQL
SOURCE_CONFIG = {
    "host": "<your-server>.database.windows.net",
    "port": 1433,
    "database": "<your-database>",
    "user": "<your-username>",
    "password": dbutils.secrets.get(scope="sql_server", key="password")
}

# Destination: UC Volume
UC_VOLUME_PATH = "/Volumes/main/fh_ingestion/landing"

# Extraction config
BATCH_SIZE = 10_000
SLEEP_MS = 200

# Tables with selective columns
TABLES_CONFIG = {
    "dbo.Assets": {
        "columns": ["AssetID", "AssetNumber", "AssetName", "AssetType", "Location", "Status", "ModifiedDate"],
        "cdc_column": "ModifiedDate",
        "partition_column": "ModifiedDate"
    },
    "dbo.WorkOrders": {
        "columns": ["WorkOrderID", "WorkOrderNumber", "Description", "Status", "Priority", "AssetID", "CreatedDate", "ModifiedDate"],
        "cdc_column": "ModifiedDate",
        "partition_column": "CreatedDate"
    },
    "dbo.MaintenanceRecords": {
        "columns": ["MaintenanceID", "AssetID", "WorkOrderID", "MaintenanceType", "MaintenanceDate", "Cost", "Duration", "ModifiedDate"],
        "cdc_column": "ModifiedDate",
        "partition_column": "MaintenanceDate"
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Last Checkpoint (for incremental mode)

# COMMAND ----------

# For this demo, get MAX(ModifiedDate) from existing data as checkpoint
# In production, you'd read from a checkpoint table

checkpoints = {}

for table_name, config in TABLES_CONFIG.items():
    table_safe_name = table_name.replace(".", "_")
    table_path = f"{UC_VOLUME_PATH}/{table_safe_name}"

    try:
        # Read existing Parquet to find max ModifiedDate
        df = spark.read.parquet(table_path)
        max_date = df.selectExpr(f"MAX({config['cdc_column']}) as max_date").first()[0]
        checkpoints[table_name] = max_date
        print(f"✅ {table_name}: Checkpoint = {max_date}")
    except Exception as e:
        print(f"⚠️  {table_name}: No checkpoint (will extract all data)")
        checkpoints[table_name] = None

print()
print(f"Checkpoints: {checkpoints}")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental Extraction Function

# COMMAND ----------

def extract_incremental(table_name, columns, cdc_column, partition_column, last_cdc_value):
    """
    Extract only changed rows (WHERE cdc_column > last_cdc_value)
    """
    print(f"\\n{'='*80}")
    print(f"Incremental Extraction: {table_name}")
    print(f"{'='*80}")
    print(f"Last checkpoint: {last_cdc_value}")
    print(f"Extracting rows where {cdc_column} > '{last_cdc_value}'")
    print()

    start_time = time.time()
    total_rows = 0

    conn = pymssql.connect(
        server=SOURCE_CONFIG['host'],
        port=SOURCE_CONFIG['port'],
        database=SOURCE_CONFIG['database'],
        user=SOURCE_CONFIG['user'],
        password=SOURCE_CONFIG['password'],
        timeout=300
    )

    try:
        cursor = conn.cursor(as_dict=True)
        columns_str = ", ".join(columns)

        # Incremental query
        query = f"""
        SELECT {columns_str}
        FROM {table_name}
        WHERE {cdc_column} > '{last_cdc_value}'
        ORDER BY {cdc_column}
        """

        print(f"Query: {query}")
        print()

        cursor.execute(query)
        rows = cursor.fetchall()

        total_rows = len(rows)
        print(f"✅ Extracted {total_rows:,} changed rows")

        if total_rows > 0:
            # Write to UC Volume with Hive partitioning
            table_safe_name = table_name.replace(".", "_")
            table_path = f"{UC_VOLUME_PATH}/{table_safe_name}"

            # Convert to Spark DataFrame directly
            spark_df = spark.createDataFrame(rows)

            if partition_column in spark_df.columns:
                spark_df = spark_df.withColumn("date", to_date(partition_column))

                (spark_df
                    .write
                    .mode("append")
                    .partitionBy("date")
                    .parquet(table_path))
            else:
                (spark_df
                    .write
                    .mode("append")
                    .parquet(table_path))

            print(f"✅ Written to {table_path}")

        elapsed = time.time() - start_time
        print(f"✅ Completed in {elapsed:.1f}s")
        print()

        return {
            "table_name": table_name,
            "rows_extracted": total_rows,
            "elapsed_sec": elapsed,
            "status": "success"
        }

    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return {
            "table_name": table_name,
            "status": "failed",
            "error": str(e)
        }
    finally:
        conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Incremental Extraction for All Tables

# COMMAND ----------

results = []

for table_name, config in TABLES_CONFIG.items():
    last_checkpoint = checkpoints.get(table_name)

    if last_checkpoint:
        result = extract_incremental(
            table_name=table_name,
            columns=config['columns'],
            cdc_column=config['cdc_column'],
            partition_column=config['partition_column'],
            last_cdc_value=last_checkpoint
        )
        results.append(result)
    else:
        print(f"⚠️  Skipping {table_name} - no checkpoint available")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("="*80)
print("INCREMENTAL EXTRACTION SUMMARY")
print("="*80)

total_cdc_rows = 0
for result in results:
    if result['status'] == 'success':
        total_cdc_rows += result['rows_extracted']
        print(f"✅ {result['table_name']:40s} | {result['rows_extracted']:,} CDC rows extracted")
    else:
        print(f"❌ {result['table_name']:40s} | FAILED: {result.get('error', 'Unknown')}")

print()
print(f"Total CDC rows extracted: {total_cdc_rows:,}")
print()
print("Next: Auto Loader will automatically pick up these new files")
print("      DLT pipeline will process them into Bronze/Silver tables")

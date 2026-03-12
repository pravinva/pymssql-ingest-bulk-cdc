# Databricks notebook source
# MAGIC %md
# MAGIC # Python Custom MSSQL Source (Direct-to-Delta)
# MAGIC
# MAGIC Custom Python data source that:
# MAGIC - Connects to SQL Server using pymssql
# MAGIC - Extracts data in batches (append-only bulk load)
# MAGIC - Writes directly to Bronze Delta tables
# MAGIC - Runs natively on SDP Serverless (no driver installation required)
# MAGIC - Aligned with Lakeflow Connect managed connector future state
# MAGIC
# MAGIC **Key Features:**
# MAGIC - Schema inference + evolution enabled
# MAGIC - Change Data Feed enabled (for DLT consumption)
# MAGIC - Bandwidth throttling for network-constrained environments

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Dependencies

# COMMAND ----------

%pip install pymssql --quiet
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import pymssql
from datetime import datetime, date
import time
from pyspark.sql.types import *

# Source: Azure SQL (use Secrets in production)
SOURCE_CONFIG = {
    "host": dbutils.secrets.get(scope="sql_server", key="host"),
    "port": 1433,
    "database": dbutils.secrets.get(scope="sql_server", key="database"),
    "user": dbutils.secrets.get(scope="sql_server", key="username"),
    "password": dbutils.secrets.get(scope="sql_server", key="password")
}

# For production with Kapua, use:
# SOURCE_CONFIG = {
#     "host": dbutils.secrets.get("fh-scope", "kapua-host"),
#     "port": 1433,
#     "database": dbutils.secrets.get("fh-scope", "kapua-database"),
#     "user": dbutils.secrets.get("fh-scope", "kapua-user"),
#     "password": dbutils.secrets.get("fh-scope", "kapua-password")
# }

# Extraction config
BATCH_SIZE = 10_000
SLEEP_MS = 200

# Throttling config (for network-constrained environments like Fulton Hogan)
ENABLE_THROTTLING = True      # Set to False for same-region Azure VM sources
TARGET_BANDWIDTH_MBPS = 150   # Target bandwidth in Megabits per second (150 Mbps = 18.75 MB/s)

# Tables with selective columns (as validated in testing)
TABLES_CONFIG = {
    "dbo.Assets": {
        "columns": ["AssetID", "AssetNumber", "AssetName", "AssetType", "Location", "Status", "ModifiedDate"]
    },
    "dbo.WorkOrders": {
        "columns": ["WorkOrderID", "WorkOrderNumber", "Description", "Status", "Priority", "AssetID", "CreatedDate", "ModifiedDate"]
    },
    "dbo.MaintenanceRecords": {
        "columns": ["MaintenanceID", "AssetID", "WorkOrderID", "MaintenanceType", "MaintenanceDate", "Cost", "Duration", "ModifiedDate"]
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Python Custom MSSQL Source Class (Direct-to-Delta)

# COMMAND ----------

class DirectDeltaMSSQLSource:
    """
    Custom Python MSSQL Source for Direct Delta Writes

    Features:
    - Connects to SQL Server (no primary access required)
    - Bulk load with append mode (no CDC for now)
    - Writes directly to Bronze Delta tables
    - Schema inference + evolution enabled
    - Change Data Feed enabled (for DLT)
    - Bandwidth throttling support
    """

    def __init__(self, config):
        self.config = config
        self.stats = []

    def connect(self):
        """Establish connection to SQL Server"""
        return pymssql.connect(
            server=self.config['host'],
            port=self.config['port'],
            database=self.config['database'],
            user=self.config['user'],
            password=self.config['password'],
            timeout=300
        )

    def extract_and_write_delta(self, table_name, columns):
        """
        Extract table and write directly to Bronze Delta table

        Args:
            table_name: SQL Server table name (e.g., 'dbo.Assets')
            columns: List of columns to extract

        Returns:
            Extraction statistics
        """
        print(f"\n{'='*80}")
        print(f"Extracting: {table_name} (append-only bulk load)")
        print(f"{'='*80}")
        print(f"Columns: {len(columns)}")
        print(f"Target: main.fh_bronze.{table_name.replace('.', '_')}")
        print()

        start_time = time.time()
        total_rows = 0
        batch_num = 0

        conn = self.connect()

        try:
            # Build query
            columns_str = ", ".join(columns)
            base_query = f"""
            SELECT {columns_str}
            FROM {table_name}
            ORDER BY (SELECT NULL)
            OFFSET {{offset}} ROWS
            FETCH NEXT {BATCH_SIZE} ROWS ONLY
            """

            # Get row count (use non-dict cursor for COUNT)
            count_cursor = conn.cursor()
            count_cursor.execute(f"SELECT COUNT_BIG(*) FROM {table_name}")
            row_count = count_cursor.fetchone()[0]
            count_cursor.close()

            print(f"Total rows: {row_count:,}")
            print()

            # Create dict cursor for data extraction
            cursor = conn.cursor(as_dict=True)

            # Target Delta table
            target_table = f"main.fh_bronze.{table_name.replace('.', '_')}"

            # Extract in batches and write to Delta
            offset = 0
            while True:
                batch_num += 1
                batch_start = time.time()

                # Execute query
                query = base_query.format(offset=offset)
                cursor.execute(query)
                rows = cursor.fetchall()

                if not rows:
                    break

                batch_rows = len(rows)
                total_rows += batch_rows

                # Convert to Spark DataFrame (schema inference)
                spark_df = spark.createDataFrame(rows)

                # Add metadata columns
                from pyspark.sql.functions import current_timestamp, lit
                spark_df = spark_df \
                    .withColumn("_ingestion_timestamp", current_timestamp()) \
                    .withColumn("_source_table", lit(table_name))

                # Write to Bronze Delta table with append mode
                (spark_df
                    .write
                    .format("delta")
                    .mode("append")
                    .option("mergeSchema", "true")  # Schema evolution
                    .option("delta.enableChangeDataFeed", "true")  # For DLT
                    .saveAsTable(target_table))

                batch_elapsed = time.time() - batch_start
                throughput = batch_rows / batch_elapsed if batch_elapsed > 0 else 0

                pct = (total_rows / row_count * 100) if row_count > 0 else 0
                print(f"Batch {batch_num:4d} | "
                      f"Rows: {batch_rows:6,} | "
                      f"Total: {total_rows:10,} ({pct:5.1f}%) | "
                      f"Time: {batch_elapsed:5.2f}s | "
                      f"Throughput: {throughput:8.0f} rows/s")

                # Throttle if enabled
                if ENABLE_THROTTLING and SLEEP_MS > 0:
                    time.sleep(SLEEP_MS / 1000.0)

                offset += BATCH_SIZE

                # Safety limit
                if batch_num > 10000:
                    break

            cursor.close()

            elapsed = time.time() - start_time
            avg_throughput = total_rows / elapsed if elapsed > 0 else 0

            print()
            print(f"✅ Extraction complete: {total_rows:,} rows in {elapsed:.1f}s")
            print(f"   Written to: {target_table}")
            print()

            return {
                "table_name": table_name,
                "target_table": target_table,
                "total_rows": total_rows,
                "elapsed_sec": elapsed,
                "throughput_rows_sec": avg_throughput,
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
# MAGIC ## Run Extraction

# COMMAND ----------

# Initialize source
source = DirectDeltaMSSQLSource(SOURCE_CONFIG)

# Extract all tables
results = []

for table_name, config in TABLES_CONFIG.items():
    result = source.extract_and_write_delta(
        table_name=table_name,
        columns=config['columns']
    )
    results.append(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("="*80)
print("EXTRACTION SUMMARY")
print("="*80)

for result in results:
    if result['status'] == 'success':
        print(f"✅ {result['table_name']:40s} | {result['total_rows']:12,} rows | {result['throughput_rows_sec']:8,.0f} rows/s")
        print(f"   Target: {result['target_table']}")
    else:
        print(f"❌ {result['table_name']:40s} | FAILED: {result.get('error', 'Unknown')}")

print()
print("Next Step: Run DLT pipeline (Notebook 03) to transform Bronze → Silver")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Bronze Delta Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show Bronze tables
# MAGIC SHOW TABLES IN main.fh_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check one table's properties
# MAGIC DESCRIBE EXTENDED main.fh_bronze.dbo_Assets;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quick row count check
# MAGIC SELECT
#  'dbo_Assets' as table_name,
#   COUNT(*) as row_count,
#   MIN(_ingestion_timestamp) as first_ingestion,
#   MAX(_ingestion_timestamp) as last_ingestion
# MAGIC FROM main.fh_bronze.dbo_Assets;

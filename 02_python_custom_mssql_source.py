# Databricks notebook source
# MAGIC %md
# MAGIC # Python Custom MSSQL Source (SDP Component)
# MAGIC
# MAGIC Custom Python data source that:
# MAGIC - Connects to SQL Server read replica using pymssql
# MAGIC - Extracts data with CDC support (timestamp-based or Change Tracking)
# MAGIC - Writes to UC Volume as Hive-partitioned Parquet files
# MAGIC - Runs natively on SDP Serverless (no driver installation required)

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

# Source: Azure SQL (use Secrets in production)
SOURCE_CONFIG = {
    "host": "<your-server>.database.windows.net",
    "port": 1433,
    "database": "<your-database>",
    "user": "<your-username>",
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

# Destination: UC Volume
UC_VOLUME_PATH = "/Volumes/main/fh_ingestion/landing"

# Extraction config
BATCH_SIZE = 10_000
SLEEP_MS = 200

# Tables with selective columns (as validated in testing)
TABLES_CONFIG = {
    "dbo.Assets": {
        "columns": ["AssetID", "AssetNumber", "AssetName", "AssetType", "Location", "Status", "ModifiedDate"],
        "cdc_column": "ModifiedDate",  # For timestamp-based CDC
        "partition_column": "ModifiedDate"  # For Hive partitioning
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
# MAGIC ## Python Custom MSSQL Source Class

# COMMAND ----------

class PythonCustomMSSQLSource:
    """
    Custom Python MSSQL Source for SDP + Lakeflow Framework

    Features:
    - Connects to SQL Server read replica (no primary access required)
    - Supports CDC via timestamp-based extraction
    - Writes Hive-partitioned Parquet to UC Volume
    - Runs on Serverless (no driver installation needed)
    """

    def __init__(self, config, uc_volume_path):
        self.config = config
        self.uc_volume_path = uc_volume_path
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

    def get_last_extracted_value(self, table_name, cdc_column):
        """
        Get last extracted CDC value from checkpoint

        In production, read from Delta checkpoint table.
        For now, extract all data (bulk mode).
        """
        # TODO: Implement checkpoint reading
        # return spark.read.table("main.fh_ingestion.checkpoints") \
        #     .filter(f"table_name = '{table_name}'") \
        #     .select("last_cdc_value") \
        #     .first()[0]

        # For bulk extract, return None
        return None

    def save_checkpoint(self, table_name, cdc_column, last_value):
        """
        Save CDC checkpoint for incremental extraction

        In production, write to Delta checkpoint table.
        """
        # TODO: Implement checkpoint saving
        # checkpoint_df = spark.createDataFrame([{
        #     "table_name": table_name,
        #     "cdc_column": cdc_column,
        #     "last_cdc_value": last_value,
        #     "extraction_timestamp": datetime.now()
        # }])
        # checkpoint_df.write.mode("merge").saveAsTable("main.fh_ingestion.checkpoints")
        pass

    def extract_table(self, table_name, columns, cdc_column, partition_column, mode='bulk'):
        """
        Extract table with CDC support and Hive partitioning

        Args:
            table_name: SQL Server table name (e.g., 'dbo.Assets')
            columns: List of columns to extract
            cdc_column: Column for CDC (e.g., 'ModifiedDate')
            partition_column: Column for Hive partitioning (e.g., 'CreatedDate')
            mode: 'bulk' or 'incremental'

        Returns:
            Extraction statistics
        """
        print(f"\n{'='*80}")
        print(f"Extracting: {table_name} (mode={mode})")
        print(f"{'='*80}")
        print(f"Columns: {len(columns)}")
        print(f"CDC Column: {cdc_column}")
        print(f"Partition Column: {partition_column}")
        print()

        start_time = time.time()
        total_rows = 0
        batch_num = 0

        conn = self.connect()

        try:
            # Get last extracted value for incremental mode
            last_cdc_value = None
            if mode == 'incremental':
                last_cdc_value = self.get_last_extracted_value(table_name, cdc_column)

            # Build query
            columns_str = ", ".join(columns)

            if mode == 'bulk':
                # Bulk: Extract everything
                base_query = f"""
                SELECT {columns_str}
                FROM {table_name}
                ORDER BY (SELECT NULL)
                OFFSET {{offset}} ROWS
                FETCH NEXT {BATCH_SIZE} ROWS ONLY
                """
            else:
                # Incremental: Extract only new/changed records
                base_query = f"""
                SELECT {columns_str}
                FROM {table_name}
                WHERE {cdc_column} > '{{last_cdc_value}}'
                ORDER BY {cdc_column}
                OFFSET {{offset}} ROWS
                FETCH NEXT {BATCH_SIZE} ROWS ONLY
                """

            # Get row count (use non-dict cursor for COUNT)
            count_cursor = conn.cursor()
            if mode == 'bulk':
                count_cursor.execute(f"SELECT COUNT_BIG(*) FROM {table_name}")
            else:
                count_cursor.execute(f"SELECT COUNT_BIG(*) FROM {table_name} WHERE {cdc_column} > '{last_cdc_value}'")

            row_count = count_cursor.fetchone()[0]

            # Now create dict cursor for data extraction
            cursor = conn.cursor(as_dict=True)
            print(f"Total rows: {row_count:,}")
            print()

            # Extract in batches with Hive partitioning
            offset = 0
            while True:
                batch_num += 1
                batch_start = time.time()

                # Execute query
                if mode == 'bulk':
                    query = base_query.format(offset=offset)
                else:
                    query = base_query.format(last_cdc_value=last_cdc_value, offset=offset)

                cursor.execute(query)
                rows = cursor.fetchall()

                if not rows:
                    break

                batch_rows = len(rows)
                total_rows += batch_rows

                # Write to UC Volume with Hive partitioning by date
                # Use Spark directly (no pandas)
                table_safe_name = table_name.replace(".", "_")
                table_path = f"{self.uc_volume_path}/{table_safe_name}"

                # Convert rows to Spark DataFrame directly
                spark_df = spark.createDataFrame(rows)

                if partition_column in spark_df.columns:
                    # Add partition date column for Hive partitioning
                    from pyspark.sql.functions import to_date
                    spark_df = spark_df.withColumn("date", to_date(partition_column))

                    # Write with Hive partitioning by date
                    (spark_df
                        .write
                        .mode("append")
                        .partitionBy("date")
                        .parquet(table_path))

                else:
                    # No partition column, write without partitioning
                    (spark_df
                        .write
                        .mode("append")
                        .parquet(table_path))

                batch_elapsed = time.time() - batch_start
                throughput = batch_rows / batch_elapsed if batch_elapsed > 0 else 0

                pct = (total_rows / row_count * 100) if row_count > 0 else 0
                print(f"Batch {batch_num:4d} | "
                      f"Rows: {batch_rows:6,} | "
                      f"Total: {total_rows:10,} ({pct:5.1f}%) | "
                      f"Time: {batch_elapsed:5.2f}s | "
                      f"Throughput: {throughput:8.0f} rows/s")

                # Throttle
                if SLEEP_MS > 0:
                    time.sleep(SLEEP_MS / 1000.0)

                offset += BATCH_SIZE

                if batch_num > 10000:
                    break

            elapsed = time.time() - start_time
            avg_throughput = total_rows / elapsed if elapsed > 0 else 0

            # Save checkpoint for next incremental run
            if total_rows > 0 and cdc_column:
                # Use non-dict cursor for MAX query (unnamed column)
                count_cursor.execute(f"SELECT MAX({cdc_column}) FROM {table_name}")
                max_cdc_value = count_cursor.fetchone()[0]
                self.save_checkpoint(table_name, cdc_column, max_cdc_value)

            print()
            print(f"✅ Extraction complete: {total_rows:,} rows in {elapsed:.1f}s")
            print()

            return {
                "table_name": table_name,
                "mode": mode,
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
                "mode": mode,
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
source = PythonCustomMSSQLSource(SOURCE_CONFIG, UC_VOLUME_PATH)

# Extract all tables
results = []

for table_name, config in TABLES_CONFIG.items():
    result = source.extract_table(
        table_name=table_name,
        columns=config['columns'],
        cdc_column=config['cdc_column'],
        partition_column=config['partition_column'],
        mode='bulk'  # Change to 'incremental' for CDC mode
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
    else:
        print(f"❌ {result['table_name']:40s} | FAILED: {result.get('error', 'Unknown')}")

print()
print("Next Step: Run DLT pipeline to ingest into Bronze/Silver tables")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Files in UC Volume

# COMMAND ----------

# List files in UC Volume
display(dbutils.fs.ls(UC_VOLUME_PATH))

# COMMAND ----------

# Check one table's Hive partitions
table_path = f"{UC_VOLUME_PATH}/dbo_Assets"
try:
    display(dbutils.fs.ls(table_path))
except:
    print(f"No files yet in {table_path}")

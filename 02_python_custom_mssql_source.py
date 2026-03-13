# Databricks notebook source
# MAGIC %md
# MAGIC # Python Custom MSSQL Source (Direct-to-Delta with Checkpointing)
# MAGIC
# MAGIC Custom Python data source that:
# MAGIC - Connects to SQL Server using pymssql
# MAGIC - Extracts data in batches (append-only bulk load)
# MAGIC - Writes directly to Bronze Delta tables
# MAGIC - **Batch-level checkpointing for resume capability**
# MAGIC - **Retry logic for resilience**
# MAGIC - **Audit logging for traceability**
# MAGIC - Runs natively on SDP Serverless (no driver installation required)
# MAGIC - Aligned with Lakeflow Connect managed connector future state
# MAGIC
# MAGIC **Key Features:**
# MAGIC - Schema inference + evolution enabled
# MAGIC - Change Data Feed enabled (for DLT consumption)
# MAGIC - Bandwidth throttling for network-constrained environments
# MAGIC - Resume from last successful batch on failure

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
from pyspark.sql.functions import lit, current_timestamp

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
MAX_RETRIES = 3  # Retry failed batches up to 3 times

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
# MAGIC ## Setup Checkpoint and Audit Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create checkpoint table for batch-level progress tracking
# MAGIC CREATE TABLE IF NOT EXISTS main.fh_bronze.extraction_checkpoints (
# MAGIC   extraction_run_id STRING COMMENT 'Unique ID for this extraction run',
# MAGIC   table_name STRING COMMENT 'Source table name',
# MAGIC   batch_number INT COMMENT 'Batch number (1-based)',
# MAGIC   batch_start_offset BIGINT COMMENT 'Starting offset for this batch',
# MAGIC   batch_end_offset BIGINT COMMENT 'Ending offset for this batch',
# MAGIC   batch_row_count INT COMMENT 'Rows in this batch',
# MAGIC   status STRING COMMENT 'Status: processing, success, failed',
# MAGIC   start_timestamp TIMESTAMP COMMENT 'When batch processing started',
# MAGIC   end_timestamp TIMESTAMP COMMENT 'When batch processing completed',
# MAGIC   error_message STRING COMMENT 'Error message if failed',
# MAGIC   retry_count INT COMMENT 'Number of retries attempted'
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Batch-level checkpoints for extraction resume capability';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create audit log table for batch-level traceability
# MAGIC CREATE TABLE IF NOT EXISTS main.fh_bronze.extraction_audit_log (
# MAGIC   extraction_run_id STRING COMMENT 'Unique ID for this extraction run',
# MAGIC   table_name STRING COMMENT 'Source table name',
# MAGIC   target_table STRING COMMENT 'Target Delta table name',
# MAGIC   total_batches INT COMMENT 'Total number of batches',
# MAGIC   completed_batches INT COMMENT 'Successfully completed batches',
# MAGIC   failed_batches INT COMMENT 'Failed batches',
# MAGIC   total_rows_extracted BIGINT COMMENT 'Total rows extracted',
# MAGIC   start_timestamp TIMESTAMP COMMENT 'Extraction start time',
# MAGIC   end_timestamp TIMESTAMP COMMENT 'Extraction end time',
# MAGIC   duration_seconds DOUBLE COMMENT 'Total duration in seconds',
# MAGIC   avg_throughput_rows_sec DOUBLE COMMENT 'Average throughput',
# MAGIC   status STRING COMMENT 'Overall status: success, partial, failed'
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Audit log for extraction runs (simulates file-level audit trail)';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Python Custom MSSQL Source Class (Enhanced with Checkpointing)

# COMMAND ----------

class ResilientDeltaMSSQLSource:
    """
    Resilient Python MSSQL Source for Direct Delta Writes

    Features:
    - Connects to SQL Server (no primary access required)
    - Bulk load with append mode (no CDC for now)
    - Writes directly to Bronze Delta tables
    - Batch-level checkpointing for resume capability
    - Automatic retry logic for failed batches
    - Comprehensive audit logging
    - Schema inference + evolution enabled
    - Change Data Feed enabled (for DLT)
    - Bandwidth throttling support
    """

    def __init__(self, config, extraction_run_id=None):
        self.config = config
        self.extraction_run_id = extraction_run_id or datetime.now().strftime("%Y%m%d_%H%M%S_%f")

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

    def get_last_successful_batch(self, table_name):
        """Get the last successfully completed batch for resume capability"""
        try:
            checkpoint_df = spark.sql(f"""
                SELECT MAX(batch_end_offset) as last_offset
                FROM main.fh_bronze.extraction_checkpoints
                WHERE table_name = '{table_name}'
                  AND status = 'success'
                  AND extraction_run_id = '{self.extraction_run_id}'
            """)

            last_offset = checkpoint_df.first()["last_offset"]
            return last_offset if last_offset else 0
        except:
            return 0

    def log_batch_checkpoint(self, table_name, batch_number, start_offset, end_offset,
                            row_count, status, start_time, end_time=None,
                            error_message=None, retry_count=0):
        """Log batch-level checkpoint for audit and resume"""
        checkpoint_data = [{
            "extraction_run_id": self.extraction_run_id,
            "table_name": table_name,
            "batch_number": batch_number,
            "batch_start_offset": start_offset,
            "batch_end_offset": end_offset,
            "batch_row_count": row_count,
            "status": status,
            "start_timestamp": start_time,
            "end_timestamp": end_time or datetime.now(),
            "error_message": error_message,
            "retry_count": retry_count
        }]

        checkpoint_df = spark.createDataFrame(checkpoint_data)
        checkpoint_df.write.format("delta").mode("append").saveAsTable("main.fh_bronze.extraction_checkpoints")

    def log_extraction_audit(self, table_name, target_table, total_batches, completed_batches,
                            failed_batches, total_rows, start_time, end_time, status):
        """Log extraction run audit summary"""
        duration = (end_time - start_time).total_seconds()
        avg_throughput = total_rows / duration if duration > 0 else 0

        audit_data = [{
            "extraction_run_id": self.extraction_run_id,
            "table_name": table_name,
            "target_table": target_table,
            "total_batches": total_batches,
            "completed_batches": completed_batches,
            "failed_batches": failed_batches,
            "total_rows_extracted": total_rows,
            "start_timestamp": start_time,
            "end_timestamp": end_time,
            "duration_seconds": duration,
            "avg_throughput_rows_sec": avg_throughput,
            "status": status
        }]

        audit_df = spark.createDataFrame(audit_data)
        audit_df.write.format("delta").mode("append").saveAsTable("main.fh_bronze.extraction_audit_log")

    def extract_batch_with_retry(self, cursor, query, batch_number, offset, target_table, table_name):
        """Extract a single batch with retry logic"""
        for retry in range(MAX_RETRIES):
            batch_start_time = datetime.now()

            try:
                # Execute query
                cursor.execute(query.format(offset=offset))
                rows = cursor.fetchall()

                if not rows:
                    return None, 0

                batch_rows = len(rows)

                # Convert to Spark DataFrame (schema inference)
                spark_df = spark.createDataFrame(rows)

                # Add metadata columns
                spark_df = spark_df \
                    .withColumn("_ingestion_timestamp", current_timestamp()) \
                    .withColumn("_source_table", lit(table_name)) \
                    .withColumn("_extraction_run_id", lit(self.extraction_run_id)) \
                    .withColumn("_batch_number", lit(batch_number))

                # Write to Bronze Delta table with append mode
                (spark_df
                    .write
                    .format("delta")
                    .mode("append")
                    .option("mergeSchema", "true")  # Schema evolution
                    .option("delta.enableChangeDataFeed", "true")  # For DLT
                    .saveAsTable(target_table))

                # Log successful batch
                self.log_batch_checkpoint(
                    table_name=table_name,
                    batch_number=batch_number,
                    start_offset=offset,
                    end_offset=offset + batch_rows,
                    row_count=batch_rows,
                    status="success",
                    start_time=batch_start_time,
                    end_time=datetime.now(),
                    retry_count=retry
                )

                return rows, batch_rows

            except Exception as e:
                error_msg = str(e)
                print(f"   ⚠️  Batch {batch_number} failed (attempt {retry + 1}/{MAX_RETRIES}): {error_msg}")

                # Log failed attempt
                self.log_batch_checkpoint(
                    table_name=table_name,
                    batch_number=batch_number,
                    start_offset=offset,
                    end_offset=offset + BATCH_SIZE,
                    row_count=0,
                    status="failed",
                    start_time=batch_start_time,
                    error_message=error_msg,
                    retry_count=retry
                )

                if retry < MAX_RETRIES - 1:
                    # Wait before retry (exponential backoff)
                    wait_time = 2 ** retry
                    print(f"   Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    print(f"   ❌ Batch {batch_number} failed after {MAX_RETRIES} attempts")
                    raise

    def extract_and_write_delta(self, table_name, columns):
        """
        Extract table and write directly to Bronze Delta table with checkpointing

        Args:
            table_name: SQL Server table name (e.g., 'dbo.Assets')
            columns: List of columns to extract

        Returns:
            Extraction statistics
        """
        print(f"\n{'='*80}")
        print(f"Extracting: {table_name}")
        print(f"Extraction Run ID: {self.extraction_run_id}")
        print(f"{'='*80}")
        print(f"Columns: {len(columns)}")

        target_table = f"main.fh_bronze.{table_name.replace('.', '_')}"
        print(f"Target: {target_table}")
        print()

        extraction_start_time = datetime.now()
        total_rows = 0
        batch_num = 0
        completed_batches = 0
        failed_batches = 0

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

            # Get row count
            count_cursor = conn.cursor()
            count_cursor.execute(f"SELECT COUNT_BIG(*) FROM {table_name}")
            row_count = count_cursor.fetchone()[0]
            count_cursor.close()

            print(f"Total rows: {row_count:,}")

            # Check for resume point
            resume_offset = self.get_last_successful_batch(table_name)
            if resume_offset > 0:
                print(f"Resuming from offset: {resume_offset:,}")
            print()

            # Create dict cursor for data extraction
            cursor = conn.cursor(as_dict=True)

            # Extract in batches
            offset = resume_offset
            while True:
                batch_num += 1

                rows, batch_rows = self.extract_batch_with_retry(
                    cursor=cursor,
                    query=base_query,
                    batch_number=batch_num,
                    offset=offset,
                    target_table=target_table,
                    table_name=table_name
                )

                if rows is None:
                    break

                total_rows += batch_rows
                completed_batches += 1

                pct = (total_rows / row_count * 100) if row_count > 0 else 0
                print(f"✅ Batch {batch_num:4d} | "
                      f"Rows: {batch_rows:6,} | "
                      f"Total: {total_rows:10,} ({pct:5.1f}%)")

                # Throttle if enabled
                if ENABLE_THROTTLING and SLEEP_MS > 0:
                    time.sleep(SLEEP_MS / 1000.0)

                offset += BATCH_SIZE

                # Safety limit
                if batch_num > 10000:
                    break

            cursor.close()

            extraction_end_time = datetime.now()

            # Log audit summary
            status = "success" if failed_batches == 0 else "partial"
            self.log_extraction_audit(
                table_name=table_name,
                target_table=target_table,
                total_batches=batch_num,
                completed_batches=completed_batches,
                failed_batches=failed_batches,
                total_rows=total_rows,
                start_time=extraction_start_time,
                end_time=extraction_end_time,
                status=status
            )

            elapsed = (extraction_end_time - extraction_start_time).total_seconds()
            avg_throughput = total_rows / elapsed if elapsed > 0 else 0

            print()
            print(f"✅ Extraction complete: {total_rows:,} rows in {elapsed:.1f}s")
            print(f"   Target: {target_table}")
            print(f"   Completed batches: {completed_batches}")
            print(f"   Failed batches: {failed_batches}")
            print()

            return {
                "table_name": table_name,
                "target_table": target_table,
                "extraction_run_id": self.extraction_run_id,
                "total_rows": total_rows,
                "completed_batches": completed_batches,
                "failed_batches": failed_batches,
                "elapsed_sec": elapsed,
                "throughput_rows_sec": avg_throughput,
                "status": status
            }

        except Exception as e:
            print(f"❌ ERROR: {e}")
            import traceback
            traceback.print_exc()

            extraction_end_time = datetime.now()

            # Log failed extraction
            self.log_extraction_audit(
                table_name=table_name,
                target_table=target_table,
                total_batches=batch_num,
                completed_batches=completed_batches,
                failed_batches=failed_batches + 1,
                total_rows=total_rows,
                start_time=extraction_start_time,
                end_time=extraction_end_time,
                status="failed"
            )

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

# Initialize source (generates unique extraction_run_id)
source = ResilientDeltaMSSQLSource(SOURCE_CONFIG)

print(f"Extraction Run ID: {source.extraction_run_id}")
print()

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
    if result['status'] in ['success', 'partial']:
        print(f"✅ {result['table_name']:40s} | {result['total_rows']:12,} rows | {result['throughput_rows_sec']:8,.0f} rows/s")
        print(f"   Target: {result['target_table']}")
        print(f"   Batches: {result['completed_batches']} completed, {result['failed_batches']} failed")
    else:
        print(f"❌ {result['table_name']:40s} | FAILED: {result.get('error', 'Unknown')}")

print()
print("Next Step: Run DLT pipeline (Notebook 03) to transform Bronze → Silver")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Checkpoint and Audit Logs

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View extraction audit summary
# MAGIC SELECT
# MAGIC   extraction_run_id,
# MAGIC   table_name,
# MAGIC   total_rows_extracted,
# MAGIC   completed_batches,
# MAGIC   failed_batches,
# MAGIC   ROUND(duration_seconds, 2) as duration_sec,
# MAGIC   ROUND(avg_throughput_rows_sec, 0) as throughput,
# MAGIC   status,
# MAGIC   start_timestamp
# MAGIC FROM main.fh_bronze.extraction_audit_log
# MAGIC ORDER BY start_timestamp DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View batch-level checkpoints (for troubleshooting)
# MAGIC SELECT
# MAGIC   extraction_run_id,
# MAGIC   table_name,
# MAGIC   batch_number,
# MAGIC   batch_row_count,
# MAGIC   status,
# MAGIC   retry_count,
# MAGIC   error_message,
# MAGIC   start_timestamp
# MAGIC FROM main.fh_bronze.extraction_checkpoints
# MAGIC WHERE status = 'failed'  -- Show only failed batches
# MAGIC ORDER BY start_timestamp DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Bronze Delta Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show Bronze tables
# MAGIC SHOW TABLES IN main.fh_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check one table's row count and metadata
# MAGIC SELECT
# MAGIC   COUNT(*) as total_rows,
# MAGIC   COUNT(DISTINCT _extraction_run_id) as extraction_runs,
# MAGIC   MIN(_ingestion_timestamp) as first_ingestion,
# MAGIC   MAX(_ingestion_timestamp) as last_ingestion
# MAGIC FROM main.fh_bronze.dbo_Assets;

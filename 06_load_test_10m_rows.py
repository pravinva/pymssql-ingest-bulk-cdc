# Databricks notebook source
# MAGIC %md
# MAGIC # Load Test: 10M Rows with 150 Mbps Bandwidth Simulation
# MAGIC
# MAGIC Tests extraction pattern at scale with simulated bandwidth constraints:
# MAGIC - 10 million rows (~100 MB of data)
# MAGIC - Application-layer bandwidth throttling at 150 Mbps (18.75 MB/s)
# MAGIC - Comparison: Throttled vs Unthrottled performance

# COMMAND ----------

%pip install pymssql --quiet
dbutils.library.restartPython()

# COMMAND ----------

import pymssql
from datetime import datetime, timedelta
import time
import random
from pyspark.sql.functions import to_date, col

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Source: Azure SQL
SOURCE_CONFIG = {
    "host": dbutils.secrets.get(scope="sql_server", key="host"),
    "port": 1433,
    "database": dbutils.secrets.get(scope="sql_server", key="database"),
    "user": dbutils.secrets.get(scope="sql_server", key="username"),
    "password": dbutils.secrets.get(scope="sql_server", key="password")
}

# Destination: UC Volume
UC_VOLUME_PATH = "/Volumes/main/fh_ingestion/landing"

# Load Test Configuration
LOAD_TEST_CONFIG = {
    "target_rows": 10_000_000,       # 10 million rows
    "batch_size": 10_000,            # 10K rows per batch
    "enable_throttling": False,      # BASELINE TEST: Disable throttling for maximum speed
    "target_bandwidth_mbps": 150,    # 150 Megabits per second (75% of 200 Mbps link)
    "target_bandwidth_MBps": 18.75,  # 18.75 Megabytes per second (150 / 8)
}

print("="*80)
print("LOAD TEST CONFIGURATION")
print("="*80)
print(f"Target rows:           {LOAD_TEST_CONFIG['target_rows']:,}")
print(f"Batch size:            {LOAD_TEST_CONFIG['batch_size']:,}")
print(f"Target bandwidth:      {LOAD_TEST_CONFIG['target_bandwidth_mbps']} Mbps ({LOAD_TEST_CONFIG['target_bandwidth_MBps']} MB/s)")
print(f"Throttling enabled:    {LOAD_TEST_CONFIG['enable_throttling']}")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Generate 10M Row Test Dataset in Azure SQL

# COMMAND ----------

print("="*80)
print("GENERATING 10M ROW TEST DATASET IN AZURE SQL")
print("="*80)
print()

conn = pymssql.connect(
    server=SOURCE_CONFIG['host'],
    port=SOURCE_CONFIG['port'],
    database=SOURCE_CONFIG['database'],
    user=SOURCE_CONFIG['user'],
    password=SOURCE_CONFIG['password'],
    timeout=600
)

cursor = conn.cursor()

try:
    # Drop and recreate test table
    print("1. Creating test table: dbo.LoadTestData...")

    cursor.execute("DROP TABLE IF EXISTS dbo.LoadTestData")

    cursor.execute("""
        CREATE TABLE dbo.LoadTestData (
            LoadTestID BIGINT PRIMARY KEY,
            RecordNumber INT NOT NULL,
            TextData VARCHAR(100),
            NumericValue DECIMAL(18,2),
            DateValue DATE,
            ModifiedDate DATETIME DEFAULT GETDATE()
        )
    """)
    conn.commit()
    print("   ✅ Table created")
    print()

    # Insert in large batches
    print(f"2. Inserting {LOAD_TEST_CONFIG['target_rows']:,} rows in batches...")
    batch_size = 100_000  # 100K rows per insert batch
    total_rows = LOAD_TEST_CONFIG['target_rows']
    batches = total_rows // batch_size

    start_time = time.time()

    for batch_num in range(batches):
        batch_start = time.time()
        offset = batch_num * batch_size

        # Use efficient bulk insert with CTE to avoid ROW_NUMBER in WHERE clause
        cursor.execute(f"""
            WITH numbered AS (
                SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as rn
                FROM sys.all_columns a
                CROSS JOIN sys.all_columns b
            )
            INSERT INTO dbo.LoadTestData (LoadTestID, RecordNumber, TextData, NumericValue, DateValue, ModifiedDate)
            SELECT
                rn + {offset},
                (rn + {offset}) % 1000,
                'Test data for row ' + CAST((rn + {offset}) AS VARCHAR(20)),
                (rn + {offset}) * 1.5,
                DATEADD(DAY, -((rn + {offset}) % 365), GETDATE()),
                GETDATE()
            FROM numbered
            WHERE rn <= {batch_size}
        """)
        conn.commit()

        batch_elapsed = time.time() - batch_start
        total_inserted = (batch_num + 1) * batch_size
        pct_complete = (total_inserted / total_rows) * 100

        print(f"   Batch {batch_num+1:3d}/{batches} | Rows: {total_inserted:12,} ({pct_complete:5.1f}%) | Time: {batch_elapsed:6.2f}s")

    elapsed = time.time() - start_time
    print()
    print(f"✅ Generated {total_rows:,} rows in {elapsed:.1f}s ({total_rows/elapsed:,.0f} rows/s)")
    print()

    # Verify row count
    cursor.execute("SELECT COUNT_BIG(*) FROM dbo.LoadTestData")
    actual_count = cursor.fetchone()[0]
    print(f"✅ Verified: {actual_count:,} rows in dbo.LoadTestData")
    print()

except Exception as e:
    print(f"❌ ERROR: {e}")
    conn.rollback()
    raise

finally:
    cursor.close()
    conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Bandwidth Throttled Extraction

# COMMAND ----------

class BandwidthThrottledExtractor:
    """
    Extractor with application-layer bandwidth throttling

    Simulates a 150 Mbps (18.75 MB/s) network link by:
    1. Tracking bytes transferred over time
    2. Calculating current bandwidth utilization
    3. Inserting sleep delays when exceeding target bandwidth
    """

    def __init__(self, target_bandwidth_MBps=18.75):
        self.target_bandwidth_MBps = target_bandwidth_MBps  # 150 Mbps = 18.75 MB/s
        self.bytes_transferred = 0
        self.start_time = None
        self.batch_metrics = []

    def estimate_batch_size_bytes(self, num_rows, avg_row_bytes=50):
        """
        Estimate batch size in bytes

        For our test data:
        - LoadTestID: 8 bytes (BIGINT)
        - RecordNumber: 4 bytes (INT)
        - TextData: ~30 bytes (VARCHAR(100) with ~30 chars)
        - NumericValue: 8 bytes (DECIMAL)
        - DateValue: 3 bytes (DATE)
        - ModifiedDate: 8 bytes (DATETIME)
        Total: ~61 bytes per row (use 50 bytes conservatively for compression)
        """
        return num_rows * avg_row_bytes

    def calculate_sleep_time(self, batch_bytes):
        """Calculate sleep time to maintain target bandwidth"""
        if not self.start_time:
            self.start_time = time.time()
            return 0

        self.bytes_transferred += batch_bytes
        elapsed = time.time() - self.start_time

        if elapsed == 0:
            return 0

        # Current bandwidth in MB/s
        current_bandwidth_MBps = self.bytes_transferred / elapsed / (1024 * 1024)

        # If we're going too fast, sleep
        if current_bandwidth_MBps > self.target_bandwidth_MBps:
            # Calculate ideal time for bytes transferred so far
            ideal_time = (self.bytes_transferred / (1024 * 1024)) / self.target_bandwidth_MBps
            sleep_time = ideal_time - elapsed
            return max(0, sleep_time)

        return 0

    def extract_with_throttling(self, table_name, columns, partition_column):
        """Extract table with bandwidth throttling"""
        print(f"\n{'='*80}")
        print(f"THROTTLED EXTRACTION: {table_name}")
        print(f"{'='*80}")
        print(f"Target bandwidth: {self.target_bandwidth_MBps:.2f} MB/s ({self.target_bandwidth_MBps * 8:.0f} Mbps)")
        print()

        start_time = time.time()
        total_rows = 0
        batch_num = 0

        conn = pymssql.connect(
            server=SOURCE_CONFIG['host'],
            port=SOURCE_CONFIG['port'],
            database=SOURCE_CONFIG['database'],
            user=SOURCE_CONFIG['user'],
            password=SOURCE_CONFIG['password'],
            timeout=600
        )

        try:
            # Get row count
            count_cursor = conn.cursor()
            count_cursor.execute(f"SELECT COUNT_BIG(*) FROM {table_name}")
            row_count = count_cursor.fetchone()[0]
            print(f"Total rows to extract: {row_count:,}")
            print()

            # Extract in batches
            cursor = conn.cursor(as_dict=True)
            columns_str = ", ".join(columns)
            offset = 0
            batch_size = LOAD_TEST_CONFIG['batch_size']

            while True:
                batch_num += 1
                batch_start = time.time()

                # Query batch
                query = f"""
                SELECT {columns_str}
                FROM {table_name}
                ORDER BY LoadTestID
                OFFSET {offset} ROWS
                FETCH NEXT {batch_size} ROWS ONLY
                """

                cursor.execute(query)
                rows = cursor.fetchall()

                if not rows:
                    break

                batch_rows = len(rows)
                total_rows += batch_rows

                # Estimate batch size in bytes
                batch_bytes = self.estimate_batch_size_bytes(batch_rows)

                # Write to UC Volume
                table_safe_name = table_name.replace(".", "_")
                table_path = f"{UC_VOLUME_PATH}/{table_safe_name}_load_test"

                spark_df = spark.createDataFrame(rows)

                if partition_column in spark_df.columns:
                    spark_df = spark_df.withColumn("date", to_date(partition_column))
                    (spark_df.write.mode("append").partitionBy("date").parquet(table_path))
                else:
                    (spark_df.write.mode("append").parquet(table_path))

                batch_elapsed = time.time() - batch_start

                # Calculate sleep time for throttling
                sleep_time = self.calculate_sleep_time(batch_bytes)

                if sleep_time > 0:
                    time.sleep(sleep_time)

                # Calculate metrics
                total_elapsed = time.time() - self.start_time if self.start_time else batch_elapsed
                current_bandwidth_MBps = (self.bytes_transferred / (1024 * 1024)) / total_elapsed if total_elapsed > 0 else 0
                throughput = batch_rows / batch_elapsed if batch_elapsed > 0 else 0
                pct = (total_rows / row_count * 100) if row_count > 0 else 0

                # Store batch metrics
                self.batch_metrics.append({
                    "batch_num": batch_num,
                    "rows": batch_rows,
                    "total_rows": total_rows,
                    "batch_time": batch_elapsed,
                    "sleep_time": sleep_time,
                    "bandwidth_MBps": current_bandwidth_MBps,
                    "throughput_rows_sec": throughput
                })

                # Print progress
                print(f"Batch {batch_num:4d} | "
                      f"Rows: {batch_rows:6,} | "
                      f"Total: {total_rows:10,} ({pct:5.1f}%) | "
                      f"BW: {current_bandwidth_MBps:5.2f} MB/s | "
                      f"Sleep: {sleep_time:5.2f}s")

                offset += batch_size

                # Safety limit
                if batch_num > 1100:
                    break

            elapsed = time.time() - start_time
            avg_bandwidth = (self.bytes_transferred / (1024 * 1024)) / elapsed if elapsed > 0 else 0
            avg_throughput = total_rows / elapsed if elapsed > 0 else 0

            print()
            print(f"✅ Extraction complete:")
            print(f"   Rows extracted:      {total_rows:,}")
            print(f"   Total time:          {elapsed:.1f}s")
            print(f"   Avg bandwidth:       {avg_bandwidth:.2f} MB/s ({avg_bandwidth * 8:.0f} Mbps)")
            print(f"   Avg throughput:      {avg_throughput:,.0f} rows/s")
            print()

            return {
                "table_name": table_name,
                "total_rows": total_rows,
                "elapsed_sec": elapsed,
                "avg_bandwidth_MBps": avg_bandwidth,
                "avg_bandwidth_Mbps": avg_bandwidth * 8,
                "avg_throughput_rows_sec": avg_throughput,
                "status": "success",
                "batch_metrics": self.batch_metrics
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

# Run throttled extraction
throttled_extractor = BandwidthThrottledExtractor(
    target_bandwidth_MBps=LOAD_TEST_CONFIG['target_bandwidth_MBps']
)

throttled_result = throttled_extractor.extract_with_throttling(
    table_name="dbo.LoadTestData",
    columns=["LoadTestID", "RecordNumber", "TextData", "NumericValue", "DateValue", "ModifiedDate"],
    partition_column="DateValue"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Unthrottled Extraction (Baseline)

# COMMAND ----------

def extract_unthrottled(table_name, columns, partition_column):
    """Extract table without bandwidth throttling (baseline)"""
    print(f"\n{'='*80}")
    print(f"UNTHROTTLED EXTRACTION (BASELINE): {table_name}")
    print(f"{'='*80}")
    print()

    start_time = time.time()
    total_rows = 0
    batch_num = 0
    bytes_transferred = 0

    conn = pymssql.connect(
        server=SOURCE_CONFIG['host'],
        port=SOURCE_CONFIG['port'],
        database=SOURCE_CONFIG['database'],
        user=SOURCE_CONFIG['user'],
        password=SOURCE_CONFIG['password'],
        timeout=600
    )

    try:
        # Get row count
        count_cursor = conn.cursor()
        count_cursor.execute(f"SELECT COUNT_BIG(*) FROM {table_name}")
        row_count = count_cursor.fetchone()[0]
        print(f"Total rows to extract: {row_count:,}")
        print()

        # Extract in batches
        cursor = conn.cursor(as_dict=True)
        columns_str = ", ".join(columns)
        offset = 0
        batch_size = LOAD_TEST_CONFIG['batch_size']

        while True:
            batch_num += 1
            batch_start = time.time()

            # Query batch
            query = f"""
            SELECT {columns_str}
            FROM {table_name}
            ORDER BY LoadTestID
            OFFSET {offset} ROWS
            FETCH NEXT {batch_size} ROWS ONLY
            """

            cursor.execute(query)
            rows = cursor.fetchall()

            if not rows:
                break

            batch_rows = len(rows)
            total_rows += batch_rows

            # Estimate batch size
            batch_bytes = batch_rows * 50  # ~50 bytes per row
            bytes_transferred += batch_bytes

            # Write to UC Volume
            table_safe_name = table_name.replace(".", "_")
            table_path = f"{UC_VOLUME_PATH}/{table_safe_name}_baseline"

            spark_df = spark.createDataFrame(rows)

            if partition_column in spark_df.columns:
                spark_df = spark_df.withColumn("date", to_date(partition_column))
                (spark_df.write.mode("append").partitionBy("date").parquet(table_path))
            else:
                (spark_df.write.mode("append").parquet(table_path))

            batch_elapsed = time.time() - batch_start

            # Calculate metrics
            total_elapsed = time.time() - start_time
            current_bandwidth_MBps = (bytes_transferred / (1024 * 1024)) / total_elapsed if total_elapsed > 0 else 0
            throughput = batch_rows / batch_elapsed if batch_elapsed > 0 else 0
            pct = (total_rows / row_count * 100) if row_count > 0 else 0

            # Print progress every 100 batches
            if batch_num % 100 == 0:
                print(f"Batch {batch_num:4d} | "
                      f"Rows: {batch_rows:6,} | "
                      f"Total: {total_rows:10,} ({pct:5.1f}%) | "
                      f"BW: {current_bandwidth_MBps:5.2f} MB/s | "
                      f"Throughput: {throughput:8.0f} rows/s")

            offset += batch_size

            # Safety limit
            if batch_num > 1100:
                break

        elapsed = time.time() - start_time
        avg_bandwidth = (bytes_transferred / (1024 * 1024)) / elapsed if elapsed > 0 else 0
        avg_throughput = total_rows / elapsed if elapsed > 0 else 0

        print()
        print(f"✅ Extraction complete:")
        print(f"   Rows extracted:      {total_rows:,}")
        print(f"   Total time:          {elapsed:.1f}s")
        print(f"   Avg bandwidth:       {avg_bandwidth:.2f} MB/s ({avg_bandwidth * 8:.0f} Mbps)")
        print(f"   Avg throughput:      {avg_throughput:,.0f} rows/s")
        print()

        return {
            "table_name": table_name,
            "total_rows": total_rows,
            "elapsed_sec": elapsed,
            "avg_bandwidth_MBps": avg_bandwidth,
            "avg_bandwidth_Mbps": avg_bandwidth * 8,
            "avg_throughput_rows_sec": avg_throughput,
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

# Run unthrottled extraction (only if you want baseline comparison)
# Comment out if you only want to test throttled extraction

# unthrottled_result = extract_unthrottled(
#     table_name="dbo.LoadTestData",
#     columns=["LoadTestID", "RecordNumber", "TextData", "NumericValue", "DateValue", "ModifiedDate"],
#     partition_column="DateValue"
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: Load Test Results

# COMMAND ----------

print("="*80)
print("LOAD TEST SUMMARY")
print("="*80)
print()

print("Test Configuration:")
print(f"  Target rows:          {LOAD_TEST_CONFIG['target_rows']:,}")
print(f"  Batch size:           {LOAD_TEST_CONFIG['batch_size']:,}")
print(f"  Target bandwidth:     {LOAD_TEST_CONFIG['target_bandwidth_MBps']:.2f} MB/s ({LOAD_TEST_CONFIG['target_bandwidth_mbps']} Mbps)")
print()

print("Throttled Extraction Results:")
if throttled_result['status'] == 'success':
    print(f"  ✅ Rows extracted:      {throttled_result['total_rows']:,}")
    print(f"  ✅ Time:                {throttled_result['elapsed_sec']:.1f}s")
    print(f"  ✅ Avg bandwidth:       {throttled_result['avg_bandwidth_MBps']:.2f} MB/s ({throttled_result['avg_bandwidth_Mbps']:.0f} Mbps)")
    print(f"  ✅ Avg throughput:      {throttled_result['avg_throughput_rows_sec']:,.0f} rows/s")

    # Bandwidth accuracy
    target_bw = LOAD_TEST_CONFIG['target_bandwidth_MBps']
    actual_bw = throttled_result['avg_bandwidth_MBps']
    bw_accuracy = (actual_bw / target_bw) * 100
    print(f"  ✅ Bandwidth accuracy:  {bw_accuracy:.1f}% of target")
else:
    print(f"  ❌ FAILED: {throttled_result.get('error', 'Unknown')}")

print()

# if 'unthrottled_result' in locals():
#     print("Unthrottled Extraction Results (Baseline):")
#     if unthrottled_result['status'] == 'success':
#         print(f"  ✅ Rows extracted:      {unthrottled_result['total_rows']:,}")
#         print(f"  ✅ Time:                {unthrottled_result['elapsed_sec']:.1f}s")
#         print(f"  ✅ Avg bandwidth:       {unthrottled_result['avg_bandwidth_MBps']:.2f} MB/s ({unthrottled_result['avg_bandwidth_Mbps']:.0f} Mbps)")
#         print(f"  ✅ Avg throughput:      {unthrottled_result['avg_throughput_rows_sec']:,.0f} rows/s")
#
#         # Comparison
#         speedup = unthrottled_result['elapsed_sec'] / throttled_result['elapsed_sec']
#         print()
#         print(f"Comparison:")
#         print(f"  Throttled is {speedup:.2f}x slower than unthrottled (expected with bandwidth limits)")
#     else:
#         print(f"  ❌ FAILED: {unthrottled_result.get('error', 'Unknown')}")
#     print()

print("="*80)
print("LOAD TEST COMPLETE")
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)

# COMMAND ----------

# # Uncomment to drop test table after testing
#
# conn = pymssql.connect(
#     server=SOURCE_CONFIG['host'],
#     port=SOURCE_CONFIG['port'],
#     database=SOURCE_CONFIG['database'],
#     user=SOURCE_CONFIG['user'],
#     password=SOURCE_CONFIG['password']
# )
#
# cursor = conn.cursor()
# cursor.execute("DROP TABLE IF EXISTS dbo.LoadTestData")
# conn.commit()
# cursor.close()
# conn.close()
#
# print("✅ Dropped dbo.LoadTestData")

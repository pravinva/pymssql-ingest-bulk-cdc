# Setup and Bulk Ingestion Guide

## Overview

This guide covers the setup and one-time bulk ingestion process for extracting data from SQL Server (or JDE Edwards) to Databricks using the **SDP + Lakeflow** pattern.

**Architecture:**
- **Custom Python Source (SDP)**: Extracts data from SQL Server using `pymssql`, writes to Unity Catalog Volume as Hive-partitioned Parquet files
- **DLT Pipeline (Lakeflow)**: Uses AutoLoader to continuously process files from UC Volume into Bronze and Silver streaming tables
- **Serverless**: Runs entirely on Databricks Serverless (no cluster management required)

**Scope:**
This guide covers the **initial bulk load**. For incremental CDC (Change Data Capture), see separate documentation.

---

## Prerequisites

### 1. SQL Server / JDE Edwards Source System

- **Read replica** recommended (avoid impacting production)
- **Network connectivity** from Databricks workspace to SQL Server
- **Firewall rules** allowing Databricks Serverless compute to connect
- **SQL Server credentials** with read permissions

### 2. Databricks Environment

- **Databricks workspace** (AWS, Azure, or GCP)
- **Serverless compute** enabled
- **Unity Catalog** enabled with permissions to:
  - Create schemas
  - Create volumes
  - Create tables
  - Run DLT pipelines

### 3. Databricks Secrets

Store SQL Server credentials securely in Databricks secrets:

```bash
# Create secret scope
databricks secrets create-scope --scope sql_server

# Add secrets
databricks secrets put-secret --scope sql_server --key host --string-value "<your-server>.database.windows.net"
databricks secrets put-secret --scope sql_server --key database --string-value "<your-database>"
databricks secrets put-secret --scope sql_server --key username --string-value "<your-username>"
databricks secrets put-secret --scope sql_server --key password --string-value "<your-password>"
```

---

## Step 1: Infrastructure Setup

**Notebook:** `01_fh_setup_infrastructure.py`

**Purpose:** Creates Unity Catalog infrastructure (schemas and volumes) required for the data pipeline.

### What It Creates

1. **Schema:** `main.fh_ingestion`
   - Purpose: Contains the landing UC Volume

2. **Volume:** `main.fh_ingestion.landing`
   - Purpose: Landing zone for extracted Parquet files from SQL Server
   - Path: `/Volumes/main/fh_ingestion/landing`
   - Structure: Hive-partitioned by date
     ```
     /Volumes/main/fh_ingestion/landing/
       ├── dbo_Assets/date=2024-03-10/*.parquet
       ├── dbo_WorkOrders/date=2024-03-10/*.parquet
       └── dbo_MaintenanceRecords/date=2024-03-10/*.parquet
     ```

3. **Schema:** `main.fh_bronze`
   - Purpose: Bronze layer streaming tables (AutoLoader ingestion)

4. **Schema:** `main.fh_silver`
   - Purpose: Silver layer streaming tables (cleaned and deduplicated)

### How to Run

1. Import `01_fh_setup_infrastructure.py` to your Databricks workspace
2. Attach to a Serverless notebook compute (or any cluster)
3. Run all cells
4. Verify success:
   ```sql
   SHOW VOLUMES IN main.fh_ingestion;
   SHOW SCHEMAS IN main LIKE 'fh_%';
   ```

**Expected Output:**
```
✅ Infrastructure Setup Complete

UC Volume Path:
  /Volumes/main/fh_ingestion/landing

Schemas Created:
  • main.fh_ingestion (landing zone)
  • main.fh_bronze (Bronze streaming tables)
  • main.fh_silver (Silver streaming tables)
```

---

## Step 2: Bulk Data Extraction

**Notebook:** `02_python_custom_mssql_source.py`

**Purpose:** Extracts all data from SQL Server tables and writes to Unity Catalog Volume as Hive-partitioned Parquet files.

### What It Does

1. **Connects to SQL Server** using `pymssql` (pure Python, no ODBC drivers required)
2. **Extracts data** in batches with configurable batch sizes and throttling
3. **Writes to UC Volume** as Parquet files partitioned by date
4. **Supports large datasets** with memory-efficient streaming extraction

### Configuration

Before running, update these sections in the notebook:

#### Source Configuration

```python
SOURCE_CONFIG = {
    "host": dbutils.secrets.get(scope="sql_server", key="host"),
    "port": 1433,
    "database": dbutils.secrets.get(scope="sql_server", key="database"),
    "user": dbutils.secrets.get(scope="sql_server", key="username"),
    "password": dbutils.secrets.get(scope="sql_server", key="password")
}
```

#### Table Configuration

Define which tables to extract and which columns to include:

```python
TABLES_CONFIG = {
    "dbo.Assets": {
        "columns": ["AssetID", "AssetNumber", "AssetName", "AssetType", "Location", "Status", "ModifiedDate"],
        "cdc_column": "ModifiedDate",      # Timestamp column for CDC
        "partition_column": "ModifiedDate"  # Column to use for Hive partitioning
    },
    "dbo.WorkOrders": {
        "columns": ["WorkOrderID", "WorkOrderNumber", "Description", "Status", "Priority", "AssetID", "CreatedDate", "ModifiedDate"],
        "cdc_column": "ModifiedDate",
        "partition_column": "CreatedDate"   # Partition by CreatedDate for better distribution
    },
    "dbo.MaintenanceRecords": {
        "columns": ["MaintenanceID", "AssetID", "WorkOrderID", "MaintenanceType", "MaintenanceDate", "Cost", "Duration", "ModifiedDate"],
        "cdc_column": "ModifiedDate",
        "partition_column": "MaintenanceDate"
    }
}
```

**Important Notes:**
- Only specify the **columns you need** (selective extraction reduces bandwidth and storage)
- Use **date/timestamp columns** for partitioning (improves query performance)
- Choose a **CDC column** (timestamp) for incremental loads later

#### Extraction Settings

```python
BATCH_SIZE = 10_000          # Rows per batch (10K recommended)
SLEEP_MS = 200               # Sleep between batches (milliseconds)
ENABLE_THROTTLING = True     # Enable bandwidth throttling
TARGET_BANDWIDTH_MBPS = 150  # Target bandwidth (Megabits per second)
```

**Recommendations:**
- **BATCH_SIZE:** 10,000 rows (balance between memory and network efficiency)
- **SLEEP_MS:** 200ms (prevents overwhelming source database)
- **THROTTLING:** Enable for production to control network bandwidth
- **TARGET_BANDWIDTH_MBPS:** Adjust based on your network capacity

### JDE Edwards 9.1/9.2 Specific Guidance

If extracting from **JDE Edwards E1 9.1 or 9.2**, follow these recommendations:

#### Column Selection

**Be very selective about columns to avoid duplicate data:**

```python
# Example: F0101 (Address Book Master)
"PRODDTA.F0101": {
    "columns": [
        "ABAN8",      # Address Book Number (Primary Key)
        "ABALPH",     # Name - Alpha
        "ABAT1",      # Address Line 1
        "ABCITY",     # City
        "ABCTR",      # Country
        "ABMCU",      # Business Unit
        # Do NOT include: ABEFTB (duplicate of ABAN8)
    ],
    "cdc_column": "ABUPMJ",       # Date - Last Updated
    "partition_column": "ABUPMJ"
}
```

**Common JDE tables for extraction:**
- `F0101` - Address Book Master
- `F4801` - Work Order Master
- `F1201` - Account Ledger
- `F0411` - Accounts Payable Ledger
- `F03B11` - Customer Ledger

**Column naming:**
- JDE uses cryptic column names (e.g., `ABAN8`, `ABALPH`)
- Document column meanings in your configuration comments
- Use JDE Data Dictionary or EnterpriseOne Table Browser to identify columns

**Date columns:**
- JDE stores dates as Julian dates (e.g., `ABUPMJ` = Update Date)
- Common JDE date columns: `UPMJ` (Update Date), `TRDJ` (Transaction Date)
- These can be used for CDC and partitioning

### How to Run

1. Import `02_python_custom_mssql_source.py` to Databricks workspace
2. Update configuration sections (SOURCE_CONFIG, TABLES_CONFIG)
3. Attach to **Serverless compute**
4. Run all cells
5. Monitor extraction progress:
   ```
   ================================================================================
   Extracting: dbo.Assets
   ================================================================================
   Batch 1: Extracted 10,000 rows (10,000 total) - 2.3s
   Batch 2: Extracted 10,000 rows (20,000 total) - 2.1s
   ...
   ✅ Extracted 50,000 rows to /Volumes/main/fh_ingestion/landing/dbo_Assets
   ```

### Validation

After extraction, verify data was written correctly:

```python
# Check UC Volume contents
dbutils.fs.ls("/Volumes/main/fh_ingestion/landing")

# Read extracted data
df = spark.read.parquet("/Volumes/main/fh_ingestion/landing/dbo_Assets")
display(df.limit(10))

# Check row counts
print(f"Total rows: {df.count():,}")

# Check partitions
spark.sql("SELECT date, COUNT(*) FROM parquet.`/Volumes/main/fh_ingestion/landing/dbo_Assets` GROUP BY date").show()
```

---

## Step 3: DLT Pipeline (Bronze → Silver)

**Notebook:** `03_dlt_autoloader_bronze_silver.py`

**Purpose:** Continuously processes files from UC Volume into Bronze and Silver streaming tables using Delta Live Tables (DLT) with AutoLoader.

### What It Does

1. **Bronze Layer (AutoLoader)**
   - Monitors UC Volume for new/updated Parquet files
   - Loads data into streaming Bronze tables
   - Adds audit columns (`_ingestion_timestamp`, `_source_file`)
   - Enables Change Data Feed for downstream processing

2. **Silver Layer (Transformations)**
   - Deduplicates records based on primary keys
   - Adds data quality constraints
   - Creates cleaned, curated streaming tables
   - Optimized with Z-ordering on key columns

### Pipeline Structure

```
UC Volume (Parquet files)
    ↓ [AutoLoader]
Bronze Streaming Tables (Raw + Audit)
    ↓ [Dedup + Quality]
Silver Streaming Tables (Clean + Curated)
```

**Tables Created:**
- `main.fh_bronze.dbo_Assets_bronze`
- `main.fh_bronze.dbo_WorkOrders_bronze`
- `main.fh_bronze.dbo_MaintenanceRecords_bronze`
- `main.fh_silver.dbo_Assets_silver`
- `main.fh_silver.dbo_WorkOrders_silver`
- `main.fh_silver.dbo_MaintenanceRecords_silver`

### How to Run

**Option 1: Create DLT Pipeline via UI**

1. Go to **Databricks Workflows** → **Delta Live Tables**
2. Click **Create Pipeline**
3. Configure:
   - **Name:** `FH_Ingestion_Pipeline`
   - **Notebook:** `03_dlt_autoloader_bronze_silver.py`
   - **Target:** `main.fh_bronze` (Bronze schema)
   - **Storage Location:** (leave default or specify `/mnt/dlt/fh_ingestion`)
   - **Pipeline Mode:** Triggered (for one-time bulk load) or Continuous (for ongoing CDC)
   - **Compute:** Serverless
4. Click **Create**
5. Click **Start** to run the pipeline

**Option 2: Create DLT Pipeline via CLI**

```bash
databricks pipelines create \
  --name "FH_Ingestion_Pipeline" \
  --libraries '[{"notebook":{"path":"/Users/your.email@company.com/03_dlt_autoloader_bronze_silver"}}]' \
  --target "main.fh_bronze" \
  --continuous false \
  --channel CURRENT
```

### Validation

After pipeline completes, verify tables were created:

```sql
-- Check Bronze tables
SELECT COUNT(*) FROM main.fh_bronze.dbo_Assets_bronze;
SELECT COUNT(*) FROM main.fh_bronze.dbo_WorkOrders_bronze;
SELECT COUNT(*) FROM main.fh_bronze.dbo_MaintenanceRecords_bronze;

-- Check Silver tables
SELECT COUNT(*) FROM main.fh_silver.dbo_Assets_silver;
SELECT COUNT(*) FROM main.fh_silver.dbo_WorkOrders_silver;
SELECT COUNT(*) FROM main.fh_silver.dbo_MaintenanceRecords_silver;

-- Verify data quality
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT AssetID) AS unique_assets,
  MIN(_ingestion_timestamp) AS first_ingestion,
  MAX(_ingestion_timestamp) AS last_ingestion
FROM main.fh_silver.dbo_Assets_silver;
```

---

## Summary: End-to-End Bulk Ingestion

**Complete workflow for one-time bulk load:**

```
┌─────────────────────────────────────────────────────────────────┐
│ Step 1: Infrastructure Setup (01_fh_setup_infrastructure.py)   │
│ - Creates UC schemas and volumes                                │
│ - Runtime: ~30 seconds                                          │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ Step 2: Bulk Extraction (02_python_custom_mssql_source.py)     │
│ - Extracts all tables from SQL Server                           │
│ - Writes Hive-partitioned Parquet to UC Volume                  │
│ - Runtime: Depends on data volume (e.g., 10M rows = 15-30 min)  │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ Step 3: DLT Pipeline (03_dlt_autoloader_bronze_silver.py)      │
│ - Processes Parquet files into Bronze and Silver tables         │
│ - Runtime: Depends on data volume (e.g., 10M rows = 5-10 min)   │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ Result: Clean Silver Tables Ready for Analytics                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Troubleshooting

### Issue: "pymssql connection timeout"

**Symptoms:**
```
pymssql.OperationalError: (20002, b'DB-Lib error message 20002, severity 9:\nAdaptive Server connection timed out\n')
```

**Solutions:**
1. **Check firewall rules** - Ensure Databricks Serverless IP ranges are allowed
2. **Verify SQL Server hostname** - Use FQDN (e.g., `server.database.windows.net`)
3. **Test connectivity** - Use `nc` or `telnet` to test port 1433
4. **Increase timeout** - Add `timeout=60` to pymssql.connect()

### Issue: "Out of memory" during extraction

**Symptoms:**
```
java.lang.OutOfMemoryError: Java heap space
```

**Solutions:**
1. **Reduce BATCH_SIZE** - Try 5,000 or 2,000 rows per batch
2. **Enable throttling** - Set `ENABLE_THROTTLING = True`
3. **Reduce concurrent tables** - Extract one table at a time
4. **Use selective columns** - Only extract necessary columns

### Issue: "DLT pipeline stuck in STARTING"

**Symptoms:**
- Pipeline shows "STARTING" for >5 minutes
- No tasks appear in Spark UI

**Solutions:**
1. **Check UC Volume path** - Verify path exists and has files
2. **Check permissions** - Ensure DLT has read access to UC Volume
3. **Restart pipeline** - Stop and start the pipeline
4. **Check logs** - Review event logs for detailed error messages

### Issue: "Duplicate records in Silver tables"

**Symptoms:**
- Silver table has more rows than expected
- Primary key violations

**Solutions:**
1. **Check deduplication logic** - Verify `QUALIFY ROW_NUMBER()` is working
2. **Check source data** - Source may have true duplicates
3. **Add UNIQUE constraint** - Add expectations to enforce uniqueness
4. **Review partition strategy** - May be missing partitioning on key columns

---

## Performance Tuning

### Extraction Performance

**Optimize for large datasets (10M+ rows):**

```python
# Recommended settings for 10M+ rows
BATCH_SIZE = 10_000               # Larger batches = fewer round trips
SLEEP_MS = 100                    # Reduce sleep for faster extraction
ENABLE_THROTTLING = True          # Control bandwidth usage
TARGET_BANDWIDTH_MBPS = 150       # Adjust based on network capacity
```

**Optimize for small datasets (<1M rows):**

```python
# Recommended settings for <1M rows
BATCH_SIZE = 50_000               # Larger batches OK for small datasets
SLEEP_MS = 0                      # No sleep needed
ENABLE_THROTTLING = False         # Disable throttling
```

### DLT Pipeline Performance

**Optimize AutoLoader:**

```python
# In Bronze tables, tune AutoLoader settings
.option("cloudFiles.schemaEvolutionMode", "rescue")  # Handle schema changes
.option("cloudFiles.inferColumnTypes", "true")       # Infer types automatically
.option("cloudFiles.maxFilesPerTrigger", 1000)       # Process more files per micro-batch
```

**Optimize Z-ordering:**

```python
# Add Z-ordering on frequently filtered columns
table_properties={
    "pipelines.autoOptimize.zOrderCols": "AssetID,ModifiedDate"
}
```

---

## Next Steps

After completing bulk ingestion:

1. **Verify data quality**
   - Run SQL queries to validate row counts and data integrity
   - Compare source vs. Silver table counts

2. **Set up BI dashboards**
   - Connect BI tools (Tableau, Power BI, Databricks SQL) to Silver tables
   - Create initial reports and dashboards

3. **Plan for incremental loads**
   - See separate CDC documentation for ongoing incremental updates
   - Schedule extraction jobs (hourly, daily, etc.)

4. **Monitor and optimize**
   - Set up monitoring for DLT pipelines
   - Review query performance and add indexes/Z-ordering as needed
   - Monitor storage costs and optimize file sizes

---

## Additional Resources

- **Databricks Documentation:**
  - [Delta Live Tables](https://docs.databricks.com/delta-live-tables/index.html)
  - [Unity Catalog Volumes](https://docs.databricks.com/en/connect/unity-catalog/volumes.html)
  - [AutoLoader](https://docs.databricks.com/en/ingestion/auto-loader/index.html)

- **pymssql Documentation:**
  - [pymssql User Guide](https://pymssql.readthedocs.io/)
  - [Connection Strings](https://pymssql.readthedocs.io/en/stable/ref/pymssql.html#pymssql.connect)

- **JDE Edwards Resources:**
  - [JDE E1 Table Reference](https://docs.oracle.com/cd/E26228_01/index.htm)
  - [Understanding JDE Date Fields](https://www.jdelist.com/jde-date-conversion/)

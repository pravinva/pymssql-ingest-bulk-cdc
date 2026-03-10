# Databricks notebook source
# MAGIC %md
# MAGIC # Verify CDC Flow Completion
# MAGIC
# MAGIC Checks:
# MAGIC 1. CDC data landed in UC Volume
# MAGIC 2. DLT Bronze/Silver tables updated with CDC rows
# MAGIC 3. Selective columns still being extracted

# COMMAND ----------

print("="*80)
print("CDC FLOW VERIFICATION")
print("="*80)
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Check UC Volume - CDC Files Landed

# COMMAND ----------

UC_VOLUME_PATH = "/Volumes/main/fh_ingestion/landing"

print("1. CHECKING UC VOLUME FOR CDC FILES:")
print("-"*80)

# Check dbo_Assets
assets_path = f"{UC_VOLUME_PATH}/dbo_Assets"
try:
    partitions = dbutils.fs.ls(assets_path)
    print(f"✅ dbo_Assets: {len(partitions)} date partitions")

    # Count total files
    total_files = 0
    for partition in partitions:
        files = dbutils.fs.ls(partition.path)
        total_files += len(files)
    print(f"   Total Parquet files: {total_files}")
except Exception as e:
    print(f"❌ dbo_Assets: ERROR - {e}")

print()

# Check dbo_WorkOrders
wo_path = f"{UC_VOLUME_PATH}/dbo_WorkOrders"
try:
    partitions = dbutils.fs.ls(wo_path)
    print(f"✅ dbo_WorkOrders: {len(partitions)} date partitions")

    total_files = 0
    for partition in partitions:
        files = dbutils.fs.ls(partition.path)
        total_files += len(files)
    print(f"   Total Parquet files: {total_files}")
except Exception as e:
    print(f"❌ dbo_WorkOrders: ERROR - {e}")

print()

# Check dbo_MaintenanceRecords
maint_path = f"{UC_VOLUME_PATH}/dbo_MaintenanceRecords"
try:
    partitions = dbutils.fs.ls(maint_path)
    print(f"✅ dbo_MaintenanceRecords: {len(partitions)} date partitions")

    total_files = 0
    for partition in partitions:
        files = dbutils.fs.ls(partition.path)
        total_files += len(files)
    print(f"   Total Parquet files: {total_files}")
except Exception as e:
    print(f"❌ dbo_MaintenanceRecords: ERROR - {e}")

print()
print("Expected: More files than bulk load (bulk + incremental CDC files)")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Check Bronze Tables Row Counts

# COMMAND ----------

print("2. BRONZE TABLE ROW COUNTS (BEFORE vs AFTER CDC):")
print("-"*80)

# Previous bulk load counts:
# - dbo_Assets: 5,000
# - dbo_WorkOrders: 10,000
# - dbo_MaintenanceRecords: 7,500

# Expected CDC additions:
# - dbo_Assets: +500 (updated rows)
# - dbo_WorkOrders: +1,000
# - dbo_MaintenanceRecords: +750

bronze_tables = [
    ("main.fh_bronze.dbo_Assets_bronze", 5000, 500),
    ("main.fh_bronze.dbo_WorkOrders_bronze", 10000, 1000),
    ("main.fh_bronze.dbo_MaintenanceRecords_bronze", 7500, 750)
]

total_before = 0
total_after = 0
total_expected_increase = 0

for table_name, bulk_count, cdc_count in bronze_tables:
    try:
        current_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name}").first()[0]
        expected_count = bulk_count + cdc_count
        delta = current_count - bulk_count

        status = "✅" if delta >= cdc_count else "⚠️"
        print(f"{status} {table_name}")
        print(f"   Bulk load:      {bulk_count:,} rows")
        print(f"   Expected CDC:   +{cdc_count:,} rows")
        print(f"   Current total:  {current_count:,} rows")
        print(f"   Actual delta:   +{delta:,} rows")
        print()

        total_before += bulk_count
        total_after += current_count
        total_expected_increase += cdc_count

    except Exception as e:
        print(f"❌ {table_name}: ERROR - {e}")
        print()

print("-"*80)
print(f"TOTALS:")
print(f"  Initial (bulk):       {total_before:,} rows")
print(f"  Expected CDC delta:   +{total_expected_increase:,} rows")
print(f"  Current total:        {total_after:,} rows")
print(f"  Actual delta:         +{total_after - total_before:,} rows")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Check Silver Tables Row Counts

# COMMAND ----------

print("3. SILVER TABLE ROW COUNTS (After deduplication):")
print("-"*80)

silver_tables = [
    ("main.fh_bronze.dbo_Assets_silver", 5000),
    ("main.fh_bronze.dbo_WorkOrders_silver", 10000),
    ("main.fh_bronze.dbo_MaintenanceRecords_silver", 7500)
]

for table_name, expected_count in silver_tables:
    try:
        current_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name}").first()[0]

        # Silver tables should have SAME count as bulk load (CDC updates same rows)
        # NOT additive since we're updating existing rows
        status = "✅" if current_count == expected_count else "⚠️"

        print(f"{status} {table_name}")
        print(f"   Expected (unique):  {expected_count:,} rows")
        print(f"   Current count:      {current_count:,} rows")
        print(f"   Difference:         {current_count - expected_count:,} rows")
        print()

    except Exception as e:
        print(f"❌ {table_name}: ERROR - {e}")
        print()

print("Note: Silver tables deduplicate by ID, so CDC updates don't increase count")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify Selective Columns (Assets Bronze)

# COMMAND ----------

print("4. VERIFY SELECTIVE COLUMNS EXTRACTED:")
print("-"*80)

# Check Assets Bronze table columns
assets_cols = spark.table("main.fh_bronze.dbo_Assets_bronze").columns

print(f"dbo_Assets_bronze has {len(assets_cols)} columns:")
print()

# Expected columns:
# Source: 7 columns (AssetID, AssetNumber, AssetName, AssetType, Location, Status, ModifiedDate)
# Audit: 3 columns (_ingestion_timestamp, _source_file, date partition)
# Total: 10 columns

expected_source_cols = [
    "AssetID", "AssetNumber", "AssetName", "AssetType",
    "Location", "Status", "ModifiedDate"
]

expected_audit_cols = [
    "_ingestion_timestamp", "_source_file", "date"
]

print("Source columns (should be only 7):")
for col in expected_source_cols:
    status = "✅" if col in assets_cols else "❌"
    print(f"  {status} {col}")

print()
print("Audit columns:")
for col in expected_audit_cols:
    status = "✅" if col in assets_cols else "❌"
    print(f"  {status} {col}")

print()
print(f"Total columns: {len(assets_cols)}")
print(f"Expected: 10 columns (7 source + 3 audit)")

if len(assets_cols) == 10:
    print("✅ CORRECT - Only selective columns extracted!")
else:
    print(f"⚠️  UNEXPECTED - Expected 10 columns, got {len(assets_cols)}")
    print(f"All columns: {assets_cols}")

print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Sample CDC Data

# COMMAND ----------

print("5. SAMPLE CDC DATA (Most recent by _ingestion_timestamp):")
print("-"*80)

# Show most recent Assets records (likely CDC data)
print("dbo_Assets_bronze (5 most recent by ingestion):")
spark.sql("""
    SELECT AssetID, AssetNumber, AssetName, Status, Location, ModifiedDate, _ingestion_timestamp
    FROM main.fh_bronze.dbo_Assets_bronze
    ORDER BY _ingestion_timestamp DESC
    LIMIT 5
""").show(truncate=False)

print()
print("="*80)
print("CDC FLOW VERIFICATION COMPLETE")
print("="*80)

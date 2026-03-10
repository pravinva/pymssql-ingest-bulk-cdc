# Databricks notebook source
# Verify DLT Bronze and Silver tables

print("="*80)
print("VERIFYING DLT TABLES")
print("="*80)
print()

# Bronze Tables
print("BRONZE TABLES:")
print("-"*80)

bronze_tables = [
    "main.fh_bronze.dbo_Assets_bronze",
    "main.fh_bronze.dbo_WorkOrders_bronze",
    "main.fh_bronze.dbo_MaintenanceRecords_bronze"
]

for table in bronze_tables:
    try:
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table}").first()[0]
        cols = spark.table(table).columns
        print(f"✅ {table:50s}: {count:,} rows, {len(cols)} columns")
    except Exception as e:
        print(f"❌ {table:50s}: ERROR - {e}")

print()
print("SILVER TABLES:")
print("-"*80)

silver_tables = [
    "main.fh_bronze.dbo_Assets_silver",
    "main.fh_bronze.dbo_WorkOrders_silver",
    "main.fh_bronze.dbo_MaintenanceRecords_silver"
]

for table in silver_tables:
    try:
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table}").first()[0]
        cols = spark.table(table).columns
        print(f"✅ {table:50s}: {count:,} rows, {len(cols)} columns")
    except Exception as e:
        print(f"❌ {table:50s}: ERROR - {e}")

print()
print("GOLD TABLES:")
print("-"*80)

gold_tables = [
    "main.fh_bronze.work_orders_daily_summary"
]

for table in gold_tables:
    try:
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table}").first()[0]
        cols = spark.table(table).columns
        print(f"✅ {table:50s}: {count:,} rows, {len(cols)} columns")
    except Exception as e:
        print(f"❌ {table:50s}: ERROR - {e}")

print()
print("="*80)
print("SAMPLE DATA - Assets Bronze:")
print("="*80)
spark.sql("SELECT * FROM main.fh_bronze.dbo_Assets_bronze LIMIT 5").show(truncate=False)

print()
print("="*80)
print("COLUMN VERIFICATION - Assets Bronze (should have only 7 source columns + 3 audit columns):")
print("="*80)
print("Columns:", spark.table("main.fh_bronze.dbo_Assets_bronze").columns)

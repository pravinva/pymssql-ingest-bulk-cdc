# Databricks notebook source
# MAGIC %md
# MAGIC # Fulton Hogan - SDP + Lakeflow Infrastructure Setup
# MAGIC
# MAGIC Creates UC volumes and schemas for the Custom Python Data Source pattern

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use existing main catalog
# MAGIC USE CATALOG main;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create schema for FH ingestion
# MAGIC CREATE SCHEMA IF NOT EXISTS fh_ingestion
# MAGIC COMMENT 'Fulton Hogan data ingestion using SDP + Lakeflow framework';

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA fh_ingestion;
# MAGIC
# MAGIC -- Create UC Volume for landing zone
# MAGIC CREATE VOLUME IF NOT EXISTS landing
# MAGIC COMMENT 'Landing zone for CDC records from SQL Server (Hive partitioned by date)';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Bronze schema
# MAGIC CREATE SCHEMA IF NOT EXISTS main.fh_bronze
# MAGIC COMMENT 'Bronze layer - streaming tables ingested via AutoLoader';
# MAGIC
# MAGIC -- Create Silver schema
# MAGIC CREATE SCHEMA IF NOT EXISTS main.fh_silver
# MAGIC COMMENT 'Silver layer - cleaned and transformed streaming tables';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify setup
# MAGIC SHOW VOLUMES IN main.fh_ingestion;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN main LIKE 'fh_%';

# COMMAND ----------

# Display UC Volume path
print("✅ Infrastructure Setup Complete")
print()
print("UC Volume Path:")
print("  /Volumes/main/fh_ingestion/landing")
print()
print("Expected Structure:")
print("  /Volumes/main/fh_ingestion/landing/")
print("    ├── dbo_Assets/date=2024-03-10/*.parquet")
print("    ├── dbo_WorkOrders/date=2024-03-10/*.parquet")
print("    └── dbo_MaintenanceRecords/date=2024-03-10/*.parquet")
print()
print("Schemas Created:")
print("  • main.fh_ingestion (landing zone)")
print("  • main.fh_bronze (Bronze streaming tables)")
print("  • main.fh_silver (Silver streaming tables)")

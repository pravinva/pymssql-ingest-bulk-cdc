# Databricks notebook source
# MAGIC %md
# MAGIC # Fulton Hogan - Infrastructure Setup (Direct-to-Delta)
# MAGIC
# MAGIC Creates schemas for direct Delta write pattern (append-only bulk load).
# MAGIC Aligned with Lakeflow Connect managed connector future state.
# MAGIC
# MAGIC **Architecture:**
# MAGIC - No UC Volumes (writes directly to Bronze Delta tables)
# MAGIC - Schema inference + evolution enabled
# MAGIC - Append-only mode (no CDC for now)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use existing main catalog
# MAGIC USE CATALOG main;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Bronze schema (for directly written Delta tables)
# MAGIC CREATE SCHEMA IF NOT EXISTS main.fh_bronze
# MAGIC COMMENT 'Bronze layer - direct writes from SQL Server extraction (append-only)';
# MAGIC
# MAGIC -- Create Silver schema (for DLT transformed tables)
# MAGIC CREATE SCHEMA IF NOT EXISTS main.fh_silver
# MAGIC COMMENT 'Silver layer - cleaned and transformed via DLT';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show schemas
# MAGIC SHOW SCHEMAS IN main LIKE 'fh_%';

# COMMAND ----------

# Display setup summary
print("="*80)
print("✅ INFRASTRUCTURE SETUP COMPLETE")
print("="*80)
print()
print("Schemas Created:")
print("  • main.fh_bronze  - Bronze Delta tables (direct writes, append-only)")
print("  • main.fh_silver  - Silver Delta tables (DLT transformations)")
print()
print("Expected Bronze Tables (created by Notebook 02):")
print("  • main.fh_bronze.dbo_Assets")
print("  • main.fh_bronze.dbo_WorkOrders")
print("  • main.fh_bronze.dbo_MaintenanceRecords")
print()
print("Table Properties:")
print("  ✓ Schema inference enabled")
print("  ✓ Schema evolution enabled (mergeSchema)")
print("  ✓ Change Data Feed enabled (for DLT)")
print("  ✓ Append-only mode (bulk load for network testing)")
print()
print("="*80)

# Databricks notebook source
# MAGIC %md
# MAGIC # Generate CDC Data
# MAGIC
# MAGIC Updates random rows in Azure SQL to simulate CDC changes

# COMMAND ----------

# Install pymssql
%pip install pymssql --quiet
dbutils.library.restartPython()

# COMMAND ----------

import pymssql
from datetime import datetime
import random

# Azure SQL Configuration
AZURE_SQL_CONFIG = {
    "host": "<your-server>.database.windows.net",
    "port": 1433,
    "database": "<your-database>",
    "user": "<your-username>",
    "password": dbutils.secrets.get(scope="sql_server", key="password")
}

print("="*80)
print("GENERATING CDC DATA")
print("="*80)
print()

conn = pymssql.connect(
    server=AZURE_SQL_CONFIG['host'],
    port=AZURE_SQL_CONFIG['port'],
    database=AZURE_SQL_CONFIG['database'],
    user=AZURE_SQL_CONFIG['user'],
    password=AZURE_SQL_CONFIG['password'],
    timeout=300
)

cursor = conn.cursor()

try:
    # 1. Update 500 random Assets
    print("1. Updating 500 Assets...")
    statuses = ['Active', 'Inactive', 'Maintenance', 'Retired']
    locations = ['Sydney', 'Melbourne', 'Brisbane', 'Perth', 'Adelaide']

    # Get 500 random AssetIDs
    cursor.execute("SELECT TOP 500 AssetID FROM dbo.Assets ORDER BY NEWID()")
    asset_ids = [row[0] for row in cursor.fetchall()]

    for asset_id in asset_ids:
        new_status = random.choice(statuses)
        new_location = random.choice(locations)

        cursor.execute("""
            UPDATE dbo.Assets
            SET Status = %s,
                Location = %s,
                ModifiedDate = GETDATE()
            WHERE AssetID = %s
        """, (new_status, new_location, asset_id))

    conn.commit()
    print(f"   ✅ Updated {len(asset_ids)} Assets")
    print()

    # 2. Update 1000 random WorkOrders
    print("2. Updating 1000 WorkOrders...")
    wo_statuses = ['Open', 'In Progress', 'Completed', 'Cancelled']
    priorities = ['Low', 'Medium', 'High', 'Critical']

    cursor.execute("SELECT TOP 1000 WorkOrderID FROM dbo.WorkOrders ORDER BY NEWID()")
    wo_ids = [row[0] for row in cursor.fetchall()]

    for wo_id in wo_ids:
        new_status = random.choice(wo_statuses)
        new_priority = random.choice(priorities)

        cursor.execute("""
            UPDATE dbo.WorkOrders
            SET Status = %s,
                Priority = %s,
                ModifiedDate = GETDATE()
            WHERE WorkOrderID = %s
        """, (new_status, new_priority, wo_id))

    conn.commit()
    print(f"   ✅ Updated {len(wo_ids)} WorkOrders")
    print()

    # 3. Update 750 random MaintenanceRecords
    print("3. Updating 750 MaintenanceRecords...")

    cursor.execute("SELECT TOP 750 MaintenanceID FROM dbo.MaintenanceRecords ORDER BY NEWID()")
    maint_ids = [row[0] for row in cursor.fetchall()]

    for maint_id in maint_ids:
        # Increase cost by 10-30%
        cost_increase = random.uniform(1.1, 1.3)
        # Adjust duration by -20% to +20%
        duration_change = random.uniform(0.8, 1.2)

        cursor.execute("""
            UPDATE dbo.MaintenanceRecords
            SET Cost = Cost * %s,
                Duration = CAST(Duration * %s AS INT),
                ModifiedDate = GETDATE()
            WHERE MaintenanceID = %s
        """, (cost_increase, duration_change, maint_id))

    conn.commit()
    print(f"   ✅ Updated {len(maint_ids)} MaintenanceRecords")
    print()

    # Print summary
    print("="*80)
    print("CDC DATA GENERATION SUMMARY")
    print("="*80)
    print(f"✅ Assets updated:              {len(asset_ids):,}")
    print(f"✅ WorkOrders updated:          {len(wo_ids):,}")
    print(f"✅ MaintenanceRecords updated:  {len(maint_ids):,}")
    print(f"✅ Total rows updated:          {len(asset_ids) + len(wo_ids) + len(maint_ids):,}")
    print()
    print("All updated rows have new ModifiedDate = GETDATE()")
    print("Ready for incremental CDC extraction!")
    print()

except Exception as e:
    print(f"❌ ERROR: {e}")
    conn.rollback()
    raise

finally:
    cursor.close()
    conn.close()

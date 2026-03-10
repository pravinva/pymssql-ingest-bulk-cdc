# Security Configuration

## Databricks Secrets Setup

This repository uses Databricks Secrets to securely store database credentials. **NEVER** hardcode passwords or connection strings in notebooks.

### Step 1: Create Secret Scope

```bash
# Create secret scope (one-time setup)
databricks secrets create-scope --scope sql_server
```

### Step 2: Store SQL Server Credentials

```bash
# Store connection details as secrets
databricks secrets put --scope sql_server --key host
databricks secrets put --scope sql_server --key database
databricks secrets put --scope sql_server --key username
databricks secrets put --scope sql_server --key password
```

When prompted, enter your actual values (e.g., `your-server.database.windows.net`).

### Step 3: Update Notebooks

All notebooks are pre-configured to use secrets. The configuration looks like this:

```python
SOURCE_CONFIG = {
    "host": dbutils.secrets.get(scope="sql_server", key="host"),
    "port": 1433,
    "database": dbutils.secrets.get(scope="sql_server", key="database"),
    "user": dbutils.secrets.get(scope="sql_server", key="username"),
    "password": dbutils.secrets.get(scope="sql_server", key="password")
}
```

### For Testing Only

If you need to test locally with hardcoded values:

1. Create a `config_local.py` file (gitignored)
2. Define your connection config there
3. Import it in notebooks: `from config_local import SOURCE_CONFIG`

**Important:** `config_local.py` is in `.gitignore` and will never be committed.

## Security Best Practices

1. ✅ Use Databricks Secrets for all credentials
2. ✅ Use Azure Key Vault-backed secret scopes for production
3. ✅ Enable network restrictions (ExpressRoute/Private Link)
4. ✅ Use SQL Server read-only replicas (Kapua) for CDC
5. ✅ Rotate passwords regularly
6. ❌ **NEVER** commit passwords to Git
7. ❌ **NEVER** log or print passwords in notebook output

## Exposed Credentials Response

If credentials are accidentally exposed:

1. **Immediately rotate the password** in Azure SQL Server
2. Delete the repository from GitHub (to purge from GitHub's servers)
3. Create a new repository with cleaned code
4. Notify security team

## Azure SQL Server Password Rotation

```bash
# Reset SQL Server password
az sql server update \
  --resource-group <rg-name> \
  --name <server-name> \
  --admin-password '<NewSecurePassword!>'

# Update Databricks secret
databricks secrets put --scope sql_server --key password
```

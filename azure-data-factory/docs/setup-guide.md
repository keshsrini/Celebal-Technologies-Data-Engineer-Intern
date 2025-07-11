# Azure Data Factory Setup Guide

## Prerequisites
- Azure subscription with appropriate permissions
- On-premises server with network connectivity to Azure
- PowerShell 5.1 or later
- Azure PowerShell module

## Deployment Steps

### 1. Deploy Azure Data Factory
```powershell
.\scripts\powershell\deploy-adf.ps1 -ResourceGroupName "rg-adf-demo" -DataFactoryName "adf-demo-001"
```

### 2. Install Self-hosted Integration Runtime
```powershell
.\integration-runtime\install-shir.ps1 -AuthKey "<your-auth-key>"
```

### 3. Create Watermark Table
Execute the SQL script in your Azure SQL Database:
```sql
-- Run scripts\sql\create-watermark-table.sql
```

### 4. Configure Linked Services
- Update connection strings in linked-services folder
- Deploy using Azure Data Factory Studio or ARM templates

### 5. Deploy Pipelines and Triggers
- Import pipeline definitions from pipelines folder
- Configure trigger schedules as needed

## Key Features

### Incremental Loading
- Uses watermark-based change tracking
- Processes only new/modified records
- Automatic watermark updates

### Resilient Data Transfer
- Built-in retry logic (3 attempts)
- 30-second retry intervals
- Graceful error handling

### Scheduled Execution
- Daily incremental loads at 2 AM UTC
- Monthly reports on last Saturday
- Customizable trigger schedules

## Monitoring
- Use Azure Monitor for pipeline execution
- Set up alerts for failed runs
- Review activity logs for troubleshooting
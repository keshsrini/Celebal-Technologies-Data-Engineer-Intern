# Azure Data Factory Integration Solution

Complete ADF solution with Self-hosted Integration Runtime for secure on-premises to cloud data integration.

## Features
- Self-hosted Integration Runtime setup
- Incremental loading with watermark tracking  
- SFTP/FTP server integration
- Automated daily and monthly triggers
- Resilient data transfer with retry logic

## Quick Start
```powershell
# Deploy ADF resources
.\azure-data-factory\scripts\powershell\deploy-adf.ps1 -ResourceGroupName "your-rg" -DataFactoryName "your-adf"

# Install SHIR
.\azure-data-factory\integration-runtime\install-shir.ps1 -AuthKey "your-auth-key"
```

See [Setup Guide](azure-data-factory/docs/setup-guide.md) for detailed instructions.
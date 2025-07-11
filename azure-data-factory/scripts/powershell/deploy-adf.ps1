# Deploy Azure Data Factory resources
param(
    [Parameter(Mandatory=$true)]
    [string]$ResourceGroupName,
    
    [Parameter(Mandatory=$true)]
    [string]$DataFactoryName,
    
    [Parameter(Mandatory=$true)]
    [string]$Location = "East US"
)

# Install required modules
Install-Module -Name Az.DataFactory -Force -AllowClobber

# Connect to Azure
Connect-AzAccount

# Create Data Factory
$dataFactory = Set-AzDataFactoryV2 -ResourceGroupName $ResourceGroupName -Name $DataFactoryName -Location $Location

# Create Self-hosted Integration Runtime
$integrationRuntime = Set-AzDataFactoryV2IntegrationRuntime -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name "SelfHostedIR" -Type SelfHosted

# Get authentication key for SHIR
$authKey = Get-AzDataFactoryV2IntegrationRuntimeKey -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -Name "SelfHostedIR"

Write-Host "Data Factory created successfully!"
Write-Host "SHIR Authentication Key: $($authKey.AuthKey1)"
Write-Host "Use this key to register your Self-hosted Integration Runtime"
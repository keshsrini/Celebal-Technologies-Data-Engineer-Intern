# Install Self-hosted Integration Runtime
param(
    [Parameter(Mandatory=$true)]
    [string]$AuthKey
)

$downloadUrl = "https://download.microsoft.com/download/E/4/7/E4771905-1079-445B-8BF9-8A1A075D8A10/IntegrationRuntime_5.34.8723.1.msi"
$installerPath = "$env:TEMP\IntegrationRuntime.msi"

Write-Host "Downloading SHIR installer..."
Invoke-WebRequest -Uri $downloadUrl -OutFile $installerPath

Write-Host "Installing SHIR..."
Start-Process msiexec.exe -ArgumentList "/i $installerPath /quiet" -Wait

Write-Host "Registering SHIR with authentication key..."
$shirPath = "${env:ProgramFiles}\Microsoft Integration Runtime\5.0\Shared\dmgcmd.exe"
& $shirPath -RegisterNewNode $AuthKey

Write-Host "SHIR installation and registration completed successfully!"
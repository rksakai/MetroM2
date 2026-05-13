// infra/main.bicep
targetScope = 'resourceGroup'

@description('Nome do ambiente (dev, staging, prod)')
param environment string = 'dev'

@description('Região Azure')
param location string = resourceGroup().location

@description('Prefixo dos recursos')
param prefix string = 'realestate'

var uniqueSuffix = uniqueString(resourceGroup().id)
var storageAccountName = '\${prefix}dls\${uniqueSuffix}'
var databricksName = '\${prefix}-databricks-\${environment}'
var keyVaultName = '\${prefix}-kv-\${environment}'
var appServicePlanName = '\${prefix}-plan-\${environment}'
var webAppName = '\${prefix}-api-\${environment}'

// ─── Storage Account (Azure Data Lake Gen2) ───────────────────────
resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: storageAccountName
  location: location
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true  // Hierarchical Namespace = Data Lake Gen2
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    supportsHttpsTrafficOnly: true
  }
}

// Containers da camada Medallion
resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-01-01' = {
  parent: storageAccount
  name: 'default'
}

resource bronzeContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: blobService
  name: 'bronze'
  properties: { publicAccess: 'None' }
}

resource silverContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: blobService
  name: 'silver'
  properties: { publicAccess: 'None' }
}

resource goldContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: blobService
  name: 'gold'
  properties: { publicAccess: 'None' }
}

// ─── Azure Key Vault ───────────────────────────────────────────────
resource keyVault 'Microsoft.KeyVault/vaults@2023-02-01' = {
  name: keyVaultName
  location: location
  properties: {
    sku: { family: 'A', name: 'standard' }
    tenantId: subscription().tenantId
    enableSoftDelete: true
    softDeleteRetentionInDays: 7
    accessPolicies: []
  }
}

// ─── Azure Databricks Workspace ───────────────────────────────────
resource databricksWorkspace 'Microsoft.Databricks/workspaces@2023-02-01' = {
  name: databricksName
  location: location
  sku: { name: 'standard' }
  properties: {
    managedResourceGroupId: '\${subscription().id}/resourceGroups/\${prefix}-databricks-managed-\${environment}'
  }
}

// ─── Azure App Service (FastAPI) ──────────────────────────────────
resource appServicePlan 'Microsoft.Web/serverfarms@2022-09-01' = {
  name: appServicePlanName
  location: location
  sku: { name: 'B2', tier: 'Basic' }
  kind: 'linux'
  properties: { reserved: true }
}

resource webApp 'Microsoft.Web/sites@2022-09-01' = {
  name: webAppName
  location: location
  properties: {
    serverFarmId: appServicePlan.id
    siteConfig: {
      linuxFxVersion: 'PYTHON|3.11'
      appSettings: [
        { name: 'AZURE_STORAGE_ACCOUNT', value: storageAccountName }
        { name: 'KEY_VAULT_URI', value: keyVault.properties.vaultUri }
        { name: 'ENVIRONMENT', value: environment }
      ]
    }
  }
}

output storageAccountName string = storageAccount.name
output databricksUrl string = databricksWorkspace.properties.workspaceUrl
output webAppUrl string = 'https://\${webApp.properties.defaultHostName}'
output keyVaultUri string = keyVault.properties.vaultUri

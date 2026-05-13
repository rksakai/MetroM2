#!/bin/bash
# infra/deploy.sh

set -e

RESOURCE_GROUP="rg-real-estate-analysis"
LOCATION="brazilsouth"
ENVIRONMENT="dev"

echo "🚀 Criando Resource Group..."
az group create \
  --name \$RESOURCE_GROUP \
  --location \$LOCATION

echo "🏗️  Deploy da Infraestrutura (Bicep)..."
az deployment group create \
  --resource-group \$RESOURCE_GROUP \
  --template-file infra/main.bicep \
  --parameters environment=\$ENVIRONMENT \
  --output table

echo "✅ Infraestrutura provisionada com sucesso!"
echo "📦 Instalando dependências Python..."
pip install -r requirements.txt

echo "🎉 Deploy completo!"

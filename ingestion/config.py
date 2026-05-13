# ingestion/config.py
import os
from dataclasses import dataclass
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

@dataclass
class AzureConfig:
    storage_account: str = os.getenv("AZURE_STORAGE_ACCOUNT", "")
    key_vault_uri: str = os.getenv("KEY_VAULT_URI", "")
    environment: str = os.getenv("ENVIRONMENT", "dev")

    @property
    def storage_url(self) -> str:
        return f"https://{self.storage_account}.blob.core.windows.net"

    def get_blob_client(self) -> BlobServiceClient:
        credential = DefaultAzureCredential()
        return BlobServiceClient(self.storage_url, credential=credential)

    def get_secret(self, secret_name: str) -> str:
        credential = DefaultAzureCredential()
        client = SecretClient(vault_url=self.key_vault_uri, credential=credential)
        return client.get_secret(secret_name).value


azure_config = AzureConfig()

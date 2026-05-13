# ingestion/ibge_ingestion.py
import httpx
import pandas as pd
import asyncio
import logging
from typing import Optional
from azure.storage.blob import BlobServiceClient
from io import BytesIO
from ingestion.config import azure_config

logger = logging.getLogger(__name__)

IBGE_BASE_URL = "https://servicodados.ibge.gov.br/api/v1"

class IBGEIngestion:
    """
    Ingere dados do IBGE: municípios, estados, mesorregiões e
    informações socioeconômicas para enriquecimento geográfico.
    """

    def __init__(self):
        self.client = httpx.AsyncClient(timeout=30.0)
        self.blob_client = azure_config.get_blob_client()

    async def fetch_municipios(self, uf: Optional[str] = None) -> pd.DataFrame:
        """Busca todos os municípios, opcionalmente filtrando por UF."""
        url = f"{IBGE_BASE_URL}/localidades/municipios"
        if uf:
            url = f"{IBGE_BASE_URL}/localidades/estados/{uf}/municipios"

        response = await self.client.get(url)
        response.raise_for_status()
        data = response.json()

        df = pd.DataFrame([{
            "municipio_id": m["id"],
            "municipio_nome": m["nome"],
            "uf_sigla": m["microrregiao"]["mesorregiao"]["UF"]["sigla"],
            "uf_nome": m["microrregiao"]["mesorregiao"]["UF"]["nome"],
            "mesorregiao_nome": m["microrregiao"]["mesorregiao"]["nome"],
            "microrregiao_nome": m["microrregiao"]["nome"],
            "regiao_nome": m["microrregiao"]["mesorregiao"]["UF"]["regiao"]["nome"]
        } for m in data])

        logger.info(f"✅ {len(df)} municípios carregados do IBGE")
        return df

    async def fetch_estados(self) -> pd.DataFrame:
        """Busca todos os estados brasileiros."""
        url = f"{IBGE_BASE_URL}/localidades/estados"
        response = await self.client.get(url)
        response.raise_for_status()

        df = pd.DataFrame(response.json())
        df = df.rename(columns={"id": "estado_id", "nome": "estado_nome",
                                 "sigla": "estado_sigla"})
        return df

    async def save_to_bronze(self, df: pd.DataFrame, dataset_name: str):
        """Salva DataFrame no container Bronze do Data Lake."""
        buffer = BytesIO()
        df.to_parquet(buffer, index=False, engine='pyarrow')
        buffer.seek(0)

        blob_path = f"ibge/{dataset_name}.parquet"
        container_client = self.blob_client.get_container_client("bronze")
        container_client.upload_blob(
            name=blob_path,
            data=buffer,
            overwrite=True
        )
        logger.info(f"📦 Salvo em bronze/{blob_path}")

    async def run_full_ingestion(self):
        """Executa toda a ingestão do IBGE."""
        logger.info("🚀 Iniciando ingestão IBGE...")

        municipios = await self.fetch_municipios()
        await self.save_to_bronze(municipios, "municipios")

        estados = await self.fetch_estados()
        await self.save_to_bronze(estados, "estados")

        await self.client.aclose()
        logger.info("✅ Ingestão IBGE concluída!")
        return {"municipios": len(municipios), "estados": len(estados)}

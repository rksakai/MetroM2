# processing/bronze_to_silver.py
import pandas as pd
import numpy as np
from azure.storage.blob import BlobServiceClient
from io import BytesIO
import logging
from ingestion.config import azure_config

logger = logging.getLogger(__name__)

class BronzeToSilverProcessor:
    """
    Transforma dados brutos (Bronze) em dados limpos e validados (Silver).
    Aplica: deduplicação, normalização, validação de schema, tipagem.
    """

    def __init__(self):
        self.blob_client = azure_config.get_blob_client()

    def _read_parquet_from_blob(
        self, container: str, blob_path: str
    ) -> pd.DataFrame:
        container_client = self.blob_client.get_container_client(container)
        blob_client = container_client.get_blob_client(blob_path)
        data = blob_client.download_blob().readall()
        return pd.read_parquet(BytesIO(data))

    def _save_parquet_to_blob(
        self, df: pd.DataFrame, container: str, blob_path: str
    ):
        buffer = BytesIO()
        df.to_parquet(buffer, index=False, engine='pyarrow')
        buffer.seek(0)
        container_client = self.blob_client.get_container_client(container)
        container_client.upload_blob(name=blob_path, data=buffer, overwrite=True)

    def process_fipezap(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpa e valida dados FipeZAP."""
        # Normaliza nomes de colunas
        df.columns = (df.columns.str.lower()
                                 .str.strip()
                                 .str.replace(" ", "_")
                                 .str.replace("-", "_"))

        # Remove duplicatas
        df = df.drop_duplicates(subset=["data_referencia", "cidade"])

        # Validações
        df = df[df["preco_m2_venda"] > 0]
        df = df[df["preco_m2_venda"] < 100_000]  # Outlier cap
        df = df[df["preco_m2_aluguel"] > 0]

        # Garante tipos corretos
        df["data_referencia"] = pd.to_datetime(
            df["data_referencia"], format="%Y-%m"
        )
        df["preco_m2_venda"] = df["preco_m2_venda"].astype(float)
        df["preco_m2_aluguel"] = df["preco_m2_aluguel"].astype(float)

        # Calcula yield imobiliário (cap rate anual)
        df["cap_rate_anual"] = (
            (df["preco_m2_aluguel"] * 12) / df["preco_m2_venda"] * 100
        ).round(4)

        # Adiciona metadados
        df["processado_em"] = pd.Timestamp.now()
        df["camada"] = "silver"

        logger.info(f"✅ FipeZAP Silver: {len(df)} registros válidos")
        return df

    def process_ibge_municipios(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normaliza dados de municípios do IBGE."""
        df = df.drop_duplicates(subset=["municipio_id"])
        df["municipio_nome"] = df["municipio_nome"].str.strip().str.title()
        df["uf_sigla"] = df["uf_sigla"].str.upper()

        # Mapeamento de regiões
        regiao_map = {
            "Norte": "N", "Nordeste": "NE", "Centro-Oeste": "CO",
            "Sudeste": "SE", "Sul": "S"
        }
        df["regiao_sigla"] = df["regiao_nome"].map(regiao_map)
        df["processado_em"] = pd.Timestamp.now()

        return df

    def run(self):
        """Executa transformação Bronze → Silver."""
        logger.info("⚙️ Iniciando Bronze → Silver...")

        # Processa FipeZAP
        try:
            # Em produção leria o arquivo mais recente via listagem
            fipezap_bronze = self._read_parquet_from_blob(
                "bronze", "fipezap/latest.parquet"
            )
            fipezap_silver = self.process_fipezap(fipezap_bronze)
            self._save_parquet_to_blob(fipezap_silver, "silver", "fipezap.parquet")
        except Exception as e:
            logger.warning(f"FipeZAP Bronze→Silver: {e}")

        # Processa IBGE
        try:
            ibge_bronze = self._read_parquet_from_blob(
                "bronze", "ibge/municipios.parquet"
            )
            ibge_silver = self.process_ibge_municipios(ibge_bronze)
            self._save_parquet_to_blob(ibge_silver, "silver", "municipios.parquet")
        except Exception as e:
            logger.warning(f"IBGE Bronze→Silver: {e}")

        logger.info("✅ Bronze → Silver concluído!")

# processing/silver_to_gold.py
import pandas as pd
import numpy as np
from io import BytesIO
import logging
from ingestion.config import azure_config

logger = logging.getLogger(__name__)

class SilverToGoldProcessor:
    """
    Cria tabelas analíticas Gold prontas para consumo via API e dashboards.
    Inclui agregações, índices de mercado e métricas calculadas.
    """

    def __init__(self):
        self.blob_client = azure_config.get_blob_client()

    def _read_silver(self, name: str) -> pd.DataFrame:
        container = self.blob_client.get_container_client("silver")
        data = container.get_blob_client(f"{name}.parquet").download_blob().readall()
        return pd.read_parquet(BytesIO(data))

    def _save_gold(self, df: pd.DataFrame, name: str):
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        container = self.blob_client.get_container_client("gold")
        container.upload_blob(name=f"{name}.parquet", data=buffer, overwrite=True)
        logger.info(f"✅ Gold/{name}: {len(df)} registros")

    def build_market_summary(self, df_fipezap: pd.DataFrame) -> pd.DataFrame:
        """
        Tabela Gold: resumo de mercado por cidade com últimas métricas.
        """
        latest = (df_fipezap
                  .sort_values("data_referencia")
                  .groupby("cidade")
                  .last()
                  .reset_index())

        # Calcula variação 12 meses
        df_12m = df_fipezap.copy()
        df_12m["data_12m_atras"] = (
            df_12m["data_referencia"] - pd.DateOffset(months=12)
        )

        # Pega preço de 12 meses atrás para cada cidade
        price_12m_ago = (df_fipezap
                         .sort_values("data_referencia")
                         .groupby("cidade")
                         .nth(-13)  # 13º registro mais recente
                         [["cidade", "preco_m2_venda"]]
                         .rename(columns={"preco_m2_venda": "preco_m2_12m_atras"}))

        summary = latest.merge(price_12m_ago, on="cidade", how="left")
        summary["variacao_12m"] = (
            (summary["preco_m2_venda"] - summary["preco_m2_12m_atras"])
            / summary["preco_m2_12m_atras"] * 100
        ).round(2)

        # Score de mercado (0–100): combina cap_rate + valorização
        summary["score_investimento"] = (
            (summary["cap_rate_anual"] / summary["cap_rate_anual"].max() * 50) +
            (summary["variacao_12m"].clip(0) /
             summary["variacao_12m"].clip(0).max() * 50)
        ).round(1)

        summary["categoria_mercado"] = pd.cut(
            summary["score_investimento"],
            bins=[0, 30, 50, 70, 100],
            labels=["Estável", "Moderado", "Aquecido", "Muito Aquecido"]
        )

        return summary

    def build_time_series_gold(self, df: pd.DataFrame) -> pd.DataFrame:
        """Série histórica completa para gráficos de linha."""
        df = df.sort_values(["cidade", "data_referencia"])
        df["media_movel_3m"] = (df.groupby("cidade")["preco_m2_venda"]
                                  .transform(lambda x: x.rolling(3).mean()))
        df["media_movel_6m"] = (df.groupby("cidade")["preco_m2_venda"]
                                  .transform(lambda x: x.rolling(6).mean()))
        return df

    def run(self):
        logger.info("⚙️ Iniciando Silver → Gold...")
        fipezap = self._read_silver("fipezap")

        summary = self.build_market_summary(fipezap)
        self._save_gold(summary, "market_summary")

        ts = self.build_time_series_gold(fipezap)
        self._save_gold(ts, "time_series")

        logger.info("✅ Silver → Gold concluído!")

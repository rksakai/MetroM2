# tests/test_ingestion.py
import pytest
import pandas as pd
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch


# ─── FipeZAP ──────────────────────────────────────────────────────
class TestFipeZAPIngestion:

    def test_generate_synthetic_data_shape(self):
        from ingestion.fipezap_ingestion import FipeZAPIngestion
        ingestor = FipeZAPIngestion()
        df = ingestor._generate_synthetic_data(
            ["São Paulo", "Curitiba"], months=12
        )
        assert len(df) == 24  # 2 cidades × 12 meses
        assert "cidade" in df.columns
        assert "preco_m2_venda" in df.columns
        assert "preco_m2_aluguel" in df.columns
        assert "cap_rate_anual" in df.columns

    def test_generated_prices_are_positive(self):
        from ingestion.fipezap_ingestion import FipeZAPIngestion
        ingestor = FipeZAPIngestion()
        df = ingestor._generate_synthetic_data(["São Paulo"], months=24)
        assert (df["preco_m2_venda"] > 0).all()
        assert (df["preco_m2_aluguel"] > 0).all()

    def test_generated_cap_rate_is_realistic(self):
        from ingestion.fipezap_ingestion import FipeZAPIngestion
        ingestor = FipeZAPIngestion()
        df = ingestor._generate_synthetic_data(["Fortaleza"], months=12)
        # Cap rate anual esperado entre 2% e 15% para mercado BR
        assert (df["cap_rate_anual"] > 2).all()
        assert (df["cap_rate_anual"] < 15).all()

    def test_all_default_cities_generated(self):
        from ingestion.fipezap_ingestion import FipeZAPIngestion, FIPEZAP_CITIES
        ingestor = FipeZAPIngestion()
        df = ingestor._generate_synthetic_data(FIPEZAP_CITIES, months=6)
        assert df["cidade"].nunique() == len(FIPEZAP_CITIES)

    @pytest.mark.asyncio
    async def test_fetch_or_generate_returns_dataframe(self):
        from ingestion.fipezap_ingestion import FipeZAPIngestion
        ingestor = FipeZAPIngestion()
        # Força fallback sintético (sem Azure)
        with patch.object(ingestor, 'blob_client', MagicMock()):
            df = await ingestor.fetch_or_generate()
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0


# ─── IBGE ─────────────────────────────────────────────────────────
class TestIBGEIngestion:

    @pytest.mark.asyncio
    async def test_fetch_estados_returns_valid_df(self):
        from ingestion.ibge_ingestion import IBGEIngestion
        import httpx

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"id": 35, "nome": "São Paulo", "sigla": "SP",
             "regiao": {"id": 3, "sigla": "SE", "nome": "Sudeste"}},
            {"id": 41, "nome": "Paraná", "sigla": "PR",
             "regiao": {"id": 4, "sigla": "S", "nome": "Sul"}},
        ]

        with patch("httpx.AsyncClient.get",
                   new_callable=AsyncMock,
                   return_value=mock_response):
            ingestor = IBGEIngestion()
            df = await ingestor.fetch_estados()

        assert len(df) == 2
        assert "estado_sigla" in df.columns

    @pytest.mark.asyncio
    async def test_fetch_municipios_filters_by_uf(self):
        from ingestion.ibge_ingestion import IBGEIngestion

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "id": 4106902, "nome": "Curitiba",
                "microrregiao": {
                    "id": 41037,
                    "nome": "Curitiba",
                    "mesorregiao": {
                        "id": 4105,
                        "nome": "Metropolitana de Curitiba",
                        "UF": {
                            "id": 41, "sigla": "PR", "nome": "Paraná",
                            "regiao": {"id": 4, "sigla": "S", "nome": "Sul"}
                        }
                    }
                }
            }
        ]

        with patch("httpx.AsyncClient.get",
                   new_callable=AsyncMock,
                   return_value=mock_response):
            ingestor = IBGEIngestion()
            df = await ingestor.fetch_municipios(uf="PR")

        assert len(df) == 1
        assert df.iloc[0]["municipio_nome"] == "Curitiba"
        assert df.iloc[0]["uf_sigla"] == "PR"
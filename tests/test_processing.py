# tests/test_processing.py
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch


def _make_fipezap_df(n_cidades: int = 3, n_meses: int = 24) -> pd.DataFrame:
    """Cria DataFrame FipeZAP de teste."""
    cidades = ["São Paulo", "Curitiba", "Fortaleza"][:n_cidades]
    records = []
    for cidade in cidades:
        base = 8000 if cidade == "São Paulo" else 5000
        for i in range(n_meses):
            data = datetime.now() - timedelta(days=30 * (n_meses - i))
            records.append({
                "data_referencia": data.strftime("%Y-%m"),
                "cidade": cidade,
                "preco_m2_venda": base + i * 50 + np.random.normal(0, 50),
                "preco_m2_aluguel": (base + i * 50) * 0.0045,
                "variacao_mensal": 0.005 + np.random.normal(0, 0.002),
                "variacao_anual": 0.07 + np.random.normal(0, 0.01),
                "indice_fipezap": 100 + i * 0.5,
                "fonte": "FipeZAP"
            })
    return pd.DataFrame(records)


# ─── Bronze → Silver ─────────────────────────────────────────────
class TestBronzeToSilverProcessor:

    def test_process_fipezap_removes_negative_prices(self):
        from processing.bronze_to_silver import BronzeToSilverProcessor
        processor = BronzeToSilverProcessor()
        df = _make_fipezap_df()
        df.loc[0, "preco_m2_venda"] = -100  # Injeta valor inválido
        df_clean = processor.process_fipezap(df)
        assert (df_clean["preco_m2_venda"] > 0).all()

    def test_process_fipezap_adds_cap_rate(self):
        from processing.bronze_to_silver import BronzeToSilverProcessor
        processor = BronzeToSilverProcessor()
        df = _make_fipezap_df()
        df_clean = processor.process_fipezap(df)
        assert "cap_rate_anual" in df_clean.columns
        assert (df_clean["cap_rate_anual"] > 0).all()

    def test_process_fipezap_normalizes_column_names(self):
        from processing.bronze_to_silver import BronzeToSilverProcessor
        processor = BronzeToSilverProcessor()
        df = _make_fipezap_df()
        df_clean = processor.process_fipezap(df)
        for col in df_clean.columns:
            assert col == col.lower(), f"Coluna não normalizada: {col}"
            assert " " not in col

    def test_process_fipezap_converts_date_type(self):
        from processing.bronze_to_silver import BronzeToSilverProcessor
        processor = BronzeToSilverProcessor()
        df = _make_fipezap_df()
        df_clean = processor.process_fipezap(df)
        assert pd.api.types.is_datetime64_any_dtype(df_clean["data_referencia"])

    def test_process_fipezap_removes_duplicates(self):
        from processing.bronze_to_silver import BronzeToSilverProcessor
        processor = BronzeToSilverProcessor()
        df = _make_fipezap_df(n_cidades=1, n_meses=5)
        df_dup = pd.concat([df, df])  # duplica tudo
        df_clean = processor.process_fipezap(df_dup)
        assert len(df_clean) == len(df)

    def test_process_ibge_municipios(self):
        from processing.bronze_to_silver import BronzeToSilverProcessor
        processor = BronzeToSilverProcessor()
        df = pd.DataFrame([
            {"municipio_id": 3550308, "municipio_nome": "São Paulo",
             "uf_sigla": "SP", "uf_nome": "São Paulo",
             "mesorregiao_nome": "Metropolitana de São Paulo",
             "microrregiao_nome": "São Paulo",
             "regiao_nome": "Sudeste"},
        ])
        df_clean = processor.process_ibge_municipios(df)
        assert "regiao_sigla" in df_clean.columns
        assert df_clean.iloc[0]["regiao_sigla"] == "SE"


# ─── Silver → Gold ────────────────────────────────────────────────
class TestSilverToGoldProcessor:

    def test_build_market_summary_has_required_columns(self):
        from processing.silver_to_gold import SilverToGoldProcessor
        processor = SilverToGoldProcessor()
        df = _make_fipezap_df()
        from processing.bronze_to_silver import BronzeToSilverProcessor
        df_silver = BronzeToSilverProcessor().process_fipezap(df)
        summary = processor.build_market_summary(df_silver)
        for col in ["cidade", "preco_m2_venda", "cap_rate_anual",
                    "score_investimento"]:
            assert col in summary.columns, f"Coluna faltando: {col}"

    def test_score_investimento_between_0_and_100(self):
        from processing.silver_to_gold import SilverToGoldProcessor
        from processing.bronze_to_silver import BronzeToSilverProcessor
        df = _make_fipezap_df()
        df_silver = BronzeToSilverProcessor().process_fipezap(df)
        summary = SilverToGoldProcessor().build_market_summary(df_silver)
        assert (summary["score_investimento"] >= 0).all()
        assert (summary["score_investimento"] <= 100).all()

    def test_build_time_series_adds_moving_averages(self):
        from processing.silver_to_gold import SilverToGoldProcessor
        from processing.bronze_to_silver import BronzeToSilverProcessor
        df = _make_fipezap_df(n_meses=12)
        df_silver = BronzeToSilverProcessor().process_fipezap(df)
        ts = SilverToGoldProcessor().build_time_series_gold(df_silver)
        assert "media_movel_3m" in ts.columns
        assert "media_movel_6m" in ts.columns


# ─── Geo Enrichment ───────────────────────────────────────────────
class TestGeoEnrichmentProcessor:

    def test_geocode_known_capital(self):
        from processing.geo_enrichment import GeoEnrichmentProcessor
        geo = GeoEnrichmentProcessor()
        result = geo.geocode_city("São Paulo")
        assert result.encontrado is True
        assert result.latitude is not None
        assert abs(result.latitude - (-23.55)) < 0.5

    def test_geocode_unknown_city_returns_not_found(self):
        from processing.geo_enrichment import GeoEnrichmentProcessor
        geo = GeoEnrichmentProcessor()
        with patch.object(geo, "_load_municipios",
                          return_value=pd.DataFrame(columns=[
                              "municipio_nome", "municipio_id",
                              "uf_sigla", "regiao_nome"
                          ])):
            result = geo.geocode_city("CidadeInexistente12345")
        assert result.encontrado is False

    def test_haversine_sp_rj(self):
        from processing.geo_enrichment import GeoEnrichmentProcessor
        dist = GeoEnrichmentProcessor.haversine_km(
            -23.5505, -46.6333,  # São Paulo
            -22.9068, -43.1729   # Rio de Janeiro
        )
        # Distância real SP-RJ ~360 km
        assert 300 < dist < 450, f"Distância inesperada: {dist:.1f} km"

    def test_normalize_text(self):
        from processing.geo_enrichment import GeoEnrichmentProcessor
        assert GeoEnrichmentProcessor.normalize_text("são paulo") == "Sao Paulo"
        assert GeoEnrichmentProcessor.normalize_text("CURITIBA") == "Curitiba"

    def test_enrich_dataframe_adds_geo_columns(self):
        from processing.geo_enrichment import GeoEnrichmentProcessor
        geo = GeoEnrichmentProcessor()
        df = pd.DataFrame({"cidade": ["São Paulo", "Curitiba"]})
        df_enriched = geo.enrich_dataframe(df)
        for col in ["uf_sigla", "regiao", "latitude",
                    "longitude", "geo_encontrado"]:
            assert col in df_enriched.columns, f"Coluna faltando: {col}"

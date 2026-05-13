# tests/test_api.py
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock


def _mock_summary_df() -> pd.DataFrame:
    """DataFrame de mock para market_summary."""
    return pd.DataFrame([
        {
            "cidade": "São Paulo", "preco_m2_venda": 10500.0,
            "preco_m2_aluguel": 47.25, "cap_rate_anual": 0.054,
            "variacao_12m": 8.5, "score_investimento": 72.0,
            "categoria_mercado": "Aquecido",
            "data_referencia": pd.Timestamp("2024-12-01"),
            "preco_m2_12m_atras": 9677.0
        },
        {
            "cidade": "Curitiba", "preco_m2_venda": 7200.0,
            "preco_m2_aluguel": 36.0, "cap_rate_anual": 0.06,
            "variacao_12m": 6.2, "score_investimento": 65.0,
            "categoria_mercado": "Moderado",
            "data_referencia": pd.Timestamp("2024-12-01"),
            "preco_m2_12m_atras": 6780.0
        },
        {
            "cidade": "Fortaleza", "preco_m2_venda": 5500.0,
            "preco_m2_aluguel": 29.7, "cap_rate_anual": 0.065,
            "variacao_12m": 5.1, "score_investimento": 58.0,
            "categoria_mercado": "Moderado",
            "data_referencia": pd.Timestamp("2024-12-01"),
            "preco_m2_12m_atras": 5233.0
        }
    ])


def _mock_ts_df() -> pd.DataFrame:
    """DataFrame de série temporal de mock."""
    records = []
    cidades = ["São Paulo", "Curitiba", "Fortaleza"]
    for cidade in cidades:
        base = {"São Paulo": 10000, "Curitiba": 7000, "Fortaleza": 5000}[cidade]
        for i in range(36):
            data = datetime.now() - timedelta(days=30 * (36 - i))
            records.append({
                "cidade":           cidade,
                "data_referencia":  data,
                "preco_m2_venda":   base + i * 30 + np.random.normal(0, 50),
                "preco_m2_aluguel": base * 0.0045 + i * 0.1,
                "cap_rate_anual":   0.054 + np.random.normal(0, 0.002),
                "variacao_mensal":  0.005 + np.random.normal(0, 0.002),
                "media_movel_3m":   base + i * 28,
                "media_movel_6m":   base + i * 25,
            })
    return pd.DataFrame(records)


@pytest.fixture
def client():
    """FastAPI TestClient com mocks de dados."""
    from api.main import app, _cache
    mock_ts  = _mock_ts_df()
    mock_sum = _mock_summary_df()

    with patch("api.main._load_gold") as mock_load:
        def side_effect(name):
            if name == "time_series":    return mock_ts
            if name == "market_summary": return mock_sum
            return pd.DataFrame()
        mock_load.side_effect = side_effect
        _cache.clear()
        yield TestClient(app)


# ─── Health Check ─────────────────────────────────────────────────
class TestHealthEndpoint:

    def test_root_returns_ok(self, client):
        r = client.get("/")
        assert r.status_code == 200
        assert r.json()["status"] == "✅ online"


# ─── /regioes ─────────────────────────────────────────────────────
class TestRegioesEndpoint:

    def test_returns_list_of_strings(self, client):
        r = client.get("/regioes")
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list)
        assert len(data) > 0
        assert all(isinstance(c, str) for c in data)

    def test_known_city_in_list(self, client):
        r = client.get("/regioes")
        assert "São Paulo" in r.json()


# ─── /mercado/resumo ──────────────────────────────────────────────
class TestMercadoResumoEndpoint:

    def test_returns_list(self, client):
        r = client.get("/mercado/resumo")
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list)
        assert len(data) > 0

    def test_summary_has_required_fields(self, client):
        r = client.get("/mercado/resumo")
        item = r.json()[0]
        required = [
            "cidade", "preco_m2_venda", "preco_m2_aluguel",
            "cap_rate_anual", "variacao_12m",
            "score_investimento", "categoria_mercado"
        ]
        for field in required:
            assert field in item, f"Campo faltando: {field}"

    def test_filter_by_cidade(self, client):
        r = client.get("/mercado/resumo?cidade=Curitiba")
        assert r.status_code == 200
        data = r.json()
        assert all("Curitiba" in item["cidade"] for item in data)

    def test_top_n_limit(self, client):
        r = client.get("/mercado/resumo?top_n=2")
        assert r.status_code == 200
        assert len(r.json()) <= 2


# ─── /mercado/{cidade}/analise ────────────────────────────────────
class TestCityAnaliseEndpoint:

    def test_valid_city_returns_200(self, client):
        r = client.get("/mercado/São Paulo/analise")
        assert r.status_code == 200

    def test_analise_has_serie_temporal(self, client):
        r = client.get("/mercado/São Paulo/analise?meses=12")
        data = r.json()
        assert "serie_temporal" in data
        assert len(data["serie_temporal"]) > 0

    def test_analise_has_summary(self, client):
        r = client.get("/mercado/São Paulo/analise")
        data = r.json()
        assert "summary" in data
        assert "preco_m2_venda" in data["summary"]

    def test_unknown_city_returns_404(self, client):
        r = client.get("/mercado/CidadeInexistente99999/analise")
        assert r.status_code == 404

    def test_meses_parameter_limits_results(self, client):
        r6  = client.get("/mercado/São Paulo/analise?meses=6")
        r12 = client.get("/mercado/São Paulo/analise?meses=12")
        assert (r6.json()["total_registros"] <=
                r12.json()["total_registros"])


# ─── /mercado/comparacao ──────────────────────────────────────────
class TestComparacaoEndpoint:

    def test_comparacao_returns_dict(self, client):
        r = client.get("/mercado/comparacao?cidades=São Paulo,Curitiba")
        assert r.status_code == 200
        data = r.json()
        assert "São Paulo" in data or "Curitiba" in data

    def test_unknown_city_has_error_key(self, client):
        r = client.get("/mercado/comparacao?cidades=CidadeXXX")
        data = r.json()
        assert "CidadeXXX" in data
        assert "erro" in data["CidadeXXX"]


# ─── ML: Market Scoring ───────────────────────────────────────────
class TestMarketScoringModel:

    def test_scores_all_cities(self):
        from ml.market_scoring import MarketScoringModel
        model = MarketScoringModel()
        df = _mock_ts_df()
        scores = model.score_all(df)
        assert len(scores) == 3
        for s in scores:
            assert 0 <= s.score <= 100
            assert s.categoria in [
                "Estável", "Moderado", "Aquecido", "Muito Aquecido"
            ]

    def test_score_city_by_name(self):
        from ml.market_scoring import MarketScoringModel
        model = MarketScoringModel()
        df = _mock_ts_df()
        s = model.score_city(df, "São Paulo")
        assert s is not None
        assert s.cidade == "São Paulo"

    def test_score_componentes_sum_approximately_equals_total(self):
        from ml.market_scoring import MarketScoringModel
        model = MarketScoringModel()
        df = _mock_ts_df()
        scores = model.score_all(df)
        weights = MarketScoringModel.WEIGHTS
        for s in scores:
            weighted_sum = sum(
                s.score_componentes[k] * weights[k]
                for k in weights
            )
            assert abs(weighted_sum - s.score) < 1.0


# ─── ML: Anomaly Detection ────────────────────────────────────────
class TestAnomalyDetectionModel:

    def test_detect_returns_report(self):
        from ml.anomaly_detection import AnomalyDetectionModel
        model = AnomalyDetectionModel(contamination=0.1)
        df = _mock_ts_df()
        report = model.detect(df)
        assert hasattr(report, "total_anomalias")
        assert hasattr(report, "anomalias")
        assert isinstance(report.anomalias, list)

    def test_detect_city_filters_correctly(self):
        from ml.anomaly_detection import AnomalyDetectionModel
        model = AnomalyDetectionModel(contamination=0.1)
        df = _mock_ts_df()
        report = model.detect_city(df, "Curitiba")
        for a in report.anomalias:
            assert "Curitiba" in a.cidade

    def test_empty_df_returns_empty_report(self):
        from ml.anomaly_detection import AnomalyDetectionModel
        model = AnomalyDetectionModel()
        report = model.detect(pd.DataFrame())
        assert report.total_anomalias == 0
        assert report.anomalias == []

    def test_injected_price_spike_is_detected(self):
        """Injeta um pico absurdo de preço e verifica se é detectado."""
        from ml.anomaly_detection import AnomalyDetectionModel
        model = AnomalyDetectionModel(contamination=0.05)
        df = _mock_ts_df()
        # Injeta spike: 10x o preço normal em São Paulo
        spike_idx = df[df["cidade"] == "São Paulo"].index[10]
        df.loc[spike_idx, "preco_m2_venda"] = 999_999.0

        report = model.detect(df, "São Paulo")
        # Deve detectar pelo menos uma anomalia
        assert report.total_anomalias > 0

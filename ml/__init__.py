# ml/__init__.py
"""
Módulo de Machine Learning para análise de mercado imobiliário.

Modelos disponíveis:
- RealEstateForecastModel : Previsão de preços (Prophet + regressores)
- MarketScoringModel      : Score de atratividade de investimento (XGBoost)
- AnomalyDetectionModel   : Detecção de anomalias de preços (Isolation Forest)
"""

from ml.price_forecast import RealEstateForecastModel, ForecastResult
from ml.market_scoring import MarketScoringModel, MarketScore
from ml.anomaly_detection import AnomalyDetectionModel, AnomalyReport

__all__ = [
    "RealEstateForecastModel",
    "ForecastResult",
    "MarketScoringModel",
    "MarketScore",
    "AnomalyDetectionModel",
    "AnomalyReport",
]

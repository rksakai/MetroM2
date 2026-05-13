# api/__init__.py
"""
API REST de Análise de Mercado Imobiliário.

Stack:
- FastAPI   : framework web assíncrono
- Pydantic  : validação e serialização de dados
- Azure SDK : acesso ao Data Lake e Key Vault

Routers disponíveis:
- /regioes              → regions.py   (busca e listagem de regiões)
- /mercado/analytics    → analytics.py (métricas, rankings, comparações)
- /mercado/forecast     → forecast.py  (previsões Prophet e XGBoost)
"""
# api/main.py
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import pandas as pd
import numpy as np
from io import BytesIO
from typing import Optional, List
import logging
import os

from api.models.schemas import (
    MarketSummaryResponse,
    ForecastResponse,
    ForecastPoint,
    RegionAnalysisResponse,
    TimeSeriesPoint
)
from ingestion.config import azure_config
from ml.price_forecast import RealEstateForecastModel

logger = logging.getLogger(__name__)

app = FastAPI(
    title="🏠 API de Análise do Mercado Imobiliário",
    description=(
        "Análise e previsão de preços imobiliários por região do Brasil. "
        "Dados: FipeZAP + IBGE + BCB."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

# ─── Cache in-memory dos DataFrames Gold ──────────────────────────
_cache: dict = {}

def _load_gold(name: str) -> pd.DataFrame:
    """Carrega tabela Gold do Data Lake com cache local."""
    if name in _cache:
        return _cache[name]
    try:
        blob_client = azure_config.get_blob_client()
        container  = blob_client.get_container_client("gold")
        data = container.get_blob_client(f"{name}.parquet").download_blob().readall()
        df = pd.read_parquet(BytesIO(data))
        _cache[name] = df
        return df
    except Exception as e:
        logger.warning(f"Azure indisponível: {e}. Usando dados locais.")
        return _load_local_fallback(name)

def _load_local_fallback(name: str) -> pd.DataFrame:
    """Fallback: carrega dados locais para desenvolvimento."""
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from ingestion.fipezap_ingestion import FipeZAPIngestion
    from processing.silver_to_gold import SilverToGoldProcessor
    import asyncio

    if "time_series" not in _cache:
        ingestor = FipeZAPIngestion()
        loop = asyncio.new_event_loop()
        df_raw = loop.run_until_complete(ingestor.fetch_or_generate())
        loop.close()

        # Processa sem Azure
        from processing.bronze_to_silver import BronzeToSilverProcessor
        processor = BronzeToSilverProcessor()
        df_silver = processor.process_fipezap(df_raw)

        gold_proc = SilverToGoldProcessor()
        _cache["time_series"] = gold_proc.build_time_series_gold(df_silver)
        _cache["market_summary"] = gold_proc.build_market_summary(df_silver)

    return _cache.get(name, pd.DataFrame())

# ─── Endpoints ────────────────────────────────────────────────────

@app.get("/", summary="Health Check")
async def root():
    return {"status": "✅ online", "service": "Real Estate Market Analysis API"}

@app.get(
    "/regioes",
    summary="Lista regiões disponíveis",
    response_model=List[str]
)
async def list_regions():
    """Retorna lista de todas as cidades disponíveis para análise."""
    df = _load_gold("market_summary")
    if df.empty:
        return []
    return sorted(df["cidade"].unique().tolist())

@app.get(
    "/mercado/resumo",
    summary="Resumo do mercado por cidade",
    response_model=List[MarketSummaryResponse]
)
async def market_summary(
    cidade: Optional[str] = Query(None, description="Filtra por cidade"),
    top_n: int = Query(20, ge=1, le=100, description="Número de cidades")
):
    """
    Retorna resumo do mercado imobiliário com scores e métricas recentes.
    """
    df = _load_gold("market_summary")
    if df.empty:
        raise HTTPException(status_code=503, detail="Dados indisponíveis")

    if cidade:
        df = df[df["cidade"].str.contains(cidade, case=False, na=False)]

    df = df.head(top_n)

    return [
        MarketSummaryResponse(
            cidade=row["cidade"],
            preco_m2_venda=float(row.get("preco_m2_venda", 0)),
            preco_m2_aluguel=float(row.get("preco_m2_aluguel", 0)),
            cap_rate_anual=float(row.get("cap_rate_anual", 0)),
            variacao_12m=float(row.get("variacao_12m", 0) or 0),
            score_investimento=float(row.get("score_investimento", 0) or 0),
            categoria_mercado=str(row.get("categoria_mercado", "N/A")),
            data_referencia=str(
                row.get("data_referencia", pd.Timestamp.now())
            )[:10]
        )
        for _, row in df.iterrows()
    ]

@app.get(
    "/mercado/{cidade}/analise",
    summary="Análise completa de uma cidade",
    response_model=RegionAnalysisResponse
)
async def city_analysis(
    cidade: str,
    meses: int = Query(24, ge=3, le=60, description="Janela histórica em meses")
):
    """
    Retorna análise completa com série temporal, estatísticas e ranking.
    """
    df_ts  = _load_gold("time_series")
    df_sum = _load_gold("market_summary")

    city_ts = df_ts[df_ts["cidade"].str.contains(cidade, case=False, na=False)]
    if city_ts.empty:
        raise HTTPException(
            status_code=404,
            detail=f"Cidade '{cidade}' não encontrada"
        )

    city_ts = city_ts.sort_values("data_referencia").tail(meses)

    summary_row = df_sum[
        df_sum["cidade"].str.contains(cidade, case=False, na=False)
    ]
    if summary_row.empty:
        raise HTTPException(status_code=404, detail=f"Resumo não encontrado")

    r = summary_row.iloc[0]

    return RegionAnalysisResponse(
        cidade=city_ts["cidade"].iloc[0],
        total_registros=len(city_ts),
        periodo_inicio=str(city_ts["data_referencia"].min())[:10],
        periodo_fim=str(city_ts["data_referencia"].max())[:10],
        preco_minimo=float(city_ts["preco_m2_venda"].min()),
        preco_maximo=float(city_ts["preco_m2_venda"].max()),
        preco_medio=float(city_ts["preco_m2_venda"].mean()),
        preco_mediano=float(city_ts["preco_m2_venda"].median()),
        serie_temporal=[
            TimeSeriesPoint(
                data=str(row["data_referencia"])[:10],
                preco_m2_venda=float(row["preco_m2_venda"]),
                preco_m2_aluguel=float(row["preco_m2_aluguel"]),
                cap_rate_anual=float(row["cap_rate_anual"]),
                variacao_mensal=float(row.get("variacao_mensal", 0) or 0),
                media_movel_3m=(
                    float(row["media_movel_3m"])
                    if pd.notna(row.get("media_movel_3m")) else None
                ),
                media_movel_6m=(
                    float(row["media_movel_6m"])
                    if pd.notna(row.get("media_movel_6m")) else None
                )
            )
            for _, row in city_ts.iterrows()
        ],
        summary=MarketSummaryResponse(
            cidade=r["cidade"],
            preco_m2_venda=float(r.get("preco_m2_venda", 0)),
            preco_m2_aluguel=float(r.get("preco_m2_aluguel", 0)),
            cap_rate_anual=float(r.get("cap_rate_anual", 0)),
            variacao_12m=float(r.get("variacao_12m", 0) or 0),
            score_investimento=float(r.get("score_investimento", 0) or 0),
            categoria_mercado=str(r.get("categoria_mercado", "N/A")),
            data_referencia=str(r.get("data_referencia", ""))[:10]
        )
    )

@app.get(
    "/mercado/{cidade}/forecast",
    summary="Previsão de preços",
    response_model=ForecastResponse
)
async def city_forecast(
    cidade: str,
    horizonte_meses: int = Query(12, ge=3, le=24, description="Meses a prever")
):
    """
    Gera previsão de preço por m² para os próximos meses usando Prophet.
    """
    df_ts = _load_gold("time_series")
    city_df = df_ts[df_ts["cidade"].str.contains(cidade, case=False, na=False)]

    if city_df.empty:
        raise HTTPException(status_code=404, detail=f"'{cidade}' não encontrada")

    cidade_real = city_df["cidade"].iloc[0]

    try:
        forecaster = RealEstateForecastModel()
        result = forecaster.train(
            df=df_ts,
            cidade=cidade_real,
            horizonte_meses=horizonte_meses
        )

        previsao = [
            ForecastPoint(
                data=str(row["data"])[:10],
                preco_previsto=float(row["preco_previsto"]),
                limite_inferior=float(row["limite_inferior"]),
                limite_superior=float(row["limite_superior"])
            )
            for _, row in result.forecast_df.iterrows()
        ]

        return ForecastResponse(
            cidade=cidade_real,
            horizonte_meses=horizonte_meses,
            modelo="Prophet (Meta)",
            mae=round(result.metrics["mae"], 2),
            mape=round(result.metrics["mape"], 2),
            previsao=previsao
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao gerar forecast: {str(e)}"
        )

@app.get(
    "/mercado/comparacao",
    summary="Compara múltiplas cidades"
)
async def compare_cities(
    cidades: str = Query(
        ...,
        description="Cidades separadas por vírgula: SP,RJ,BH"
    )
):
    """Compara métricas entre múltiplas cidades lado a lado."""
    lista = [c.strip() for c in cidades.split(",")]
    df_sum = _load_gold("market_summary")

    result = {}
    for cidade in lista:
        row = df_sum[df_sum["cidade"].str.contains(cidade, case=False, na=False)]
        if not row.empty:
            r = row.iloc[0]
            result[cidade] = {
                "preco_m2_venda": float(r.get("preco_m2_venda", 0)),
                "preco_m2_aluguel": float(r.get("preco_m2_aluguel", 0)),
                "cap_rate_anual": float(r.get("cap_rate_anual", 0)),
                "variacao_12m": float(r.get("variacao_12m", 0) or 0),
                "score_investimento": float(r.get("score_investimento", 0) or 0),
                "categoria_mercado": str(r.get("categoria_mercado", "N/A"))
            }
        else:
            result[cidade] = {"erro": "Cidade não encontrada"}

    return result

# api/routers/forecast.py
"""
Router de Forecast — previsões de preços e tendências.

Endpoints:
    GET /forecast/{cidade}              → previsão Prophet para uma cidade
    GET /forecast/{cidade}/tendencia    → análise de tendência (short/long)
    POST /forecast/batch                → forecast em lote para múltiplas cidades
"""

from fastapi import APIRouter, HTTPException, Query, Body
from typing import List, Optional
import pandas as pd
import logging
from datetime import datetime

from api.models.responses import (
    APIResponse, ForecastDetailResponse,
    ForecastPointResponse, ModelMetrics
)
from ml.price_forecast import RealEstateForecastModel

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/forecast", tags=["Forecast"])

_forecast_model = RealEstateForecastModel()


def _get_ts_df() -> pd.DataFrame:
    from api.main import _load_gold
    return _load_gold("time_series")


# ──────────────────────────────────────────────────────────────────
@router.get(
    "/{cidade}",
    summary="Previsão de preços para uma cidade",
    response_model=ForecastDetailResponse
)
async def city_forecast(
    cidade: str,
    horizonte_meses: int = Query(12, ge=3, le=24,
                                 description="Meses a prever (3–24)"),
):
    """
    Gera previsão de preço por m² usando Prophet com regressores externos.
    Retorna previsão ponto a ponto com intervalo de confiança de 95%.
    """
    df_ts    = _get_ts_df()
    city_df  = df_ts[df_ts["cidade"].str.contains(cidade, case=False, na=False)]

    if city_df.empty:
        raise HTTPException(status_code=404, detail=f"'{cidade}' não encontrada")

    cidade_real = city_df["cidade"].iloc[0]
    preco_atual = float(city_df.sort_values("data_referencia")
                               ["preco_m2_venda"].iloc[-1])

    try:
        result = _forecast_model.train(
            df=df_ts,
            cidade=cidade_real,
            horizonte_meses=horizonte_meses
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao gerar forecast: {str(e)}"
        )

    preco_final = float(result.forecast_df["preco_previsto"].iloc[-1])
    var_pct = round((preco_final - preco_atual) / preco_atual * 100, 2)

    return ForecastDetailResponse(
        cidade=cidade_real,
        horizonte_meses=horizonte_meses,
        modelo="Prophet (Meta) + Regressores",
        treinado_em=datetime.utcnow().isoformat() + "Z",
        metricas=ModelMetrics(
            mae=round(result.metrics["mae"], 2),
            mape=round(result.metrics["mape"], 2),
            rmse=round(result.metrics.get("rmse", 0), 2),
        ),
        preco_atual=preco_atual,
        preco_previsto_final=round(preco_final, 2),
        variacao_prevista_pct=var_pct,
        previsao=[
            ForecastPointResponse(
                data=str(row["data"])[:10],
                preco_previsto=round(float(row["preco_previsto"]), 2),
                limite_inferior=round(float(row["limite_inferior"]), 2),
                limite_superior=round(float(row["limite_superior"]), 2),
                tendencia=round(float(row["tendencia"]), 2)
                    if "tendencia" in result.forecast_df.columns else None
            )
            for _, row in result.forecast_df.iterrows()
        ]
    )


# ──────────────────────────────────────────────────────────────────
@router.get(
    "/{cidade}/tendencia",
    summary="Análise de tendência de curto e longo prazo"
)
async def city_trend(
    cidade: str,
    janela_curta: int = Query(3,  ge=1, le=12, description="Meses curto prazo"),
    janela_longa: int = Query(12, ge=6, le=48, description="Meses longo prazo"),
):
    """
    Analisa tendência de preços em duas janelas temporais.
    Retorna coeficientes de regressão linear, aceleração e sinal de momentum.
    """
    df_ts   = _get_ts_df()
    city_df = df_ts[
        df_ts["cidade"].str.contains(cidade, case=False, na=False)
    ].sort_values("data_referencia")

    if city_df.empty:
        raise HTTPException(status_code=404, detail=f"'{cidade}' não encontrada")

    import numpy as np

    def _tendencia(precos):
        if len(precos) < 2:
            return {"coef": 0, "r2": 0, "direcao": "estável"}
        x     = np.arange(len(precos))
        coef  = np.polyfit(x, precos, 1)
        y_hat = np.polyval(coef, x)
        ss_res = np.sum((precos - y_hat) ** 2)
        ss_tot = np.sum((precos - np.mean(precos)) ** 2)
        r2    = 1 - ss_res / (ss_tot + 1e-9)
        direcao = ("alta" if coef[0] > 0.5
                   else "queda" if coef[0] < -0.5
                   else "estável")
        return {
            "coef_mensal":       round(float(coef[0]), 2),
            "r2":                round(float(r2), 4),
            "direcao":           direcao,
            "variacao_total_pct": round(
                float((precos[-1] - precos[0]) / precos[0] * 100), 2
            )
        }

    p       = city_df["preco_m2_venda"].values
    curto   = _tendencia(p[-janela_curta:])
    longo   = _tendencia(p[-janela_longa:])

    aceleracao = curto["coef_mensal"] - longo["coef_mensal"]
    momentum   = ("acelerando" if aceleracao > 1
                  else "desacelerando" if aceleracao < -1
                  else "estável")

    return {
        "cidade":             city_df["cidade"].iloc[0],
        "preco_atual":        round(float(p[-1]), 2),
        "tendencia_curto_prazo": {**curto, "janela_meses": janela_curta},
        "tendencia_longo_prazo": {**longo, "janela_meses": janela_longa},
        "aceleracao":         round(aceleracao, 4),
        "momentum":           momentum,
        "n_observacoes":      len(p),
    }


# ──────────────────────────────────────────────────────────────────
@router.post(
    "/batch",
    summary="Forecast em lote para múltiplas cidades",
    response_model=List[dict]
)
async def batch_forecast(
    cidades: List[str] = Body(
        ...,
        example=["São Paulo", "Curitiba", "Fortaleza"],
        description="Lista de cidades (máx. 10)"
    ),
    horizonte_meses: int = Query(12, ge=3, le=24)
):
    """
    Gera forecasts para múltiplas cidades de uma só vez.
    Ideal para relatórios comparativos. Máximo de 10 cidades por requisição.
    """
    if len(cidades) > 10:
        raise HTTPException(
            status_code=400,
            detail="Máximo de 10 cidades por requisição batch"
        )

    df_ts   = _get_ts_df()
    results = []

    for cidade in cidades:
        city_df = df_ts[
            df_ts["cidade"].str.contains(cidade, case=False, na=False)
        ]
        if city_df.empty:
            results.append({"cidade": cidade, "erro": "Não encontrada"})
            continue

        cidade_real = city_df["cidade"].iloc[0]
        preco_atual = float(
            city_df.sort_values("data_referencia")["preco_m2_venda"].iloc[-1]
        )

        try:
            result = _forecast_model.train(df_ts, cidade_real, horizonte_meses)
            preco_final = float(result.forecast_df["preco_previsto"].iloc[-1])
            results.append({
                "cidade":                cidade_real,
                "preco_atual":           round(preco_atual, 2),
                "preco_previsto_final":  round(preco_final, 2),
                "variacao_prevista_pct": round(
                    (preco_final - preco_atual) / preco_atual * 100, 2
                ),
                "mape":                  round(result.metrics["mape"], 2),
                "horizonte_meses":       horizonte_meses,
            })
        except Exception as e:
            results.append({"cidade": cidade_real, "erro": str(e)})

    return results

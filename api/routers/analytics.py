# api/routers/analytics.py
"""
Router de Analytics — métricas, rankings, comparações e anomalias.

Endpoints:
    GET /analytics/resumo               → resumo geral do mercado nacional
    GET /analytics/ranking              → ranking de cidades por métrica
    GET /analytics/{cidade}/metricas    → métricas detalhadas de uma cidade
    GET /analytics/comparacao           → comparação entre múltiplas cidades
    GET /analytics/{cidade}/anomalias   → anomalias detectadas em uma cidade
    GET /analytics/anomalias            → anomalias em todo o mercado
    GET /analytics/{cidade}/scoring     → score detalhado de investimento
"""

from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
import pandas as pd
import logging

from api.models.responses import (
    APIResponse, PaginatedResponse, RankingItem, ComparisonItem,
    MarketMetrics, AnomalyReportResponse, AnomalyItem
)
from ml.market_scoring import MarketScoringModel
from ml.anomaly_detection import AnomalyDetectionModel

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/analytics", tags=["Analytics"])

_scoring_model     = MarketScoringModel()
_anomaly_model     = AnomalyDetectionModel(contamination=0.05)


def _get_dfs():
    """Carrega DataFrames Gold necessários."""
    from api.main import _load_gold
    return _load_gold("time_series"), _load_gold("market_summary")


def _row_to_metrics(row: pd.Series) -> MarketMetrics:
    return MarketMetrics(
        preco_m2_venda=float(row.get("preco_m2_venda", 0)),
        preco_m2_aluguel=float(row.get("preco_m2_aluguel", 0)),
        cap_rate_anual=float(row.get("cap_rate_anual", 0)),
        variacao_12m=float(row.get("variacao_12m", 0) or 0),
    )


# ──────────────────────────────────────────────────────────────────
@router.get(
    "/resumo",
    summary="Resumo geral do mercado nacional"
)
async def market_overview():
    """
    Estatísticas agregadas do mercado imobiliário nacional:
    médias, medianas, totais, destaques e alertas.
    """
    df_ts, df_sum = _get_dfs()
    if df_sum.empty:
        raise HTTPException(status_code=503, detail="Dados indisponíveis")

    return {
        "total_cidades":         int(df_sum["cidade"].nunique()),
        "preco_medio_nacional":  round(float(df_sum["preco_m2_venda"].mean()), 2),
        "preco_mediano_nacional": round(float(df_sum["preco_m2_venda"].median()), 2),
        "preco_maximo":  {
            "valor":  round(float(df_sum["preco_m2_venda"].max()), 2),
            "cidade": str(df_sum.loc[df_sum["preco_m2_venda"].idxmax(), "cidade"])
        },
        "preco_minimo": {
            "valor":  round(float(df_sum["preco_m2_venda"].min()), 2),
            "cidade": str(df_sum.loc[df_sum["preco_m2_venda"].idxmin(), "cidade"])
        },
        "cap_rate_medio": round(float(df_sum["cap_rate_anual"].mean()), 4),
        "melhor_cap_rate": {
            "valor":  round(float(df_sum["cap_rate_anual"].max()), 4),
            "cidade": str(df_sum.loc[df_sum["cap_rate_anual"].idxmax(), "cidade"])
        },
        "distribuicao_categorias": (
            df_sum["categoria_mercado"]
            .value_counts()
            .to_dict()
        ),
        "variacao_12m_media": round(
            float(df_sum["variacao_12m"].mean(skipna=True)), 2
        ),
    }


# ──────────────────────────────────────────────────────────────────
@router.get(
    "/ranking",
    summary="Ranking de cidades",
    response_model=List[RankingItem]
)
async def ranking(
    metrica: str = Query(
        "score_investimento",
        description="Métrica de ordenação: score_investimento | preco_m2_venda | "
                    "cap_rate_anual | variacao_12m"
    ),
    top_n: int = Query(15, ge=1, le=50),
    ordem: str = Query("desc", description="'asc' ou 'desc'")
):
    """Ranking das cidades por qualquer métrica disponível."""
    df_ts, df_sum = _get_dfs()

    metricas_validas = {
        "score_investimento", "preco_m2_venda",
        "cap_rate_anual", "variacao_12m", "preco_m2_aluguel"
    }
    if metrica not in metricas_validas:
        raise HTTPException(
            status_code=400,
            detail=f"Métrica inválida. Use: {metricas_validas}"
        )

    df_rank = df_sum.sort_values(
        metrica, ascending=(ordem == "asc")
    ).head(top_n).reset_index(drop=True)

    return [
        RankingItem(
            posicao=i + 1,
            cidade=str(row["cidade"]),
            score_investimento=float(row.get("score_investimento", 0) or 0),
            categoria_mercado=str(row.get("categoria_mercado", "N/A")),
            metrics=_row_to_metrics(row)
        )
        for i, (_, row) in enumerate(df_rank.iterrows())
    ]


# ──────────────────────────────────────────────────────────────────
@router.get(
    "/comparacao",
    summary="Compara múltiplas cidades",
    response_model=List[ComparisonItem]
)
async def compare(
    cidades: str = Query(
        ...,
        description="Cidades separadas por vírgula: 'São Paulo,Curitiba,Fortaleza'"
    )
):
    """Comparação lado a lado entre múltiplas cidades (máx. 8)."""
    lista = [c.strip() for c in cidades.split(",")][:8]
    df_ts, df_sum = _get_dfs()

    all_scores = _scoring_model.score_all(df_ts)
    score_map  = {s.cidade: s.score for s in all_scores}

    result = []
    for cidade in lista:
        row = df_sum[df_sum["cidade"].str.contains(cidade, case=False, na=False)]
        if row.empty:
            continue
        r = row.iloc[0]
        # Ranking nacional
        df_ranked = df_sum.sort_values("score_investimento", ascending=False)
        rank_pos  = df_ranked[
            df_ranked["cidade"] == r["cidade"]
        ].index.tolist()

        result.append(ComparisonItem(
            cidade=str(r["cidade"]),
            metrics=_row_to_metrics(r),
            score_investimento=float(r.get("score_investimento", 0) or 0),
            categoria_mercado=str(r.get("categoria_mercado", "N/A")),
            ranking_nacional=int(rank_pos[0] + 1) if rank_pos else None
        ))

    if not result:
        raise HTTPException(status_code=404, detail="Nenhuma cidade encontrada")
    return result


# ──────────────────────────────────────────────────────────────────
@router.get(
    "/{cidade}/metricas",
    summary="Métricas detalhadas de uma cidade"
)
async def city_metrics(
    cidade: str,
    meses: int = Query(12, ge=1, le=60)
):
    """Métricas estatísticas detalhadas com janela temporal configurável."""
    df_ts, _ = _get_dfs()
    city_df = df_ts[
        df_ts["cidade"].str.contains(cidade, case=False, na=False)
    ].sort_values("data_referencia").tail(meses)

    if city_df.empty:
        raise HTTPException(status_code=404, detail=f"'{cidade}' não encontrada")

    p = city_df["preco_m2_venda"]
    return {
        "cidade":          city_df["cidade"].iloc[0],
        "periodo_meses":   meses,
        "preco_atual":     round(float(p.iloc[-1]), 2),
        "preco_medio":     round(float(p.mean()), 2),
        "preco_mediano":   round(float(p.median()), 2),
        "preco_minimo":    round(float(p.min()), 2),
        "preco_maximo":    round(float(p.max()), 2),
        "desvio_padrao":   round(float(p.std()), 2),
        "coef_variacao":   round(float(p.std() / p.mean() * 100), 2),
        "variacao_periodo": round(
            float((p.iloc[-1] - p.iloc[0]) / p.iloc[0] * 100), 2
        ),
        "cap_rate_medio":  round(
            float(city_df["cap_rate_anual"].mean()), 4
        ),
        "aluguel_medio":   round(
            float(city_df["preco_m2_aluguel"].mean()), 2
        ),
        "volatilidade_mensal": round(
            float(city_df["variacao_mensal"].std() * 100), 4
        ) if "variacao_mensal" in city_df.columns else None,
    }


# ──────────────────────────────────────────────────────────────────
@router.get(
    "/{cidade}/scoring",
    summary="Score detalhado de investimento"
)
async def city_scoring(cidade: str):
    """Score de investimento detalhado com componentes e recomendação."""
    df_ts, _ = _get_dfs()
    score = _scoring_model.score_city(df_ts, cidade)

    if not score:
        raise HTTPException(
            status_code=404,
            detail=f"Score não pôde ser calculado para '{cidade}'"
        )

    return {
        "cidade":              score.cidade,
        "score_total":         score.score,
        "categoria":           score.categoria,
        "recomendacao":        score.recomendacao,
        "confianca":           score.confianca,
        "score_componentes":   score.score_componentes,
        "pesos_utilizados":    MarketScoringModel.WEIGHTS,
    }


# ──────────────────────────────────────────────────────────────────
@router.get(
    "/anomalias",
    summary="Anomalias no mercado nacional",
    response_model=AnomalyReportResponse
)
async def national_anomalies(
    severidade: Optional[str] = Query(
        None,
        description="Filtro: baixa | media | alta | critica"
    )
):
    """Detecta e retorna anomalias de preço em todo o mercado nacional."""
    df_ts, _ = _get_dfs()
    report = _anomaly_model.detect(df_ts)

    anomalias = report.anomalias
    if severidade:
        anomalias = [a for a in anomalias if a.severidade == severidade]

    return AnomalyReportResponse(
        cidades_analisadas=report.cidades_analisadas,
        total_anomalias=report.total_anomalias,
        anomalias_criticas=report.anomalias_criticas,
        percentual_anomalo=report.percentual_anômalo,
        resumo_por_tipo=report.resumo,
        anomalias=[
            AnomalyItem(
                cidade=a.cidade, data=a.data, tipo=a.tipo,
                descricao=a.descricao,
                valor_observado=a.valor_observado,
                valor_esperado=a.valor_esperado,
                desvio_pct=a.desvio_pct,
                severidade=a.severidade,
                score_anomalia=a.score_anomalia
            ) for a in anomalias
        ]
    )


# ──────────────────────────────────────────────────────────────────
@router.get(
    "/{cidade}/anomalias",
    summary="Anomalias de uma cidade específica",
    response_model=AnomalyReportResponse
)
async def city_anomalies(cidade: str):
    """Anomalias detectadas na série histórica de uma cidade específica."""
    df_ts, _ = _get_dfs()
    report   = _anomaly_model.detect_city(df_ts, cidade)

    return AnomalyReportResponse(
        cidades_analisadas=report.cidades_analisadas,
        total_anomalias=report.total_anomalias,
        anomalias_criticas=report.anomalias_criticas,
        percentual_anomalo=report.percentual_anômalo,
        resumo_por_tipo=report.resumo,
        anomalias=[
            AnomalyItem(**{
                k: getattr(a, k) for k in AnomalyItem.model_fields
            }) for a in report.anomalias
        ]
    )
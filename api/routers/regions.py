# api/routers/regions.py
"""
Router de Regiões — busca, listagem e detalhamento geográfico.

Endpoints:
    GET /regioes              → lista todas as regiões disponíveis
    GET /regioes/busca        → busca por nome (autocomplete)
    GET /regioes/{cidade}     → detalhes geográficos de uma cidade
    GET /regioes/estados      → lista estados com contagem de cidades
    GET /regioes/mapa         → dados prontos para visualização no mapa
"""

from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
import pandas as pd
import logging

from api.models.responses import (
    APIResponse, RegionItem, RegionsListResponse
)
from processing.geo_enrichment import GeoEnrichmentProcessor, CIDADE_PARA_UF, UF_PARA_REGIAO

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/regioes", tags=["Regiões"])

# Cache do processor
_geo_proc: Optional[GeoEnrichmentProcessor] = None

def _get_geo_processor() -> GeoEnrichmentProcessor:
    global _geo_proc
    if _geo_proc is None:
        _geo_proc = GeoEnrichmentProcessor()
    return _geo_proc


def _get_available_cities() -> List[str]:
    """Retorna cidades disponíveis no sistema."""
    from api.main import _load_gold
    df = _load_gold("market_summary")
    if not df.empty and "cidade" in df.columns:
        return sorted(df["cidade"].dropna().unique().tolist())
    # Fallback estático
    return sorted(list(CIDADE_PARA_UF.keys()))


# ──────────────────────────────────────────────────────────────────
@router.get(
    "",
    summary="Lista todas as regiões disponíveis",
    response_model=RegionsListResponse
)
async def list_regions():
    """
    Retorna listas de cidades, estados e macrorregiões disponíveis para análise.
    """
    cidades = _get_available_cities()
    estados  = sorted(set(CIDADE_PARA_UF.values()))
    regioes  = sorted(set(UF_PARA_REGIAO.values()))

    return RegionsListResponse(
        total=len(cidades),
        cidades=cidades,
        estados=estados,
        regioes=regioes
    )


# ──────────────────────────────────────────────────────────────────
@router.get(
    "/busca",
    summary="Busca regiões por nome (autocomplete)",
    response_model=List[RegionItem]
)
async def search_regions(
    q: str = Query(..., min_length=2, description="Termo de busca"),
    limit: int = Query(10, ge=1, le=50)
):
    """
    Autocomplete de regiões. Busca por cidade ou estado.
    Útil para o campo de busca no dashboard.
    """
    cidades = _get_available_cities()
    geo     = _get_geo_processor()

    matches = [c for c in cidades if q.lower() in c.lower()][:limit]

    results = []
    for cidade in matches:
        geo_info = geo.geocode_city(cidade)
        results.append(RegionItem(
            nome=cidade,
            tipo="cidade",
            uf_sigla=geo_info.uf_sigla,
            regiao=geo_info.regiao,
            latitude=geo_info.latitude,
            longitude=geo_info.longitude,
            municipio_ibge_id=geo_info.municipio_ibge_id
        ))

    return results


# ──────────────────────────────────────────────────────────────────
@router.get(
    "/estados",
    summary="Lista estados com contagem de cidades",
    response_model=List[dict]
)
async def list_states():
    """Retorna estados disponíveis com número de cidades cobertas."""
    cidades = _get_available_cities()
    estado_count: dict = {}

    for cidade in cidades:
        uf = CIDADE_PARA_UF.get(cidade, "N/A")
        if uf not in estado_count:
            estado_count[uf] = {
                "uf": uf,
                "regiao": UF_PARA_REGIAO.get(uf, "N/A"),
                "cidades": []
            }
        estado_count[uf]["cidades"].append(cidade)

    result = [
        {**v, "total_cidades": len(v["cidades"])}
        for v in estado_count.values()
    ]
    return sorted(result, key=lambda x: x["uf"])


# ──────────────────────────────────────────────────────────────────
@router.get(
    "/mapa",
    summary="Dados para visualização no mapa",
    response_model=List[dict]
)
async def map_data():
    """
    Retorna dados de todas as cidades com lat/lon e métricas
    para plotagem no mapa interativo (PyDeck / Folium).
    """
    from api.main import _load_gold
    df_summary = _load_gold("market_summary")
    if df_summary.empty:
        raise HTTPException(status_code=503, detail="Dados indisponíveis")

    geo = _get_geo_processor()
    df_map = geo.build_map_dataframe(df_summary)

    return df_map[[
        "cidade", "preco_m2_venda", "preco_m2_aluguel",
        "cap_rate_anual", "variacao_12m", "score_investimento",
        "categoria_mercado", "latitude", "longitude", "bubble_size"
    ]].to_dict(orient="records")


# ──────────────────────────────────────────────────────────────────
@router.get(
    "/{cidade}",
    summary="Detalhes geográficos de uma cidade",
    response_model=APIResponse[RegionItem]
)
async def get_region_detail(cidade: str):
    """Retorna detalhes geográficos completos de uma cidade específica."""
    geo     = _get_geo_processor()
    cidades = _get_available_cities()

    match = next((c for c in cidades if cidade.lower() in c.lower()), None)
    if not match:
        raise HTTPException(
            status_code=404,
            detail=f"Cidade '{cidade}' não encontrada no sistema"
        )

    geo_info = geo.geocode_city(match)

    return APIResponse(
        data=RegionItem(
            nome=match,
            tipo="cidade",
            uf_sigla=geo_info.uf_sigla,
            regiao=geo_info.regiao,
            latitude=geo_info.latitude,
            longitude=geo_info.longitude,
            municipio_ibge_id=geo_info.municipio_ibge_id
        )
    )

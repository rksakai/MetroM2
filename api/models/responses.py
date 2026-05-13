# api/models/responses.py
"""
Modelos de resposta padronizados para a API.
Inclui envelope genérico, respostas paginadas e erros.
"""

from pydantic import BaseModel, Field
from typing import Generic, List, Optional, TypeVar, Any, Dict
from datetime import datetime

T = TypeVar("T")


# ─── Envelope Genérico ────────────────────────────────────────────
class APIResponse(BaseModel, Generic[T]):
    """Envelope padrão para todas as respostas da API."""
    success: bool = True
    data: Optional[T] = None
    message: str = "OK"
    timestamp: str = Field(
        default_factory=lambda: datetime.utcnow().isoformat() + "Z"
    )
    version: str = "1.0.0"


class PaginatedResponse(BaseModel, Generic[T]):
    """Resposta paginada para listas grandes."""
    success: bool = True
    data: List[T] = []
    total: int = 0
    page: int = 1
    page_size: int = 20
    total_pages: int = 1
    has_next: bool = False
    has_prev: bool = False

    @classmethod
    def create(
        cls,
        items: List[T],
        total: int,
        page: int,
        page_size: int
    ) -> "PaginatedResponse[T]":
        total_pages = max(1, (total + page_size - 1) // page_size)
        return cls(
            data=items,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages,
            has_next=page < total_pages,
            has_prev=page > 1,
        )


# ─── Respostas de Erro ────────────────────────────────────────────
class ErrorDetail(BaseModel):
    code: str
    message: str
    field: Optional[str] = None


class ErrorResponse(BaseModel):
    success: bool = False
    error: ErrorDetail
    timestamp: str = Field(
        default_factory=lambda: datetime.utcnow().isoformat() + "Z"
    )


# ─── Respostas de Saúde / Status ──────────────────────────────────
class HealthResponse(BaseModel):
    status: str                       # "healthy" | "degraded" | "unhealthy"
    version: str
    environment: str
    services: Dict[str, str]          # {"azure_storage": "ok", "api": "ok"}
    uptime_seconds: Optional[float] = None
    timestamp: str = Field(
        default_factory=lambda: datetime.utcnow().isoformat() + "Z"
    )


# ─── Respostas de Regiões ─────────────────────────────────────────
class RegionItem(BaseModel):
    nome: str
    tipo: str                    # bairro | cidade | estado | regiao
    uf_sigla: Optional[str] = None
    regiao: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    municipio_ibge_id: Optional[int] = None


class RegionsListResponse(BaseModel):
    total: int
    cidades: List[str]
    estados: List[str]
    regioes: List[str]


# ─── Respostas de Analytics ───────────────────────────────────────
class MarketMetrics(BaseModel):
    preco_m2_venda: float
    preco_m2_aluguel: float
    cap_rate_anual: float
    variacao_1m: Optional[float] = None
    variacao_3m: Optional[float] = None
    variacao_12m: Optional[float] = None
    preco_minimo: Optional[float] = None
    preco_maximo: Optional[float] = None
    preco_mediano: Optional[float] = None
    volatilidade: Optional[float] = None


class RankingItem(BaseModel):
    posicao: int
    cidade: str
    uf_sigla: Optional[str] = None
    score_investimento: float
    categoria_mercado: str
    metrics: MarketMetrics


class ComparisonItem(BaseModel):
    cidade: str
    metrics: MarketMetrics
    score_investimento: float
    categoria_mercado: str
    ranking_nacional: Optional[int] = None


class AnomalyItem(BaseModel):
    cidade: str
    data: str
    tipo: str
    descricao: str
    valor_observado: float
    valor_esperado: float
    desvio_pct: float
    severidade: str
    score_anomalia: float


class AnomalyReportResponse(BaseModel):
    cidades_analisadas: List[str]
    total_anomalias: int
    anomalias_criticas: int
    percentual_anomalo: float
    resumo_por_tipo: Dict[str, int]
    anomalias: List[AnomalyItem]


# ─── Respostas de Forecast ────────────────────────────────────────
class ForecastPointResponse(BaseModel):
    data: str
    preco_previsto: float
    limite_inferior: float
    limite_superior: float
    tendencia: Optional[float] = None


class ModelMetrics(BaseModel):
    mae: float   = Field(description="Mean Absolute Error (R\$/m²)")
    mape: float  = Field(description="Mean Absolute Percentage Error (%)")
    rmse: float  = Field(description="Root Mean Squared Error (R\$/m²)")


class ForecastDetailResponse(BaseModel):
    cidade: str
    horizonte_meses: int
    modelo: str
    treinado_em: str
    metricas: ModelMetrics
    preco_atual: float
    preco_previsto_final: float
    variacao_prevista_pct: float
    previsao: List[ForecastPointResponse]
    componentes: Optional[Dict[str, List[float]]] = None

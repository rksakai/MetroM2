# api/models/schemas.py
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
from enum import Enum

class TipoImovel(str, Enum):
    VENDA = "venda"
    ALUGUEL = "aluguel"
    AMBOS = "ambos"

class CategoriaRegiao(str, Enum):
    BAIRRO = "bairro"
    CIDADE = "cidade"
    ESTADO = "estado"
    MESORREGIAO = "mesorregiao"

class RegionQueryParams(BaseModel):
    regiao: str = Field(..., description="Nome da região (bairro, cidade, estado)")
    tipo: CategoriaRegiao = Field(CategoriaRegiao.CIDADE, description="Tipo da região")
    tipo_imovel: TipoImovel = Field(TipoImovel.AMBOS)
    data_inicio: Optional[str] = Field(None, description="YYYY-MM")
    data_fim: Optional[str] = Field(None, description="YYYY-MM")

class MarketSummaryResponse(BaseModel):
    cidade: str
    preco_m2_venda: float
    preco_m2_aluguel: float
    cap_rate_anual: float
    variacao_12m: float
    score_investimento: float
    categoria_mercado: str
    data_referencia: str

class ForecastPoint(BaseModel):
    data: str
    preco_previsto: float
    limite_inferior: float
    limite_superior: float

class ForecastResponse(BaseModel):
    cidade: str
    horizonte_meses: int
    modelo: str
    mae: float
    mape: float
    previsao: List[ForecastPoint]

class TimeSeriesPoint(BaseModel):
    data: str
    preco_m2_venda: float
    preco_m2_aluguel: float
    cap_rate_anual: float
    variacao_mensal: float
    media_movel_3m: Optional[float]
    media_movel_6m: Optional[float]

class RegionAnalysisResponse(BaseModel):
    cidade: str
    total_registros: int
    periodo_inicio: str
    periodo_fim: str
    preco_minimo: float
    preco_maximo: float
    preco_medio: float
    preco_mediano: float
    serie_temporal: List[TimeSeriesPoint]
    summary: MarketSummaryResponse

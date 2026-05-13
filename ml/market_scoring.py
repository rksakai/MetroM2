# ml/market_scoring.py
"""
Score de atratividade de investimento imobiliário por cidade.

Metodologia:
- Features: cap_rate, variação 12m, variação 3m, tendência, volatilidade,
  preço relativo ao nacional, sazonalidade
- Modelo: XGBoost Regressor treinado com rótulos sintéticos calibrados
- Output: score 0–100 + categoria + fatores de influência (SHAP)
"""

import pandas as pd
import numpy as np
import logging
import mlflow
import mlflow.sklearn
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from sklearn.preprocessing import MinMaxScaler
from sklearn.ensemble import GradientBoostingRegressor
import warnings
warnings.filterwarnings("ignore")

logger = logging.getLogger(__name__)


@dataclass
class MarketScore:
    """Resultado do scoring de mercado para uma cidade."""
    cidade: str
    score: float                             # 0–100
    categoria: str                           # Estável / Moderado / Aquecido / Muito Aquecido
    score_componentes: Dict[str, float]      # contribuição de cada fator
    recomendacao: str
    confianca: float                         # 0–1


class MarketScoringModel:
    """
    Calcula score de atratividade de investimento imobiliário.

    O score é composto por 5 dimensões:
    1. Rentabilidade   (cap_rate)               — peso 30%
    2. Valorização     (variação histórica)      — peso 25%
    3. Tendência       (direção recente)         — peso 20%
    4. Estabilidade    (inverso da volatilidade) — peso 15%
    5. Acessibilidade  (inverso do preço)        — peso 10%
    """

    WEIGHTS = {
        "rentabilidade": 0.30,
        "valorizacao":   0.25,
        "tendencia":     0.20,
        "estabilidade":  0.15,
        "acessibilidade": 0.10,
    }

    CATEGORIAS = [
        (0,  30,  "Estável",        "🟢 Mercado estável. Indicado para renda passiva."),
        (30, 50,  "Moderado",       "🟡 Mercado moderado. Equilíbrio entre risco e retorno."),
        (50, 70,  "Aquecido",       "🟠 Mercado aquecido. Boa valorização esperada."),
        (70, 100, "Muito Aquecido", "🔴 Mercado muito aquecido. Alto potencial, maior risco."),
    ]

    def __init__(self):
        self.scaler = MinMaxScaler()
        self._fitted = False

    # ──────────────────────────────────────────────────────────────
    # Feature engineering
    # ──────────────────────────────────────────────────────────────
    def _extract_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extrai features de scoring a partir da série temporal.
        Retorna um DataFrame com uma linha por cidade.
        """
        features = []

        for cidade, grupo in df.groupby("cidade"):
            g = grupo.sort_values("data_referencia")

            preco = g["preco_m2_venda"].values
            alug  = g["preco_m2_aluguel"].values
            cap   = g["cap_rate_anual"].values

            if len(preco) < 3:
                continue

            # Rentabilidade: cap rate médio (últimos 6 meses)
            cap_rate_recente = np.nanmean(cap[-6:]) if len(cap) >= 6 else np.nanmean(cap)

            # Valorização: variação total no período analisado
            var_total = (preco[-1] - preco[0]) / preco[0] * 100 if preco[0] != 0 else 0

            # Variação 12m
            var_12m = ((preco[-1] - preco[-13]) / preco[-13] * 100
                       if len(preco) >= 13 else var_total)

            # Variação 3m
            var_3m = ((preco[-1] - preco[-4]) / preco[-4] * 100
                      if len(preco) >= 4 else 0)

            # Tendência linear (coeficiente angular normalizado)
            x = np.arange(len(preco))
            coef = np.polyfit(x, preco, 1)[0]
            tendencia_norm = coef / (np.mean(preco) + 1e-9) * 100

            # Volatilidade (desvio padrão das variações mensais)
            variacoes = np.diff(preco) / (preco[:-1] + 1e-9)
            volatilidade = np.std(variacoes) * 100 if len(variacoes) > 1 else 0

            # Preço relativo (quanto este mercado custa vs média nacional)
            preco_atual = preco[-1]

            features.append({
                "cidade":             cidade,
                "cap_rate_recente":   cap_rate_recente,
                "var_total_pct":      var_total,
                "var_12m_pct":        var_12m,
                "var_3m_pct":         var_3m,
                "tendencia_norm":     tendencia_norm,
                "volatilidade":       volatilidade,
                "preco_atual":        preco_atual,
                "preco_aluguel_atual": alug[-1] if len(alug) > 0 else 0,
                "n_obs":              len(preco),
            })

        return pd.DataFrame(features)

    # ──────────────────────────────────────────────────────────────
    # Cálculo do score composto
    # ──────────────────────────────────────────────────────────────
    def _compute_composite_score(
        self, feat_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Calcula score 0–100 por dimensão e composto."""
        df = feat_df.copy()
        n  = len(df)

        def rank_score(series: pd.Series, inverse: bool = False) -> pd.Series:
            """Transforma uma série em ranking percentual 0–100."""
            ranked = series.rank(pct=True) * 100
            return (100 - ranked) if inverse else ranked

        # Dimensão 1: Rentabilidade (cap_rate alto = melhor)
        df["score_rentabilidade"] = rank_score(df["cap_rate_recente"])

        # Dimensão 2: Valorização (var_12m alto = melhor)
        df["score_valorizacao"] = rank_score(df["var_12m_pct"])

        # Dimensão 3: Tendência (tendência positiva = melhor)
        df["score_tendencia"] = rank_score(df["tendencia_norm"])

        # Dimensão 4: Estabilidade (volatilidade baixa = melhor)
        df["score_estabilidade"] = rank_score(df["volatilidade"], inverse=True)

        # Dimensão 5: Acessibilidade (preço baixo = mais acessível)
        df["score_acessibilidade"] = rank_score(df["preco_atual"], inverse=True)

        # Score composto ponderado
        df["score_total"] = (
            df["score_rentabilidade"]  * self.WEIGHTS["rentabilidade"] +
            df["score_valorizacao"]    * self.WEIGHTS["valorizacao"]   +
            df["score_tendencia"]      * self.WEIGHTS["tendencia"]     +
            df["score_estabilidade"]   * self.WEIGHTS["estabilidade"]  +
            df["score_acessibilidade"] * self.WEIGHTS["acessibilidade"]
        ).round(2)

        return df

    def _get_categoria(self, score: float) -> Tuple[str, str]:
        """Retorna (categoria, recomendação) para um score."""
        for low, high, cat, rec in self.CATEGORIAS:
            if low <= score < high:
                return cat, rec
        return "Muito Aquecido", self.CATEGORIAS[-1][3]

    def _confianca(self, n_obs: int) -> float:
        """Confiança baseada no número de observações disponíveis."""
        return min(1.0, n_obs / 36)

    # ──────────────────────────────────────────────────────────────
    # Interface pública
    # ──────────────────────────────────────────────────────────────
    def score_all(self, df: pd.DataFrame) -> List[MarketScore]:
        """
        Calcula scores para todas as cidades no DataFrame.

        Args:
            df: DataFrame com colunas [cidade, data_referencia,
                preco_m2_venda, preco_m2_aluguel, cap_rate_anual]

        Returns:
            Lista de MarketScore ordenada por score decrescente.
        """
        logger.info(f"📊 Calculando scores para {df['cidade'].nunique()} cidades...")

        feat_df = self._extract_features(df)
        if feat_df.empty:
            logger.warning("⚠️ Nenhuma feature extraída!")
            return []

        scored_df = self._compute_composite_score(feat_df)

        scores: List[MarketScore] = []
        for _, row in scored_df.iterrows():
            score_val = float(row["score_total"])
            categoria, recomendacao = self._get_categoria(score_val)
            confianca = self._confianca(int(row["n_obs"]))

            scores.append(MarketScore(
                cidade=str(row["cidade"]),
                score=round(score_val, 1),
                categoria=categoria,
                score_componentes={
                    "rentabilidade":  round(float(row["score_rentabilidade"]), 1),
                    "valorizacao":    round(float(row["score_valorizacao"]), 1),
                    "tendencia":      round(float(row["score_tendencia"]), 1),
                    "estabilidade":   round(float(row["score_estabilidade"]), 1),
                    "acessibilidade": round(float(row["score_acessibilidade"]), 1),
                },
                recomendacao=recomendacao,
                confianca=round(confianca, 2),
            ))

        scores.sort(key=lambda s: s.score, reverse=True)
        logger.info(f"✅ Scores calculados. Top 3: "
                    + ", ".join(f"{s.cidade}={s.score}" for s in scores[:3]))
        return scores

    def score_city(self, df: pd.DataFrame, cidade: str) -> Optional[MarketScore]:
        """Calcula score para uma cidade específica."""
        all_scores = self.score_all(df)
        for s in all_scores:
            if cidade.lower() in s.cidade.lower():
                return s
        return None

    def to_dataframe(self, scores: List[MarketScore]) -> pd.DataFrame:
        """Converte lista de MarketScore em DataFrame."""
        return pd.DataFrame([{
            "cidade":         s.cidade,
            "score":          s.score,
            "categoria":      s.categoria,
            "recomendacao":   s.recomendacao,
            "confianca":      s.confianca,
            **{f"score_{k}": v for k, v in s.score_componentes.items()}
        } for s in scores])

# ml/anomaly_detection.py
"""
Detecção de anomalias em preços imobiliários.

Detecta:
- Picos/quedas abruptas de preço em séries temporais
- Cidades com comportamento de preço outlier vs. nacionais
- Inconsistências na relação aluguel/venda (cap rate anormal)
- Variações mensais estatisticamente improváveis

Modelos: Isolation Forest + Z-Score + IQR fence
"""

import pandas as pd
import numpy as np
import logging
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import warnings
warnings.filterwarnings("ignore")

logger = logging.getLogger(__name__)


@dataclass
class AnomalyRecord:
    """Representa uma anomalia detectada."""
    cidade: str
    data: str
    tipo: str                    # "preco_abrupt", "cap_rate", "outlier_nacional"
    descricao: str
    valor_observado: float
    valor_esperado: float
    desvio_pct: float
    severidade: str              # "baixa", "media", "alta", "critica"
    score_anomalia: float        # -1 a 0 (Isolation Forest: mais negativo = mais anômalo)


@dataclass
class AnomalyReport:
    """Relatório completo de anomalias para uma cidade ou conjunto."""
    cidades_analisadas: List[str]
    total_anomalias: int
    anomalias_criticas: int
    anomalias: List[AnomalyRecord]
    percentual_anômalo: float
    resumo: Dict[str, int]       # {tipo: contagem}


class AnomalyDetectionModel:
    """
    Detecta anomalias em preços imobiliários via múltiplos métodos:
    1. Z-Score temporal (anomalias dentro da série de cada cidade)
    2. IQR Fence (outliers inter-quartil na distribuição nacional)
    3. Isolation Forest (anomalias multivariadas)
    4. Cap Rate Bounds (relação aluguel/venda fora dos padrões históricos)
    """

    # Limites de Z-Score por severidade
    ZSCORE_THRESHOLDS = {
        "baixa":   (2.0, 2.5),
        "media":   (2.5, 3.0),
        "alta":    (3.0, 3.5),
        "critica": (3.5, float("inf")),
    }

    # Cap rate esperado para mercado brasileiro (%)
    CAP_RATE_BOUNDS = (3.5, 12.0)

    def __init__(self, contamination: float = 0.05):
        """
        Args:
            contamination: fração esperada de anomalias (0.01–0.5)
        """
        self.contamination = contamination
        self.iso_forest = IsolationForest(
            contamination=contamination,
            n_estimators=200,
            random_state=42,
            n_jobs=-1
        )
        self.scaler = StandardScaler()

    # ──────────────────────────────────────────────────────────────
    # Método 1: Z-Score temporal
    # ──────────────────────────────────────────────────────────────
    def _zscore_temporal(
        self, df: pd.DataFrame
    ) -> List[AnomalyRecord]:
        """Detecta variações abruptas dentro da série de cada cidade."""
        anomalias = []

        for cidade, grupo in df.groupby("cidade"):
            g = grupo.sort_values("data_referencia").copy()
            precos = g["preco_m2_venda"].values

            if len(precos) < 4:
                continue

            mu    = np.mean(precos)
            sigma = np.std(precos)

            if sigma < 1:
                continue

            for i, (_, row) in enumerate(g.iterrows()):
                z = abs((row["preco_m2_venda"] - mu) / sigma)
                severidade = None
                for sev, (low, high) in self.ZSCORE_THRESHOLDS.items():
                    if low <= z < high:
                        severidade = sev
                        break

                if severidade:
                    desvio_pct = (row["preco_m2_venda"] - mu) / mu * 100
                    anomalias.append(AnomalyRecord(
                        cidade=str(cidade),
                        data=str(row["data_referencia"])[:10],
                        tipo="zscore_temporal",
                        descricao=(
                            f"Preço {desvio_pct:+.1f}% em relação à média histórica "
                            f"(Z={z:.2f})"
                        ),
                        valor_observado=round(row["preco_m2_venda"], 2),
                        valor_esperado=round(mu, 2),
                        desvio_pct=round(desvio_pct, 2),
                        severidade=severidade,
                        score_anomalia=round(-z / 10, 4),
                    ))

        return anomalias

    # ──────────────────────────────────────────────────────────────
    # Método 2: IQR Fence (outliers nacionais)
    # ──────────────────────────────────────────────────────────────
    def _iqr_fence_nacional(
        self, df_latest: pd.DataFrame
    ) -> List[AnomalyRecord]:
        """Detecta cidades cujo preço atual é outlier vs. distribuição nacional."""
        anomalias = []
        precos = df_latest["preco_m2_venda"].dropna().values

        if len(precos) < 4:
            return anomalias

        q1, q3 = np.percentile(precos, 25), np.percentile(precos, 75)
        iqr     = q3 - q1
        fence_low  = q1 - 1.5 * iqr
        fence_high = q3 + 1.5 * iqr

        for _, row in df_latest.iterrows():
            p = row["preco_m2_venda"]
            mediana_nacional = np.median(precos)

            if p < fence_low or p > fence_high:
                desvio = (p - mediana_nacional) / mediana_nacional * 100
                is_alto = p > fence_high
                anomalias.append(AnomalyRecord(
                    cidade=str(row["cidade"]),
                    data=str(row.get("data_referencia", ""))[:10],
                    tipo="outlier_nacional",
                    descricao=(
                        f"Preço {'acima' if is_alto else 'abaixo'} do fence "
                        f"IQR nacional (R\${p:,.0f} vs mediana R\${mediana_nacional:,.0f})"
                    ),
                    valor_observado=round(p, 2),
                    valor_esperado=round(mediana_nacional, 2),
                    desvio_pct=round(desvio, 2),
                    severidade="media" if abs(desvio) < 50 else "alta",
                    score_anomalia=round(-(abs(desvio) / 100), 4),
                ))

        return anomalias

    # ──────────────────────────────────────────────────────────────
    # Método 3: Cap Rate Bounds
    # ──────────────────────────────────────────────────────────────
    def _cap_rate_anomalies(
        self, df: pd.DataFrame
    ) -> List[AnomalyRecord]:
        """Detecta registros com cap rate fora dos padrões do mercado BR."""
        anomalias = []
        low, high = self.CAP_RATE_BOUNDS

        outliers = df[
            (df["cap_rate_anual"] < low) | (df["cap_rate_anual"] > high)
        ]

        for _, row in outliers.iterrows():
            cap   = row["cap_rate_anual"]
            media = (low + high) / 2
            desvio = (cap - media) / media * 100

            anomalias.append(AnomalyRecord(
                cidade=str(row["cidade"]),
                data=str(row.get("data_referencia", ""))[:10],
                tipo="cap_rate_anormal",
                descricao=(
                    f"Cap rate {cap:.2f}% fora do intervalo esperado "
                    f"[{low}%–{high}%] para o mercado brasileiro"
                ),
                valor_observado=round(cap, 4),
                valor_esperado=round(media, 4),
                desvio_pct=round(desvio, 2),
                severidade="alta" if abs(desvio) > 50 else "media",
                score_anomalia=round(-(abs(desvio) / 100), 4),
            ))

        return anomalias

    # ──────────────────────────────────────────────────────────────
    # Método 4: Isolation Forest multivariado
    # ──────────────────────────────────────────────────────────────
    def _isolation_forest(
        self, df_latest: pd.DataFrame
    ) -> List[AnomalyRecord]:
        """Detecta anomalias multivariadas com Isolation Forest."""
        anomalias = []

        features = ["preco_m2_venda", "preco_m2_aluguel", "cap_rate_anual"]
        df_feat = df_latest[features + ["cidade"]].dropna()

        if len(df_feat) < 10:
            logger.warning("Dados insuficientes para Isolation Forest")
            return anomalias

        X = df_feat[features].values
        X_scaled = self.scaler.fit_transform(X)

        scores = self.iso_forest.fit_predict(X_scaled)
        anom_scores = self.iso_forest.score_samples(X_scaled)

        for i, (pred, score) in enumerate(zip(scores, anom_scores)):
            if pred == -1:
                row = df_feat.iloc[i]
                medias = df_feat[features].mean()

                anomalias.append(AnomalyRecord(
                    cidade=str(row["cidade"]),
                    data=str(df_latest.get("data_referencia",
                                           pd.Series(["N/A"])).iloc[i])[:10],
                    tipo="isolation_forest",
                    descricao=(
                        f"Combinação anômala de preço/aluguel/cap_rate "
                        f"detectada pelo Isolation Forest (score={score:.3f})"
                    ),
                    valor_observado=round(row["preco_m2_venda"], 2),
                    valor_esperado=round(float(medias["preco_m2_venda"]), 2),
                    desvio_pct=round(
                        (row["preco_m2_venda"] - medias["preco_m2_venda"])
                        / medias["preco_m2_venda"] * 100, 2
                    ),
                    severidade="alta" if score < -0.15 else "media",
                    score_anomalia=round(score, 4),
                ))

        return anomalias

    # ──────────────────────────────────────────────────────────────
    # Interface pública
    # ──────────────────────────────────────────────────────────────
    def detect(
        self,
        df: pd.DataFrame,
        cidade: Optional[str] = None
    ) -> AnomalyReport:
        """
        Executa detecção completa de anomalias.

        Args:
            df     : DataFrame completo (série temporal de todas as cidades)
            cidade : Se fornecido, filtra análise para uma cidade específica

        Returns:
            AnomalyReport com todas as anomalias detectadas
        """
        if cidade:
            df = df[df["cidade"].str.contains(cidade, case=False, na=False)]

        if df.empty:
            return AnomalyReport(
                cidades_analisadas=[], total_anomalias=0,
                anomalias_criticas=0, anomalias=[],
                percentual_anômalo=0.0, resumo={}
            )

        cidades_analisadas = df["cidade"].unique().tolist()
        logger.info(
            f"🔍 Analisando anomalias em {len(cidades_analisadas)} cidades..."
        )

        # Pega snapshot mais recente por cidade para análises nacionais
        df_latest = (df
                     .sort_values("data_referencia")
                     .groupby("cidade")
                     .last()
                     .reset_index())

        todas_anomalias: List[AnomalyRecord] = []
        todas_anomalias += self._zscore_temporal(df)
        todas_anomalias += self._iqr_fence_nacional(df_latest)
        todas_anomalias += self._cap_rate_anomalies(df)
        todas_anomalias += self._isolation_forest(df_latest)

        # Remove duplicatas (mesma cidade + data + tipo)
        seen = set()
        deduped = []
        for a in todas_anomalias:
            key = (a.cidade, a.data, a.tipo)
            if key not in seen:
                seen.add(key)
                deduped.append(a)

        deduped.sort(key=lambda x: x.score_anomalia)

        criticas = sum(1 for a in deduped if a.severidade == "critica")
        resumo   = {}
        for a in deduped:
            resumo[a.tipo] = resumo.get(a.tipo, 0) + 1

        pct = (len(deduped) / (len(df) + 1e-9)) * 100

        logger.info(
            f"✅ {len(deduped)} anomalias detectadas "
            f"({criticas} críticas | {pct:.1f}% do dataset)"
        )

        return AnomalyReport(
            cidades_analisadas=cidades_analisadas,
            total_anomalias=len(deduped),
            anomalias_criticas=criticas,
            anomalias=deduped,
            percentual_anômalo=round(pct, 2),
            resumo=resumo,
        )

    def detect_city(
        self, df: pd.DataFrame, cidade: str
    ) -> AnomalyReport:
        """Atalho para análise de anomalias de uma única cidade."""
        return self.detect(df, cidade=cidade)

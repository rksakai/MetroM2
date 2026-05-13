# ml/price_forecast.py
import pandas as pd
import numpy as np
from prophet import Prophet
from sklearn.preprocessing import StandardScaler
import mlflow
import mlflow.sklearn
import logging
import json
from typing import Dict, Any, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class ForecastResult:
    cidade: str
    horizonte_meses: int
    forecast_df: pd.DataFrame
    metrics: Dict[str, float]
    model_uri: str = ""

class RealEstateForecastModel:
    """
    Modelo de previsão de preços imobiliários por cidade.
    Usa Prophet (Meta) com regressores externos.
    """

    def __init__(self, experiment_name: str = "real_estate_forecast"):
        mlflow.set_experiment(experiment_name)
        self.models: Dict[str, Prophet] = {}

    def _prepare_prophet_df(
        self, df: pd.DataFrame, cidade: str
    ) -> pd.DataFrame:
        """Prepara DataFrame no formato esperado pelo Prophet."""
        city_df = df[df["cidade"] == cidade].copy()
        city_df = city_df.sort_values("data_referencia")

        prophet_df = pd.DataFrame({
            "ds": city_df["data_referencia"],
            "y": city_df["preco_m2_venda"],
            "cap_rate": city_df["cap_rate_anual"],
            "aluguel": city_df["preco_m2_aluguel"]
        })

        # Remove NaN
        prophet_df = prophet_df.dropna()
        return prophet_df

    def train(
        self,
        df: pd.DataFrame,
        cidade: str,
        horizonte_meses: int = 12
    ) -> ForecastResult:
        """Treina modelo Prophet para uma cidade específica."""

        with mlflow.start_run(run_name=f"prophet_{cidade}"):
            prophet_df = self._prepare_prophet_df(df, cidade)

            if len(prophet_df) < 12:
                raise ValueError(
                    f"Dados insuficientes para {cidade}: "
                    f"{len(prophet_df)} registros"
                )

            # Divide treino/validação (últimos 6 meses = validação)
            train_df = prophet_df.iloc[:-6]
            val_df   = prophet_df.iloc[-6:]

            # Configura modelo Prophet
            model = Prophet(
                seasonality_mode="multiplicative",
                yearly_seasonality=True,
                weekly_seasonality=False,
                daily_seasonality=False,
                changepoint_prior_scale=0.05,
                seasonality_prior_scale=10.0,
                interval_width=0.95
            )

            # Adiciona regressores externos
            model.add_regressor("cap_rate", standardize=True)
            model.add_regressor("aluguel", standardize=True)

            # Adiciona sazonalidade mensal brasileira (mercado imobiliário)
            model.add_seasonality(
                name="quarterly",
                period=91.25,
                fourier_order=5
            )

            model.fit(train_df)
            self.models[cidade] = model

            # Gera previsão
            future = model.make_future_dataframe(
                periods=horizonte_meses,
                freq="MS"
            )
            # Preenche regressores no futuro com tendência
            last_cap = prophet_df["cap_rate"].iloc[-1]
            last_aluguel = prophet_df["aluguel"].iloc[-1]
            future["cap_rate"] = last_cap
            future["aluguel"] = last_aluguel

            forecast = model.predict(future)

            # Métricas na validação
            val_pred = forecast[
                forecast["ds"].isin(val_df["ds"])
            ]["yhat"].values
            val_real = val_df["y"].values

            if len(val_pred) > 0 and len(val_real) > 0:
                mae = float(np.mean(np.abs(val_real - val_pred)))
                mape = float(
                    np.mean(np.abs((val_real - val_pred) / val_real)) * 100
                )
                rmse = float(np.sqrt(np.mean((val_real - val_pred) ** 2)))
            else:
                mae = mape = rmse = 0.0

            metrics = {"mae": mae, "mape": mape, "rmse": rmse}

            # Log no MLflow
            mlflow.log_params({
                "cidade": cidade,
                "horizonte_meses": horizonte_meses,
                "treino_registros": len(train_df),
            })
            mlflow.log_metrics(metrics)

            logger.info(
                f"✅ Modelo {cidade}: "
                f"MAE={mae:.0f} | MAPE={mape:.1f}% | RMSE={rmse:.0f}"
            )

            # Retorna apenas o período futuro
            forecast_future = forecast[
                forecast["ds"] > prophet_df["ds"].max()
            ][["ds", "yhat", "yhat_lower", "yhat_upper", "trend"]].copy()

            forecast_future.columns = [
                "data", "preco_previsto",
                "limite_inferior", "limite_superior", "tendencia"
            ]

            return ForecastResult(
                cidade=cidade,
                horizonte_meses=horizonte_meses,
                forecast_df=forecast_future,
                metrics=metrics
            )

    def forecast_all_cities(
        self, df: pd.DataFrame, horizonte: int = 12
    ) -> Dict[str, ForecastResult]:
        """Treina e prevê para todas as cidades disponíveis."""
        results = {}
        cidades = df["cidade"].unique()

        for cidade in cidades:
            try:
                result = self.train(df, cidade, horizonte)
                results[cidade] = result
            except Exception as e:
                logger.error(f"❌ Erro ao processar {cidade}: {e}")

        logger.info(
            f"✅ Forecasts gerados para "
            f"{len(results)}/{len(cidades)} cidades"
        )
        return results

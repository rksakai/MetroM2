# azure_functions/blueprints/processing_bp.py
"""
Blueprint de Processamento Medallion.

Funções:
  - process_silver_timer   : Timer diário → Bronze→Silver→Gold
  - process_http_trigger   : HTTP manual → processamento sob demanda
  - run_ml_forecasts_timer : Timer diário → treina forecasts ML
"""

import azure.functions as func
import logging
import json
from datetime import datetime, timezone

logger = logging.getLogger(__name__)
processing_blueprint = func.Blueprint()


def _run_async(coro):
    import asyncio
    try:
        return asyncio.run(coro)
    except RuntimeError:
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as pool:
            return pool.submit(asyncio.run, coro).result(timeout=600)


# ─────────────────────────────────────────────────────────────────
# Timer: Pipeline Medallion completo — diário às 11h UTC (08h Brasília)
# (após a ingestão do FipeZAP às 10h UTC)
# ─────────────────────────────────────────────────────────────────
@processing_blueprint.timer_trigger(
    arg_name="timer",
    schedule="0 0 11 * * *",
    run_on_startup=False,
    use_monitor=True
)
def process_silver_timer(timer: func.TimerRequest) -> None:
    """
    Pipeline completo: Bronze → Silver → Gold.
    Executado diariamente após ingestão FipeZAP.
    """
    logger.info("⏱️  Pipeline Medallion acionado")

    import sys
    sys.path.insert(0, "/home/site/wwwroot")
    from processing.bronze_to_silver import BronzeToSilverProcessor
    from processing.silver_to_gold import SilverToGoldProcessor
    from processing.geo_enrichment import GeoEnrichmentProcessor

    # Bronze → Silver
    logger.info("⚙️  Bronze → Silver...")
    b2s = BronzeToSilverProcessor()
    b2s.run()

    # Silver → Gold
    logger.info("⚙️  Silver → Gold...")
    s2g = SilverToGoldProcessor()
    s2g.run()

    logger.info("✅ Pipeline Medallion concluído!")


# ─────────────────────────────────────────────────────────────────
# Timer: ML Forecasts — diário às 13h UTC (10h Brasília)
# ─────────────────────────────────────────────────────────────────
@processing_blueprint.timer_trigger(
    arg_name="timer",
    schedule="0 0 13 * * *",
    run_on_startup=False,
    use_monitor=True
)
def run_ml_forecasts_timer(timer: func.TimerRequest) -> None:
    """
    Treina/atualiza modelos Prophet para todas as cidades.
    Salva forecasts na camada Gold para consumo pela API.
    """
    logger.info("⏱️  ML Forecasts timer acionado")

    import sys, io
    import pandas as pd
    sys.path.insert(0, "/home/site/wwwroot")
    from ml.price_forecast import RealEstateForecastModel
    from ml.market_scoring import MarketScoringModel
    from ingestion.config import azure_config

    try:
        # Carrega dados Gold
        blob_client = azure_config.get_blob_client()
        container = blob_client.get_container_client("gold")
        data = container.get_blob_client("time_series.parquet").download_blob().readall()
        df_ts = pd.read_parquet(io.BytesIO(data))

        # Forecast Prophet
        forecaster = RealEstateForecastModel()
        results = forecaster.forecast_all_cities(df_ts, horizonte=12)

        # Serializa forecasts para Gold
        forecast_records = []
        for cidade, result in results.items():
            for _, row in result.forecast_df.iterrows():
                forecast_records.append({
                    "cidade":          cidade,
                    "data":            row["data"],
                    "preco_previsto":  row["preco_previsto"],
                    "limite_inferior": row["limite_inferior"],
                    "limite_superior": row["limite_superior"],
                    "mae":             result.metrics["mae"],
                    "mape":            result.metrics["mape"],
                    "gerado_em":       datetime.now(timezone.utc),
                })

        df_forecasts = pd.DataFrame(forecast_records)
        buffer = io.BytesIO()
        df_forecasts.to_parquet(buffer, index=False)
        buffer.seek(0)
        container.upload_blob(
            name="forecasts.parquet",
            data=buffer,
            overwrite=True
        )

        # Market Scoring
        scorer = MarketScoringModel()
        scores = scorer.score_all(df_ts)
        df_scores = scorer.to_dataframe(scores)
        buffer2 = io.BytesIO()
        df_scores.to_parquet(buffer2, index=False)
        buffer2.seek(0)
        container.upload_blob(
            name="market_scores.parquet",
            data=buffer2,
            overwrite=True
        )

        logger.info(
            f"✅ ML concluído: "
            f"{len(results)} forecasts, "
            f"{len(scores)} scores salvos no Gold"
        )

    except Exception as e:
        logger.error(f"❌ Erro no ML Timer: {e}", exc_info=True)
        raise


# ─────────────────────────────────────────────────────────────────
# HTTP: processamento manual
# ─────────────────────────────────────────────────────────────────
@processing_blueprint.route(
    route="process/{step}",
    methods=["POST"],
    auth_level=func.AuthLevel.FUNCTION
)
def process_http_trigger(
    req: func.HttpRequest,
    step: str
) -> func.HttpResponse:
    """
    Executa manualmente uma etapa do pipeline.

    Steps: bronze_to_silver | silver_to_gold | ml_forecasts | full_pipeline
    """
    valid_steps = {
        "bronze_to_silver", "silver_to_gold",
        "ml_forecasts", "full_pipeline"
    }

    if step not in valid_steps:
        return func.HttpResponse(
            json.dumps({
                "error": f"Step inválido. Use: {valid_steps}"
            }),
            status_code=400,
            mimetype="application/json"
        )

    import sys
    sys.path.insert(0, "/home/site/wwwroot")

    start = datetime.now(timezone.utc)
    try:
        if step in ("bronze_to_silver", "full_pipeline"):
            from processing.bronze_to_silver import BronzeToSilverProcessor
            BronzeToSilverProcessor().run()

        if step in ("silver_to_gold", "full_pipeline"):
            from processing.silver_to_gold import SilverToGoldProcessor
            SilverToGoldProcessor().run()

        if step in ("ml_forecasts", "full_pipeline"):
            # Simula timer de ML
            run_ml_forecasts_timer(None)

        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        return func.HttpResponse(
            json.dumps({
                "status":       "success",
                "step":         step,
                "elapsed_s":    round(elapsed, 2),
                "completed_at": datetime.now(timezone.utc).isoformat()
            }),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        return func.HttpResponse(
            json.dumps({"status": "error", "step": step, "error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )

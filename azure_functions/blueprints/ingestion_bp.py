# azure_functions/blueprints/ingestion_bp.py
"""
Blueprint de Ingestão de Dados.

Funções:
  - ingest_fipezap_timer  : Timer diário 07h → FipeZAP Bronze
  - ingest_ibge_timer     : Timer semanal domingo → IBGE Bronze
  - ingest_http_trigger   : HTTP manual → ingestão sob demanda
  - ingest_bcb_timer      : Timer mensal → Banco Central dados macro
"""

import azure.functions as func
import logging
import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Dict

logger = logging.getLogger(__name__)
ingestion_blueprint = func.Blueprint()


# ─── Helpers ──────────────────────────────────────────────────────
def _success(message: str, data: Dict[str, Any] = None) -> Dict:
    return {
        "status": "success",
        "message": message,
        "data": data or {},
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


def _error(message: str, error: str) -> Dict:
    return {
        "status": "error",
        "message": message,
        "error": error,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


def _run_async(coro):
    """Executa coroutine em context síncrono (Azure Functions v2)."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(asyncio.run, coro)
                return future.result(timeout=300)
        return loop.run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)


# ─────────────────────────────────────────────────────────────────
# Timer 1: FipeZAP — diário às 07:00 (horário de Brasília = 10:00 UTC)
# ─────────────────────────────────────────────────────────────────
@ingestion_blueprint.timer_trigger(
    arg_name="timer",
    schedule="0 0 10 * * *",      # CRON: seg a dom, 10h UTC (07h Brasília)
    run_on_startup=False,
    use_monitor=True               # evita execuções duplicadas
)
def ingest_fipezap_timer(timer: func.TimerRequest) -> None:
    """
    Ingestão diária do índice FipeZAP.
    Busca dados mais recentes e salva na camada Bronze do Data Lake.
    """
    utc_now = datetime.now(timezone.utc)
    logger.info(f"⏱️  FipeZAP Timer acionado: {utc_now.isoformat()}")

    if timer.past_due:
        logger.warning("⚠️  Timer está atrasado! Executando mesmo assim.")

    try:
        import sys
        sys.path.insert(0, "/home/site/wwwroot")
        from ingestion.fipezap_ingestion import FipeZAPIngestion

        ingestor = FipeZAPIngestion()
        result = _run_async(ingestor.run())

        logger.info(
            f"✅ FipeZAP concluído: "
            f"{result.get('registros', 0)} registros, "
            f"{result.get('cidades', 0)} cidades"
        )

    except Exception as e:
        logger.error(f"❌ Erro na ingestão FipeZAP: {e}", exc_info=True)
        raise  # Re-lança para que o Azure Functions marque como falha


# ─────────────────────────────────────────────────────────────────
# Timer 2: IBGE — semanal (domingo às 03:00 UTC)
# ─────────────────────────────────────────────────────────────────
@ingestion_blueprint.timer_trigger(
    arg_name="timer",
    schedule="0 0 3 * * 0",        # Domingo, 03h UTC
    run_on_startup=False,
    use_monitor=True
)
def ingest_ibge_timer(timer: func.TimerRequest) -> None:
    """
    Ingestão semanal de dados IBGE (municípios, estados, mesorregiões).
    Dados mudam raramente; atualização semanal é suficiente.
    """
    logger.info("⏱️  IBGE Timer acionado")

    try:
        import sys
        sys.path.insert(0, "/home/site/wwwroot")
        from ingestion.ibge_ingestion import IBGEIngestion

        ingestor = IBGEIngestion()
        result = _run_async(ingestor.run_full_ingestion())

        logger.info(
            f"✅ IBGE concluído: "
            f"{result.get('municipios', 0)} municípios, "
            f"{result.get('estados', 0)} estados"
        )

    except Exception as e:
        logger.error(f"❌ Erro na ingestão IBGE: {e}", exc_info=True)
        raise


# ─────────────────────────────────────────────────────────────────
# Timer 3: Banco Central — mensal (1º dia do mês às 08h UTC)
# ─────────────────────────────────────────────────────────────────
@ingestion_blueprint.timer_trigger(
    arg_name="timer",
    schedule="0 0 8 1 * *",        # 1º dia de cada mês, 08h UTC
    run_on_startup=False,
    use_monitor=True
)
def ingest_bcb_timer(timer: func.TimerRequest) -> None:
    """
    Ingestão mensal de dados do Banco Central:
    - SELIC, CDI, IPCA (para contextualização macroeconômica)
    - Dados de crédito imobiliário
    """
    logger.info("⏱️  BCB Timer acionado")

    try:
        import httpx
        import sys
        sys.path.insert(0, "/home/site/wwwroot")
        from ingestion.config import azure_config
        from io import BytesIO
        import pandas as pd

        # Série 432 = SELIC anualizada | 189 = IPCA | 12 = CDI
        series_map = {
            "selic_anual":   432,
            "ipca_mensal":   189,
            "cdi_diario":    12,
            "credito_imob":  4175,
        }

        all_data = {}
        with httpx.Client(timeout=30.0) as client:
            for nome, serie_id in series_map.items():
                url = (
                    f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{serie_id}"
                    f"/dados?formato=json&dataInicial=01/01/2020"
                )
                r = client.get(url)
                if r.status_code == 200:
                    df = pd.DataFrame(r.json())
                    all_data[nome] = df
                    logger.info(f"  ✅ BCB série {nome}: {len(df)} registros")

        # Salva no Bronze
        blob_client = azure_config.get_blob_client()
        container = blob_client.get_container_client("bronze")
        for nome, df in all_data.items():
            buffer = BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)
            timestamp = datetime.now().strftime("%Y%m")
            container.upload_blob(
                name=f"bcb/{nome}_{timestamp}.parquet",
                data=buffer,
                overwrite=True
            )

        logger.info(f"✅ BCB: {len(all_data)} séries salvas no Bronze")

    except Exception as e:
        logger.error(f"❌ Erro na ingestão BCB: {e}", exc_info=True)
        raise


# ─────────────────────────────────────────────────────────────────
# HTTP Trigger: ingestão manual sob demanda
# ─────────────────────────────────────────────────────────────────
@ingestion_blueprint.route(
    route="ingest/{source}",
    methods=["POST"],
    auth_level=func.AuthLevel.FUNCTION
)
def ingest_http_trigger(
    req: func.HttpRequest,
    source: str
) -> func.HttpResponse:
    """
    Trigger HTTP para ingestão manual de uma fonte específica.

    Endpoint: POST /api/ingest/{source}
    Sources:  fipezap | ibge | bcb | all

    Headers: x-functions-key: <function-key>

    Body (opcional):
    {
        "force_refresh": true,
        "months_back": 12
    }
    """
    logger.info(f"📥 HTTP Ingestão manual: fonte={source}")

    # Valida a fonte
    valid_sources = {"fipezap", "ibge", "bcb", "all"}
    if source.lower() not in valid_sources:
        return func.HttpResponse(
            json.dumps(_error(
                f"Fonte inválida: '{source}'",
                f"Fontes válidas: {valid_sources}"
            )),
            status_code=400,
            mimetype="application/json"
        )

    # Parse do body (opcional)
    try:
        body = req.get_json() if req.get_body() else {}
    except ValueError:
        body = {}

    results = {}
    import sys
    sys.path.insert(0, "/home/site/wwwroot")

    # FipeZAP
    if source in ("fipezap", "all"):
        try:
            from ingestion.fipezap_ingestion import FipeZAPIngestion
            ingestor = FipeZAPIngestion()
            result = _run_async(ingestor.run())
            results["fipezap"] = _success("Ingestão concluída", result)
            logger.info(f"✅ FipeZAP manual: {result}")
        except Exception as e:
            results["fipezap"] = _error("Falha na ingestão", str(e))
            logger.error(f"❌ FipeZAP manual: {e}")

    # IBGE
    if source in ("ibge", "all"):
        try:
            from ingestion.ibge_ingestion import IBGEIngestion
            ingestor = IBGEIngestion()
            result = _run_async(ingestor.run_full_ingestion())
            results["ibge"] = _success("Ingestão concluída", result)
        except Exception as e:
            results["ibge"] = _error("Falha na ingestão", str(e))

    overall_status = (
        "success" if all(r["status"] == "success" for r in results.values())
        else "partial"
    )
    http_status = 200 if overall_status != "error" else 500

    return func.HttpResponse(
        json.dumps({
            "overall_status": overall_status,
            "results": results,
            "triggered_at": datetime.now(timezone.utc).isoformat()
        }),
        status_code=http_status,
        mimetype="application/json"
    )


# ─────────────────────────────────────────────────────────────────
# Blob Trigger: processa arquivo recém-chegado no Bronze
# ─────────────────────────────────────────────────────────────────
@ingestion_blueprint.blob_trigger(
    arg_name="blob",
    path="bronze/{name}",
    connection="AzureWebJobsStorage"
)
def process_new_bronze_file(blob: func.InputStream) -> None:
    """
    Trigger automático: sempre que um arquivo chega no container Bronze,
    inicia o pipeline Bronze → Silver automaticamente.
    """
    logger.info(
        f"📂 Novo arquivo no Bronze: {blob.name} "
        f"({blob.length / 1024:.1f} KB)"
    )

    # Determina o tipo de dado pelo path
    nome = blob.name.lower()
    if "fipezap" in nome:
        tipo = "fipezap"
    elif "ibge" in nome:
        tipo = "ibge"
    elif "bcb" in nome:
        tipo = "bcb"
    else:
        logger.warning(f"⚠️  Arquivo não reconhecido: {blob.name}")
        return

    try:
        import sys, io
        import pandas as pd
        sys.path.insert(0, "/home/site/wwwroot")
        from processing.bronze_to_silver import BronzeToSilverProcessor

        # Lê o blob recebido
        data = blob.read()
        df_raw = pd.read_parquet(io.BytesIO(data))
        logger.info(f"📊 Lidos {len(df_raw)} registros do Bronze")

        processor = BronzeToSilverProcessor()
        if tipo == "fipezap":
            df_silver = processor.process_fipezap(df_raw)
        elif tipo == "ibge":
            df_silver = processor.process_ibge_municipios(df_raw)
        else:
            logger.info(f"Tipo '{tipo}' sem processamento Silver definido")
            return

        logger.info(
            f"✅ Bronze → Silver para '{tipo}': "
            f"{len(df_silver)} registros processados"
        )

    except Exception as e:
        logger.error(
            f"❌ Erro ao processar Bronze → Silver "
            f"para '{blob.name}': {e}",
            exc_info=True
        )
        raise

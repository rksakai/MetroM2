# azure_functions/blueprints/health_bp.py
"""
Blueprint de Health Check — endpoints HTTP de monitoramento.
"""

import azure.functions as func
import logging
import json
import os
from datetime import datetime, timezone

logger = logging.getLogger(__name__)
health_blueprint = func.Blueprint()

_START_TIME = datetime.now(timezone.utc)


@health_blueprint.route(
    route="health",
    methods=["GET"],
    auth_level=func.AuthLevel.ANONYMOUS   # público para load balancer
)
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """
    Health check das Azure Functions.
    Verifica: conectividade com Data Lake, variáveis de ambiente.
    """
    checks = {}

    # Verifica variáveis de ambiente obrigatórias
    required_env = [
        "AZURE_STORAGE_ACCOUNT",
        "KEY_VAULT_URI",
        "AzureWebJobsStorage"
    ]
    missing = [e for e in required_env if not os.getenv(e)]
    checks["env_vars"] = (
        "ok" if not missing
        else f"missing: {missing}"
    )

    # Verifica conectividade com Azure Storage
    try:
        import sys
        sys.path.insert(0, "/home/site/wwwroot")
        from ingestion.config import azure_config
        client = azure_config.get_blob_client()
        # Tenta listar containers (operação leve)
        list(client.list_containers(max_results=1))
        checks["azure_storage"] = "ok"
    except Exception as e:
        checks["azure_storage"] = f"error: {str(e)[:100]}"

    uptime = (datetime.now(timezone.utc) - _START_TIME).total_seconds()
    all_ok = all(v == "ok" for v in checks.values())

    return func.HttpResponse(
        json.dumps({
            "status":      "healthy" if all_ok else "degraded",
            "uptime_s":    round(uptime, 1),
            "environment": os.getenv("ENVIRONMENT", "unknown"),
            "checks":      checks,
            "timestamp":   datetime.now(timezone.utc).isoformat()
        }),
        status_code=200 if all_ok else 503,
        mimetype="application/json"
    )


---

### `azure_functions/function_app.py` — Entry Point

```python
# azure_functions/function_app.py
"""
Azure Functions App — Modelo de Programação Python v2 (decorator-based).
Centraliza todos os triggers em um único entry point com Blueprints.
"""

import azure.functions as func
import logging

from azure_functions.blueprints.ingestion_bp import ingestion_blueprint
from azure_functions.blueprints.processing_bp import processing_blueprint
from azure_functions.blueprints.health_bp import health_blueprint

# Configura logging estruturado
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

# Cria a app principal registrando os blueprints
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

app.register_blueprint(ingestion_blueprint)
app.register_blueprint(processing_blueprint)
app.register_blueprint(health_blueprint)

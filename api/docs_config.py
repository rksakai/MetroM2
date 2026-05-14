# api/docs_config.py
"""
Configuração completa e enriquecida da documentação OpenAPI/Swagger.

Inclui:
- Descrição Markdown com exemplos de uso
- Tags com descrições detalhadas
- Informações de contato e licença
- Servers (dev, staging, prod)
- Extensões x-logo, x-tagGroups (ReDoc)
- Exemplos de requisição/resposta por endpoint
- Schemas de segurança
"""

from fastapi.openapi.utils import get_openapi
from typing import Dict, Any

# ─── Descrição Markdown longa ─────────────────────────────────────
API_DESCRIPTION = """
# 🏠 Real Estate Market Analysis API

API completa para análise do mercado imobiliário brasileiro com dados históricos,
forecasts e métricas de investimento por região.

---

## 📊 Fontes de Dados

| Fonte | Cobertura | Atualização | Tipo |
|---|---|---|---|
| **FipeZAP** | 19 capitais + metrópoles | Diária | Preços anunciados |
| **IBGE** | 5.570 municípios | Semanal | Dados geográficos |
| **BCB** | Nacional | Mensal | Macro (SELIC, IPCA) |
| **Dados Abertos** | Cidades selecionadas | Mensal | ITBI (transações reais) |

---

## 🏗️ Arquitetura de Dados
FipeZAP / IBGE / BCB
│
▼
[Bronze Layer]   ← dados brutos (Parquet no ADLS Gen2)
│
▼
[Silver Layer]   ← limpos, validados, tipados
│
▼
[Gold Layer]     ← agregados, scores, forecasts
│
▼
[API REST]    ← FastAPI (este serviço)




---

## 🔑 Autenticação

Esta API usa **API Keys** via header `x-api-key` para endpoints protegidos.

```bash
# Exemplo com curl
curl -H "x-api-key: SEU_TOKEN" \\
     https://realestate-api.azurewebsites.net/mercado/resumo

Nota: Ambientes de desenvolvimento não requerem autenticação.


🚀 Quick Start
1. Listar cidades disponíveis
GET /regioes

2. Análise de uma cidade
GET /mercado/São Paulo/analise?meses=24

3. Previsão de preços
GET /forecast/Curitiba?horizonte_meses=12

4. Comparar cidades
GET /analytics/comparacao?cidades=São Paulo,Curitiba,Fortaleza

📈 Métricas Calculadas
Cap Rate Anual: (aluguel_mensal × 12) / preco_venda × 100
Score de Investimento: índice composto 0–100 (rentabilidade + valorização + tendência + estabilidade + acessibilidade)
MAPE: erro percentual médio absoluto do modelo de forecast

📞 Suporte
📧 Email: api-support@realestate-analytics.com
📖 Docs: https://docs.realestate-analytics.com
🐛 Issues: GitHub Issues """
─── Tags com descrições ─────────────────────────────────────────
TAGS_METADATA = [
{
"name": "🌎 Regiões",
"description": """
Endpoints para descoberta e detalhamento de regiões geográficas.
Permite buscar cidades por nome (autocomplete), listar estados e
obter dados de localização (lat/lon, UF, macrorregião).

GET /regioes/busca?q=curit&limit=5


    """,
    "externalDocs": {
        "description": "Documentação da API IBGE",
        "url": "https://servicodados.ibge.gov.br/api/docs/"
    }
},
{
    "name": "📊 Mercado",
    "description": """




Análise completa do mercado imobiliário.
Retorna séries históricas de preços, resumos e comparações.
Dados atualizados diariamente via pipeline Azure Data Factory.
Periodicidade dos dados: diária (FipeZAP), semanal (IBGE)
Granularidade: cidade (mensal)
"""
},
{
"name": "📈 Analytics",
"description": """
Métricas avançadas, rankings, comparações e detecção de anomalias.
Inclui:
Score de investimento (0–100) por cidade
Ranking nacional por qualquer métrica
Comparação radar entre até 8 cidades
Detecção de anomalias via Isolation Forest + Z-Score
Algoritmo de score: ponderação de 5 dimensões:
rentabilidade (30%) + valorização (25%) + tendência (20%) +
estabilidade (15%) + acessibilidade (10%)
"""
},
{
"name": "🔮 Forecast",
"description": """
Previsão de preços usando modelos de séries temporais.
Modelo principal: Prophet (Meta) com:
Sazonalidade anual multiplicativa
Sazonalidade trimestral (mercado BR)
Regressores externos: cap_rate, aluguel
Intervalo de confiança 95%
Horizonte: 3 a 24 meses
Métricas de avaliação:
MAE (Mean Absolute Error): erro médio em R$/m²
MAPE (Mean Abs. Percentage Error): erro percentual
RMSE (Root Mean Squared Error) """ }, { "name": "🔧 Sistema", "description": "Health checks, versão e status do sistema." }, ]
─── Exemplos de request/response por endpoint ───────────────────
ENDPOINT_EXAMPLES: Dict[str, Any] = {
# GET /regioes/busca
"busca_regioes": {
    "summary": "Busca 'curit'",
    "value": [
        {
            "nome": "Curitiba",
            "tipo": "cidade",
            "uf_sigla": "PR",
            "regiao": "Sul",
            "latitude": -25.4290,
            "longitude": -49.2671,
            "municipio_ibge_id": 4106902
        }
    ]
},

# GET /mercado/{cidade}/analise
"city_analise": {
    "summary": "Análise de São Paulo",
    "value": {
        "cidade": "São Paulo",
        "total_registros": 24,
        "periodo_inicio": "2022-12-01",
        "periodo_fim": "2024-12-01",
        "preco_minimo": 9200.50,
        "preco_maximo": 11800.00,
        "preco_medio": 10450.25,
        "preco_mediano": 10380.00,
        "serie_temporal": [
            {
                "data": "2024-12-01",
                "preco_m2_venda": 11250.00,
                "preco_m2_aluguel": 50.62,
                "cap_rate_anual": 0.054,
                "variacao_mensal": 0.006,
                "media_movel_3m": 11100.00,
                "media_movel_6m": 10850.00
            }
        ],
        "summary": {
            "cidade": "São Paulo",
            "preco_m2_venda": 11250.00,
            "preco_m2_aluguel": 50.62,
            "cap_rate_anual": 0.054,
            "variacao_12m": 8.5,
            "score_investimento": 72.0,
            "categoria_mercado": "Aquecido",
            "data_referencia": "2024-12-01"
        }
    }
},

# GET /forecast/{cidade}
"city_forecast": {
    "summary": "Forecast para Curitiba — 12 meses",
    "value": {
        "cidade": "Curitiba",
        "horizonte_meses": 12,
        "modelo": "Prophet (Meta) + Regressores",
        "treinado_em": "2024-12-15T10:00:00Z",
        "metricas": {
            "mae": 215.40,
            "mape": 2.9,
            "rmse": 310.20
        },
        "preco_atual": 7350.00,
        "preco_previsto_final": 7980.00,
        "variacao_prevista_pct": 8.57,
        "previsao": [
            {
                "data": "2025-01-01",
                "preco_previsto": 7410.00,
                "limite_inferior": 7200.00,
                "limite_superior": 7620.00,
                "tendencia": 7390.00
            },
            {
                "data": "2025-12-01",
                "preco_previsto": 7980.00,
                "limite_inferior": 7600.00,
                "limite_superior": 8360.00,
                "tendencia": 7940.00
            }
        ]
    }
},

# GET /analytics/comparacao
"comparacao": {
    "summary": "Comparação SP × RJ × Curitiba",
    "value": [
        {
            "cidade": "São Paulo",
            "metrics": {
                "preco_m2_venda": 11250.0,
                "preco_m2_aluguel": 50.62,
                "cap_rate_anual": 0.054,
                "variacao_12m": 8.5
            },
            "score_investimento": 72.0,
            "categoria_mercado": "Aquecido",
            "ranking_nacional": 1
        },
        {
            "cidade": "Curitiba",
            "metrics": {
                "preco_m2_venda": 7350.0,
                "preco_m2_aluguel": 44.10,
                "cap_rate_anual": 0.072,
                "variacao_12m": 6.2
            },
            "score_investimento": 68.0,
            "categoria_mercado": "Moderado",
            "ranking_nacional": 3
        }
    ]
},

# GET /analytics/{cidade}/anomalias
"anomalias": {
    "summary": "Anomalias em Fortaleza",
    "value": {
        "cidades_analisadas": ["Fortaleza"],
        "total_anomalias": 2,
        "anomalias_criticas": 0,
        "percentual_anomalo": 5.5,
        "resumo_por_tipo": {
            "zscore_temporal": 1,
            "cap_rate_anormal": 1
        },
        "anomalias": [
            {
                "cidade": "Fortaleza",
                "data": "2023-07-01",
                "tipo": "zscore_temporal",
                "descricao": "Preço +18.3% em relação à média histórica (Z=2.71)",
                "valor_observado": 6350.0,
                "valor_esperado": 5366.0,
                "desvio_pct": 18.3,
                "severidade": "media",
                "score_anomalia": -0.271
            }
        ]
    }
}
}

─── Função que customiza o schema OpenAPI ────────────────────────
def custom_openapi_schema(app) -> Dict[str, Any]:
"""
Gera schema OpenAPI enriquecido com exemplos, extensões ReDoc
e documentação detalhada por endpoint.
"""
if app.openapi_schema:
return app.openapi_schema

schema = get_openapi(
    title="🏠 Real Estate Market Analysis API",
    version="1.0.0",
    summary="Análise e previsão do mercado imobiliário brasileiro",
    description=API_DESCRIPTION,
    routes=app.routes,
    tags=TAGS_METADATA,
    contact={
        "name": "Real Estate Analytics Team",
        "email": "api-support@realestate-analytics.com",
        "url": "https://docs.realestate-analytics.com"
    },
    license_info={
        "name": "MIT License",
        "url": "https://opensource.org/licenses/MIT"
    },
    servers=[
        {
            "url": "https://realestate-api-prod.azurewebsites.net",
            "description": "🟢 Produção"
        },
        {
            "url": "https://realestate-api-staging.azurewebsites.net",
            "description": "🟡 Staging"
        },
        {
            "url": "http://localhost:8000",
            "description": "🔵 Desenvolvimento local"
        }
    ]
)

# ── Extensões ReDoc ──────────────────────────────────────────
schema["info"]["x-logo"] = {
    "url": "https://upload.wikimedia.org/wikipedia/commons/thumb/"
           "8/8f/Flat_home_icon.svg/240px-Flat_home_icon.svg.png",
    "altText": "Real Estate Analysis"
}

schema["x-tagGroups"] = [
    {
        "name": "🗺️ Geográfico",
        "tags": ["🌎 Regiões"]
    },
    {
        "name": "📊 Mercado & Analytics",
        "tags": ["📊 Mercado", "📈 Analytics"]
    },
    {
        "name": "🤖 Machine Learning",
        "tags": ["🔮 Forecast"]
    },
    {
        "name": "⚙️ Operacional",
        "tags": ["🔧 Sistema"]
    }
]

# ── Segurança global ─────────────────────────────────────────
schema["components"]["securitySchemes"] = {
    "ApiKeyHeader": {
        "type": "apiKey",
        "in": "header",
        "name": "x-api-key",
        "description": "API Key obtida no portal do desenvolvedor"
    },
    "BearerAuth": {
        "type": "http",
        "scheme": "bearer",
        "bearerFormat": "JWT",
        "description": "JWT Token (Azure AD)"
    }
}

# ── Adiciona exemplos e x-codeSamples por endpoint ───────────
_enrich_paths(schema)

app.openapi_schema = schema
return schema

def _enrich_paths(schema: Dict[str, Any]) -> None:
"""Adiciona exemplos de código e respostas a cada path."""

code_samples_map = {

    # ── GET /regioes ─────────────────────────────────────────
    ("/regioes", "get"): {
        "x-codeSamples": [
            {
                "lang": "Python",
                "label": "Python (httpx)",
                "source": (
                    "import httpx\n"
                    "\n"
                    "r = httpx.get('http://localhost:8000/regioes')\n"
                    "cidades = r.json()['cidades']\n"
                    "print(f'{len(cidades)} cidades disponíveis')\n"
                    "# → 19 cidades disponíveis"
                )
            },
            {
                "lang": "Shell",
                "label": "cURL",
                "source": (
                    "curl -X GET 'http://localhost:8000/regioes' \\\n"
                    "  -H 'Accept: application/json'"
                )
            },
            {
                "lang": "JavaScript",
                "label": "JavaScript (fetch)",
                "source": (
                    "const resp = await fetch('/regioes');\n"
                    "const data = await resp.json();\n"
                    "console.log(data.cidades); // ['Aracaju', 'Belém', ...]"
                )
            }
        ]
    },

    # ── GET /mercado/{cidade}/analise ─────────────────────────
    ("/mercado/{cidade}/analise", "get"): {
        "x-codeSamples": [
            {
                "lang": "Python",
                "label": "Python (httpx)",
                "source": (
                    "import httpx\n"
                    "\n"
                    "r = httpx.get(\n"
                    "    'http://localhost:8000/mercado/São Paulo/analise',\n"
                    "    params={'meses': 24}\n"
                    ")\n"
                    "data = r.json()\n"
                    "print(f\"Preço atual: R\$ {data['summary']['preco_m2_venda']:,.0f}/m²\")\n"
                    "# → Preço atual: R\$ 11.250/m²"
                )
            },
            {
                "lang": "Shell",
                "label": "cURL",
                "source": (
                    "curl -X GET \\\n"
                    "  'http://localhost:8000/mercado/S%C3%A3o%20Paulo/analise?meses=24' \\\n"
                    "  -H 'Accept: application/json'"
                )
            },
            {
                "lang": "R",
                "label": "R (httr2)",
                "source": (
                    "library(httr2)\n"
                    "\n"
                    "resp <- request('http://localhost:8000') |>\n"
                    "  req_url_path('/mercado/São Paulo/analise') |>\n"
                    "  req_url_query(meses = 24) |>\n"
                    "  req_perform()\n"
                    "\n"
                    "data <- resp_body_json(resp)\n"
                    "cat('Preço:', data\$summary\$preco_m2_venda)"
                )
            }
        ]
    },

    # ── GET /forecast/{cidade} ─────────────────────────────────
    ("/forecast/{cidade}", "get"): {
        "x-codeSamples": [
            {
                "lang": "Python",
                "label": "Python (pandas)",
                "source": (
                    "import httpx, pandas as pd\n"
                    "\n"
                    "r = httpx.get(\n"
                    "    'http://localhost:8000/forecast/Curitiba',\n"
                    "    params={'horizonte_meses': 12},\n"
                    "    timeout=60.0\n"
                    ")\n"
                    "data = r.json()\n"
                    "df = pd.DataFrame(data['previsao'])\n"
                    "df['data'] = pd.to_datetime(df['data'])\n"
                    "\n"
                    "print(f\"MAPE: {data['metricas']['mape']:.1f}%\")\n"
                    "print(df[['data','preco_previsto']].tail(3))"
                )
            },
            {
                "lang": "Shell",
                "label": "cURL",
                "source": (
                    "# Forecast para Curitiba, 12 meses\n"
                    "curl -X GET \\\n"
                    "  'http://localhost:8000/forecast/Curitiba?horizonte_meses=12' \\\n"
                    "  -H 'Accept: application/json'"
                )
            }
        ]
    },

    # ── GET /analytics/ranking ────────────────────────────────
    ("/analytics/ranking", "get"): {
        "x-codeSamples": [
            {
                "lang": "Python",
                "label": "Python — Top 10 Cap Rate",
                "source": (
                    "import httpx\n"
                    "\n"
                    "r = httpx.get(\n"
                    "    'http://localhost:8000/analytics/ranking',\n"
                    "    params={\n"
                    "        'metrica': 'cap_rate_anual',\n"
                    "        'top_n': 10,\n"
                    "        'ordem': 'desc'\n"
                    "    }\n"
                    ")\n"
                    "for item in r.json():\n"
                    "    print(f\"{item['posicao']}. {item['cidade']}: \"\n"
                    "          f\"{item['metrics']['cap_rate_anual']:.2%}\")"
                )
            }
        ]
    },

    # ── POST /forecast/batch ──────────────────────────────────
    ("/forecast/batch", "post"): {
        "x-codeSamples": [
            {
                "lang": "Python",
                "label": "Python — Batch Forecast",
                "source": (
                    "import httpx\n"
                    "\n"
                    "r = httpx.post(\n"
                    "    'http://localhost:8000/forecast/batch',\n"
                    "    json=['São Paulo', 'Curitiba', 'Fortaleza'],\n"
                    "    params={'horizonte_meses': 6},\n"
                    "    timeout=120.0\n"
                    ")\n"
                    "for item in r.json():\n"
                    "    print(f\"{item['cidade']}: \"\n"
                    "          f\"atual R\${item['preco_atual']:,.0f} → \"\n"
                    "          f\"previsto R\${item['preco_previsto_final']:,.0f} \"\n"
                    "          f\"({item['variacao_prevista_pct']:+.1f}%)\")"
                )
            },
            {
                "lang": "Shell",
                "label": "cURL",
                "source": (
                    "curl -X POST 'http://localhost:8000/forecast/batch?horizonte_meses=6' \\\n"
                    "  -H 'Content-Type: application/json' \\\n"
                    "  -d '[\"São Paulo\", \"Curitiba\", \"Fortaleza\"]'"
                )
            }
        ]
    },
}

# Injeta os code samples no schema
for (path, method), extensions in code_samples_map.items():
    if path in schema.get("paths", {}):
        path_item = schema["paths"][path]
        if method in path_item:
            path_item[method].update(extensions)

─── Configurações do Swagger UI ─────────────────────────────────
SWAGGER_UI_PARAMETERS = {
"docExpansion": "list",           # expande tags, colapsa operações
"defaultModelsExpandDepth": 2,    # profundidade dos schemas
"defaultModelExpandDepth": 3,
"filter": True,                   # habilita busca no Swagger UI
"syntaxHighlight.theme": "monokai",
"tryItOutEnabled": True,          # Try-it-out habilitado por padrão
"requestSnippetsEnabled": True,   # snippets de código no Try-it-out
"persistAuthorization": True,     # mantém auth entre reloads
"displayRequestDuration": True,   # mostra tempo de resposta
"showExtensions": True,
"showCommonExtensions": True,
}

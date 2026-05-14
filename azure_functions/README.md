2. Azure Functions (Python v2 — decorator-based)
|Função | Trigger |Horário|
|ingest_fipezap_timer |Timer |Diário 07h Brasília |
|ingest_ibge_timer |Timer |Domingo 00h UTC |
|ingest_bcb_timer | Timer |1º do mês 05h UTC |
|ingest_http_trigger |HTTP POST |Manual |
|process_new_bronze_file |Blob |Ao chegar arquivo no Bronze |
|process_silver_timer |Timer |Diário 08h Brasília |
|run_ml_forecasts_timer |Timer |Diário 10h Brasília |
|process_http_trigger |HTTP POST |Manual |
|health_check |HTTP GET |Anônimo |

3. Power BI DAX
40+ medidas organizadas em 8 pastas temáticas
Métricas: preços, variações (MoM/3M/12M/YTD/2Y), cap rate, ROI, payback, forecast, scores, médias móveis, ranking, benchmark nacional
Tabela dim_data completa com atributos de tempo e flag de forecast

4. Documentação OpenAPI
Descrição Markdown rica com tabelas, exemplos e arquitetura
Tags com descrições detalhadas e links externos
x-codeSamples em Python, cURL, JavaScript e R por endpoint
x-tagGroups (ReDoc) agrupando endpoints por domínio
x-logo para branding no ReDoc
Swagger UI com Try-it-out, busca, highlight e auth persistente
3 servers declarados: prod, staging e localhost

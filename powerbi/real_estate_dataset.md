# 🏠 Power BI — Real Estate Market Analysis
## Modelo Semântico (Star Schema)

### Tabelas de Fato
- fact_precos           → Preços/m² históricos (granularidade: cidade × mês)
- fact_forecasts        → Previsões Prophet (granularidade: cidade × mês futuro)

### Tabelas de Dimensão
- dim_cidade            → Atributos geográficos de cada cidade
- dim_data              → Calendário completo com atributos de tempo
- dim_categoria_mercado → Categorias de temperatura do mercado

### Relacionamentos
fact_precos[cidade_id]  → dim_cidade[cidade_id]  (N:1)
fact_precos[data_id]    → dim_data[data_id]       (N:1)
fact_forecasts[cidade_id] → dim_cidade[cidade_id] (N:1)

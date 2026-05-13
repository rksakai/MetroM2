# dashboard/app.py
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import httpx
import asyncio
import os
from typing import Optional

# ─── Configuração da Página ────────────────────────────────────────
st.set_page_config(
    page_title="🏠 Análise de Mercado Imobiliário",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

API_URL = os.getenv("API_URL", "http://localhost:8000")

# ─── Estilo CSS Customizado ────────────────────────────────────────
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #1e3a5f, #2d6a9f);
        padding: 1.2rem;
        border-radius: 12px;
        color: white;
        text-align: center;
        margin: 0.3rem;
        box-shadow: 0 4px 15px rgba(0,0,0,0.2);
    }
    .metric-value {
        font-size: 1.8rem;
        font-weight: 700;
        color: #4fc3f7;
    }
    .metric-label {
        font-size: 0.85rem;
        opacity: 0.85;
        margin-top: 4px;
    }
    .category-badge {
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 600;
    }
    .badge-aquecido    { background: #ff4444; color: white; }
    .badge-moderado    { background: #ff8c00; color: white; }
    .badge-estavel     { background: #2e7d32; color: white; }
    .badge-mto_aquecido{ background: #7b1fa2; color: white; }
</style>
""", unsafe_allow_html=True)

# ─── Funções de Dados ──────────────────────────────────────────────

@st.cache_data(ttl=300)
def fetch_regions():
    try:
        r = httpx.get(f"{API_URL}/regioes", timeout=10)
        return r.json() if r.status_code == 200 else []
    except:
        return ["São Paulo", "Rio de Janeiro", "Belo Horizonte",
                "Brasília", "Curitiba", "Porto Alegre",
                "Florianópolis", "Salvador", "Fortaleza"]

@st.cache_data(ttl=300)
def fetch_market_summary():
    try:
        r = httpx.get(f"{API_URL}/mercado/resumo?top_n=50", timeout=10)
        return pd.DataFrame(r.json()) if r.status_code == 200 else pd.DataFrame()
    except:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def fetch_city_analysis(cidade: str, meses: int = 36):
    try:
        r = httpx.get(
            f"{API_URL}/mercado/{cidade}/analise?meses={meses}",
            timeout=15
        )
        if r.status_code == 200:
            data = r.json()
            ts_df = pd.DataFrame(data["serie_temporal"])
            ts_df["data"] = pd.to_datetime(ts_df["data"])
            return data, ts_df
    except:
        pass
    return None, pd.DataFrame()

@st.cache_data(ttl=600)
def fetch_forecast(cidade: str, horizonte: int = 12):
    try:
        r = httpx.get(
            f"{API_URL}/mercado/{cidade}/forecast?horizonte_meses={horizonte}",
            timeout=60
        )
        if r.status_code == 200:
            data = r.json()
            fc_df = pd.DataFrame(data["previsao"])
            fc_df["data"] = pd.to_datetime(fc_df["data"])
            return data, fc_df
    except Exception as e:
        st.warning(f"Forecast indisponível: {e}")
    return None, pd.DataFrame()

# ─── Sidebar ───────────────────────────────────────────────────────
with st.sidebar:
    st.image(
        "https://upload.wikimedia.org/wikipedia/commons/"
        "thumb/8/8f/Flat_home_icon.svg/240px-Flat_home_icon.svg.png",
        width=60
    )
    st.title("🏠 Mercado Imobiliário")
    st.caption("Análise de Mercado — Brasil")
    st.divider()

    regioes = fetch_regions()
    cidade_selecionada = st.selectbox(
        "📍 Selecione a Região",
        options=regioes,
        index=0 if regioes else None,
        help="Escolha a cidade para análise detalhada"
    )

    st.divider()
    meses_historico = st.slider(
        "📅 Janela Histórica (meses)",
        min_value=6, max_value=48, value=24, step=6
    )
    horizonte_forecast = st.slider(
        "🔮 Horizonte de Previsão (meses)",
        min_value=3, max_value=24, value=12, step=3
    )

    st.divider()
    tipo_imovel = st.radio(
        "🏢 Tipo de Análise",
        ["Venda", "Aluguel", "Ambos"],
        index=2
    )

    st.divider()
    st.caption("📊 Dados: FipeZAP + IBGE + BCB")
    st.caption("🔄 Atualização: Diária (ADF)")
    st.caption("🤖 Modelo: Prophet (Meta)")

# ─── Conteúdo Principal ────────────────────────────────────────────
st.title(f"🏠 Análise de Mercado Imobiliário")

if not cidade_selecionada:
    st.warning("⚠️ Selecione uma região na barra lateral.")
    st.stop()

analise, df_ts = fetch_city_analysis(cidade_selecionada, meses_historico)
df_summary = fetch_market_summary()

# ─── Header com nome da cidade ─────────────────────────────────────
col_title, col_badge = st.columns([3, 1])
with col_title:
    st.header(f"📍 {cidade_selecionada}")
with col_badge:
    if analise and analise.get("summary"):
        cat = analise["summary"].get("categoria_mercado", "N/A")
        badge_class = {
            "Muito Aquecido": "badge-mto_aquecido",
            "Aquecido": "badge-aquecido",
            "Moderado": "badge-moderado",
            "Estável": "badge-estavel"
        }.get(cat, "badge-estavel")
        st.markdown(
            f'<div style="padding-top:2rem">'
            f'<span class="category-badge {badge_class}">🔥 {cat}</span>'
            f'</div>',
            unsafe_allow_html=True
        )

# ─── KPIs principais ───────────────────────────────────────────────
if analise and analise.get("summary"):
    s = analise["summary"]
    c1, c2, c3, c4, c5 = st.columns(5)

    with c1:
        st.metric(
            "💰 Preço/m² Venda",
            f"R\$ {s['preco_m2_venda']:,.0f}",
            help="Preço médio por m² para venda"
        )
    with c2:
        st.metric(
            "🏡 Preço/m² Aluguel",
            f"R\$ {s['preco_m2_aluguel']:,.2f}",
            help="Preço médio por m² para aluguel/mês"
        )
    with c3:
        delta_color = "normal" if s["variacao_12m"] >= 0 else "inverse"
        st.metric(
            "📈 Variação 12 meses",
            f"{s['variacao_12m']:+.1f}%",
            delta=f"{s['variacao_12m']:+.1f}%"
        )
    with c4:
        st.metric(
            "💹 Cap Rate Anual",
            f"{s['cap_rate_anual']:.2f}%",
            help="Retorno anual sobre aluguel / valor de venda"
        )
    with c5:
        st.metric(
            "⭐ Score Investimento",
            f"{s['score_investimento']:.0f}/100",
            help="Score composto: cap rate + valorização"
        )

st.divider()

# ─── Tabs principais ───────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Série Histórica",
    "🔮 Previsão de Preços",
    "🏆 Ranking Nacional",
    "⚖️ Comparação de Cidades"
])

# ────────────────────────────────────────────────────────────────────
# TAB 1 — SÉRIE HISTÓRICA
# ────────────────────────────────────────────────────────────────────
with tab1:
    if not df_ts.empty:
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=[
                "Evolução do Preço/m² (Venda)",
                "Evolução do Preço/m² (Aluguel)",
                "Cap Rate Anual (%)",
                "Variação Mensal (%)"
            ],
            vertical_spacing=0.12
        )

        # Preço Venda + Médias Móveis
        fig.add_trace(go.Scatter(
            x=df_ts["data"], y=df_ts["preco_m2_venda"],
            name="Preço Venda", line=dict(color="#2196F3", width=2.5),
            mode="lines+markers", marker=dict(size=4)
        ), row=1, col=1)

        if "media_movel_3m" in df_ts.columns:
            fig.add_trace(go.Scatter(
                x=df_ts["data"], y=df_ts["media_movel_3m"],
                name="MM 3m", line=dict(color="#FF9800", width=1.5,
                                        dash="dash")
            ), row=1, col=1)
            fig.add_trace(go.Scatter(
                x=df_ts["data"], y=df_ts["media_movel_6m"],
                name="MM 6m", line=dict(color="#F44336", width=1.5,
                                        dash="dot")
            ), row=1, col=1)

        # Preço Aluguel
        fig.add_trace(go.Scatter(
            x=df_ts["data"], y=df_ts["preco_m2_aluguel"],
            name="Aluguel", line=dict(color="#4CAF50", width=2.5),
            fill="tozeroy", fillcolor="rgba(76,175,80,0.1)"
        ), row=1, col=2)

        # Cap Rate
        fig.add_trace(go.Scatter(
            x=df_ts["data"], y=df_ts["cap_rate_anual"],
            name="Cap Rate", line=dict(color="#9C27B0", width=2.5),
            fill="tozeroy", fillcolor="rgba(156,39,176,0.1)"
        ), row=2, col=1)

        # Variação Mensal (barras coloridas)
        colors = [
            "#4CAF50" if v >= 0 else "#F44336"
            for v in df_ts["variacao_mensal"]
        ]
        fig.add_trace(go.Bar(
            x=df_ts["data"], y=df_ts["variacao_mensal"] * 100,
            name="Var. Mensal", marker_color=colors
        ), row=2, col=2)

        fig.update_layout(
            height=600,
            title_text=f"📊 Análise Histórica — {cidade_selecionada}",
            showlegend=True,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(size=11)
        )
        fig.update_xaxes(showgrid=True, gridwidth=0.5,
                         gridcolor="rgba(200,200,200,0.3)")
        fig.update_yaxes(showgrid=True, gridwidth=0.5,
                         gridcolor="rgba(200,200,200,0.3)")

        st.plotly_chart(fig, use_container_width=True)

        # Tabela de dados
        with st.expander("📋 Ver Dados Brutos"):
            st.dataframe(
                df_ts[["data", "preco_m2_venda", "preco_m2_aluguel",
                        "cap_rate_anual", "variacao_mensal"]]
                  .sort_values("data", ascending=False)
                  .style.format({
                      "preco_m2_venda": "R\$ {:,.0f}",
                      "preco_m2_aluguel": "R\$ {:,.2f}",
                      "cap_rate_anual": "{:.2f}%",
                      "variacao_mensal": "{:.2%}"
                  }),
                use_container_width=True
            )
    else:
        st.info(f"📭 Sem dados históricos para {cidade_selecionada}")

# ────────────────────────────────────────────────────────────────────
# TAB 2 — FORECAST
# ────────────────────────────────────────────────────────────────────
with tab2:
    st.subheader(f"🔮 Previsão de Preços — {cidade_selecionada}")
    st.caption(
        "Modelo Prophet (Meta) com sazonalidade e componente de tendência. "
        "Intervalo de confiança de 95%."
    )

    with st.spinner("⚙️ Gerando previsão com Prophet..."):
        fc_data, fc_df = fetch_forecast(cidade_selecionada, horizonte_forecast)

    if not fc_df.empty and not df_ts.empty:
        fig_fc = go.Figure()

        # Histórico
        fig_fc.add_trace(go.Scatter(
            x=df_ts["data"], y=df_ts["preco_m2_venda"],
            name="Histórico",
            line=dict(color="#2196F3", width=2),
            mode="lines"
        ))

        # Intervalo de confiança
        fig_fc.add_trace(go.Scatter(
            x=pd.concat([fc_df["data"], fc_df["data"][::-1]]),
            y=pd.concat([fc_df["limite_superior"], fc_df["limite_inferior"][::-1]]),
            fill="toself",
            fillcolor="rgba(255,152,0,0.2)",
            line=dict(color="rgba(255,255,255,0)"),
            name="IC 95%"
        ))

        # Previsão
        fig_fc.add_trace(go.Scatter(
            x=fc_df["data"], y=fc_df["preco_previsto"],
            name="Previsão",
            line=dict(color="#FF9800", width=3, dash="dash"),
            mode="lines+markers",
            marker=dict(size=8, symbol="diamond")
        ))

        fig_fc.update_layout(
            height=480,
            title=f"Previsão de Preço/m² — Próximos {horizonte_forecast} meses",
            xaxis_title="Data",
            yaxis_title="R\$/m²",
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            hovermode="x unified"
        )
        st.plotly_chart(fig_fc, use_container_width=True)

        if fc_data:
            col1, col2, col3 = st.columns(3)
            col1.metric("Modelo", fc_data.get("modelo", "Prophet"))
            col2.metric("MAE", f"R\$ {fc_data.get('mae', 0):,.0f}")
            col3.metric("MAPE", f"{fc_data.get('mape', 0):.1f}%")

        st.dataframe(
            fc_df.rename(columns={
                "data": "Data",
                "preco_previsto": "Preço Previsto (R\$/m²)",
                "limite_inferior": "Limite Inferior",
                "limite_superior": "Limite Superior"
            }).style.format({
                "Preço Previsto (R\$/m²)": "R\$ {:,.0f}",
                "Limite Inferior": "R\$ {:,.0f}",
                "Limite Superior": "R\$ {:,.0f}"
            }),
            use_container_width=True
        )

# ────────────────────────────────────────────────────────────────────
# TAB 3 — RANKING
# ────────────────────────────────────────────────────────────────────
with tab3:
    st.subheader("🏆 Ranking Nacional de Mercados Imobiliários")

    if not df_summary.empty:
        col_rank, col_filter = st.columns([2, 1])
        with col_filter:
            sort_by = st.selectbox(
                "Ordenar por",
                ["score_investimento", "preco_m2_venda",
                 "cap_rate_anual", "variacao_12m"]
            )

        df_rank = df_summary.sort_values(sort_by, ascending=False).head(15)

        fig_rank = px.bar(
            df_rank,
            x="cidade",
            y=sort_by,
            color="score_investimento",
            color_continuous_scale="RdYlGn",
            labels={
                "cidade": "Cidade",
                sort_by: sort_by.replace("_", " ").title(),
                "score_investimento": "Score"
            },
            title=f"Top 15 Cidades — {sort_by.replace('_',' ').title()}"
        )
        fig_rank.update_layout(
            height=420,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)"
        )
        st.plotly_chart(fig_rank, use_container_width=True)

        # Scatter: Cap Rate vs Valorização
        fig_scatter = px.scatter(
            df_summary,
            x="variacao_12m",
            y="cap_rate_anual",
            size="preco_m2_venda",
            color="score_investimento",
            hover_name="cidade",
            color_continuous_scale="RdYlGn",
            labels={
                "variacao_12m": "Valorização 12m (%)",
                "cap_rate_anual": "Cap Rate Anual (%)",
                "preco_m2_venda": "Preço/m²",
                "score_investimento": "Score"
            },
            title="📈 Mapa de Oportunidades: Cap Rate vs Valorização"
        )
        fig_scatter.update_layout(
            height=450,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)"
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

# ────────────────────────────────────────────────────────────────────
# TAB 4 — COMPARAÇÃO
# ────────────────────────────────────────────────────────────────────
with tab4:
    st.subheader("⚖️ Comparação entre Cidades")

    all_cities = fetch_regions()
    cidades_comp = st.multiselect(
        "Selecione até 6 cidades para comparar",
        options=all_cities,
        default=all_cities[:4] if len(all_cities) >= 4 else all_cities,
        max_selections=6
    )

    if len(cidades_comp) >= 2 and not df_summary.empty:
        df_comp = df_summary[df_summary["cidade"].isin(cidades_comp)]

        metricas = ["preco_m2_venda", "preco_m2_aluguel",
                    "cap_rate_anual", "variacao_12m", "score_investimento"]

        fig_radar = go.Figure()
        for _, row in df_comp.iterrows():
            # Normaliza para radar (0–1)
            vals = []
            for m in metricas:
                col_max = df_summary[m].max()
                col_min = df_summary[m].min()
                val = (row[m] - col_min) / (col_max - col_min + 1e-9)
                vals.append(round(val, 3))
            vals.append(vals[0])  # fecha o polígono

            fig_radar.add_trace(go.Scatterpolar(
                r=vals,
                theta=[m.replace("_", " ").title() for m in metricas]
                      + [metricas[0].replace("_", " ").title()],
                fill="toself",
                name=row["cidade"],
                opacity=0.7
            ))

        fig_radar.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
            title="🕸️ Radar de Métricas Normalizadas",
            height=500
        )
        st.plotly_chart(fig_radar, use_container_width=True)

        # Tabela comparativa
        st.dataframe(
            df_comp[["cidade"] + metricas]
              .set_index("cidade")
              .style.format({
                  "preco_m2_venda": "R\$ {:,.0f}",
                  "preco_m2_aluguel": "R\$ {:,.2f}",
                  "cap_rate_anual": "{:.2f}%",
                  "variacao_12m": "{:+.1f}%",
                  "score_investimento": "{:.0f}/100"
              })
              .background_gradient(cmap="RdYlGn", axis=0),
            use_container_width=True
        )
    else:
        st.info("Selecione pelo menos 2 cidades para comparar.")

# ─── Footer ────────────────────────────────────────────────────────
st.divider()
st.caption(
    "📌 **Fontes:** FipeZAP (FIPE) · IBGE API · Banco Central do Brasil · "
    "Dados Abertos · Portal ITBI  |  "
    "🔒 Dados tratados via Azure Data Lake Gen2 + ADF  |  "
    "🤖 ML: Prophet + XGBoost via Azure ML + MLflow"
)

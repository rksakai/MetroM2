# dashboard/pages/01_overview.py
"""
Página 1: Visão Geral Nacional do Mercado Imobiliário.
"""

import streamlit as st
import httpx
import pandas as pd
from dashboard.components.charts import (
    bar_chart_ranking, scatter_oportunidades,
    heatmap_variacao, histogram_distribuicao
)
from dashboard.components.maps import (
    markers_map_folium, render_folium
)
import os

st.set_page_config(
    page_title="Visão Geral — Mercado Imobiliário",
    page_icon="🌎", layout="wide"
)

API_URL = os.getenv("API_URL", "http://localhost:8000")


@st.cache_data(ttl=300)
def _fetch_summary():
    try:
        r = httpx.get(f"{API_URL}/mercado/resumo?top_n=50", timeout=10)
        return pd.DataFrame(r.json()) if r.status_code == 200 else pd.DataFrame()
    except:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def _fetch_overview():
    try:
        r = httpx.get(f"{API_URL}/analytics/resumo", timeout=10)
        return r.json() if r.status_code == 200 else {}
    except:
        return {}

@st.cache_data(ttl=300)
def _fetch_map_data():
    try:
        r = httpx.get(f"{API_URL}/regioes/mapa", timeout=10)
        return pd.DataFrame(r.json()) if r.status_code == 200 else pd.DataFrame()
    except:
        return pd.DataFrame()


# ─── Header ───────────────────────────────────────────────────────
st.title("🌎 Visão Geral — Mercado Imobiliário Brasileiro")
st.caption("Análise consolidada de todas as cidades monitoradas")

overview   = _fetch_overview()
df_summary = _fetch_summary()
df_map     = _fetch_map_data()

# ─── KPIs Nacionais ───────────────────────────────────────────────
if overview:
    st.subheader("📊 Indicadores Nacionais")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("🏙️ Cidades Monitoradas",
              overview.get("total_cidades", "—"))
    c2.metric("💰 Preço Médio/m²",
              f"R\$ {overview.get('preco_medio_nacional', 0):,.0f}")
    c3.metric("📈 Variação Média 12m",
              f"{overview.get('variacao_12m_media', 0):+.1f}%")
    c4.metric("💹 Cap Rate Médio",
              f"{overview.get('cap_rate_medio', 0):.2f}%")

    mais_caro = overview.get("preco_maximo", {})
    c5.metric("🏆 Cidade Mais Cara",
              mais_caro.get("cidade", "—"),
              f"R\$ {mais_caro.get('valor', 0):,.0f}/m²")

    st.divider()

# ─── Distribuição por Categoria ───────────────────────────────────
if overview.get("distribuicao_categorias"):
    st.subheader("🔥 Distribuição por Temperatura de Mercado")
    dist = overview["distribuicao_categorias"]
    cols = st.columns(len(dist))
    cat_emojis = {
        "Muito Aquecido": "🔴",
        "Aquecido":       "🟠",
        "Moderado":       "🟡",
        "Estável":        "🟢"
    }
    for col, (cat, qtd) in zip(cols, dist.items()):
        emoji = cat_emojis.get(cat, "⚪")
        col.metric(f"{emoji} {cat}", f"{qtd} cidades")

    st.divider()

# ─── Mapa Nacional ────────────────────────────────────────────────
if not df_map.empty:
    st.subheader("🗺️ Mapa Nacional de Preços")
    tipo_mapa = st.radio(
        "Tipo de mapa:",
        ["📍 Marcadores", "🌡️ Calor"],
        horizontal=True
    )

    if tipo_mapa == "📍 Marcadores":
        from dashboard.components.maps import markers_map_folium, render_folium
        m = markers_map_folium(df_map)
        render_folium(m, height=480)
    else:
        from dashboard.components.maps import heatmap_folium, render_folium
        m = heatmap_folium(df_map)
        render_folium(m, height=480)

    st.divider()

# ─── Ranking + Scatter ────────────────────────────────────────────
if not df_summary.empty:
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("🏆 Top 10 — Score de Investimento")
        fig_rank = bar_chart_ranking(
            df_summary.sort_values("score_investimento", ascending=False).head(10),
            col_y="score_investimento",
            title=""
        )
        st.plotly_chart(fig_rank, use_container_width=True)

    with col_right:
        st.subheader("📈 Cap Rate vs Valorização")
        fig_scatter = scatter_oportunidades(df_summary)
        st.plotly_chart(fig_scatter, use_container_width=True)

    st.divider()

    # Histograma de distribuição
    st.subheader("📊 Distribuição Nacional de Preços/m²")
    fig_hist = histogram_distribuicao(df_summary)
    st.plotly_chart(fig_hist, use_container_width=True)

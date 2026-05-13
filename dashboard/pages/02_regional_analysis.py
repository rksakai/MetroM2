# dashboard/pages/02_regional_analysis.py
"""
Página 2: Análise Regional Detalhada.
"""

import streamlit as st
import httpx
import pandas as pd
import os
from dashboard.components.charts import (
    painel_cidade, gauge_score, histogram_distribuicao
)
from dashboard.components.maps import markers_map_folium, render_folium

st.set_page_config(
    page_title="Análise Regional", page_icon="📍", layout="wide"
)

API_URL = os.getenv("API_URL", "http://localhost:8000")


@st.cache_data(ttl=300)
def _regioes():
    try:
        r = httpx.get(f"{API_URL}/regioes", timeout=10)
        return r.json().get("cidades", []) if r.status_code == 200 else []
    except:
        return ["São Paulo", "Rio de Janeiro", "Curitiba"]

@st.cache_data(ttl=300)
def _analise(cidade, meses):
    try:
        r = httpx.get(
            f"{API_URL}/mercado/{cidade}/analise?meses={meses}", timeout=15
        )
        if r.status_code == 200:
            d = r.json()
            ts = pd.DataFrame(d["serie_temporal"])
            ts["data"] = pd.to_datetime(ts["data"])
            return d, ts
    except:
        pass
    return None, pd.DataFrame()

@st.cache_data(ttl=300)
def _scoring(cidade):
    try:
        r = httpx.get(f"{API_URL}/analytics/{cidade}/scoring", timeout=10)
        return r.json() if r.status_code == 200 else {}
    except:
        return {}

@st.cache_data(ttl=300)
def _anomalias(cidade):
    try:
        r = httpx.get(f"{API_URL}/analytics/{cidade}/anomalias", timeout=10)
        return r.json() if r.status_code == 200 else {}
    except:
        return {}


# ─── Sidebar ──────────────────────────────────────────────────────
with st.sidebar:
    st.title("📍 Análise Regional")
    regioes = _regioes()
    cidade  = st.selectbox("Selecione a Cidade", regioes)
    meses   = st.slider("Janela Histórica (meses)", 6, 48, 24, 6)

# ─── Conteúdo ─────────────────────────────────────────────────────
st.title(f"📍 Análise Regional — {cidade}")

analise, df_ts = _analise(cidade, meses)
score_data     = _scoring(cidade)
anom_data      = _anomalias(cidade)

if not analise:
    st.error(f"Não foi possível carregar dados para {cidade}.")
    st.stop()

s = analise.get("summary", {})

# ─── KPIs ─────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)
col1.metric("💰 Preço/m² Venda",
            f"R\$ {s.get('preco_m2_venda', 0):,.0f}")
col2.metric("🏡 Preço/m² Aluguel",
            f"R\$ {s.get('preco_m2_aluguel', 0):,.2f}")
col3.metric("📈 Variação 12 meses",
            f"{s.get('variacao_12m', 0):+.1f}%")
col4.metric("💹 Cap Rate Anual",
            f"{s.get('cap_rate_anual', 0):.2f}%")

st.divider()

# ─── Gauge + Painel gráficos ──────────────────────────────────────
col_gauge, col_panel = st.columns([1, 3])

with col_gauge:
    if score_data:
        fig_g = gauge_score(
            score_data.get("score_total", 0),
            cidade,
            score_data.get("categoria", "N/A")
        )
        st.plotly_chart(fig_g, use_container_width=True)

        st.markdown("**Componentes do Score**")
        comps = score_data.get("score_componentes", {})
        for comp, val in comps.items():
            st.progress(int(val), text=f"{comp.title()}: {val:.0f}/100")

        if score_data.get("recomendacao"):
            st.info(score_data["recomendacao"])

with col_panel:
    if not df_ts.empty:
        fig_p = painel_cidade(df_ts, cidade)
        st.plotly_chart(fig_p, use_container_width=True)

st.divider()

# ─── Estatísticas ─────────────────────────────────────────────────
st.subheader("📊 Estatísticas do Período")
stat_cols = st.columns(4)
stat_cols[0].metric("📉 Preço Mínimo",
                    f"R\$ {analise.get('preco_minimo', 0):,.0f}")
stat_cols[1].metric("📈 Preço Máximo",
                    f"R\$ {analise.get('preco_maximo', 0):,.0f}")
stat_cols[2].metric("📊 Preço Médio",
                    f"R\$ {analise.get('preco_medio', 0):,.0f}")
stat_cols[3].metric("📏 Mediana",
                    f"R\$ {analise.get('preco_mediano', 0):,.0f}")

# ─── Anomalias ────────────────────────────────────────────────────
if anom_data and anom_data.get("total_anomalias", 0) > 0:
    st.divider()
    st.subheader(
        f"⚠️ Anomalias Detectadas "
        f"({anom_data['total_anomalias']} registros)"
    )
    df_anom = pd.DataFrame(anom_data.get("anomalias", []))
    if not df_anom.empty:
        severity_colors = {
            "critica": "🔴",
            "alta":    "🟠",
            "media":   "🟡",
            "baixa":   "🟢"
        }
        df_anom["🔔"] = df_anom["severidade"].map(severity_colors)
        st.dataframe(
            df_anom[["🔔", "data", "tipo", "descricao",
                     "valor_observado", "desvio_pct", "severidade"]],
            use_container_width=True
        )

# ─── Dados brutos ─────────────────────────────────────────────────
with st.expander("📋 Exportar Dados"):
    if not df_ts.empty:
        csv = df_ts.to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇️ Baixar CSV",
            data=csv,
            file_name=f"{cidade}_historico.csv",
            mime="text/csv"
        )

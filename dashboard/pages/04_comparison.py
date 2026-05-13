# dashboard/pages/04_comparison.py
"""
Página 4: Comparação entre Cidades.
"""

import streamlit as st
import httpx
import pandas as pd
import os
from dashboard.components.charts import (
    radar_chart_comparacao, bar_chart_ranking,
    histogram_distribuicao
)

st.set_page_config(
    page_title="Comparação de Cidades", page_icon="⚖️", layout="wide"
)

API_URL = os.getenv("API_URL", "http://localhost:8000")


@st.cache_data(ttl=300)
def _regioes():
    try:
        r = httpx.get(f"{API_URL}/regioes", timeout=10)
        return r.json().get("cidades", []) if r.status_code == 200 else []
    except:
        return []

@st.cache_data(ttl=300)
def _comparacao(cidades_str: str):
    try:
        r = httpx.get(
            f"{API_URL}/analytics/comparacao?cidades={cidades_str}",
            timeout=15
        )
        return r.json() if r.status_code == 200 else []
    except:
        return []

@st.cache_data(ttl=300)
def _summary():
    try:
        r = httpx.get(f"{API_URL}/mercado/resumo?top_n=50", timeout=10)
        return pd.DataFrame(r.json()) if r.status_code == 200 else pd.DataFrame()
    except:
        return pd.DataFrame()


# ─── Sidebar ──────────────────────────────────────────────────────
with st.sidebar:
    st.title("⚖️ Comparação")
    regioes = _regioes()
    padroes = regioes[:4] if len(regioes) >= 4 else regioes
    cidades_sel = st.multiselect(
        "Selecione Cidades (máx. 8)",
        options=regioes,
        default=padroes,
        max_selections=8
    )

# ─── Conteúdo ─────────────────────────────────────────────────────
st.title("⚖️ Comparação entre Cidades")

if len(cidades_sel) < 2:
    st.warning("⚠️ Selecione pelo menos 2 cidades para comparar.")
    st.stop()

cidades_str = ",".join(cidades_sel)
comp_data   = _comparacao(cidades_str)
df_summary  = _summary()

if not comp_data:
    st.error("Não foi possível carregar os dados de comparação.")
    st.stop()

df_comp = pd.DataFrame([{
    "cidade":            item["cidade"],
    "preco_m2_venda":    item["metrics"]["preco_m2_venda"],
    "preco_m2_aluguel":  item["metrics"]["preco_m2_aluguel"],
    "cap_rate_anual":    item["metrics"]["cap_rate_anual"],
    "variacao_12m":      item["metrics"].get("variacao_12m", 0) or 0,
    "score_investimento": item["score_investimento"],
    "categoria_mercado": item["categoria_mercado"],
    "ranking_nacional":  item.get("ranking_nacional"),
} for item in comp_data])

# ─── Tabela Resumo ────────────────────────────────────────────────
st.subheader("📋 Resumo Comparativo")
metricas_fmt = {
    "preco_m2_venda":    "R\$ {:,.0f}",
    "preco_m2_aluguel":  "R\$ {:,.2f}",
    "cap_rate_anual":    "{:.2f}%",
    "variacao_12m":      "{:+.1f}%",
    "score_investimento": "{:.0f}/100",
}
st.dataframe(
    df_comp.set_index("cidade")[list(metricas_fmt.keys())]
           .style.format(metricas_fmt)
           .background_gradient(cmap="RdYlGn", axis=0),
    use_container_width=True
)

st.divider()

# ─── Gráficos de comparação ───────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("🕸️ Radar de Métricas")
    metricas_radar = [
        "preco_m2_venda", "cap_rate_anual",
        "variacao_12m", "score_investimento"
    ]
    fig_radar = radar_chart_comparacao(df_comp, metricas_radar)
    st.plotly_chart(fig_radar, use_container_width=True)

with col2:
    st.subheader("🏆 Score de Investimento")
    fig_bar = bar_chart_ranking(
        df_comp.sort_values("score_investimento", ascending=False),
        col_y="score_investimento",
        title=""
    )
    st.plotly_chart(fig_bar, use_container_width=True)

st.divider()

# ─── Comparação de métricas individuais ──────────────────────────
st.subheader("📊 Comparação Detalhada por Métrica")
metrica_escolhida = st.selectbox(
    "Métrica para comparar:",
    options=list(metricas_fmt.keys()),
    format_func=lambda x: x.replace("_", " ").title()
)
fig_detalhe = bar_chart_ranking(
    df_comp.sort_values(metrica_escolhida, ascending=False),
    col_y=metrica_escolhida,
    col_color="score_investimento",
    title=f"Comparação: {metrica_escolhida.replace('_',' ').title()}"
)
st.plotly_chart(fig_detalhe, use_container_width=True)

# ─── Posição no ranking nacional ──────────────────────────────────
if df_comp["ranking_nacional"].notna().any():
    st.divider()
    st.subheader("🌎 Posição no Ranking Nacional")
    for _, row in df_comp.iterrows():
        rank = row.get("ranking_nacional")
        if rank:
            total = len(df_summary) if not df_summary.empty else "N/A"
            st.write(
                f"**{row['cidade']}**: "
                f"#{int(rank)} de {total} cidades monitoradas "
                f"— Score: {row['score_investimento']:.0f}/100 "
                f"({row['categoria_mercado']})"
            )

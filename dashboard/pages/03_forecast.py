# dashboard/pages/03_forecast.py
"""
Página 3: Previsão de Preços (Prophet).
"""

import streamlit as st
import httpx
import pandas as pd
import plotly.graph_objects as go
import os

API_URL = os.getenv("API_URL", "http://localhost:8000")

st.set_page_config(
    page_title="Previsão de Preços", page_icon="🔮", layout="wide"
)


@st.cache_data(ttl=300)
def _regioes():
    try:
        r = httpx.get(f"{API_URL}/regioes", timeout=10)
        return r.json().get("cidades", []) if r.status_code == 200 else []
    except:
        return []

@st.cache_data(ttl=600)
def _forecast(cidade, horizonte):
    try:
        r = httpx.get(
            f"{API_URL}/forecast/{cidade}?horizonte_meses={horizonte}",
            timeout=90
        )
        if r.status_code == 200:
            d = r.json()
            df = pd.DataFrame(d["previsao"])
            df["data"] = pd.to_datetime(df["data"])
            return d, df
    except Exception as e:
        st.warning(f"Erro ao buscar forecast: {e}")
    return None, pd.DataFrame()

@st.cache_data(ttl=600)
def _tendencia(cidade, jc, jl):
    try:
        r = httpx.get(
            f"{API_URL}/forecast/{cidade}/tendencia"
            f"?janela_curta={jc}&janela_longa={jl}",
            timeout=10
        )
        return r.json() if r.status_code == 200 else {}
    except:
        return {}

@st.cache_data(ttl=300)
def _historico(cidade, meses=36):
    try:
        r = httpx.get(
            f"{API_URL}/mercado/{cidade}/analise?meses={meses}", timeout=15
        )
        if r.status_code == 200:
            d = r.json()
            ts = pd.DataFrame(d["serie_temporal"])
            ts["data"] = pd.to_datetime(ts["data"])
            return ts
    except:
        pass
    return pd.DataFrame()


# ─── Sidebar ──────────────────────────────────────────────────────
with st.sidebar:
    st.title("🔮 Forecast")
    regioes   = _regioes()
    cidade    = st.selectbox("Cidade", regioes)
    horizonte = st.slider("Horizonte (meses)", 3, 24, 12, 3)
    jc = st.slider("Janela Curto Prazo (meses)", 2, 12, 3)
    jl = st.slider("Janela Longo Prazo (meses)", 6, 48, 12)

# ─── Conteúdo ─────────────────────────────────────────────────────
st.title(f"🔮 Previsão de Preços — {cidade}")
st.caption("Modelo Prophet com sazonalidade anual e regressores externos.")

with st.spinner(f"⚙️ Gerando forecast de {horizonte} meses para {cidade}..."):
    fc_data, fc_df  = _forecast(cidade, horizonte)
    tend_data       = _tendencia(cidade, jc, jl)
    df_hist         = _historico(cidade)

# ─── KPIs do Forecast ─────────────────────────────────────────────
if fc_data:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("💰 Preço Atual",
              f"R\$ {fc_data.get('preco_atual', 0):,.0f}/m²")
    c2.metric(f"🎯 Preço em {horizonte} meses",
              f"R\$ {fc_data.get('preco_previsto_final', 0):,.0f}/m²",
              delta=f"{fc_data.get('variacao_prevista_pct', 0):+.1f}%")
    c3.metric("📐 MAE",
              f"R\$ {fc_data.get('metricas', {}).get('mae', 0):,.0f}")
    c4.metric("📊 MAPE",
              f"{fc_data.get('metricas', {}).get('mape', 0):.1f}%")

    st.divider()

# ─── Gráfico do Forecast ──────────────────────────────────────────
if not fc_df.empty and not df_hist.empty:
    fig = go.Figure()

    # Histórico
    fig.add_trace(go.Scatter(
        x=df_hist["data"], y=df_hist["preco_m2_venda"],
        name="Histórico",
        line=dict(color="#2196F3", width=2.5),
        hovertemplate="<b>%{x|%b %Y}</b><br>R\$ %{y:,.0f}<extra></extra>"
    ))

    # Banda de confiança
    fig.add_trace(go.Scatter(
        x=pd.concat([fc_df["data"], fc_df["data"][::-1]]),
        y=pd.concat([fc_df["limite_superior"], fc_df["limite_inferior"][::-1]]),
        fill="toself", fillcolor="rgba(255,152,0,0.15)",
        line=dict(color="rgba(0,0,0,0)"),
        name="IC 95%",
        hoverinfo="skip"
    ))

    # Previsão
    fig.add_trace(go.Scatter(
        x=fc_df["data"], y=fc_df["preco_previsto"],
        name="Previsão",
        line=dict(color="#FF9800", width=3, dash="dash"),
        mode="lines+markers",
        marker=dict(size=7, symbol="diamond"),
        hovertemplate=(
            "<b>%{x|%b %Y}</b><br>"
            "Previsto: R\$ %{y:,.0f}<extra></extra>"
        )
    ))

    # Linha de separação histórico / forecast
    if not df_hist.empty:
        data_corte = df_hist["data"].max()
        fig.add_vline(
            x=data_corte, line_dash="dot",
            line_color="gray", line_width=1.5,
            annotation_text=" Hoje", annotation_font_size=11
        )

    fig.update_layout(
        height=480,
        title=f"Previsão de Preço/m² — Próximos {horizonte} meses",
        xaxis_title="Data", yaxis_title="R\$/m²",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        hovermode="x unified",
        legend=dict(orientation="h", y=-0.15),
    )
    st.plotly_chart(fig, use_container_width=True)

# ─── Análise de Tendência ─────────────────────────────────────────
if tend_data:
    st.divider()
    st.subheader("📈 Análise de Tendência")

    col_curto, col_longo, col_mom = st.columns(3)

    tc = tend_data.get("tendencia_curto_prazo", {})
    tl = tend_data.get("tendencia_longo_prazo", {})

    direcao_emoji = {"alta": "⬆️", "queda": "⬇️", "estável": "➡️"}

    with col_curto:
        st.metric(
            f"Curto Prazo ({jc} meses)",
            direcao_emoji.get(tc.get("direcao", ""), "") +
            " " + tc.get("direcao", "N/A").title(),
            f"{tc.get('variacao_total_pct', 0):+.1f}%"
        )
        st.caption(f"R²: {tc.get('r2', 0):.3f} | "
                   f"Coef: {tc.get('coef_mensal', 0):.0f} R\$/m²/mês")

    with col_longo:
        st.metric(
            f"Longo Prazo ({jl} meses)",
            direcao_emoji.get(tl.get("direcao", ""), "") +
            " " + tl.get("direcao", "N/A").title(),
            f"{tl.get('variacao_total_pct', 0):+.1f}%"
        )
        st.caption(f"R²: {tl.get('r2', 0):.3f} | "
                   f"Coef: {tl.get('coef_mensal', 0):.0f} R\$/m²/mês")

    with col_mom:
        mom = tend_data.get("momentum", "estável")
        mom_emoji = {"acelerando": "🚀", "desacelerando": "🛑", "estável": "⚖️"}
        st.metric("Momentum", f"{mom_emoji.get(mom,'')} {mom.title()}")
        st.caption(
            f"Aceleração: {tend_data.get('aceleracao', 0):+.2f} R\$/m²/mês²"
        )

# ─── Tabela de previsões ──────────────────────────────────────────
if not fc_df.empty:
    st.divider()
    with st.expander("📋 Tabela de Previsões"):
        st.dataframe(
            fc_df.rename(columns={
                "data": "Data",
                "preco_previsto": "Previsto (R\$/m²)",
                "limite_inferior": "Limite Inferior",
                "limite_superior": "Limite Superior",
            }).style.format({
                "Previsto (R\$/m²)": "R\$ {:,.0f}",
                "Limite Inferior": "R\$ {:,.0f}",
                "Limite Superior": "R\$ {:,.0f}",
            }),
            use_container_width=True
        )
        csv = fc_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇️ Baixar Forecast CSV",
            data=csv,
            file_name=f"{cidade}_forecast_{horizonte}m.csv",
            mime="text/csv"
        )
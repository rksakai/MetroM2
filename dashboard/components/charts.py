# dashboard/components/charts.py
"""
Componentes reutilizáveis de gráficos Plotly para o Dashboard.

Funções disponíveis:
- line_chart_historico()     → série temporal com médias móveis
- bar_chart_ranking()        → ranking de cidades
- scatter_oportunidades()    → mapa de oportunidades cap_rate vs valorização
- radar_chart_comparacao()   → radar de métricas normalizadas
- candlestick_precos()       → candlestick mensal de preços
- heatmap_variacao()         → heatmap de variações mensais
- gauge_score()              → gauge de score de investimento
- histogram_distribuicao()   → distribuição de preços
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from typing import List, Optional, Dict


# ─── Paleta de cores padrão ───────────────────────────────────────
CORES = {
    "primaria":    "#2196F3",
    "secundaria":  "#4CAF50",
    "alerta":      "#FF9800",
    "perigo":      "#F44336",
    "roxo":        "#9C27B0",
    "azul_escuro": "#1e3a5f",
    "cinza":       "rgba(200,200,200,0.3)",
}

LAYOUT_PADRAO = dict(
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    font=dict(size=11, family="Inter, sans-serif"),
    margin=dict(t=50, b=40, l=50, r=30),
    hoverlabel=dict(bgcolor="white", font_size=12),
)


# ─── Série histórica com médias móveis ────────────────────────────
def line_chart_historico(
    df: pd.DataFrame,
    cidade: str,
    col_data: str = "data",
    col_preco: str = "preco_m2_venda",
    mostrar_mm: bool = True,
    height: int = 400
) -> go.Figure:
    """Gráfico de linha com histórico de preços e médias móveis."""
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df[col_data], y=df[col_preco],
        name="Preço/m² (Venda)",
        line=dict(color=CORES["primaria"], width=2.5),
        mode="lines+markers",
        marker=dict(size=4),
        hovertemplate="<b>%{x|%b %Y}</b><br>R\$ %{y:,.0f}/m²<extra></extra>"
    ))

    if mostrar_mm and "media_movel_3m" in df.columns:
        fig.add_trace(go.Scatter(
            x=df[col_data], y=df["media_movel_3m"],
            name="MM 3 meses",
            line=dict(color=CORES["alerta"], width=1.8, dash="dash"),
            hovertemplate="MM 3m: R\$ %{y:,.0f}<extra></extra>"
        ))

    if mostrar_mm and "media_movel_6m" in df.columns:
        fig.add_trace(go.Scatter(
            x=df[col_data], y=df["media_movel_6m"],
            name="MM 6 meses",
            line=dict(color=CORES["perigo"], width=1.8, dash="dot"),
            hovertemplate="MM 6m: R\$ %{y:,.0f}<extra></extra>"
        ))

    fig.update_layout(
        **LAYOUT_PADRAO,
        height=height,
        title=f"📈 Evolução do Preço/m² — {cidade}",
        xaxis_title="Data",
        yaxis_title="R\$/m²",
        yaxis_tickformat="R\$,.0f",
        hovermode="x unified",
        legend=dict(orientation="h", y=-0.15),
        xaxis=dict(showgrid=True, gridcolor=CORES["cinza"]),
        yaxis=dict(showgrid=True, gridcolor=CORES["cinza"]),
    )
    return fig


# ─── Painel 4 gráficos (overview de uma cidade) ───────────────────
def painel_cidade(df: pd.DataFrame, cidade: str) -> go.Figure:
    """Painel com 4 subplots: venda, aluguel, cap rate, variação mensal."""
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=[
            "Preço/m² Venda (R\$)",
            "Preço/m² Aluguel (R\$)",
            "Cap Rate Anual (%)",
            "Variação Mensal (%)"
        ],
        vertical_spacing=0.14,
        horizontal_spacing=0.10,
    )

    # Venda
    fig.add_trace(go.Scatter(
        x=df["data"], y=df["preco_m2_venda"],
        line=dict(color=CORES["primaria"], width=2.2),
        name="Venda", showlegend=False,
        hovertemplate="R\$ %{y:,.0f}<extra></extra>"
    ), row=1, col=1)

    # Aluguel (área preenchida)
    fig.add_trace(go.Scatter(
        x=df["data"], y=df["preco_m2_aluguel"],
        line=dict(color=CORES["secundaria"], width=2.2),
        fill="tozeroy", fillcolor="rgba(76,175,80,0.10)",
        name="Aluguel", showlegend=False,
        hovertemplate="R\$ %{y:,.2f}<extra></extra>"
    ), row=1, col=2)

    # Cap Rate
    fig.add_trace(go.Scatter(
        x=df["data"], y=df["cap_rate_anual"],
        line=dict(color=CORES["roxo"], width=2.2),
        fill="tozeroy", fillcolor="rgba(156,39,176,0.10)",
        name="Cap Rate", showlegend=False,
        hovertemplate="%{y:.2f}%<extra></extra>"
    ), row=2, col=1)

    # Variação Mensal (barras coloridas)
    col_var = "variacao_mensal"
    cores_var = [
        CORES["secundaria"] if v >= 0 else CORES["perigo"]
        for v in df.get(col_var, [0] * len(df))
    ]
    fig.add_trace(go.Bar(
        x=df["data"],
        y=df.get(col_var, pd.Series([0] * len(df))) * 100,
        marker_color=cores_var,
        name="Var. Mensal", showlegend=False,
        hovertemplate="%{y:+.2f}%<extra></extra>"
    ), row=2, col=2)

    fig.update_layout(
        **LAYOUT_PADRAO,
        height=580,
        title_text=f"📊 Painel de Análise — {cidade}",
        hovermode="x unified",
    )
    for i in range(1, 5):
        r, c = (1, 1) if i == 1 else (1, 2) if i == 2 else (2, 1) if i == 3 else (2, 2)
        fig.update_xaxes(showgrid=True, gridcolor=CORES["cinza"], row=r, col=c)
        fig.update_yaxes(showgrid=True, gridcolor=CORES["cinza"], row=r, col=c)
    return fig


# ─── Bar chart de ranking ─────────────────────────────────────────
def bar_chart_ranking(
    df: pd.DataFrame,
    col_x: str = "cidade",
    col_y: str = "score_investimento",
    col_color: str = "score_investimento",
    title: str = "🏆 Ranking",
    height: int = 420
) -> go.Figure:
    """Bar chart para ranking de cidades."""
    color_scale = "RdYlGn"
    fig = px.bar(
        df,
        x=col_x, y=col_y,
        color=col_color,
        color_continuous_scale=color_scale,
        labels={col_x: "Cidade", col_y: col_y.replace("_", " ").title()},
        title=title,
        text_auto=".1f"
    )
    fig.update_layout(**LAYOUT_PADRAO, height=height,
                      coloraxis_showscale=False)
    fig.update_traces(textfont_size=10, textangle=0, cliponaxis=False)
    return fig


# ─── Scatter oportunidades ────────────────────────────────────────
def scatter_oportunidades(
    df: pd.DataFrame,
    col_x: str = "variacao_12m",
    col_y: str = "cap_rate_anual",
    col_size: str = "preco_m2_venda",
    col_color: str = "score_investimento",
    height: int = 480
) -> go.Figure:
    """Mapa de oportunidades: Cap Rate vs Valorização."""
    fig = px.scatter(
        df,
        x=col_x, y=col_y,
        size=col_size,
        color=col_color,
        hover_name="cidade",
        color_continuous_scale="RdYlGn",
        labels={
            col_x:    "Valorização 12 meses (%)",
            col_y:    "Cap Rate Anual (%)",
            col_size: "Preço/m²",
            col_color: "Score",
        },
        title="📈 Mapa de Oportunidades: Cap Rate vs Valorização"
    )

    # Linhas de referência (mediana)
    med_x = df[col_x].median()
    med_y = df[col_y].median()

    fig.add_hline(y=med_y, line_dash="dash",
                  line_color="gray", opacity=0.5,
                  annotation_text=f"Mediana cap rate {med_y:.2f}%")
    fig.add_vline(x=med_x, line_dash="dash",
                  line_color="gray", opacity=0.5,
                  annotation_text=f"Mediana var. {med_x:.1f}%")

    fig.update_layout(**LAYOUT_PADRAO, height=height)
    return fig


# ─── Radar de comparação ──────────────────────────────────────────
def radar_chart_comparacao(
    df: pd.DataFrame,
    metricas: List[str],
    col_cidade: str = "cidade"
) -> go.Figure:
    """Radar normalizado para comparação entre cidades."""
    fig = go.Figure()

    for _, row in df.iterrows():
        vals = []
        for m in metricas:
            col_max = df[m].max()
            col_min = df[m].min()
            vals.append(
                round((row[m] - col_min) / (col_max - col_min + 1e-9), 3)
            )
        vals.append(vals[0])

        labels = [m.replace("_", " ").title() for m in metricas]
        labels.append(labels[0])

        fig.add_trace(go.Scatterpolar(
            r=vals, theta=labels,
            fill="toself", name=row[col_cidade], opacity=0.75
        ))

    fig.update_layout(
        **LAYOUT_PADRAO,
        polar=dict(radialaxis=dict(visible=True, range=[0, 1],
                                   tickformat=".1f")),
        title="🕸️ Comparação de Métricas (Normalizado)",
        height=500,
        showlegend=True,
        legend=dict(orientation="h", y=-0.1)
    )
    return fig


# ─── Gauge de score ───────────────────────────────────────────────
def gauge_score(
    score: float,
    cidade: str,
    categoria: str
) -> go.Figure:
    """Gauge (velocímetro) para visualização do score de investimento."""
    cor = ("#2e7d32" if score < 30 else
           "#ff8c00" if score < 50 else
           "#e53935" if score < 70 else
           "#7b1fa2")

    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=score,
        domain={"x": [0, 1], "y": [0, 1]},
        title={"text": f"Score de Investimento<br><b>{cidade}</b>",
               "font": {"size": 14}},
        delta={"reference": 50, "increasing": {"color": cor}},
        gauge={
            "axis":       {"range": [0, 100], "tickwidth": 1},
            "bar":        {"color": cor},
            "bgcolor":    "white",
            "borderwidth": 2,
            "steps": [
                {"range": [0,  30], "color": "#e8f5e9"},
                {"range": [30, 50], "color": "#fff9c4"},
                {"range": [50, 70], "color": "#ffe0b2"},
                {"range": [70, 100], "color": "#fce4ec"},
            ],
            "threshold": {
                "line": {"color": "black", "width": 3},
                "thickness": 0.75,
                "value": score
            }
        }
    ))
    fig.update_layout(
        **LAYOUT_PADRAO,
        height=280,
        annotations=[dict(
            text=f"<b>{categoria}</b>",
            x=0.5, y=0.15, showarrow=False,
            font=dict(size=16, color=cor)
        )]
    )
    return fig


# ─── Heatmap de variações mensais ─────────────────────────────────
def heatmap_variacao(
    df: pd.DataFrame,
    col_data: str = "data_referencia",
    col_cidade: str = "cidade",
    col_var: str = "variacao_mensal"
) -> go.Figure:
    """Heatmap: cidades × meses, colorido por variação mensal."""
    df_copy = df.copy()
    df_copy["mes_ano"] = pd.to_datetime(df_copy[col_data]).dt.strftime("%Y-%m")
    pivot = df_copy.pivot_table(
        index=col_cidade, columns="mes_ano",
        values=col_var, aggfunc="mean"
    ) * 100

    fig = px.imshow(
        pivot,
        color_continuous_scale="RdYlGn",
        color_continuous_midpoint=0,
        aspect="auto",
        title="🌡️ Heatmap de Variação Mensal (%)",
        labels=dict(x="Mês/Ano", y="Cidade", color="Var. (%)")
    )
    fig.update_layout(**LAYOUT_PADRAO, height=max(300, len(pivot) * 25 + 80))
    return fig


# ─── Histograma de distribuição ───────────────────────────────────
def histogram_distribuicao(
    df: pd.DataFrame,
    col: str = "preco_m2_venda",
    cidade_destaque: Optional[str] = None,
    height: int = 360
) -> go.Figure:
    """Histograma da distribuição de preços com destaque para a cidade."""
    fig = go.Figure()

    fig.add_trace(go.Histogram(
        x=df[col],
        nbinsx=20,
        marker_color=CORES["primaria"],
        opacity=0.75,
        name="Distribuição Nacional",
        hovertemplate="R\$ %{x:,.0f}<br>Cidades: %{y}<extra></extra>"
    ))

    if cidade_destaque:
        val = df[df["cidade"] == cidade_destaque][col]
        if not val.empty:
            fig.add_vline(
                x=float(val.iloc[0]),
                line_dash="dash",
                line_color=CORES["perigo"],
                line_width=2.5,
                annotation_text=f" {cidade_destaque}",
                annotation_font_color=CORES["perigo"]
            )

    media = df[col].mean()
    fig.add_vline(
        x=media, line_dash="dot",
        line_color=CORES["alerta"], line_width=1.5,
        annotation_text=f" Média R\${media:,.0f}",
        annotation_font_color=CORES["alerta"]
    )

    fig.update_layout(
        **LAYOUT_PADRAO,
        height=height,
        title="📊 Distribuição Nacional de Preços/m²",
        xaxis_title="R\$/m²",
        yaxis_title="Número de Cidades",
        bargap=0.05,
    )
    return fig

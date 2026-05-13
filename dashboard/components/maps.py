# dashboard/components/maps.py
"""
Componentes de mapa para o Dashboard.
Usa PyDeck (deck.gl) e Folium para visualizações geográficas.
"""

import pandas as pd
import numpy as np
import streamlit as st
import folium
from folium.plugins import MarkerCluster, HeatMap
import pydeck as pdk
from typing import Optional, List
import json


# ─── Mapa de bolhas (PyDeck) ──────────────────────────────────────
def bubble_map(
    df: pd.DataFrame,
    col_lat: str = "latitude",
    col_lon: str = "longitude",
    col_cidade: str = "cidade",
    col_preco: str = "preco_m2_venda",
    col_score: str = "score_investimento",
    zoom: int = 4
) -> Optional[pdk.Deck]:
    """
    Mapa de bolhas com PyDeck.
    Tamanho = preço/m², Cor = score de investimento.
    """
    df_map = df.dropna(subset=[col_lat, col_lon]).copy()
    if df_map.empty:
        return None

    max_preco = df_map[col_preco].max()
    df_map["radius"] = (df_map[col_preco] / max_preco * 60_000 + 5_000).astype(int)

    def score_to_color(score):
        """Converte score 0–100 em [R, G, B, A]."""
        if score < 30:   return [46,  125, 50,  200]
        if score < 50:   return [255, 193, 7,   200]
        if score < 70:   return [255, 87,  34,  200]
        return               [156, 39,  176, 200]

    df_map["color"] = df_map[col_score].apply(score_to_color)

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=df_map,
        get_position=[col_lon, col_lat],
        get_radius="radius",
        get_fill_color="color",
        get_line_color=[0, 0, 0, 100],
        pickable=True,
        stroked=True,
        line_width_min_pixels=1,
    )

    tooltip = {
        "html": (
            "<b>{cidade}</b><br>"
            "💰 Preço/m²: R\$ {" + col_preco + ":,.0f}<br>"
            "⭐ Score: {" + col_score + "}/100"
        ),
        "style": {
            "backgroundColor": "rgba(30,58,95,0.95)",
            "color": "white",
            "padding": "10px",
            "borderRadius": "8px"
        }
    }

    view_state = pdk.ViewState(
        latitude=-14.235, longitude=-51.925,
        zoom=zoom, pitch=0
    )

    return pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip=tooltip,
        map_style="mapbox://styles/mapbox/light-v10"
    )


# ─── Mapa de calor (Folium HeatMap) ──────────────────────────────
def heatmap_folium(
    df: pd.DataFrame,
    col_lat: str = "latitude",
    col_lon: str = "longitude",
    col_peso: str = "preco_m2_venda",
    titulo: str = "Mapa de Calor — Preços Imobiliários"
) -> folium.Map:
    """
    Mapa de calor Folium, intensidade proporcional ao preço/m².
    """
    df_map = df.dropna(subset=[col_lat, col_lon]).copy()

    m = folium.Map(
        location=[-14.235, -51.925],
        zoom_start=4,
        tiles="CartoDB positron"
    )

    # Normaliza pesos
    max_peso = df_map[col_peso].max()
    heat_data = [
        [row[col_lat], row[col_lon], row[col_peso] / max_peso]
        for _, row in df_map.iterrows()
        if pd.notna(row[col_lat]) and pd.notna(row[col_lon])
    ]

    HeatMap(
        heat_data,
        radius=40,
        blur=20,
        max_zoom=8,
        gradient={0.2: "#313695", 0.4: "#74add1",
                  0.6: "#fee090", 0.8: "#f46d43", 1.0: "#a50026"}
    ).add_to(m)

    folium.TileLayer(
        "CartoDB positron",
        name="Base Cartográfica"
    ).add_to(m)

    return m


# ─── Mapa de marcadores (Folium) ─────────────────────────────────
def markers_map_folium(
    df: pd.DataFrame,
    cidade_destaque: Optional[str] = None
) -> folium.Map:
    """
    Mapa com marcadores clicáveis por cidade.
    Popup exibe métricas de mercado.
    """
    df_map = df.dropna(subset=["latitude", "longitude"]).copy()

    m = folium.Map(
        location=[-14.235, -51.925],
        zoom_start=4,
        tiles="CartoDB positron"
    )

    cluster = MarkerCluster(name="Cidades").add_to(m)

    CAT_CORES = {
        "Muito Aquecido": "#7b1fa2",
        "Aquecido":       "#e53935",
        "Moderado":       "#ff8c00",
        "Estável":        "#2e7d32",
    }

    for _, row in df_map.iterrows():
        cat   = str(row.get("categoria_mercado", "Estável"))
        cor   = CAT_CORES.get(cat, "#2196F3")
        eh_destaque = (cidade_destaque and
                       cidade_destaque.lower() in str(row.get("cidade", "")).lower())

        popup_html = f"""
        <div style="font-family:Inter,sans-serif;width:220px;padding:8px">
            <h4 style="margin:0;color:{cor}">{row.get('cidade','')}</h4>
            <hr style="margin:6px 0">
            <p style="margin:3px 0">
                💰 <b>Venda:</b> R\$ {row.get('preco_m2_venda',0):,.0f}/m²
            </p>
            <p style="margin:3px 0">
                🏡 <b>Aluguel:</b> R\$ {row.get('preco_m2_aluguel',0):,.2f}/m²
            </p>
            <p style="margin:3px 0">
                📈 <b>Var. 12m:</b> {row.get('variacao_12m',0):+.1f}%
            </p>
            <p style="margin:3px 0">
                💹 <b>Cap Rate:</b> {row.get('cap_rate_anual',0):.2f}%
            </p>
            <p style="margin:3px 0">
                ⭐ <b>Score:</b> {row.get('score_investimento',0):.0f}/100
            </p>
            <span style="
                background:{cor};color:white;padding:2px 8px;
                border-radius:12px;font-size:11px
            ">{cat}</span>
        </div>
        """

        icon_color = "purple" if "Muito" in cat else (
            "red" if "Aquecido" in cat else
            "orange" if "Moderado" in cat else "green"
        )

        folium.Marker(
            location=[row["latitude"], row["longitude"]],
            popup=folium.Popup(popup_html, max_width=250),
            tooltip=f"{row.get('cidade','')} — R\${row.get('preco_m2_venda',0):,.0f}/m²",
            icon=folium.Icon(
                color="red" if eh_destaque else icon_color,
                icon="home" if eh_destaque else "info-sign",
                prefix="glyphicon"
            )
        ).add_to(cluster)

    folium.LayerControl().add_to(m)
    return m


# ─── Renderiza mapa Folium no Streamlit ──────────────────────────
def render_folium(m: folium.Map, height: int = 500):
    """Renderiza mapa Folium no Streamlit via HTML."""
    from streamlit.components.v1 import html as st_html
    html_str = m.get_root().render()
    st_html(html_str, height=height)

# processing/geo_enrichment.py
"""
Enriquecimento geográfico dos dados imobiliários.

Funcionalidades:
- Geocodificação de endereços e bairros
- Join espacial com polígonos de municípios (IBGE)
- Cálculo de distâncias a pontos de interesse
- Normalização e padronização de nomes de regiões
- Classificação de regiões em hierarquia (bairro → cidade → UF → região)
"""

import pandas as pd
import numpy as np
import logging
import re
from typing import Optional, Dict, Tuple, List
from dataclasses import dataclass, field
from io import BytesIO
import httpx

from ingestion.config import azure_config

logger = logging.getLogger(__name__)


# ─── Mapeamento estático de coordenadas das capitais brasileiras ───
CAPITAIS_COORDS: Dict[str, Tuple[float, float]] = {
    "São Paulo":        (-23.5505, -46.6333),
    "Rio de Janeiro":   (-22.9068, -43.1729),
    "Brasília":         (-15.7801, -47.9292),
    "Salvador":         (-12.9714, -38.5014),
    "Fortaleza":        (-3.7172,  -38.5433),
    "Belo Horizonte":   (-19.9191, -43.9386),
    "Manaus":           (-3.1190,  -60.0217),
    "Curitiba":         (-25.4290, -49.2671),
    "Recife":           (-8.0476,  -34.8770),
    "Porto Alegre":     (-30.0346, -51.2177),
    "Belém":            (-1.4558,  -48.5044),
    "Goiânia":          (-16.6869, -49.2648),
    "Florianópolis":    (-27.5954, -48.5480),
    "São Luís":         (-2.5297,  -44.3028),
    "Maceió":           (-9.6658,  -35.7353),
    "Natal":            (-5.7945,  -35.2110),
    "Teresina":         (-5.0920,  -42.8038),
    "Campo Grande":     (-20.4697, -54.6201),
    "João Pessoa":      (-7.1195,  -34.8450),
    "Aracaju":          (-10.9472, -37.0731),
    "Porto Velho":      (-8.7612,  -63.9004),
    "Macapá":           (0.0356,   -51.0705),
    "Rio Branco":       (-9.9754,  -67.8249),
    "Boa Vista":        (2.8235,   -60.6758),
    "Palmas":           (-10.2491, -48.3243),
    "Cuiabá":           (-15.5989, -56.0949),
    "Vitória":          (-20.3155, -40.3128),
    "São Paulo":        (-23.5505, -46.6333),
}

# Mapeamento UF → Região
UF_PARA_REGIAO: Dict[str, str] = {
    "AC": "Norte",      "AP": "Norte",     "AM": "Norte",
    "PA": "Norte",      "RO": "Norte",     "RR": "Norte",
    "TO": "Norte",      "AL": "Nordeste",  "BA": "Nordeste",
    "CE": "Nordeste",   "MA": "Nordeste",  "PB": "Nordeste",
    "PE": "Nordeste",   "PI": "Nordeste",  "RN": "Nordeste",
    "SE": "Nordeste",   "DF": "Centro-Oeste", "GO": "Centro-Oeste",
    "MT": "Centro-Oeste", "MS": "Centro-Oeste",
    "ES": "Sudeste",    "MG": "Sudeste",   "RJ": "Sudeste",
    "SP": "Sudeste",    "PR": "Sul",       "RS": "Sul",
    "SC": "Sul",
}

CIDADE_PARA_UF: Dict[str, str] = {
    "São Paulo": "SP", "Rio de Janeiro": "RJ", "Belo Horizonte": "MG",
    "Brasília": "DF",  "Curitiba": "PR",       "Porto Alegre": "RS",
    "Salvador": "BA",  "Fortaleza": "CE",       "Recife": "PE",
    "Manaus": "AM",    "Belém": "PA",           "Goiânia": "GO",
    "Florianópolis": "SC", "Natal": "RN",       "Maceió": "AL",
    "Teresina": "PI",  "Campo Grande": "MS",    "João Pessoa": "PB",
    "Aracaju": "SE",   "Porto Velho": "RO",     "Macapá": "AP",
    "Rio Branco": "AC", "Boa Vista": "RR",      "Palmas": "TO",
    "Cuiabá": "MT",    "Vitória": "ES",         "São Luís": "MA",
}


@dataclass
class GeoLocation:
    """Representa uma localização geográfica enriquecida."""
    nome_original: str
    nome_normalizado: str
    tipo: str                          # bairro, cidade, mesorregiao, uf, regiao
    cidade: Optional[str] = None
    uf_sigla: Optional[str] = None
    regiao: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    municipio_ibge_id: Optional[int] = None
    encontrado: bool = False
    fonte: str = "desconhecida"


class GeoEnrichmentProcessor:
    """
    Enriquece dados imobiliários com informações geográficas.
    Usa BrasilAPI + IBGE como fontes primárias.
    """

    BRASILAPI_BASE = "https://brasilapi.com.br/api"
    IBGE_BASE      = "https://servicodados.ibge.gov.br/api/v1"

    def __init__(self):
        self.blob_client     = azure_config.get_blob_client()
        self._municipios_df: Optional[pd.DataFrame] = None
        self._geocode_cache: Dict[str, GeoLocation] = {}

    # ──────────────────────────────────────────────────────────────
    # Normalização de texto
    # ──────────────────────────────────────────────────────────────
    @staticmethod
    def normalize_text(text: str) -> str:
        """Remove acentos e padroniza capitalização."""
        import unicodedata
        nfkd = unicodedata.normalize("NFKD", text)
        sem_acento = "".join(c for c in nfkd if not unicodedata.combining(c))
        return sem_acento.strip().title()

    @staticmethod
    def clean_region_name(name: str) -> str:
        """Limpa e padroniza nome de região."""
        name = str(name).strip()
        name = re.sub(r"\s+", " ", name)
        substitutions = {
            "Sao Paulo":        "São Paulo",
            "Sao paulo":        "São Paulo",
            "SAO PAULO":        "São Paulo",
            "Rio De Janeiro":   "Rio de Janeiro",
            "RIO DE JANEIRO":   "Rio de Janeiro",
            "Belo Horizonte":   "Belo Horizonte",
            "BELO HORIZONTE":   "Belo Horizonte",
        }
        return substitutions.get(name, name)

    # ──────────────────────────────────────────────────────────────
    # Carregamento de municípios (IBGE)
    # ──────────────────────────────────────────────────────────────
    def _load_municipios(self) -> pd.DataFrame:
        """Carrega tabela de municípios do Data Lake (Silver) ou IBGE."""
        if self._municipios_df is not None:
            return self._municipios_df

        try:
            container = self.blob_client.get_container_client("silver")
            data = (container
                    .get_blob_client("municipios.parquet")
                    .download_blob()
                    .readall())
            self._municipios_df = pd.read_parquet(BytesIO(data))
            logger.info("✅ Municípios carregados do Data Lake (Silver)")
        except Exception:
            logger.warning("⚠️ Silver indisponível — carregando IBGE on-the-fly")
            import asyncio
            from ingestion.ibge_ingestion import IBGEIngestion
            ingestor = IBGEIngestion()
            loop = asyncio.new_event_loop()
            self._municipios_df = loop.run_until_complete(
                ingestor.fetch_municipios()
            )
            loop.close()

        return self._municipios_df

    # ──────────────────────────────────────────────────────────────
    # Geocodificação
    # ──────────────────────────────────────────────────────────────
    def geocode_city(self, cidade: str) -> GeoLocation:
        """Geocodifica uma cidade, usando cache e fallback estático."""
        cidade_clean = self.clean_region_name(cidade)

        if cidade_clean in self._geocode_cache:
            return self._geocode_cache[cidade_clean]

        # 1) Tenta coordenadas estáticas (capitais)
        if cidade_clean in CAPITAIS_COORDS:
            lat, lon = CAPITAIS_COORDS[cidade_clean]
            uf = CIDADE_PARA_UF.get(cidade_clean, "")
            geo = GeoLocation(
                nome_original=cidade,
                nome_normalizado=cidade_clean,
                tipo="cidade",
                cidade=cidade_clean,
                uf_sigla=uf,
                regiao=UF_PARA_REGIAO.get(uf, ""),
                latitude=lat,
                longitude=lon,
                encontrado=True,
                fonte="estatico"
            )
            self._geocode_cache[cidade_clean] = geo
            return geo

        # 2) Tenta busca na tabela IBGE
        mun_df = self._load_municipios()
        mask = mun_df["municipio_nome"].str.contains(
            cidade_clean, case=False, na=False
        )
        matches = mun_df[mask]

        if not matches.empty:
            row = matches.iloc[0]
            uf = str(row.get("uf_sigla", ""))
            geo = GeoLocation(
                nome_original=cidade,
                nome_normalizado=cidade_clean,
                tipo="cidade",
                cidade=str(row["municipio_nome"]),
                uf_sigla=uf,
                regiao=UF_PARA_REGIAO.get(uf, ""),
                municipio_ibge_id=int(row["municipio_id"]),
                encontrado=True,
                fonte="ibge"
            )
            self._geocode_cache[cidade_clean] = geo
            return geo

        # 3) Fallback: não encontrado
        logger.warning(f"⚠️ Geocoding falhou para: {cidade}")
        geo = GeoLocation(
            nome_original=cidade,
            nome_normalizado=cidade_clean,
            tipo="cidade",
            encontrado=False,
            fonte="nenhuma"
        )
        self._geocode_cache[cidade_clean] = geo
        return geo

    # ──────────────────────────────────────────────────────────────
    # Enriquecimento de DataFrame
    # ──────────────────────────────────────────────────────────────
    def enrich_dataframe(self, df: pd.DataFrame,
                         col_cidade: str = "cidade") -> pd.DataFrame:
        """
        Adiciona colunas geográficas a um DataFrame com coluna de cidade.
        Colunas adicionadas: uf_sigla, regiao, latitude, longitude,
        municipio_ibge_id, geo_encontrado, geo_fonte.
        """
        logger.info(f"🗺️  Enriquecendo {len(df)} registros geograficamente...")

        cidades_unicas = df[col_cidade].dropna().unique()
        geos: Dict[str, GeoLocation] = {}

        for cidade in cidades_unicas:
            geos[cidade] = self.geocode_city(str(cidade))

        def _apply(row):
            geo = geos.get(row[col_cidade])
            if geo:
                return pd.Series({
                    "uf_sigla":         geo.uf_sigla or "",
                    "regiao":           geo.regiao or "",
                    "latitude":         geo.latitude,
                    "longitude":        geo.longitude,
                    "municipio_ibge_id": geo.municipio_ibge_id,
                    "geo_encontrado":   geo.encontrado,
                    "geo_fonte":        geo.fonte,
                })
            return pd.Series({
                "uf_sigla": "", "regiao": "", "latitude": None,
                "longitude": None, "municipio_ibge_id": None,
                "geo_encontrado": False, "geo_fonte": "erro",
            })

        geo_cols = df.apply(_apply, axis=1)
        df_enriched = pd.concat([df, geo_cols], axis=1)

        encontrados = df_enriched["geo_encontrado"].sum()
        logger.info(
            f"✅ Enriquecimento: {encontrados}/{len(df)} registros"
            f" geocodificados ({encontrados/len(df)*100:.1f}%)"
        )
        return df_enriched

    # ──────────────────────────────────────────────────────────────
    # Cálculo de distância
    # ──────────────────────────────────────────────────────────────
    @staticmethod
    def haversine_km(lat1: float, lon1: float,
                     lat2: float, lon2: float) -> float:
        """Distância em km entre dois pontos (fórmula de Haversine)."""
        R = 6371.0
        phi1, phi2 = np.radians(lat1), np.radians(lat2)
        dphi        = np.radians(lat2 - lat1)
        dlambda     = np.radians(lon2 - lon1)
        a = (np.sin(dphi / 2) ** 2
             + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2) ** 2)
        return R * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

    def add_distance_to_capital(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adiciona coluna 'dist_km_capital_uf' com a distância
        ao centro da capital do estado de cada cidade.
        """
        if "latitude" not in df.columns or "uf_sigla" not in df.columns:
            df = self.enrich_dataframe(df)

        capitais_uf = {
            uf: CAPITAIS_COORDS.get(cidade, (None, None))
            for cidade, uf in CIDADE_PARA_UF.items()
        }

        def _dist(row):
            if pd.isna(row.get("latitude")) or pd.isna(row.get("longitude")):
                return np.nan
            cap_coords = capitais_uf.get(row.get("uf_sigla"))
            if not cap_coords or None in cap_coords:
                return np.nan
            return round(
                self.haversine_km(
                    row["latitude"], row["longitude"],
                    cap_coords[0], cap_coords[1]
                ), 2
            )

        df["dist_km_capital_uf"] = df.apply(_dist, axis=1)
        return df

    # ──────────────────────────────────────────────────────────────
    # Construção do GeoDataFrame para mapas
    # ──────────────────────────────────────────────────────────────
    def build_map_dataframe(self, df_summary: pd.DataFrame) -> pd.DataFrame:
        """
        Cria DataFrame com lat/lon prontos para visualização em mapa.
        Usado pelo Dashboard (Folium / PyDeck).
        """
        df_map = self.enrich_dataframe(df_summary.copy())
        df_map = df_map.dropna(subset=["latitude", "longitude"])

        # Normaliza tamanho dos pontos pelo preço
        max_p = df_map["preco_m2_venda"].max()
        df_map["bubble_size"] = (df_map["preco_m2_venda"] / max_p * 80 + 10).round(1)

        # Cor por categoria de mercado
        color_map = {
            "Muito Aquecido": [123, 31,  162, 200],
            "Aquecido":       [244, 67,  54,  200],
            "Moderado":       [255, 152, 0,   200],
            "Estável":        [46,  125, 50,  200],
        }
        df_map["color"] = df_map["categoria_mercado"].map(
            lambda c: color_map.get(str(c), [100, 100, 100, 200])
        )
        return df_map

    # ──────────────────────────────────────────────────────────────
    # Busca CEP via BrasilAPI
    # ──────────────────────────────────────────────────────────────
    async def fetch_cep_info(self, cep: str) -> Optional[Dict]:
        """Busca informações de endereço a partir de um CEP."""
        cep_clean = re.sub(r"\D", "", cep)
        if len(cep_clean) != 8:
            return None
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                r = await client.get(
                    f"{self.BRASILAPI_BASE}/cep/v2/{cep_clean}"
                )
                if r.status_code == 200:
                    return r.json()
        except Exception as e:
            logger.warning(f"CEP lookup falhou para {cep}: {e}")
        return None

    # ──────────────────────────────────────────────────────────────
    # Pipeline completo
    # ──────────────────────────────────────────────────────────────
    def run(self, df: pd.DataFrame,
            col_cidade: str = "cidade") -> pd.DataFrame:
        """Executa enriquecimento geográfico completo."""
        logger.info("🗺️  Iniciando enriquecimento geográfico...")
        df_enriched = self.enrich_dataframe(df, col_cidade)
        df_enriched = self.add_distance_to_capital(df_enriched)

        # Salva resultado enriquecido no Silver
        try:
            buffer = BytesIO()
            df_enriched.to_parquet(buffer, index=False)
            buffer.seek(0)
            container = self.blob_client.get_container_client("silver")
            container.upload_blob(
                name="fipezap_geo.parquet",
                data=buffer,
                overwrite=True
            )
            logger.info("📦 Dados geoenriquecidos salvos em silver/fipezap_geo.parquet")
        except Exception as e:
            logger.warning(f"Não foi possível salvar no Azure: {e}")

        return df_enriched

# processing/__init__.py
"""
Módulo de processamento Medallion (Bronze → Silver → Gold).

Responsável por:
- Limpeza e validação de dados brutos (Bronze → Silver)
- Transformações, agregações e enriquecimento (Silver → Gold)
- Enriquecimento geográfico (geocodificação, joins espaciais)
"""

from processing.bronze_to_silver import BronzeToSilverProcessor
from processing.silver_to_gold import SilverToGoldProcessor
from processing.geo_enrichment import GeoEnrichmentProcessor

__all__ = [
    "BronzeToSilverProcessor",
    "SilverToGoldProcessor",
    "GeoEnrichmentProcessor",
]

"""
ETL Pipeline Package.

A modular ETL pipeline for e-commerce analytics with sentiment analysis and KPI generation.
"""

__version__ = "1.0.0"
__author__ = "Aymen Fkir"

from .main import ETLPipeline
from .models import ETLConfig, Response, Sentiments
from .extract import DataExtractor
from .transform import DataTransformer
from .load import DataLoader

__all__ = [
    "ETLPipeline",
    "main", 
    "ETLConfig",
    "Response",
    "Sentiments",
    "DataExtractor",
    "DataTransformer", 
    "DataLoader",
]
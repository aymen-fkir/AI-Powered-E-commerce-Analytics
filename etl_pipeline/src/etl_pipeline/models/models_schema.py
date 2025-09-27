from pydantic import BaseModel, Field
from typing import List


class Sentiments(BaseModel):
    """
        Data models for the ETL pipeline.
        This module contains all Pydantic models and data structures used throughout the pipeline.
    """
    item_id: int = Field(description="An identifier for product review given by user")
    sentiment: bool = Field(
        description="The user sentiment for the product based on review. True: positive, False: negative"
    )


class Response(BaseModel):
    """Model for the complete sentiment analysis response."""
    sentiments: List[Sentiments] = Field(
        description="A list of sentiments for given items.",
        min_length=25,
        max_length=25
    )


class ETLConfig(BaseModel):
    """Configuration model for ETL pipeline."""
    bucket_name: str
    path: str
    model: str
    batch_size: int = Field(default=25, description="Batch size for processing")
    files: List[str] = Field(default_factory=list,description="List of files to process")
    filesize: List[int] = Field(default_factory=list,description="List of files size")
    file_to_move: List[str] = Field(default_factory=list,description="List of files to move")
    system_prompt: str
    base_url: str
    destpath: str


class KPIResult(BaseModel):
    """Base model for KPI calculation results."""
    pass


class ShopKPI(KPIResult):
    """Model for shop-level KPI metrics."""
    shop: str
    average_profit: float
    positive_reviews: int
    negative_reviews: int
    likeness_score: float
    normalized_likeness_score: float


class UserKPI(KPIResult):
    """Model for user-level KPI metrics."""
    id: int
    average_spent: float
    positive_reviews: int
    negative_reviews: int
    likeness_score: float
    normalized_likeness_score: float


class DateKPI(KPIResult):
    """Model for date-based KPI metrics."""
    date: str
    average_profit_per_day: float
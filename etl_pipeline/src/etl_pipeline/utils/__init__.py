"""
Utility functions for the ETL pipeline.

This module contains common helper functions and utilities used across
different components of the ETL pipeline.
"""

import logging
from typing import List, Dict, Any
import polars as pl


logger = logging.getLogger(__name__)


def setup_logging(log_level: str = "INFO") -> None:
    """
    Setup logging configuration for the application.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def create_batches(data: pl.DataFrame, batch_size: int = 25) -> List[List[Dict[str, Any]]]:
    """
    Create batches from DataFrame data for processing.
    
    Args:
        data: Input Polars DataFrame
        batch_size: Size of each batch (default: 25)
        
    Returns:
        List of batches, where each batch is a list of dictionaries
    """
    data_dicts = data.to_dicts()
    batches = [
        data_dicts[i:i + batch_size] 
        for i in range(0, len(data_dicts), batch_size)
    ]
    
    logger.info(f"Created {len(batches)} batches from {len(data_dicts)} records")
    return batches


def generate_prompt(batch: List[Dict[str, Any]]) -> str:
    """
    Generate prompt text for AI model from a batch of data.
    
    Args:
        batch: List of data dictionaries containing id and review
        
    Returns:
        Formatted prompt string for the AI model
    """
    prompt = "items :"
    for item in batch:
        prompt += f"\n id : {item['id']} , review : {item['review']} \n"
    return prompt


def min_max_normalize(data: pl.DataFrame, column: str, new_column: str | None = None) -> pl.DataFrame:
    """
    Apply min-max normalization to a DataFrame column.
    
    Args:
        data: Input Polars DataFrame
        column: Column name to normalize
        new_column: Name for the normalized column (default: adds '_normalized' suffix)
        
    Returns:
        DataFrame with normalized column added
    """
    if new_column is None:
        new_column = f"{column}_normalized"
    
    min_score: float = data[column].min()  # type: ignore
    max_score: float = data[column].max()  # type: ignore
    
    if min_score == max_score:
        logger.warning(f"Column {column} has constant values, normalization will result in zeros")
        normalized_data = data.with_columns(pl.lit(0.0).alias(new_column))
    else:
        normalized_data = data.with_columns(
            ((pl.col(column) - min_score) / (max_score - min_score)).alias(new_column)
        )
    
    logger.debug(f"Normalized column {column} to {new_column}")
    return normalized_data


def validate_dataframe(df: pl.DataFrame, required_columns: List[str]) -> bool:
    """
    Validate that a DataFrame contains required columns.
    
    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        
    Returns:
        True if all required columns are present, False otherwise
    """
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        return False
    
    logger.debug("DataFrame validation passed")
    return True



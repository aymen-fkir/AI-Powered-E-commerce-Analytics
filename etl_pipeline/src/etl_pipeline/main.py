"""
Main ETL Pipeline Application.

This module orchestrates the complete ETL pipeline process, coordinating
data extraction, transformation, and loading operations.
"""

import logging
import os
import yaml
from dotenv import load_dotenv
from supabase import Client as SPClient
import os
from .models import ETLConfig
from .extract import DataExtractor
from .transform import DataTransformer
from .load import DataLoader
import asyncio

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ETLPipeline:
    """Main ETL Pipeline orchestrator."""
    def __init__(self, config: ETLConfig, sp_client: SPClient) -> None:
        self.config = config
        self.sp_client = sp_client
        self.extractor = DataExtractor(config=config, sp_client=sp_client)
        self.transformer = DataTransformer(config=config)
        self.loader = DataLoader(sp_client=sp_client, config=self.config)
    def run(self) -> None:
        """Runs the complete ETL pipeline."""
        try:
            # Step 1: Extract
            self.extractor.listFiles()
            if not self.extractor.config.files:
                logger.info("No files to process. Exiting pipeline.")
                return
            raw_data = self.extractor.downloadFiles()
            if raw_data.is_empty():
                logger.info("No data extracted. Exiting pipeline.")
                return
            logging.info("extraction process finished")
            # Step 2: Transform
            final_data,user_kpis,shop_kpis,date_kpis = self.transformer.transform(raw_data)

            if final_data.is_empty():
                logger.info("No data after transformation. Exiting pipeline.")
                return
            logging.info("transformation process finished")

            tables = [(user_kpis,"user_kpis","id"),
                    (shop_kpis,"shop_kpis","shop_id"),
                    (date_kpis,"date_kpis","date")]
            
            asyncio.run(self.loader.load(tables,final_data))
            logging.info("loading process finished")

            logger.info("ETL pipeline completed successfully.")
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")


if __name__ == "__main__":
    path = "/home/aymen/Desktop/my_work/data_engineer/.env"
    path = path if os.path.exists(path) else "/app/.env"
    if not os.path.exists(path):
        raise FileNotFoundError(f".env file not found at {path} , current dir is {os.getcwd()}")
    load_dotenv(path)
    url:str|None = os.getenv("project_url")
    key:str|None = os.getenv("project_key")
    if not url or not key:
        raise ValueError("Supabase URL or Key not found in environment variables.")
    
    sp_client:SPClient = SPClient(url,key)
    # Load configuration from YAML file
    config_path =  "/home/aymen/Desktop/my_work/data_engineer/config.yaml"
    config_path = config_path if os.path.exists(config_path) else "./config.yaml"

    with open(config_path, "r") as file:
        config_dict = yaml.safe_load(file)
    
    config = ETLConfig(**config_dict["ETLCONFIG"])
    etl_pipeline = ETLPipeline(config=config, sp_client=sp_client)
    etl_pipeline.run()
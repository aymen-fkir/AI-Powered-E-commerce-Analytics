"""
Data extraction module for the ETL pipeline.

This module handles all data extraction operations including file listing,
downloading, and initial data structuring.
"""

import logging
import io
import polars as pl
import tqdm
from supabase import Client as SPClient

from ..models import ETLConfig

logger = logging.getLogger(__name__)


class DataExtractor:
    """Handles data extraction from Supabase storage."""
    
    def __init__(self, sp_client: SPClient, config: ETLConfig) -> None:
        self.sp_client = sp_client
        self.config = config

    def listFiles(self)->None:
        """
        Lists all the files within a bucket.
        """
        try:
            
            response = (self.sp_client.storage
                        .from_(self.config.bucket_name)
                        .list(self.config.path,
                            {"sortBy": {"column": "created_at", "order": "asc"}}
                            )
                        )
            
            self.config.files = list(map(lambda res:res["name"] ,response))
            self.config.files.remove(".emptyFolderPlaceholder")
        except Exception as e:
            logging.error(f"Error listing files in bucket: {e}")
            self.config.files = []

    def downloadFiles(self)->pl.DataFrame:
        """
        Downloads a file from a private bucket .

        Note : the empty fike in the backet get ignored
        """

        data = pl.DataFrame()
        logging.info("started reading files")
        for file in tqdm.tqdm(self.config.files): 
            response = self.sp_client.storage.from_(self.config.bucket_name).download(f"{self.config.path}/{file}")
            response = response.decode("utf-8")
            temp_data :pl.DataFrame = pl.read_json(io.StringIO(response))
            data = pl.concat([data,temp_data],how="horizontal") 
            self.config.filesize.append(temp_data.shape[0])
        return data
    
    def extract(self)->pl.DataFrame:
        """
        extract process it list all files in the bucket , it down,oad them and structer them in dataframe
        """
        self.listFiles()
        data:pl.DataFrame = self.downloadFiles()
        return data
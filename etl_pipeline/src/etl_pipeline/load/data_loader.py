import logging
import polars as pl
from supabase import Client as SPClient

from ..models.models_schema import ETLConfig
import asyncio
import datetime
logger = logging.getLogger(__name__)


class DataLoader:
    """
        Data loading module for the ETL pipeline.
        This module handles all data loading operations including database operations
        and storage to various destinations (Supabase, files, etc.).
    """
    
    def __init__(self, sp_client: SPClient, config: ETLConfig) -> None:
        self.sp_client = sp_client
        self.config = config

    def saveTogold(self,data:pl.DataFrame)->None:
        try:
            file = data.write_json().encode("utf-8")
            filename = f"gold/final_data_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            (self.sp_client
                    .storage
                    .from_(self.config.bucket_name)
                    .upload(path=filename,
                            file=file,
                            file_options={"cache-control": "0","upsert":False}) # type:ignore
                    )
            logging.info("file uploaded to gold bucket successfully")
        except Exception as e:
            logging.error(f"Error uploading file to gold bucket: {e}")
            
    
    def moveFiles(self)->None:   
        files = []
        for file in self.config.file_to_move:
            try :
                (self.sp_client.
                    storage
                    .from_(self.config.bucket_name)
                    .move(f"{self.config.path}/{file}",f"{self.config.destpath}/{file}")
                    )
            except Exception as e:
                logging.error(f"Error moving file {file} to processed folder: {e}")
                files.append(file)
                continue
        self.config.file_to_move = files
    
    
    def UpsertKpis(self,data:pl.DataFrame,table_name:str,col:str)->None:
        try:
            records = data.to_dicts()
            self.sp_client.from_(table_name).upsert(records,on_conflict=col).execute()
            logging.info(f"inserted/updated {len(records)} records into {table_name} table successfully.")
        except Exception as e:
            logging.error(f"Exception during upserting KPIs: {e}")


    async def load(self,tables:list[tuple[pl.DataFrame,str,str]],final_data:pl.DataFrame)->None:
        asyncio_tasks = [asyncio.to_thread(self.UpsertKpis,table[0],table[1],table[2]) for table in tables]
        await asyncio.gather(*asyncio_tasks)
        self.saveTogold(final_data)
        self.moveFiles()
import requests 
import time
import json
from supabase import Client,create_client
import os
import uuid
from dotenv import load_dotenv
import yaml
from datetime import datetime
import logging
import polars as pl
logging.basicConfig(level=logging.INFO)

##test this file

class Collector:
    def __init__(self,url:str,apiKey,client:Client,bucket_name:str,path:str):
        self.url = url
        self.apiKey = apiKey
        self.client = client
        self.bucket_name = bucket_name
        self.path = path
        self.ids = [str(uuid.uuid4()) for _ in range(10_000)]
        self.shop_ids = [f"shop_{i}" for i in range(10_000)]

        
    def getData(self):
        try:
            response = requests.get(self.url, headers={"X-API-Key": self.apiKey})
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            raise Exception(f"HTTP error occurred: {http_err}")
        except requests.exceptions.RequestException as req_err:
            raise Exception(f"Request failed: {req_err}")
        except Exception as e:
            raise Exception(f"Unexpected error: {e}")

    def addUsers(self,data:pl.DataFrame)->pl.DataFrame:

        num_rows_df = data.height

        base_ids = pl.Series(
            self.ids,dtype=pl.String
        ).alias("id")

        num_unique_dates_in_range = base_ids.len()

        row_indices = pl.Series("row_index", range(num_rows_df))
        repeated_date_indices = row_indices % num_unique_dates_in_range

        generated_ids_column = pl.Series(
            "id",
            [base_ids[i] for i in repeated_date_indices]
        )

        # Add this new series as a new column to the DataFrame
        df_with_dates = data.with_columns(generated_ids_column)

        return df_with_dates
        
    def addShops(self,data:pl.DataFrame)->pl.DataFrame:
        num_rows_df = data.height

        base_ids = pl.Series(
            self.shop_ids,dtype=pl.String
        ).alias("shop_id")

        num_unique_dates_in_range = base_ids.len()

        row_indices = pl.Series("row_index", range(num_rows_df))
        repeated_shops_indices = row_indices % num_unique_dates_in_range

        generated_shop_ids_column = pl.Series(
            "shop_id",
            [base_ids[i] for i in repeated_shops_indices]
        )

        # Add this new series as a new column to the DataFrame
        df_with_dates = data.with_columns(generated_shop_ids_column)

        return df_with_dates    
    

    def upload(self,data:list[dict])->None:
        try:
            jsonBytes = json.dumps(data).encode("utf-8")
            filename = f"{self.path}/{datetime.now().isoformat()}_{uuid.uuid4()}.json"
            responce = self.client.storage.from_(self.bucket_name).upload(
                path=filename,
                file=jsonBytes,
                file_options={"content-type": "application/json","upsert":"True"},
            ) 
        except Exception as e:
            raise e
         
    def run_once(self):
        data = self.getData()
        self.upload(data)
        logging.info("Upload successful")


    def run_loop(self, max_size=10):
        data = []
        counter = 0
        while True:
            time.sleep(0.3)
            data.extend(self.getData())
            counter += 1
            if counter >= max_size:
                data = self.addUsers(pl.DataFrame(data))
                data = self.addShops(data)
                data = data.to_dicts()
                self.upload(data)
                logging.info("Upload successful")
                data.clear()
                counter = 0 

    def main(self,loop=True,max_size=10):
        if loop:
            self.run_loop()
        else:
            self.run_once()

if __name__ == "__main__":
    try:
        
        load_dotenv()
        url: str|None = os.getenv("project_url")
        key: str|None = os.getenv("project_key")
        apiKey: str|None = os.getenv("api_key")
        if not url or not key:
            raise ValueError("Missing environment variable: project_url or project key")
        client = create_client(url,key)

        script_current_dir = os.path.dirname(os.path.abspath(__file__))
        main_dir = os.path.dirname(script_current_dir)

        with open(os.path.join(main_dir,"config.yaml")) as file:
            config = yaml.safe_load(file)
        extract = Collector(
            url=config["mockaroo"]["url"],
            apiKey=apiKey,
            path=config["supabase"]["path_raw_data"],
            bucket_name=config["supabase"]["bucketName"],
            client=client
            )
        
        extract.main()
    except Exception as e:
        raise Exception(e)


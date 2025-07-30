import os
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from typing import List
import io
from supabase import Client as SPClient
import pandas as pd 
from datetime import datetime
import uuid
import json
import traceback
import tqdm
from ollama import Client
import yaml
import polars as pl
from datetime import date

# ------------------------------------------------------------------------------------------------------------
class ItemReview(BaseModel):
    item_name: str = Field(description="The name of the item being reviewed.")
    classification: str = Field(description="The classification of the item (e.g., Dessert, Pastry).")
    review: str = Field(description="A brief review of the item.")

class Response(BaseModel):
    reviews: List[ItemReview] = Field(description="A list of classifications and reviews for the provided items.")
# ------------------------------------------------------------------------------------------------------------

class Process:
    """
    A class to process a batch of items using the model API in a single request.
    """
    def __init__(self, client: Client|None, model: str,sp_client:SPClient,bucket_name:str,path:str) -> None:
        self.client = client
        self.model = model
        self.sp_client = sp_client
        self.bucket_name = bucket_name
        self.path = path
        self.prompt = """
            Please classify each of the following items and provide a brief review for each one.
            The items are:
            [
            {item_list_str}
            ]
            Return the result as a JSON array. Each item must be a valid JSON object with keys:
            - "item_name"
            - "classification"
            - "review"
            Use only double quotes for property names and string values.
            Return ONLY the JSON array. Do not add explanations or comments.
            """


    def getInfo(self, items: List[str],batch_size: int = 10000) -> List[dict]:
        """
        Sends batched item lists to the model API to get classification + reviews.
        Each batch is limited to `batch_size` items to avoid output truncation.
        """

        def chunk_items(data: list, size: int):
            """Yield successive n-sized chunks from a list."""
            for i in range(0, len(data), size):
                yield data[i:i + size]
        
        all_reviews: List[dict] = []
        list_of_batches = list(chunk_items(items, batch_size))
        if not self.client:
            return [{}]
        for batch_items in tqdm.tqdm(list_of_batches):
            # Build prompt
            item_list_str = ",\n".join([f'"{item}"' for item in batch_items])
            try:
                response = self.client.chat(
                    messages=[
                        {
                        'role': 'system',
                        'content': self.prompt.format(item_list_str=item_list_str),
                        }
                    ],
                    model=self.model,
                    format=Response.model_json_schema(),
                    )

                content = response.message.content
                if not content:
                    raise ValueError("model Responce is ",content)
                
                validated_response = Response.model_validate_json(content)
                all_reviews.extend([review.model_dump() for review in validated_response.reviews])
                
            except json.JSONDecodeError as e:
                # This catches errors if the response is not valid JSON
                raise ValueError(f"Failed to decode JSON from API response: {e}")
            except Exception as e:
                print(f"failed: {e}")
                traceback.print_exc()
        
        return all_reviews

    def addUsers(self,data:pl.DataFrame)->pl.DataFrame:
        ids = [str(uuid.uuid4()) for _ in range(10_000)]


        num_rows_df = data.height

        base_ids = pl.Series(
            ids,dtype=pl.String
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
        shop_ids = [f"shop_{i}" for i in range(10_000)]


        num_rows_df = data.height

        base_ids = pl.Series(
            shop_ids,dtype=pl.String
        ).alias("shop_id")

        num_unique_dates_in_range = base_ids.len()

        row_indices = pl.Series("row_index", range(num_rows_df))
        repeated_shops_indices = row_indices % num_unique_dates_in_range

        generated_dates_column = pl.Series(
            "shop_id",
            [base_ids[i] for i in repeated_shops_indices]
        )

        # Add this new series as a new column to the DataFrame
        df_with_dates = data.with_columns(generated_dates_column)

        return df_with_dates


    def getFiles(self) -> List[str]:
        """
        List all files in the 'crawler1' folder inside the specified bucket,
        sorted by creation time (ascending), and filter only JSON files.
        """
        try:
            response: List[dict] = self.sp_client.storage.from_(self.bucket_name).list(
                "crawler1",
                {"sortBy": {"column": "created_at", "order": "asc"}}
            )

            # Build full paths and filter only JSON files
            files: List[str] = [
                f"crawler1/{obj['name']}"
                for obj in response
                if 'name' in obj and "json" in obj["name"]
            ]

            return files
        except Exception as e:
            raise Exception(f"Failed to list files in Supabase bucket: {e}")

    def getListitems(self, files: List[str]) -> pl.DataFrame:
        """
        Download and parse each JSON file from Supabase Storage,
        extract the 'product_name' column, and return a combined list of product names.
        """
        try:
            df = pl.DataFrame()

            for file in files:
                    # Download file content as bytes
                    response = self.sp_client.storage.from_(self.bucket_name).download(file)

                    # Decode JSON and load into DataFrame
                    json_data: str = response.decode("utf-8")
                    tempDf: pl.DataFrame = pl.read_json(io.StringIO(json_data))

                    # Extract product names if column exists
                    if "product_name" in tempDf.columns:
                        df = pl.concat([df,tempDf])
                    else:
                        print(f"Warning: 'product_name' column not found in {file}")

            return df
        except Exception as e:
            raise Exception(f"getListItems error : {e}")
            
    def update(self,data:pl.DataFrame,products:list[dict])->pl.DataFrame:
            productsDf = pl.DataFrame(products)
            data = pl.concat([data,productsDf])
            data = self.addShops(data)
            data = self.addUsers(data)
            return data
    
    def saveTofile(self,data:pl.DataFrame)->None:
        try:

            for i in range(0,data.shape[0],50000):
                chunk = data[i:i+50000]
                #write to json and save it to supdabase
                json_bytes = chunk.write_json().encode("utf-8")
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"{self.path}/{timestamp}_{uuid.uuid4()}.json"

                # Upload to Supabase
                self.sp_client.storage.from_(self.bucket_name).upload(
                    path=filename,
                    file=json_bytes,
                    file_options={"content-type": "application/json", "upsert": "true"}
                )
        except Exception as e:
            raise e

    def main(self) -> None:
        """
        Main runner: fetch files, extract product names, and process sample info.
        """
        try:
            files: List[str] = self.getFiles()

            if not files:
                print("No files found in the bucket.")
                return

            data: pl.DataFrame = self.getListitems(files=files)

            # Call your getInfo logic on first 3 items
            responce:list[dict]|None = self.getInfo(items=data["product_name"].to_list())
            if not responce:
                raise ValueError("model responce is ",responce)
            
            data = self.update(data=data,products=responce)
            self.saveTofile(data=data)

        except Exception as e:
            print(f"ETL process failed: {e}")
            traceback.print_exc()

# ------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------------
# -----------------------------------------Main execution block-----------------------------------------------
#  -----------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    load_dotenv()
    url:str|None = os.getenv("project_url")
    spkey:str|None = os.getenv("project_key")

    if  not url or not spkey:
        raise ValueError("(GEMINI_KEY/PROJECT_URL/PROJECT_KEY) not found in environment variables.")

    try:
        script_current_dir = os.path.dirname(os.path.abspath(__file__))
        main_dir = os.path.dirname(script_current_dir)
        with open(os.path.join(main_dir,"config.yaml")) as file:
            config:dict = yaml.safe_load(file)

        # Define the list of items you want to process
        sp_client:SPClient = SPClient(url,spkey)
        client = Client(host=config["ollama"]["hostname"])
        # Instantiate the Process class with the list of items
        process:Process = Process(client=client,
                          sp_client=sp_client,
                          bucket_name=config["supabase"]["bucketName"],
                          model=config["ollama"]["model_name"], # Using a recommended model
                          path=config["supabase"]["path_review_data"])

        # Get and print the information for all items in one go
        process.main()

    except Exception as e:
        print(f"Failed to run process: {e}")
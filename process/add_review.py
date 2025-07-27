import os
from google.genai import  Client,types
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
    A class to process a batch of items using the Gemini API in a single request.
    """
    def __init__(self, client: Client, model: str,sp_client:SPClient,bucketName:str) -> None:
        self.client = client
        self.model = model
        self.sp_client = sp_client
        self.bucketName = bucketName
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

    
    def concatResponse(self,iteratore)->str:
        full_response_text = ""
        for chunk in iteratore:
            # Some chunks might be empty or contain safety metadata without text
            if hasattr(chunk, 'text'):
                full_response_text += chunk.text

        if not full_response_text:
            raise ValueError("The API response stream was empty.")
        else:
            return full_response_text

    def getInfo(self, items: List[str],batch_size: int = 100) -> List[dict]:
        """
        Sends batched item lists to the Gemini API to get classification + reviews.
        Each batch is limited to `batch_size` items to avoid output truncation.
        """

        def chunk_items(data: list, size: int):
            """Yield successive n-sized chunks from a list."""
            for i in range(0, len(data), size):
                yield data[i:i + size]
        
        all_reviews: List[dict] = []
        list_of_batches = list(chunk_items(items, batch_size))

        for batch_items in tqdm.tqdm(list_of_batches):
            # Build prompt
            item_list_str = ",\n".join([f'"{item}"' for item in batch_items])
            try:
                response_iteratore = self.client.models.generate_content_stream(
                    model=self.model,
                    contents=[self.prompt.format(item_list_str=item_list_str)],
                    config=types.GenerateContentConfig(
                        response_mime_type='application/json',
                        response_schema=Response,
                        max_output_tokens=8192,
                        temperature=0.9
                    ),
                )

                full_response = self.concatResponse(response_iteratore)

                validated_response = Response.model_validate_json(full_response)
                all_reviews.extend([review.model_dump() for review in validated_response.reviews])
                
            except json.JSONDecodeError as e:
                # This catches errors if the response is not valid JSON
                raise ValueError(f"Failed to decode JSON from API response: {e}")
            except Exception as e:
                print(f"failed: {e}")
                traceback.print_exc()
        
        return all_reviews

    
    def getFiles(self) -> List[str]:
        """
        List all files in the 'crawler1' folder inside the specified bucket,
        sorted by creation time (ascending), and filter only JSON files.
        """
        try:
            response: List[dict] = self.sp_client.storage.from_(self.bucketName).list(
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

    def getListitems(self, files: List[str]) -> pd.DataFrame:
        """
        Download and parse each JSON file from Supabase Storage,
        extract the 'product_name' column, and return a combined list of product names.
        """
        try:
            df = pd.DataFrame()

            for file in files:
                    # Download file content as bytes
                    response = self.sp_client.storage.from_(self.bucketName).download(file)

                    # Decode JSON and load into DataFrame
                    json_data: str = response.decode("utf-8")
                    tempDf: pd.DataFrame = pd.read_json(io.StringIO(json_data))

                    # Extract product names if column exists
                    if "product_name" in tempDf.columns:
                        df = pd.concat([df,tempDf])
                    else:
                        print(f"Warning: 'product_name' column not found in {file}")

            return df
        except Exception as e:
            raise e
            
    def update(self,data:pd.DataFrame,products:list[dict])->pd.DataFrame:
            productsDf = pd.DataFrame(products)
            data = pd.concat([data,productsDf],axis=1)
            return data
    
    def saveTofile(self,data:pd.DataFrame)->None:
        try:

            for i in range(0,data.shape[0],50000):
                chunk = data.iloc[i:i+50000]
                #write to json and save it to supdabase
                json_bytes = chunk.to_json(orient="records").encode("utf-8")
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"processed/{timestamp}_{uuid.uuid4()}.json"

                # Upload to Supabase
                self.sp_client.storage.from_(self.bucketName).upload(
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

            data: pd.DataFrame = self.getListitems(files=files[:1])

            # Call your getInfo logic on first 3 items
            responce:list[dict]|None = self.getInfo(items=data["product_name"].to_list())
            if not responce:
                raise ValueError("gemini responce is ",responce)
            
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
    key:str|None = os.getenv("gemini_key")
    url:str|None = os.getenv("project_url")
    spkey:str|None = os.getenv("project_key")

    if not key or not url or not spkey:
        raise ValueError("(GEMINI_KEY/PROJECT_URL/PROJECT_KEY) not found in environment variables.")

    try:
        # It's good practice to initialize the client within a try block
        client:Client = Client(api_key=key)

        # Define the list of items you want to process
        sp_client:SPClient = SPClient(url,spkey)

        # Instantiate the Process class with the list of items
        process:Process = Process(client=client,
                          sp_client=sp_client,
                          bucketName="datalake",
                          model="models/gemini-2.0-flash-lite", # Using a recommended model
                          )

        # Get and print the information for all items in one go
        process.main()

    except Exception as e:
        print(f"Failed to run process: {e}")
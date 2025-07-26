import os
from google.genai import  Client,types
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from typing import List
import io
from supabase import Client as SPClient
import pandas as pd 


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

    def get_info(self,items):
        """
        Constructs a prompt to process all items and sends it to the Gemini API.
        """
        # Create a formatted string of items for the prompt
        item_list_str = ",\n".join([f"'{item}'" for item in items])

        # Construct a detailed prompt asking the model to process the list of items
        prompt = f"""
        Please classify each of the following items and provide a brief review for each one.
        The items are:
        [
        {item_list_str}
        ]
        Return the result as a JSON object that strictly follows the provided schema.
        """

        try:
            response = self.client.models.generate_content(
                model=self.model,
                contents=[prompt],
                config=types.GenerateContentConfig(
                    response_mime_type='application/json',
                    response_schema=Response,
                ),
            )
            return response.text
        except Exception as e:
            # General exception for API errors or other issues
            raise Exception(f"An unexpected error occurred: {e}")
    
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

    def getListitems(self, files: List[str]) -> List[str]:
        """
        Download and parse each JSON file from Supabase Storage,
        extract the 'product_name' column, and return a combined list of product names.
        """
        products_names = []

        for file in files:
            try:
                # Download file content as bytes
                response = self.sp_client.storage.from_(self.bucketName).download(file)

                # Decode JSON and load into DataFrame
                json_data: str = response.decode("utf-8")
                df: pd.DataFrame = pd.read_json(io.StringIO(json_data))

                # Extract product names if column exists
                if "product_name" in df.columns:
                    products_names.extend(df["product_name"].dropna().to_list())
                else:
                    print(f"Warning: 'product_name' column not found in {file}")

            except Exception as e:
                print(f"Error processing file {file}: {e}")

        return products_names

    def main(self) -> None:
        """
        Main runner: fetch files, extract product names, and process sample info.
        """
        try:
            files: List[str] = self.getFiles()

            if not files:
                print("No files found in the bucket.")
                return

            products_names: List[str] = self.getListitems(files=files)

            if not products_names:
                print("No product names found in the files.")
                return

            # Call your get_info logic on first 3 items
            print(self.get_info(items=products_names[:3]))

        except Exception as e:
            print(f"ETL process failed: {e}")

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
                          model="models/gemini-2.0-flash", # Using a recommended model
                          )

        # Get and print the information for all items in one go
        process.main()

    except Exception as e:
        print(f"Failed to run process: {e}")
from supabase import Client as SPClient
from ollama import Client as OllamaClient
import polars as pl
import io
import tqdm
import logging
logging.basicConfig(level=logging.INFO)
import os
from dotenv import load_dotenv
import yaml
from pydantic import BaseModel,Field
from typing import List
import json
from typing import Optional

class Sentiments(BaseModel):
    id: int = Field(description="an identifier for prodcut review given by user in for id key ")
    sentiment: bool = Field(description="the user sentiment for the product base on review True : positive , False : negative")

class Response(BaseModel):
    sentiments: List[Sentiments] = Field(description="A list of sentiments for given item.")


class ETL:
    def __init__(self,sp_client:SPClient,ollama_client:OllamaClient,bucket_name:str,path:str,model:str) -> None:
        self.sp_client = sp_client
        self.ollama_client = ollama_client
        self.bucket_name = bucket_name
        self.path = path
        self.model = model
        self.prompt = """you are give a list of items each item contain these two elements 
        1 : id remrepresent the identifier for prodcut review
        2: user review 
        your job is to classify if review ethier negative (False) or positive : True

        items : {items}
        """

    
    def listFiles(self)->list[str]:
        """
        Lists all the files within a bucket.
        """
        try:
            response = (self.sp_client.storage
                        .from_(self.bucket_name)
                        .list(self.path,
                            {"sortBy": {"column": "created_at", "order": "asc"}}
                            )
                        )
            
            
            files_names = list(map(lambda res:res["name"] ,response))
            return  files_names
        except Exception as e:
            raise Exception(e)
        
    def downloadFiles(self,file_names:list[str])->pl.DataFrame:
        """
        Downloads a file from a private bucket .

        Note : the empty fike in the backet get ignored
        """

        data = pl.DataFrame()
        logging.info("started reading files")
        for file in tqdm.tqdm(file_names):
            if ".empty" in file:
                continue 
            response = self.sp_client.storage.from_(self.bucket_name).download(f"{self.path}/{file}")
            response = response.decode("utf-8")
            temp_data :pl.DataFrame = pl.read_json(io.StringIO(response))
            data = pl.concat([data,temp_data],how="horizontal") 
        return data
    
    def extract(self)->pl.DataFrame:
        """
        extract process it list all files in the bucket , it down,oad them and structer them in dataframe
        """

        files_names:list[str] = self.listFiles()
        data:pl.DataFrame = self.downloadFiles(file_names=files_names)
        return data
    
    def minMax(self,data:pl.DataFrame)->pl.DataFrame:
        
        min_score:float = data["likeness_score"].min() # type:ignore
        max_score: float = data["likeness_score"].max() # type:ignore


        normalized_scores_df = (data.
                                with_columns(
                                    ((pl.col("likeness_score") - min_score) / (max_score - min_score))
                                    .alias("normalized_likeness_score")
                                    )
                                )
        return normalized_scores_df
    
    def genrateKpis(self,data:pl.DataFrame)->pl.DataFrame:
        group_by_shop = data.lazy().group_by(pl.col("shop"))
        sales_by_shop = group_by_shop.agg(pl.col("price").mean().alias("average_profit")).collect(engine="gpu)
        count_reviews = group_by_shop.agg(pl.col("sentiment").sum().alias("postive_reviews"),
                                          (~pl.col("sentiment")).sum().alias("negative_reviews")
                                          )
        review_score = (count_reviews.
                        with_columns((
                            pl.col("positive_reviews")/
                            pl.when(pl.col("negative_reviews") > 1)
                            .then(pl.col("negative_reviews"))
                            .otherwise(1)).alias("likeness_score").cast(pl.Float64))
                        ).collect(engine="gpu")
        normalized_score = self.minMax(review_score)

        sales_by_shop = sales_by_shop.join(normalized_score,on="shope",how="left")
        # best categories (by reviews percentage) (todo)
        # Total sales by category (todo)
        
        return sales_by_shop
        

    
    # make a package that you use to reduce redandent function like this
    def sentmentAnalysis(self,batchs)->list[dict]:

        try:
            analysis = []
            for batch in batchs:
                response = self.ollama_client.chat(
                    messages=[{
                        "role":"system",
                        "content":self.prompt.format(items=batch)
                    }],
                    model=self.model,
                    format=Response.model_json_schema(),
                )
                content = response.message.content
                if not content:
                    raise ValueError("problem with model output")
                validated_response = Response.model_validate_json(content)
                analysis.extend([senitment.model_dump() for senitment in validated_response.sentiments])
            return analysis
        except json.JSONDecodeError as e:
            # This catches errors if the response is not 
            raise ValueError(f"Failed to decode JSON from API response: {e}")
        except Exception as e:
            raise Exception(e)
        
    

    def transform(self)->None:
        # add the transformation code 
        pass
    
    def load(self)->None:
        pass
    def run(self)->None:
        data = self.extract()
        print(data.head())


if __name__ == "__main__":
    load_dotenv()
    url:str|None = os.getenv("project_url")
    key:str|None = os.getenv("project_key")
    
    with open("../config.yaml") as file:
        config = yaml.safe_load(file)


    if not key or not url :
        raise ValueError(f"problem with project key")

    sp_client:SPClient = SPClient(url,key)
    ollama_client = OllamaClient(config["ollama"]["hostname"])
    etl = ETL(sp_client=sp_client,ollama_client=ollama_client,
              bucket_name=config["supabase"]["datalake"],
              path=config["supabase"]["path_review_data"],
              model=config["ollama"]["model_name"])
    
    logging.info("pipline is running")
    etl.run()

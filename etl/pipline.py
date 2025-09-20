from supabase import Client as SPClient
from openai import AsyncOpenAI,AsyncClient
from openai.types.chat import ChatCompletion
import asyncio
import polars as pl
import io
import tqdm
import logging
import os
from dotenv import load_dotenv
import yaml
from pydantic import BaseModel,Field
from typing import Any, List
import json

logging.basicConfig(level=logging.INFO)
BATCH_SIZE = 25

class Sentiments(BaseModel):
    item_id: int = Field(description="an identifier for prodcut review given by user in for id key")
    sentiment: bool = Field(description="the user sentiment for the product base on review True : positive , False : negative")

class Response(BaseModel):
    sentiments: List[Sentiments] = Field(description="A list of sentiments for given item.",min_length=BATCH_SIZE,max_length=BATCH_SIZE)


class ETL:
    def __init__(self,sp_client:SPClient,bucket_name:str,path:str,model:str) -> None:
        self.sp_client = sp_client
        self.client = AsyncOpenAI(base_url="http://localhost:8000/v1",api_key="sk-xxx")
        self.bucket_name = bucket_name
        self.path = path
        self.model = model
        self.system_prompt = """You are given a list of items, each item contains these two elements:
        1: item_id - represents the identifier for product review
        2: user review - the actual review text
        
        Your job is to classify if each review is either negative (False) or positive (True).
        
        You must return a JSON object with the following structure:
        {
            "sentiments": [
                {
                    "item_id": <review_id>,
                    "sentiment": <true_for_positive_false_for_negative>
                }
            ]
        }
        
        IMPORTANT: You must return exactly the same number of sentiment objects as input items, with each id matching the input id.
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
    

    def creatBatchs(self,data:pl.DataFrame,batch_size:int=BATCH_SIZE)->list[list[dict]]:
        """create batchs from the data to send it to the model
         Args:
            data (pl.DataFrame): The input data containing reviews.
            batch_size (int, optional): The size of each batch. Defaults to 25.
         Returns:
            list[list[dict]]: A list of batches, where each batch is a list of dictionaries.
        """
        data_dicts = data.to_dicts()
        batchs = [data_dicts[i:i + batch_size] for i in range(0, len(data_dicts), batch_size)]
        return batchs

    def generatePrompt(self,batchs:list[dict])->str:
        """generate prompt for the model
         Args:
            batchs (list[dict]): A batch of data containing reviews.
         Returns:
            str: The generated prompt string.
        """
        prompt = "items :"
        for item in batchs:
            prompt += f"\n item_id : {item['item_id']} , review : {item['review']} \n"
        return prompt


    async def generateSentiments(self,batch_prompt:str)-> ChatCompletion|Exception:
        try:
            response = await self.client.chat.completions.create(
                messages=[
                    {
                        "role": "system",
                        "content": self.system_prompt,
                    },
                    {
                        "role": "user",
                        "content": batch_prompt
                    }
                ],
                model=self.model,
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": "sentiment_analysis_response",
                        "description": "Response containing sentiment analysis for product reviews",
                        "schema": Response.model_json_schema(),
                        "strict": True
                    }
                },
                timeout=60
            )
            return response
        except Exception as e:
            return e

    async def sentimentAnaysisWorkflow(self,batch:list[dict])->str|None:
        batch_prompt = self.generatePrompt(batch)
        response = await self.generateSentiments(batch_prompt)
        if isinstance(response, Exception):
            logging.error(f"Error during sentiment analysis: {response}")
            return None
        content = response.choices[0].message.content
        return content


    def parseModelResponse(self,content:str)->list[dict]|None:
        try:
            validated_response = Response.model_validate_json(content)
            return [sentiment.model_dump() for sentiment in validated_response.sentiments]
        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode JSON from API response: {e}")
            return None
        except Exception as e:
            logging.error(f"An error occurred while parsing the model response: {e}")
            return None
        
    async def sentmentAnalysis(self, batchs, concurrency: int = 4) -> list[dict]:
        analysis = []
        nb_batchs = len(batchs)
        
        for i in range(0, nb_batchs, concurrency):
            contents:list[str|None] = await asyncio.gather(*[self.sentimentAnaysisWorkflow(batch) for batch in batchs[i:i+concurrency]])
                
            for c in range(len(contents)):
                empty_response = {"sentiments": [{"item_id": i*25+c*25+j, "sentiment": None} for j in range(BATCH_SIZE)]}
                if contents[c] is None:
                    logging.error("problem with model output")
                    analysis.extend([empty_response])
                    continue
                parsed_content = self.parseModelResponse(contents[c]) # type:ignore
                if parsed_content:
                    analysis.extend(parsed_content)
                else:
                    analysis.extend([empty_response])
        return analysis


    def KPIs(self, group_by_shop: Any ,colname:str,key:str)->pl.DataFrame:
        sales = group_by_shop.agg(pl.col("price").mean().alias(colname)).collect()
        count_reviews = group_by_shop.agg(pl.col("sentiment").sum().alias("positive_reviews"),
                                          (~pl.col("sentiment")).sum().alias("negative_reviews")
                                          ).collect()
        review_score = (count_reviews.
                        with_columns((
                            pl.col("positive_reviews")/
                            pl.when(pl.col("negative_reviews") > 0)
                            .then(pl.col("negative_reviews"))
                            .otherwise(1)).alias("likeness_score").cast(pl.Float64))
                        )
        normalized_score = self.minMax(review_score)

        sales = sales.join(normalized_score,on=key,how="left")
        return sales
    
    def generateShopKpis(self,data:pl.DataFrame)->pl.DataFrame:
        group_by_shop = data.lazy().group_by(pl.col("shop_id"))
        sales_by_shop = self.KPIs(group_by_shop,"average_profit","shop_id")
        return sales_by_shop

    def generateUserKpis(self,data:pl.DataFrame)->pl.DataFrame:
        group_by_user = data.lazy().group_by(pl.col("id"))
        sales_by_user = self.KPIs(group_by_user,"average_spent","id")
        return sales_by_user
    ## generate kpi based on the dates 
    def generateDateKpis(self,data:pl.DataFrame)->pl.DataFrame:
        group_by_date = data.lazy().group_by(pl.col("date"))
        sales_by_date = group_by_date.agg(pl.col("price").mean().alias("average_profit_per_day")).collect()
        return sales_by_date

    def transform(self,data:pl.DataFrame)->list[pl.DataFrame]:
        batchs = self.creatBatchs(data)
        analysis = asyncio.run(self.sentmentAnalysis(batchs))
        analysis_df = pl.DataFrame(analysis)
        final_data = data.join(analysis_df,on="item_id",how="left")
        user_kpis = self.generateUserKpis(final_data)
        shop_kpis = self.generateShopKpis(final_data)
        date_kpis = self.generateDateKpis(final_data)
        logging.info("transformation process finished")
        return [final_data,user_kpis,shop_kpis,date_kpis]
    
    def load(self)->None:
        #create the database on supabase
        #and load the final data to the gold bucket
        pass
    def run(self)->None:
        data = self.extract()
        logging.info("extraction process finished")
        final_data,user_kpis,shop_kpis,date_kpis = self.transform(data)
        logging.info("transformation process finished")
        print(final_data.head())
        print("*"*20)
        print(user_kpis.head())
        print("*"*20)
        print(shop_kpis.head())
        print("*"*20)
        print(date_kpis.head())
        #self.load()
        #logging.info("loading process finished")
        #load the final data to the gold bucket and the kpis to the database


if __name__ == "__main__":
    load_dotenv()
    url:str|None = os.getenv("project_url")
    key:str|None = os.getenv("project_key")
    
    path = os.path.abspath(os.getcwd())
    with open(f"{path}/config.yaml") as file:
        config = yaml.safe_load(file)


    if not key or not url :
        raise ValueError(f"problem with project key")

    sp_client:SPClient = SPClient(url,key)
    etl = ETL(sp_client=sp_client,
              bucket_name=config["supabase"]["bucketName"],
              path=config["supabase"]["path_review_data"],
              model=config["ollama"]["model_name"])
    
    logging.info("pipline is running")
    etl.run()
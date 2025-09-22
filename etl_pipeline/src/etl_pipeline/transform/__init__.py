"""
Data transformation module for the ETL pipeline.

This module handles all data transformation operations including sentiment analysis,
KPI generation, and data enrichment.
"""

import logging
import json
from typing import Any
import asyncio
import polars as pl
from openai import AsyncOpenAI
from openai.types.chat import ChatCompletion
from ..models import Response, ETLConfig
from ..utils import create_batches, generate_prompt, min_max_normalize
from tqdm import tqdm
logger = logging.getLogger(__name__)


class DataTransformer:
    """Handles data transformation including sentiment analysis and KPI generation."""

    def __init__(self, config: ETLConfig) -> None:
        self.client = AsyncOpenAI(base_url=config.base_url,api_key="key")
        self.config = config
    async def generateSentiments(self,batch_prompt:str)-> ChatCompletion|Exception:
        try:
            response = await self.client.chat.completions.create(
                messages=[
                    {
                        "role": "system",
                        "content": self.config.system_prompt,
                    },
                    {
                        "role": "user",
                        "content": batch_prompt
                    }
                ],
                model=self.config.model,
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
        batch_prompt = generate_prompt(batch)
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
                
    def calculate_file_state(self,nb_batch:int,index:int)->int:
        total_items = nb_batch * self.config.batch_size
        while total_items>0 and index < len(self.config.files):
            if self.config.filesize[index]>total_items:
                self.config.filesize[index] -= total_items
                total_items=0
            else:
                total_items -= self.config.filesize[index]
                self.config.filesize[index]=0
                self.config.file_to_move.append(self.config.files[index])
                index += 1
        return index
    

    async def sentmentAnalysis(self, batchs, concurrency: int = 4) -> list[dict]:
        analysis = []
        nb_batchs = len(batchs)
        index = 0
        for i in tqdm(range(0, nb_batchs, concurrency),unit="batch",unit_scale=concurrency):
            index = self.calculate_file_state(nb_batchs,index)
            contents:list[str|None] = await asyncio.gather(*[self.sentimentAnaysisWorkflow(batch) for batch in batchs[i:i+concurrency]])
                
            for c in range(len(contents)):
                empty_response = {"sentiments": [{"item_id": i*25+c*25+j, "sentiment": None} for j in range(self.config.batch_size)]}
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
        normalized_score = min_max_normalize(review_score,column="likeness_score",new_column="normalized_likeness_score")

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
        batchs = create_batches(data)
        analysis = asyncio.run(self.sentmentAnalysis(batchs))
        analysis_df = pl.DataFrame(analysis)
        try:
            final_data = data.join(analysis_df,on="item_id",how="left")
            user_kpis = self.generateUserKpis(final_data)
            shop_kpis = self.generateShopKpis(final_data)
            date_kpis = self.generateDateKpis(final_data)
            logging.info("transformation process finished")
            return [final_data,user_kpis,shop_kpis,date_kpis]
        except Exception as e:
            logging.error(f"Error during transformation: {e}")
            return [pl.DataFrame(),pl.DataFrame(),pl.DataFrame(),pl.DataFrame()]
    
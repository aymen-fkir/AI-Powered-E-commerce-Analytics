import polars as pl
from ollama import AsyncClient
from pydantic import BaseModel, Field
from typing import List
import json
import tqdm
import asyncio
from itertools import islice

# constants

BATCH_SIZE = 10
PATH = "/home/aymen/Desktop/my_work/data_engineer/data/data.parquet"

def read_data(path: str) -> list[dict]:

    data = pl.read_parquet(path)
    data = data.with_columns(
        pl.arange(1, data.height + 1).alias("item_id")
    )
    category_description = data[["item_id","description"]].to_dict(as_series=False)
    items = [dict(zip(category_description.keys(), values)) for values in zip(*category_description.values())]
    return items




class ItemReview(BaseModel):
    item_id: int = Field(description="unique identifier that is provided in the input.",title="item_id")
    classification: str = Field(description="The classification of the item (e.g.,Food,cloths).")
    review: str = Field(description="A brief review of the item.")

class Response(BaseModel):
    reviews: List[ItemReview] = Field(min_length=BATCH_SIZE,description="A list of classifications and reviews for the provided items.")



async def Chat(client,context):
    # set tempture to 1 
    response = await client.chat(
        messages=context,
        model="gemma-small:latest",
        format=Response.model_json_schema(),
        keep_alive=20,
        stream=False,
        options={
            "num_gpu": 30,
            }
        )
    return response
    


def dict_to_text(items: list[dict]) -> str:
    lines = []
    for item in items:
        line = "\n".join([f"{key} : {value}" for key, value in item.items()])
        lines.append(line)
    return "\n".join(lines)


def creatPrompt(batch_items):
    prompt_instruction = """
                You are a helpful assistant that classifies and reviews items.
                
                Each item has:
                - "item_id": unique id for each item
                - "description": the item's description
                
                Return a JSON array of objects with the following keys:
                - "item_id" : same as item_id from input
                - "category" : classification of item category  
                - "review" : small review 1-2 phrases max
                """
    
    # Build prompt
    number_of_items = len(batch_items)
    batch_items = dict_to_text(batch_items)
    context = [
                {'role': 'system',
                 'content': f"{prompt_instruction}"},
                {"role": "user", "content": f"The following {number_of_items} items need to be classified and reviewed:\n\n{batch_items}"},
            ]
    return context


def retry(result,BATCH_SIZE,batch_items):
    if len(result["reviews"]) != BATCH_SIZE:
        print(f"Expected {len(batch_items)} reviews, but got {len(result['reviews'])} \nwhat i got : {result} , handeling error")
        
        items_ids = [r["item_id"] for r in result["reviews"]] 
        rest = list(filter(lambda Id : Id not in items_ids , batch_items))
    else:
        rest = None
    return rest




def batch_iter(iterable, BATCH_SIZE):
    it = iter(iterable)
    while True:
        batch = list(islice(it, BATCH_SIZE))
        if not batch:
            break
        yield batch


# use async io to send multiple batches at the same time 
# process time : it used to be 13 days current time is 7 days to : 46.15% gain of process time
async def main():
    
    all_reviews = []
    rest = None
    items = read_data(PATH)
    items_iter = batch_iter(items, BATCH_SIZE)  # gives batches of 100 items
    client = AsyncClient("http://localhost:11434")
    # Initialize progress bar
    total_items = len(items)
    pbar = tqdm.tqdm(total=total_items, desc="Processing items", unit="items")
    processed_count = 0

    while True:
        if rest:
            batches = [rest]
            rest = None
        else:
            batches = [list(next(items_iter, [])) for _ in range(3)]
            batches = [b for b in batches if b]  # remove empty

        if not batches:
            break

        contexts = [creatPrompt(b) for b in batches]
        responses = await asyncio.gather(*(Chat(client, ctx) for ctx in contexts))

        for batch_items, response in zip(batches, responses):
            try:
                parsed = Response.model_validate_json(response.message.content)
                result = json.loads(parsed.model_dump_json())
                rest = retry(result, BATCH_SIZE, batch_items)
                
                # Only count as processed if no retry is needed
                if rest is None:
                    items_processed = len(batch_items)
                    processed_count += items_processed
                    pbar.update(items_processed)
                    pbar.set_postfix({"Processed": processed_count, "Reviews": len(all_reviews)})
                all_reviews.extend(result["reviews"])
                
            except json.JSONDecodeError as jd:
                print(f"JSONDecodeError: {jd}")
            except ValueError as ve:
                print(f"ValueError: {ve}")
            except Exception as e:
                print(f"Unexpected error: {e}")

    pbar.close()
    return all_reviews


asyncio.run(main())


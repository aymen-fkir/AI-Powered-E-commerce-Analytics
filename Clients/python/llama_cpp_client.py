import polars as pl
from openai import AsyncOpenAI
from pydantic import BaseModel, Field
from typing import List
import json
import tqdm
import asyncio
from itertools import islice

# constants
BATCH_SIZE = 5
PATH = "/home/aymen/Desktop/my_work/data_engineer/data/data.parquet"
LLAMA_CPP_BASE_URL = "http://localhost:8080/v1"


def read_data(path: str) -> list[dict]:

    data = pl.read_parquet(path)
    data = data.with_columns(
        pl.arange(1, data.height + 1).alias("item_id")
    )
    category_description = data[["item_id","description"]].to_dict(as_series=False)
    items = [dict(zip(category_description.keys(), values)) for values in zip(*category_description.values())]
    return items




class ItemReview(BaseModel):
    item_id: int = Field(description="unique identifier that is provided in the input.", title="item_id")
    classification: str = Field(description="The classification of the item (e.g.,Food,cloths).")
    review: str = Field(description="A brief review of the item.")

class Response(BaseModel):
    reviews: List[ItemReview] = Field(min_length=BATCH_SIZE,max_length=BATCH_SIZE, description="A list of classifications and reviews for the provided items.")

# ## Using AsyncIO with llama.cpp


# Initialize the async OpenAI client for llama.cpp
client = AsyncOpenAI(
    api_key="sk-no-key-required",  # llama.cpp doesn't require a real API key
    base_url=LLAMA_CPP_BASE_URL
)


async def Chat(client, context):
    response = await client.chat.completions.create(
        messages=context,
        model="gpt-3.5-turbo",  # This is ignored by llama.cpp, it uses whatever model is loaded
        response_format={"type": "json_object"},
        temperature=1,
        timeout=60
    )
    return response


def dict_to_text(items: list[dict]) -> str:
    lines = []
    for item in items:
        line = "\n".join([f"{key} : {value}" for key, value in item.items()])
        lines.append(line)
    return "\n".join(lines)


def createPrompt(batch_items):
    prompt_instruction = """
                You are a helpful assistant that classifies and reviews items.
                
                Each item has:
                - "item_id": unique id for each item
                - "description": the item's description
                
                Return a JSON object with a "reviews" array containing objects with the following keys:
                - "item_id" : same as item_id from input
                - "classification" : classification of item category  
                - "review" : small review 1-2 phrases max
                
                Example format:
                {
                  "reviews": [
                    {
                      "item_id": 1,
                      "classification": "Electronics",
                      "review": "Great product with excellent features."
                    }
                  ]
                }
                """
    
    # Build prompt
    number_of_items = len(batch_items)
    batch_items_text = dict_to_text(batch_items)
    context = [
                {'role': 'system',
                 'content': f"{prompt_instruction}"},
                {"role": "user", "content": f"The following {number_of_items} items need to be classified and reviewed. Please return exactly {number_of_items} reviews in JSON format:\n\n{batch_items_text}"},
            ]
    return context


def retry(result, BATCH_SIZE, batch_items):
    if len(result["reviews"]) != BATCH_SIZE:
        print(f"Expected {len(batch_items)} reviews, but got {len(result['reviews'])} \nwhat i got : {result} , handling error")
        
        items_ids = [r["item_id"] for r in result["reviews"]] 
        rest = [item for item in batch_items if item["item_id"] not in items_ids]
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
# using llama.cpp with OpenAI client
async def main():
    
    all_reviews = []
    rest = None
    items = read_data(PATH)
    items_iter = batch_iter(items, BATCH_SIZE)
    
    # Initialize progress bar
    total_items = len(items)
    pbar = tqdm.tqdm(total=total_items, desc="Processing items", unit="items")
    processed_count = 0
    end = False
    while end is False:
        if rest:
            batches = [rest]
            rest = None
        else:
            # Process fewer concurrent batches for llama.cpp to avoid overwhelming the server
            batches = [list(next(items_iter, [])) for _ in range(20)]  # Reduced from 20 to 5
            batches = [b for b in batches if b]  # remove empty

        if not batches:
            break

        contexts = [createPrompt(b) for b in batches]
        responses = await asyncio.gather(*(Chat(client, ctx) for ctx in contexts), return_exceptions=True)
        i = 0
        for batch_items, response in zip(batches, responses):
            try:
                    
                # Parse the response content
                content = response.choices[0].message.content # pyright: ignore[reportAttributeAccessIssue]
                result = json.loads(content)
                
                # Validate with pydantic
                validated_response = Response.model_validate(result)
                result = json.loads(validated_response.model_dump_json())
                
                rest = retry(result, BATCH_SIZE, batch_items)
                
                # Only count as processed if no retry is needed
                if rest is None:
                    items_processed = len(batch_items)
                    processed_count += items_processed
                    pbar.update(items_processed)
                    pbar.set_postfix({"Processed": processed_count, "Reviews": len(all_reviews)})
                
                all_reviews.extend(result["reviews"])
                
                
            except json.JSONDecodeError as jd:
                print(f"request {i}")
                print(f"JSONDecodeError: {jd}")
                print(f"Response content: {response.choices[0].message.content if hasattr(response, 'choices') else 'No content'}") # pyright: ignore[reportAttributeAccessIssue]
                print("*"*20)
                
                rest = batch_items  # Retry this batch
            except ValueError as ve:
                print(f"request {i}")
                print(f"ValueError: {ve}")
                rest = batch_items  # Retry this batch
                print("*"*20)

            except Exception as e:
                print(f"request {i}")
                print(f"Unexpected error: {e}")
                print(f"Response: {response}")
                rest = batch_items  # Retry this batch
                print("*"*20)
            finally:
                i += 1
        end = True
    pbar.close()
    return all_reviews


# Run the main function
asyncio.run(main())













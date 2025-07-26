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
logging.basicConfig(level=logging.INFO)


class Crawler:
    def __init__(self,url:str,apiKey,client:Client,bucket_name:str,path:str):
        self.url = url
        self.apiKey = apiKey
        self.client = client
        self.bucket_name = bucket_name
        self.path = path
        
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

        current_path = os.getcwd()
        with open(os.path.join(current_path,"config.yaml")) as file:
            config = yaml.safe_load(file)
        extract = Crawler(
            url=config["mockaroo"]["url"],
            apiKey=apiKey,
            path=config["supabase"]["path"],
            bucket_name=config["supabase"]["bucketName"],
            client=client
            )
        
        extract.main()
    except Exception as e:
        raise Exception(e)


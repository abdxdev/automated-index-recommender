import os
from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

load_dotenv()


def db_connect():
    mongo_uri = os.getenv("MONGO_URL")
    if mongo_uri:
        uri = mongo_uri
    else:
        uri = "mongodb://localhost:27017"

    client = MongoClient(uri, server_api=ServerApi('1'))

    return client

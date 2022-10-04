from pymongo import MongoClient
from pprint import pprint
from datetime import datetime
import json 
import pdb


if __name__ == "__main__":
    dbClient = MongoClient()

    if "logs" in dbClient.list_database_names():
        print('logs database exists')
        
    collNames = ["subsystems", "levels"] 
    logDB = dbClient["logs"]

    with open('collections.json') as f:
        metadata = json.load(f)
        presets = metadata.get('presets', None)
        for idx, name in enumerate(collNames):
            coll = logDB[name]
            docs = metadata.get(collNames[idx], {}) 
            for doc in docs:
                id = coll.insert_one(doc)

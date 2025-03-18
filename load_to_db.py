import json
from dagster import Out, op, get_dagster_logger, In
import pandas as pd
from pymongo import MongoClient
from bson import Binary, BSON

logger = get_dagster_logger()

# function to read a json file and return a python dictonary
def read_json_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            return data
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        return None
    except json.JSONDecodeError:
        print(f"Error: File '{file_path}' is not valid JSON.")
        return None
    

#this function will read the json file and store the data in mongo db
def json_load(json_file_path):

    #reada the json file
    json_data = read_json_file(json_file_path)
    df = pd.DataFrame(json_data)

    name = json_file_path.split(".")[0]

    fname = name + ".parquet"
    # will save a temporary file 
    df.to_parquet(fname)
    df = pd.read_parquet(fname)

    data_dict = df.to_dict(orient='records')

    #read the mongo db paramaters from the config file
    mongo_para = read_json_file("config.json")["mongo"]
    client = MongoClient(mongo_para.get("host"), mongo_para.get("port"))
    db = client['test-database2']
    coll = name.split("/", 1)[-1]

    collection = db[coll]
    #insert the datga into mongo db using insert many
    collection.insert_many(data_dict)
    return coll

@op()
def load_mongo(file1, file2, file3):
    # we will call the fundtion 3 times for each of the data files
    coll = json_load(file1)
    coll = json_load(file2)
    coll = json_load(file3)
    return "Done"
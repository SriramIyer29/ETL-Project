import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import pymongo
from dagster import get_dagster_logger
logger = get_dagster_logger()


#this function saves a data from to postgres sql using the to sql fucnion in the pandas
def data_frame_to_posgres(df, db_par, table_name):
    conn = psycopg2.connect(
        dbname = db_par.get("database"),
        user = db_par.get("username"),
        password = db_par.get("password"),
        host = db_par.get("host"),
        port = db_par.get("port")
    )
    connection_string = 'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'.format(**db_par)
    logger.info(connection_string)
    engine = create_engine(connection_string)
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    conn.close()


if __name__ == "__main__":
    
    client = pymongo.MongoClient("mongodb://localhost:27017/") 
    db = client["test-database"]
    collection = db["crash"]
    documents = collection.find({}, {"_id": 0})
    df = pd.DataFrame(documents)

    db_par = {"username": "postgres", "password": "mysecretpassword", "host": "127.0.0.1", "database":"test_db", "port": "5433"}
    file_name = "crash-data"
    data_frame_to_posgres(df, db_par, file_name)

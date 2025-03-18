from dagster import job, In, get_dagster_logger
from extract import *
from extract_merge import *
from load_to_db import *
from transform import *
logger = get_dagster_logger()
@job
def etl():
        extract_merge(transform(load_mongo(extract_vehical(), extract_people(), extract_crash())))
        


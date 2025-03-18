import requests
from dagster import Out, op, get_dagster_logger, In
from load_to_db import read_json_file

logger = get_dagster_logger()


# This functions download the respnse which we make to the url and saves it to a destination  mentioned
def download_file(url, destination):

    response = requests.get(url)
    
    if response.status_code == 200:
        with open(destination, 'wb') as f:
            f.write(response.content)
        logger.info("File downloaded successfully.")
    else:
        logger.info(f"Failed to download file. Status code: {response.status_code}")

# this will call the download file function
def extract(url, destination):
    ret = True
    try:
        download_file(url, destination)
    except:
        logger.error("Error occured")
        ret = False
    return destination



#individaul call tto the each of the data sets. We have used a config.json to sotre the api call url
@op(
    out=Out(str)
)
def extract_vehical():
    url = read_json_file("config.json")["data_source"]["vehicle"]["url"]
    destination = read_json_file("config.json")["data_source"]["vehicle"]["path"]
    return extract(url, destination)

@op(
    out=Out(str)
)

def extract_people():
    url = read_json_file("config.json")["data_source"]["people"]["url"]
    destination = read_json_file("config.json")["data_source"]["people"]["path"]
    return extract(url, destination)

@op(
    out=Out(str)
)
def extract_crash():
    url = read_json_file("config.json")["data_source"]["crash"]["url"]
    destination = read_json_file("config.json")["data_source"]["crash"]["path"]
    return extract(url, destination)
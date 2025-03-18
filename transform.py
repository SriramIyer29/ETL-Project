
from dagster import Out, op, get_dagster_logger, In
import pandas as pd
from pymongo import MongoClient
from mongo_to_sql import data_frame_to_posgres
from load_to_db import read_json_file
import numpy as np

logger = get_dagster_logger()

@op()
def transform(data):
    # we call the individaul transform stepms for each of athe data files
    crash_transform()
    transform_vehical()
    transform_people()
    return ""



def transform_people():
    # coll = coll.split(".", 1)[-1]
    mongo_para = read_json_file("config.json")["mongo"]
    client = MongoClient(mongo_para.get("host"), mongo_para.get("port"))
    db = client["test-database2"]
    collection = db["people"]
    #excluding these variables because they do not have data in them
    columns_to_exclude = ["_id", "zipcode", "drivers_license_state", "crash_date",
                    "drivers_license_class", "hospital", "ems_agency", "pedpedal_action", "ems_run_no",
                    "pedpedal_visibility", "pedpedal_location", "bac_resultvalue", "cell_phone_use"]
    # columns_to_exclude = ["_id"]
    query = {}
    projection = {}
    for col in columns_to_exclude:
        projection[col] = 0
    documents = collection.find(query, projection)
    df = pd.DataFrame(list(documents))

    #filling blank data as unknown
    df["sex"] = df["sex"].fillna('UNKNOWN')

    #filling the seat no data with value 9 which is unknown in the data set
    df["seat_no"] = df["seat_no"].fillna('9')

    #filling the safety_equipment data with usage unknown
    df["safety_equipment"] = df["safety_equipment"].fillna('USAGE UNKNOWN')
    
    #filling the ejection data with usage unknown
    df["ejection"] = df["ejection"].fillna('UNKNOWN')
    
    #filling the driver_action data with usage unknown
    df["driver_action"] = df["driver_action"].fillna('UNKNOWN')
    
    #filling the driver_action data with usage unknown
    df["physical_condition"] = df["physical_condition"].fillna('UNKNOWN')

    ##  Creating data types dictionary with current datatypes of dataframe and setting it all to varchar ,we will change the dictionary values of specific columns later to appropriate datatype
    ## and then using this dictionary to change datatypes of existing df to the ones in dictionary
    people_datatypes = dict(
        zip(df.columns,[object]*len(df.columns))        
    )

    for column in ["vehicle_id","age"]:
        people_datatypes[column] = np.float64

    df = df.astype(people_datatypes)

    
    db_par = read_json_file("config.json")["postgres"]
    file_name = "people"
    #this function saves the data to postgres 
    data_frame_to_posgres(df, db_par, file_name)
    return file_name


def transform_vehical():
    mongo_para = read_json_file("config.json")["mongo"]
    client = MongoClient(mongo_para.get("host"), mongo_para.get("port"))

    db = client["test-database2"]
    collection = db["vehicle"]


    columns_to_exclude = ["_id"]
    query = {}
    projection = {}
    for col in columns_to_exclude:
        projection[col] = 0
    documents = collection.find(query, projection)
    # # logger.info(len(list(documents)))
    # # ll = list(documents)
    # logger.info(list(documents)[0])
    vehicle_df = pd.DataFrame(list(documents))

    logger.info(len(vehicle_df))

    columns_to_be_dropped = ['unit_no','travel_direction', 'towed_i', 'towed_by', 'towed_to', 'area_00_i', 'area_01_i', 'area_02_i', 'area_03_i', 'area_04_i', 'area_05_i', 'area_06_i', 'area_07_i', 'area_08_i', 'area_09_i', 'area_10_i', 'area_11_i', 'area_12_i', 'area_99_i', 'cmv_id', 'usdot_no', 'ccmc_no', 'ilcc_no', 'commercial_src', 'gvwr', 'carrier_name', 'carrier_state', 'carrier_city', 'hazmat_placards_i', 'un_no', 'hazmat_report_i', 'mcs_report_i', 'mcs_vio_cause_crash_i', 'idot_permit_no', 'wide_load_i', 'trailer1_width', 'trailer2_width', 'trailer1_length', 'trailer2_length', 'cargo_body_type', 'load_type', 'hazmat_out_of_service_i', 'mcs_out_of_service_i', 'hazmat_class']
    vehicle_df.drop(columns_to_be_dropped, axis=1, inplace=True)   ##Dropping Some of these columns from the dataset as many of these columns are irrelevent and also have lots of missig values

    # Imputing nan values with 'UNKNOWN' in these below columns. This was done as these columns already has 'UNKNOWN' as its value
    columns_to_replace_nan = ['make', 'model','vehicle_defect','first_contact_point']
    vehicle_df[columns_to_replace_nan] = vehicle_df[columns_to_replace_nan].fillna('UNKNOWN')

    # Imputing nan values with 'UNKNOWN/NA' in these below columns
    columns_to_replace_nan_2 = ['vehicle_type','maneuver']
    vehicle_df[columns_to_replace_nan_2] = vehicle_df[columns_to_replace_nan_2].fillna('UNKNOWN/NA')

    # Imputing nan values with 'U' in these below columns
    columns_to_replace_nan_3 = ['hazmat_vio_cause_crash_i','hazmat_present_i']
    vehicle_df[columns_to_replace_nan_3] = vehicle_df[columns_to_replace_nan_3].fillna('U')

    # Selecting records having 'VEHICLE_YEAR' less than or equal to 2024
    vehicle_df['vehicle_year'] = pd.to_numeric(vehicle_df['vehicle_year'], errors='coerce')
    vehicle_df = vehicle_df[vehicle_df['vehicle_year'] <= 2024]

    #Removing rows with value 'PADESTRIAN' in column UNIT_TYPE as this dataset should contain only vehicle data
    vehicle_df = vehicle_df[(vehicle_df['unit_type'] != 'pedestrian') & (vehicle_df['unit_type'] != 'BICYCLE') & (vehicle_df['unit_type'] != 'EQUESTRIAN') & (vehicle_df['unit_type'] != 'NON-MOTOR VEHICLE')]

    #Removing column HAZMAT_NAME, AXLE_COUNT,VEHICLE_CONFIG,TOTAL_VEHICLE_LENGTH as they have more number of missing values
    columns_to_drop = ['hazmat_name', 'axle_cnt','vehicle_config','total_vehicle_length']
    vehicle_df.drop(columns=columns_to_drop, inplace=True)

    # Drop rows where UNIT_TYPE is null. We cant impute values to this feature as it is a categorical data.
    vehicle_df.dropna(subset=['unit_type'], inplace=True)

    # Replacing nan values with 'N' for columns 'FIRE_I','EXCEED_SPEED_LIMIT_I',CMRC_VEH_I. Assuming if the values for these columns
    # are not entered, by default it should be 'N'
    columns_to_replace_nan = ['fire_i','cmrc_veh_i']
    vehicle_df[columns_to_replace_nan] = vehicle_df[columns_to_replace_nan].fillna('N')

    #Dropping rows with nan values in column 'LIC_PLATE_STATE'
    vehicle_df = vehicle_df.dropna(subset=['lic_plate_state'])

    # Dropping 'NUM_PASSENGERS' as we already have 'OCCUPANT_CNT'
    vehicle_df.drop(columns=['num_passengers'], inplace=True)

    ##  Creating data types dictionary with current datatypes of dataframe and setting it all to varchar ,we will change the dictionary values of specific columns later to appropriate datatype
    ## and then using this dictionary to change datatypes of existing df to the ones in dictionary
    vehicle_datatypes = dict(
        zip(vehicle_df.columns,[object]*len(vehicle_df.columns))        
    )

    for column in ["vehicle_id","vehicle_year","occupant_cnt"]:
        vehicle_datatypes[column] = np.float64

    for column in ["crash_unit_id"]:
        vehicle_datatypes[column] = np.int64
    
    vehicle_df = vehicle_df.astype(vehicle_datatypes)


    db_par = read_json_file("config.json")["postgres"]
    file_name = "vehical"
    data_frame_to_posgres(vehicle_df, db_par, file_name)
    return file_name

def crash_transform():
    mongo_para = read_json_file("config.json")["mongo"]
    client = MongoClient(mongo_para.get("host"), mongo_para.get("port"))
    db = client["test-database2"]
    collection = db["crash"]
    columns_to_exclude = ["_id"]
    query = {}
    projection = {}
    for col in columns_to_exclude:
        projection[col] = 0
    documents = collection.find(query, projection)
    df = pd.DataFrame(list(documents))



    ## Removing all columns with very high missing values and redundant values /unnecssary columns


    df= df.drop(['latitude','longitude'],axis = 1)
    
    ### Since we already have crash hour ,day and month to visualize better than date data we removed CRASH_Date
    df= df.drop(['crash_date'],axis = 1)

    ## Removed these columns cause they have more than 90% missing values and their analysis will not help in terms of visualizations  
    df= df.drop(['photos_taken_i','statements_taken_i','dooring_i','work_zone_i','work_zone_type','workers_present_i'],axis = 1)   

    ## Removed these columns cause they have more than 80% missing values and their analysis will not help in terms of visualizations or analysis
    df= df.drop(['crash_date_est_i'],axis = 1)  

    ## Removed these columns cause they have more than 80% missing values and their analysis will not help us in any conclusion
    df= df.drop(['intersection_related_i','hit_and_run_i','private_property_i','lane_cnt'],axis = 1)  

    ## Removed these columns as we already have the mentioned information in Injuries total and MOST_SEVERE_INJURY columns 
    df= df.drop(['injuries_fatal','date_police_notified','injuries_incapacitating','injuries_non_incapacitating','injuries_reported_not_evident','injuries_no_indication','injuries_unknown','beat_of_occurrence','report_type'],axis = 1) 
    
    ## Now still we can see that we still have some columns with missing values but these are very low in comparison to ones we saw previously
    ## Now we will fill these NA values with value such as "Unknown" instead as dropping these columns do not make sense as they have lots of non NA values which can be used for analysis later
    df["most_severe_injury"]=df["most_severe_injury"].fillna("Unknown")

    df["injuries_total"]=df["injuries_total"].fillna(0)

    df["street_name"]=df["street_name"].fillna("Unknown")

    ## Now it does not make sense to put fill missing values as unknown in street direction and we cannot just randomly replace them with any existing values
    ## Hence as the number of NA values are very less in this regard we can drop just drop the rows containing these missing values

    df = df.dropna(subset=['street_direction'])

    ## Changing types of columns to correct type before feeding it to sql

    datatypes = dict(
        zip(df.columns,[object]*len(df.columns))
    )
    
    for column in ["posted_speed_limit","street_no","crash_hour","crash_hour","crash_month","num_units","injuries_total"]:
        datatypes[column] = np.int64
    
    df = df.astype(datatypes)
    
    db_par = read_json_file("config.json")["postgres"]
    logger.info(db_par)
    file_name = "crash"
    
    data_frame_to_posgres(df, db_par, file_name)
    return file_name


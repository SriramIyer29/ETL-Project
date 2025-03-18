
from dagster import Out, op, get_dagster_logger, In
import pandas as pd
from sqlalchemy import create_engine
from load_to_db import read_json_file
from psycopg2 import sql
import psycopg2
import docker
logger = get_dagster_logger()



# not used
def extract_merge2(data):
    logger.info("Compleated")


    db_par = read_json_file("config.json")["postgres"]
    connection_string = 'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'.format(**db_par)


    query_string = """
    SELECT * FROM crash_db c JOIN people_data p on c.CRASH_ID = p.CRASH_ID;
    """

    query_string="""CREATE TABLE IF NOT EXISTS people_vehhical AS  
        SELECT *
        FROM people
        LEFT JOIN vehical ON people.vehicle_id = vehical.vehicle_id;
    """

    engine = create_engine(connection_string) 
    with engine.connect() as connection:
        result = connection.execute(query_string)



#not used
def extract_merge3(f):
    return True
    dbname = "test_db"
    user = "postgres"
    password = "mysecretpassword"
    host = "localhost"
    port = "5433"

    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )

    cur = conn.cursor()


    table_name = "vehical"

    old_column_name = "vehicle_id"
    new_column_name = "vehicle_id_V"

    rename_column_query = f"ALTER TABLE {table_name} RENAME COLUMN {old_column_name} TO {new_column_name};"
    cur.execute(rename_column_query)
    conn.commit()

    # columns_to_drop = ["CRASH_RECORD_ID", "CRASH_DATE"]

    # for column_name in columns_to_drop:
    #     drop_column_query = f"ALTER TABLE {table_name} DROP COLUMN {column_name};"
    #     cur.execute(drop_column_query)


    merge_query = sql.SQL("""CREATE TABLE IF NOT EXISTS people_vehical AS  
        SELECT *
        FROM people
        LEFT JOIN vehical ON people.vehicle_id = vehical.vehicle_id_V;
    """)


    cur.execute(merge_query)
    conn.commit()

    print("Merge operation completed successfully. Data stored in table_c.")

    if cur:
        cur.close()
    if conn:
        conn.close()


#this step will merge the dat sets
@op()
def extract_merge(d):
    db_par = read_json_file("config.json")["postgres"]
    dbname = db_par.get("database")
    user = db_par.get("username")
    password = db_par.get("password")
    host = db_par.get("host")
    port = db_par.get("port")

    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )

    cur = conn.cursor()

    table_name = 'vehical_temp'

    # We will drop this tables if ti exists
    sql_query = f"DROP TABLE IF EXISTS {table_name};"
    cur.execute(sql_query)


    #we create a copy of the vehical tables and rename the common columns in both the data files
    original_table_name = 'vehical'
    new_table_name = 'vehical_temp'
    sql_query = f"""
    CREATE TABLE {new_table_name} AS
    SELECT * FROM {original_table_name};
    """

    cur.execute(sql_query)
    conn.commit()
    #renaming the crash_record id column
    table_name = "vehical_temp"

    old_column_name = "crash_record_id"
    new_column_name = "crash_record_id_v"

    rename_column_query = f"ALTER TABLE {table_name} RENAME COLUMN {old_column_name} TO {new_column_name};"
    cur.execute(rename_column_query)
    conn.commit()

    # executing he crash and vehical merge query
    merge_query = sql.SQL("""CREATE TABLE IF NOT EXISTS crash_vehical AS  
        SELECT *
        FROM crash
        LEFT JOIN vehical_temp ON crash.crash_record_id = vehical_temp.crash_record_id_v;
    """)


    cur.execute(merge_query)
    conn.commit()

    # we will drop this table after merging
    table_name = 'vehical_temp'

    # Execute the SQL query to drop the table
    sql_query = f"DROP TABLE IF EXISTS {table_name};"
    cur.execute(sql_query)

    # we will rename this common varaibles 
    table_name = "crash_vehical"

    old_column_name = "vehicle_id"
    new_column_name = "vehicle_id_cv"

    rename_column_query = f"ALTER TABLE {table_name} RENAME COLUMN {old_column_name} TO {new_column_name};"
    cur.execute(rename_column_query)
    conn.commit()

    old_column_name = "crash_record_id"
    new_column_name = "crash_record_id_cv"

    rename_column_query = f"ALTER TABLE {table_name} RENAME COLUMN {old_column_name} TO {new_column_name};"
    cur.execute(rename_column_query)
    conn.commit()


    # we merge tthe crash vehical data with people data set
    merge_query = sql.SQL("""CREATE TABLE IF NOT EXISTS crash_vehical_people AS  
        SELECT *
        FROM people
        LEFT JOIN crash_vehical ON people.vehicle_id = crash_vehical.vehicle_id_cv;
    """)


    cur.execute(merge_query)
    conn.commit()

    table_name = 'crash_vehical'

    # Execute the SQL query to drop the  crash vehcical data table
    sql_query = f"DROP TABLE IF EXISTS {table_name};"
    cur.execute(sql_query)
    conn.commit()
    logger.info("ETL process is compleated go to 127.0.0.1:8050 to view the dashboard")
    if cur:
        cur.close()
    if conn:
        conn.close()
    # this does not work when running in container
    # client = docker.from_env()
    # container_name_or_id = 'dap-project2-n2-plotly-1'
    # container = client.containers.get(container_name_or_id)
    # container.start()
    return True




import google.cloud.bigquery as bq
import pandas as pd
import pandas_gbq
from Project2_CloudAuth import google_cloud_authenticate

# set project parameters to use in google cloud
PROJECT_ID = "bigdata-269209"
table_name = "icu"
dataset_id = PROJECT_ID+".icu_manager"
chunksize = 10 ** 6

google_cloud_authenticate(PROJECT_ID)
BQ_CLIENT = bq.Client(PROJECT_ID)

# delete big query table if it exists
BQ_CLIENT.delete_dataset(dataset_id, delete_contents = True, not_found_ok = True)

# set bigQuery variable types
dtype = [
        {"name": "ROW_ID", "type": "INTEGER"},
        {"name": "SUBJECT_ID", "type": "INTEGER"},
        {"name": "HADM_ID", "type": "INTEGER"},
        {"name": "ICUSTAY_ID", "type": "INTEGER"},
        {"name": "ITEMID", "type": "INTEGER"},
        {"name": "CHARTTIME", "type": "TIMESTAMP"},
        {"name": "STORETIME", "type": "TIMESTAMP"},
        {"name": "CGID", "type": "INTEGER"},
        {"name": "VALUE", "type": "STRING"},
        {"name": "VALUENUM", "type": "FLOAT"},
        {"name": "VALUEUOM", "type": "STRING"},
        {"name": "WARNING", "type": "INTEGER"},
        {"name": "ERROR", "type": "INTEGER"},
        {"name": "RESULTSTATUS", "type": "STRING"},
        {"name": "STOPPED", "type": "STRING"},
        ]

# read dataframe from disc in chunks and process it
i = 0
for chunk in pd.read_csv("EVENTS.csv", chunksize=chunksize, low_memory=False):
    try:
        print("Processed " + str(((i * chunksize) * 100) / (1024 ** 2)) + "MB")
        # append data to big query table
        pandas_gbq.to_gbq(chunk, "icu_manager." + table_name, project_id=PROJECT_ID, if_exists="append", table_schema=dtype)
    except:
        print("Type error. Ignoring and continuing.")
    i = i+1


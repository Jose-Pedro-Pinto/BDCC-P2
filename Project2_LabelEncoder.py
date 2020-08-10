import google.cloud.bigquery as bq
import pandas_gbq
from Project2_CloudAuth import google_cloud_authenticate


# encode a column as a numerical variable, from 0 to number of distinct values
# receive source table, target table and target column
def encode_column(BQ_CLIENT, projectId, source, target, column):
    # obtain distinct values from source table
    query = BQ_CLIENT.query(
        '''
        SELECT DISTINCT %s FROM `%s` 
        ''' % (column, source))
    df = query.to_dataframe()

    # obtain one value for each item (0 to length of distinct values)
    encodedValues = list(range(0, len(df)))
    # place values and their encoding in a dataframe
    df.insert(loc=0, column="ENCODED"+column, value=encodedValues)

    # insert encoded item table to bigQuery
    pandas_gbq.to_gbq(df, target, project_id=projectId, if_exists="replace")


PROJECT_ID = "bigdata-269209"


def run():
    # set project parameters to use in google cloud
    dataset_id = PROJECT_ID + ".bdcc1920_project_datasets"
    table_name = "OneHotEncodedICU"

    google_cloud_authenticate(PROJECT_ID)
    BQ_CLIENT = bq.Client(PROJECT_ID)

    ds_id = '%s.%s' % (dataset_id, table_name)
    columns = ["ITEMID", "CGID", "VALUECAT", "VALUEUOM"]
    # encode each of the previous columns
    for column in columns:
        encode_column(BQ_CLIENT, PROJECT_ID, ds_id, "icu_manager.Encoded"+column+"ICU", column)


if __name__ == "__main__":
    run()

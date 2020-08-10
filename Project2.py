from pyspark import SparkContext
from pyspark.sql import SparkSession
import google.cloud.bigquery as bq
import google.cloud.storage as gcs
import pandas
import pandas_gbq

PROJECT_ID = "bigdata-269209"
table_name = "icu"
dataset_id = PROJECT_ID+".icu_manager"

def google_cloud_authenticate(projectId, keyFile=None, debug=True):
    import os
    if keyFile == None:
      keyFile='/content/bdcc-colab.json'
    if os.access(keyFile,os.R_OK):
      if debug:
        print('Using key file "%s"' % keyFile)
      os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '%s' % keyFile
      os.environ['GCP_PROJECT'] = projectId
      os.environ['GCP_ACCOUNT'] = 'bdcc-colab@' + projectId + '.iam.gserviceaccount.com'
      os.system('gcloud auth activate-service-account "$GCP_ACCOUNT" --key-file="$GOOGLE_APPLICATION_CREDENTIALS" --project="$GCP_PROJECT"')
      os.system('gcloud dataproc jobs submit spark --properties spark.jars.packages=com.google.cloud.spark:spark-bigquery_2.11:0.9.1-beta')
    os.system('gcloud info | grep -e Account -e Project')

google_cloud_authenticate(PROJECT_ID)
BQ_CLIENT = bq.Client(PROJECT_ID)

spark = SparkSession \
    .builder \
    .master('local[*]') \
    .getOrCreate()
sc = spark.sparkContext

sdf = spark.read.csv(
    "EVENTS_head.csv", header=True, mode="DROPMALFORMED", inferSchema=True
)

print(sdf.head())


BQ_CLIENT.delete_dataset(dataset_id, delete_contents = True, not_found_ok = True)
#pandas_gbq.to_gbq(sdf.toPandas(), "icu_manager."+table_name, project_id=PROJECT_ID)
sdf.saveAsBigQueryTable("bigdata-269209:icu_manager.icu")
#sdf.write.format('bigquery') \
#  .option("credentialsFile", keyFile) \
#  .option('table', PROJECT_ID+":icu_manager."+table_name) \
 # .save()


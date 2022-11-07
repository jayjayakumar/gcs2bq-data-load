import pandas as pd
import time
import pandas_gbq
from google.cloud import bigquery
from pads.io.json import build_data_schema
def data_load(request):
    #Initialize storage client
    client = storage.Client()
    # Read the file from GCS
    df = pd.read_csv('gs://yourfilelocation', encoding = 'utf-8')
    #Removing rows with all columns empty
    df=df.dropna(how='all')
    #Rename columns with any empty names to avoid erroring out during load
    df = df.rename(columns=lambda x:x.strip())
    #Initialize the BigQuery client, datsets and tables
    client = bigquery.Client(project ='enter project name')
    table_id = 'dataset_name.table_name'
    # Load to BigQuery (append if the table salready exists)
    df.to_gbq(table_id, if exists = 'append')
    # Create a backup of dataframe for DR
    df.to_csv("gs://your_bucket")
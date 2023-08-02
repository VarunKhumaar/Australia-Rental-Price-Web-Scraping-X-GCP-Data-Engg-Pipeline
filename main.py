import pandas as pd
from pandas.io import gbq
from google.cloud import bigquery

"""Triggered by a change to a Cloud Storage bucket"""
# https://www.youtube.com/watch?v=n3dMMgUdRdk (can use this for reference to load data from Cloud storage to Big query - using cloud function)

def hello_gcs(event, context):
    

    lst = []
    file_name = event['name']
    table_name = file_name.split('.')[0]

    # Event,File metadata details writing into Big Query
    dct={
         'Event_ID':context.event_id,
         'Event_type':context.event_type,
         'Bucket_name':event['bucket'],
         'File_name':event['name'],
         'Created':event['timeCreated'],
         'Updated':event['updated']
        }
    lst.append(dct)
    df_metadata = pd.DataFrame.from_records(lst)
    df_metadata.to_gbq('Domain.data_loading_metadata', 
                        project_id='data-engg-391707', 
                        if_exists='append',
                        location='us')
    
    # Actual file data , writing to Big Query
    df_data = pd.read_csv('gs://' + event['bucket'] + '/' + file_name)

    df_data.to_gbq('Domain.' + table_name, 
                        project_id='data-engg-391707', 
                        if_exists='append',
                        location='us')
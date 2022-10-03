from concurrent.futures import TimeoutError
from datetime import datetime
from lib2to3.pgen2.pgen import DFAState
from sqlite3 import Timestamp
from xmlrpc.client import Boolean
from google.cloud import bigquery
from google.cloud import storage
from google.cloud import pubsub_v1
import os
import json
from google.cloud.pubsub_v1.subscriber import message
import pandas as pd
from numpy import datetime64


# crediential_path = "/home/seok10code/anaconda3/envs/google/DA/cloocus-da-w-2022-2-c333712e5a7b.json"
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = crediential_path


project_id = "cloocus-da-w-2022-2"
subscription_id = "sub-cloocus-cc5-da-log"
timeout = 5.0
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
result = pd.DataFrame()

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    a = message.data.decode("utf-8")
    # print(a)
    data = json.loads(a)
    # print(data)
    df = pd.DataFrame(data['items'])
    # print(df)
    global result
    result = pd.concat([result, df],ignore_index=True)
    result.sort_values(by = 'publishedAt', inplace=True)
    # message.ack()
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")
# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result(timeout=timeout) 
    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.
        
        
        
        
        
print(result)
check = 0
result = result.drop_duplicates() # 중복데이터 삭제

for idx,  hval in enumerate(result['publishedAt']):
    # print(idx, hval)
    y= 'year=' + str(hval.split('-')[0])
    m= 'month=' + str(hval.split('-')[1])
    d, s=str(hval.split('-')[2]).split(' ')
    d = 'day=' + d 
    s= 'hour=' + s.split(':')[0]
    # print(y, m, d, d, s)
    if idx <=0:
        yt, mt, dt, st = y, m, d, s
    else:
        if yt > y or mt > m or dt > d or s > st:
            save = result[check:idx]
            # print(save)
            storage_clint = storage.Client()
            bucket = storage_clint.bucket("team2-bucket")
            blob = bucket.blob(f"streaming/{yt}/{mt}/{dt}/{st}/")
            blob.upload_from_string('resultishere')
            save.to_csv(f'gs://team2-bucket/streaming/{yt}/{mt}/{dt}/{st}/result.csv')
            check = idx+1
    yt, mt, dt, st = y, m, d, s
    
    
    
    
result['publishedAt'] = result['publishedAt'].astype('datetime64')
client = bigquery.Client(project='cloocus-da-w-2022-2')
# TODO(developer): Set table_id to the ID of the table to create.
table_id = "cloocus-da-w-2022-2.team2_table.youtuble_logData"
job_config = bigquery.LoadJobConfig(
    schema = [
    bigquery.SchemaField("kind", "STRING"),
    bigquery.SchemaField("videoId", "STRING"),
    bigquery.SchemaField("publishedAt", "DATETIME"),
    bigquery.SchemaField("channelId", "STRING"),
    bigquery.SchemaField("title", "STRING"),
    bigquery.SchemaField("description", "STRING")
],
        time_partitioning=bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="publishedAt",  # Name of the column to use for partitioning.
        expiration_ms=7776000000,  # 90 days.
    ),
)
load_job = client.load_table_from_dataframe(
    result, table_id, job_config=job_config
) 
result = load_job.result()
print("Written {} rows to {}".format(result.output_rows, result.destination))
print("Partitioning: {}".format(result.time_partitioning))
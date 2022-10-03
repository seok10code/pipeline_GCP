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
crediential_path = "/home/seok10code/anaconda3/envs/google/data-cloocus-ffd800735dd1.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = crediential_path
# TODO(developer)
project_id = "data-cloocus"
subscription_id = "cloocus_swkim_17"
timeout = 5.0
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
result = pd.DataFrame()

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    a = message.data.decode("utf-8")
    # print(a)
    data = json.loads(a)
    df = pd.DataFrame(data['Cloocus'])
    global result
    result = pd.concat([result, df],ignore_index=True)
    result.sort_values(by = 'date', inplace=True)
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
check = 0
result = result.drop_duplicates()
for idx, val in enumerate(result['date']):
    y= 'year=' + str(val.split('-')[0])
    m= 'month=' + str(val.split('-')[1])
    d, s=str(val.split('-')[2]).split(' ')
    d = 'day=' + d 
    s= 'hour=' + s.split(':')[0]
    if idx <=0:
        yt, mt, dt, st = y, m, d, s
    else:
        if yt > y or mt > m or dt > d or s > st:
            save = result[check:idx]
            storage_clint = storage.Client()
            bucket = storage_clint.bucket("cloocus-seok10code")
            blob = bucket.blob(f"pubsup/{yt}/{mt}/{dt}/{st}/")
            blob.upload_from_string('resultishere')
            save.to_csv(f'gs://cloocus-seok10code/pubsup/{yt}/{mt}/{dt}/{st}/result.csv')
            check = idx+1
    yt, mt, dt, st = y, m, d, s
result['date'] = result['date'].astype('datetime64')
client = bigquery.Client(project='data-cloocus')
# TODO(developer): Set table_id to the ID of the table to create.
table_id = "data-cloocus.cloocus_swkim.tableseok10"
job_config = bigquery.LoadJobConfig(
    schema = [
    bigquery.SchemaField("gender", "STRING"),
    bigquery.SchemaField("data", "INTEGER"),
    bigquery.SchemaField("Check", "STRING"),
    bigquery.SchemaField("date", "TIMESTAMP")   
],
        time_partitioning=bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="date",  # Name of the column to use for partitioning.
        expiration_ms=7776000000,  # 90 days.
    ),
)
load_job = client.load_table_from_dataframe(
    result, table_id, job_config=job_config
) 
result = load_job.result()
print("Written {} rows to {}".format(result.output_rows, result.destination))
print("Partitioning: {}".format(result.time_partitioning))
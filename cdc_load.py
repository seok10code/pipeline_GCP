from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import json
import pyspark
from pyspark import SparkContext
from google.auth import jwt
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession

project_id = "cloocus-workshop-2022"
subscription_id = "postgres.dbd0.dbd0_all_tables-sub"
# crediential_path = "/home/seok10code/anaconda3/envs/google/DA/cloocus-workshop-2022-d5ea9f012960.json"
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = crediential_path

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
result_list = {}
result_list['val'] = []
timeout = 15.0
project_id = "cloocus-workshop-2022"
dataset_id = "public"
table_id = "cdclog"
sc = SparkContext()
MAX_MESSAGES = 1000


with pubsub_v1.SubscriberClient() as subscriber:
    response = subscriber.pull(
        request={"subscription": subscription_path, "max_messages": MAX_MESSAGES},
    )

    if len(response.received_messages) == 0:
        pass
    
    for received_message in response.received_messages:
        pre_json = received_message.message.data.decode("utf-8")
        pre_json = pre_json.replace("'", "\"")
        data = json.loads(pre_json)
        appVal = data['payload']['after']
        appVal.update(data['payload']['source'])
        appVal.update({'op' : data['payload']['op']})

        # global result_list
        result_list['val'].append(appVal)



# streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
# print(f"Listening for messages on {subscription_path}..\n")
# # Wrap subscriber in a 'with' block to automatically call close() when done.
# with subscriber:
#     try:
#         # When `timeout` is not set, result() will block indefinitely,
#         # unless an exception is encountered first.
#         streaming_pull_future.result(timeout=timeout) 
#     except TimeoutError:
#         streaming_pull_future.cancel()  # Trigger the shutdown.
#         streaming_pull_future.result()  # Block until the shutdown is complete.

# file_path = "/home/seok10code/anaconda3/envs/google/DA/checkerF.json"

    jsonRDD = json.dumps(result_list['val'])
    dataset = sc.parallelize([jsonRDD])

spark = SparkSession.builder \
    .appName("PySpark PostgreSQL") \
    .master("yarn") \
    .getOrCreate()

bucket = "cloocus-gcs-dp-files"
spark.conf.set('temporaryGcsBucket', bucket)

remote_table = spark.read.json(dataset)

remote_table.createOrReplaceTempView("df")
select_df = spark.sql("SELECT * FROM df")
select_df.write\
    .mode("overwrite")\
    .format("bigquery")\
    .option('table', "{}:{}.{}".format(project_id, dataset_id, table_id))\
    .save()
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
import datetime
import os
from google.cloud import storage


project_id = "cloocus-workshop-2022"
# subscription_id = "postgres.dbd0.dbd0_all_tables-sub"
subscription_id = "ddc1.ddcet_cstlaw_ctypcd-sub"
# crediential_path = "/home/seok10code/anaconda3/envs/gcp/DA/cloocus-workshop-2022-d5ea9f012960.json"
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = crediential_path

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
result_list = {}
result_list['val'] = []
timeout = 15.0
project_id = "cloocus-workshop-2022"
dataset_id = "public"
table_id = "cdclog"
MAX_MESSAGES = 1000
flow_control = pubsub_v1.types.FlowControl(max_messages=100)	
sc = SparkContext()

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
        dt = datetime.datetime.fromtimestamp(data['payload']['source']["ts_ms"]/1000)
        # check = data['payload']['source']['table']
        # print(check)
        # print("="*40)
        dt_year = dt.strftime("%Y")
        dt_month = dt.strftime("%m")
        dt_day = dt.strftime("%d")
        table_name = data['payload']['source']['table']
        
        appVal = data['payload']['after']
        appVal.update(data['payload']['source'])
        appVal.update({'op' : data['payload']['op']})
        result_list['val'].append(appVal)
        bucket_name = "cloocus-gcs-cdc-log"
        contents = json.dumps(data)
        destination_blob_name = "{}/{}/{}/{}/{}.json".format(table_name, dt_year, dt_month, dt_day, data['payload']['source']['ts_ms'])
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(contents)

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
    



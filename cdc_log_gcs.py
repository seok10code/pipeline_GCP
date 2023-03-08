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
# import os
from google.cloud import storage


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
        appVal = data['payload']['after']
        appVal.update(data['payload']['source'])
        appVal.update({'op' : data['payload']['op']})
        result_list['val'].append(appVal)



    # print(result_list['val'])

    jsonRDD = json.dumps(result_list['val'])
    dataset = sc.parallelize([jsonRDD])

spark = SparkSession.builder \
    .appName("PySpark PostgreSQL") \
    .master("yarn") \
    .getOrCreate()

bucket = "cloocus-gcs-dp-files"
spark.conf.set('temporaryGcsBucket', bucket)



remote_table = spark.read.json(dataset)

loop = remote_table.select("ts_ms").collect()
for idx, val in enumerate(loop):
    hval = datetime.datetime.fromtimestamp(val)
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
            save = remote_table[check:idx]
            # print(save)
            storage_clint = storage.Client()
            bucket = storage_clint.bucket("cloocus-gcs-dp-logdata")
            blob = bucket.blob(f"logdata/{yt}/{mt}/{dt}/{st}/")
            blob.upload_from_string('resultishere')
            save.to_parquet(f'gs://cloocus-gcs-dp-logdata/logdata/{yt}/{mt}/{dt}/{st}/result.parquet')
            check = idx+1
    yt, mt, dt, st = y, m, d, s
    
    

# remote_table.createOrReplaceTempView("df")
# select_df = spark.sql("SELECT * FROM df")
# select_df.write\
#     .mode("overwrite")\
#     .format("bigquery")\
#     .option('table', "{}:{}.{}".format(project_id, dataset_id, table_id))\
#     .save()
    



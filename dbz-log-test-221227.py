
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import json
import datetime
from google.auth import jwt
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
import pyspark
from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession

# TODO(developer)
project_id = "cloocus-workshop-2022"
dataset_id = "public"
table_id = "change_dbz_cdctest"
MAX_MESSAGES = 100

# �뚮쭪�� Subscription�쇰줈 蹂�寃�
subscription_id = "ddc1.ddcet_cstlaw_ctypcd-sub"
subscription_name = f'projects/{project_id}/subscriptions/{subscription_id}'
timeout = 15.0

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
flow_control = pubsub_v1.types.FlowControl(max_messages=10)

result_list = {}
result_list['val'] = []

ack_ids = []

with pubsub_v1.SubscriberClient() as subscriber:
    response = subscriber.pull(
        request={"subscription": subscription_path, "max_messages": MAX_MESSAGES},
    )

    if len(response.received_messages) == 0:
        pass
    
    for received_message in response.received_messages:
        result_s= received_message.message.data.decode("utf-8") 
        result_r = result_s.replace("'", "\"")
        dict_j = json.loads(result_r)

        ts = dict_j['payload']['ts_ms']
        dt = datetime.datetime.fromtimestamp(ts/1000) 
        dt_year = dt.strftime("%Y")
        dt_month = dt.strftime("%m")
        dt_day = dt.strftime("%d")

        table_name = dict_j['payload']['source']['table']
        op = dict_j['payload']['op']
        if op == 'r': 
            ack_ids.append(received_message.ack_id)
        
        elif op == 'd':
            # dict_j['payload']['after'] = "null"
            beforeVal = dict_j['payload']['before']
            for k in beforeVal:
                beforeVal[k] = '' # appVal = "null"
            appVal = beforeVal
            appVal.update(dict_j['payload']['source'])
            appVal.update({'op' : dict_j['payload']['op']})
            result_list['val'].append(appVal)
            print(appVal)
            
            # message.ack()
        elif op == 'u':
            appVal = dict_j['payload']['after']
            # appVal �꾨옒泥섎읆 �� 以�
            # {'cd_ctyp': '4202', 'no_sral': 'Iyg=', 'nm_ctyp': '07009ad53c5ae9f6822f4e436f6c944d', 'dnt_rev': 1671440400128646, 'id_rev_prsn': 'f493d448567e449cdba7', 'ip_rev_prsn': '5ace36ad87a2b00', 'seq_pk': 367410}
            appVal.update(dict_j['payload']['source'])
            appVal.update({'op' : dict_j['payload']['op']})
            result_list['val'].append(appVal)
            print(appVal)
    
    # subscriber.acknowledge(
    #     request={"subscription": subscription_path, "ack_ids": ack_ids}
    # )


    sc = SparkContext()
    jsonRDD = json.dumps(result_list['val'])
    dataset = sc.parallelize([jsonRDD])

spark = SparkSession.builder \
    .appName("PySpark PostgreSQL") \
    .master("yarn") \
    .getOrCreate()

bucket = "cloocus-gcs-dp-files"
spark.conf.set('temporaryGcsBucket', bucket)
remote_table = spark.read.json(dataset)
remote_table = remote_table.withColumn("ts_ms", (col("ts_ms") / 1000).cast("timestamp"))
remote_table.createOrReplaceTempView("df")
select_df = spark.sql("SELECT * FROM df")
select_df.write\
    .mode("overwrite")\
    .format("bigquery")\
    .option('table', "{}:{}.{}".format(project_id, dataset_id, table_id))\
    .save()
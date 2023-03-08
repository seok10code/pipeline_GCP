from pyspark.sql import SparkSession
import sys
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import lower
import json
from pyspark import SparkContext
ip = "20.214.216.108"
port = 5432
# The Username should be in <username@hostname> format.
user = "datastudy"
passwd = "qlqjstjfwjd123!"
db = "postgres"

sc = SparkContext()

query = "(select * from pg_logical_slot_get_changes('jdbcy', NULL, NULL, 'pretty-print', '1', 'include-timestamp', 'true')) as transaction_logs"

spark = SparkSession.builder \
    .appName("PySpark PostgreSQL") \
    .config("spark.jars", "/postgresql-42.5.0.jar") \
    .master("yarn") \
    .getOrCreate()
    
bucket = "cloocus-gcs-dp-files"
spark.conf.set('temporaryGcsBucket', bucket)

dflog_init = (spark.read
  .format("jdbc")
  .option("driver", "org.postgresql.Driver")
  .option("url", "jdbc:postgresql://20.214.216.108:{0}/{1}".format( port, db ))
  .option("user", user)
  .option("password", passwd)
  .option("dbtable",query)
  .load()
)
# df = dflog_init.withColumn('items', explode("change"))
# dflog_init.withColumn("data", col("data"))
# data = dflog_init.select('data').rdd.map(lambda x: eval(x[0])['change']).collect()
# time = dflog_init.select('data').rdd.map(lambda x: eval(x[0])['timestamp']).collect()
# print(data)
# print(time)
# jsonRDD = json.dumps(data)
# dataset = sc.parallelize([jsonRDD])
# dflog_init = dflog_init.withColumn('test', explode('data'))

# dflogs = spark.read.option("inferSchema", "False").json(dataset)
project_id = "cloocus-workshop-2022"
dataset_id = "public"
table_id = "jdbclog"


dflog_init.createOrReplaceTempView("df")
# dflogs.write.format("parquet").option("path", f"gs://cloocus-gcs-init-full-load/{dataset_id}/{table_id}").save()
select_df = spark.sql("SELECT * FROM df")
select_df.write\
.mode("overwrite")\
.format("bigquery")\
.option('table', "{}:{}.{}".format(project_id, dataset_id, table_id))\
.save()
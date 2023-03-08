from pyspark.sql import SparkSession
import sys
from sqlalchemy import create_engine

ip = "20.214.216.108"
port = 5432
# The Username should be in <username@hostname> format.
user = "datastudy"
passwd = "qlqjstjfwjd123!"
db = "postgres"
dbtable = 'DDB0.DDBGT_UDTG_CTRT_ORDR'

db_table_list = ["DBD0.DBCAT_VENDOR",
"DBD0.DBCBT_SLIPDTIL",
"DDC1.DDCBT_BDGTCOMP_EST",
"DDC1.DDCCT_STOCK",
"DDC1.DDCET_CSTLAW_CTYPCD",
"DDT0.DDTBT_ACC",
"DDT0.DDTBT_AS_MTRL",
"DDT0.DDTBT_BDGT_INFO_TGT_SITE",
"DDC1.DDCCT_ORDRACPTCNTS",
"DGW51.DGWET_MBR_PAUS",
"DGW51.DGWET_MEMBER",
"DGW51.DGWET_PBRL_AGRT",
"DGW52.DGWKT_INFO_CSTM",
"DGW52.DGWKT_MBR_PAUS",
"DGW52.DGWKT_PBRL_AGRT",
"DGW82.DGWRT_LCT",
"DGW82.DGWRT_MDLE_CNTS",
"DGW82.DGWRT_RSVT"
]

project_id = "cloocus-workshop-2022"
# dataset_id = "test"
# table_id = "soomintest"

spark = SparkSession.builder \
    .appName("PySpark PostgreSQL") \
    .config("spark.jars", "/postgresql-42.5.0.jar") \
    .master("yarn") \
    .getOrCreate()

bucket = "cloocus-gcs-dp-files"
spark.conf.set('temporaryGcsBucket', bucket)

sql = """
select * from ddb0.ddbgt_udtg_ctrt_ordr limit 10
"""

# Read PostgreSQL DB table into dataframe

# df = spark.read \
#     .format("jdbc") \
#     .option("url","jdbc:postgresql://20.214.216.108:{0}/{1}".format( port, db ) )\
#     .option("query", sql)\
#     .option("user", user) \
#     .option("password", passwd) \
#     .option("driver", "org.postgresql.Driver") \
#     .load()

for idx, table in enumerate(db_table_list):
    dataset_id, table_id = table.split(".")
    print(dataset_id, table_id)
    remote_table = (
    spark.read.format("jdbc") \
        .option("driver", "org.postgresql.Driver")
        .option("url", "jdbc:postgresql://20.214.216.108:{0}/{1}".format( port, db ))
        .option("dbtable", table.lower())
        .option("user", user)
        .option("password", passwd)
        .load()
    )
    remote_table.createOrReplaceTempView(f"df{idx}")
    select_df = spark.sql(f"SELECT * FROM df{idx}")
    select_df.write\
    .mode("overwrite")\
    .format("bigquery")\
    .option('table', "{}:{}.{}".format(project_id, dataset_id, table_id))\
    .save()
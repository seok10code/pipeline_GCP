import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.conf import SparkConf
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import concat, col, lit, to_timestamp
from datetime import datetime

## @params: [JOB_NAME]
#0. Define Glue job arguments from Glue job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME','raw_s3_path','catalog','iceberg_s3_path','database','table_name','primary_key','partition_key'])

# Examples of Glue Job Parameters
# raw_s3_path : s3://icebergglueblog-raws3bucket-winby5ygqhi3/full-load
# catalog : glue_catalog
# iceberg_s3_path : s3://icebergglueblog-curateds3bucket-140bn8buyn1gn
# database : human_resources
# table_name : employee_details
# primary_key : emp_no
# partition_key : department

#1. Set variables
RAW_S3_PATH = args.get("raw_s3_path")
CATALOG = args.get("catalog")
ICEBERG_S3_PATH = args.get("iceberg_s3_path")
DATABASE = args.get("database")
TABLE_NAME = args.get("table_name")
PK = args.get("primary_key")
PARTITION = args.get("partition_key")
DYNAMODB_LOCK_TABLE = f'{TABLE_NAME}_lock'

#2. Set the Spark Configuration of Apache Iceberg. You can refer the Apache Iceberg Connector Usage Instructions. 
def set_iceberg_spark_conf() -> SparkConf:
       conf = SparkConf() \
           .set(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog") \
           .set(f"spark.sql.catalog.{CATALOG}.warehouse", ICEBERG_S3_PATH) \
           .set(f"spark.sql.catalog.{CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
           .set(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
           .set(f"spark.sql.catalog.{CATALOG}.lock-impl", "org.apache.iceberg.aws.glue.DynamoLockManager") \
           .set(f"spark.sql.catalog.{CATALOG}.lock.table", DYNAMODB_LOCK_TABLE) \
           .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
       return conf

#3. Set the Spark + Glue context
conf = set_iceberg_spark_conf()
glueContext = GlueContext(SparkContext(conf=conf))
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#4. Read data from S3 location where is full-loaded by DMS.

fullDyf = glueContext.create_dynamic_frame_from_options(
    connection_type='s3',
    connection_options={
        'paths': [f'{RAW_S3_PATH}/{DATABASE}/{TABLE_NAME}/'],
        'groupFiles': 'none',
        'recurse': True
    },
    format='parquet',
    transformation_ctx='fullDyf')

print(f"Count of data after last job bookmark:{fullDyf.count()}")
fullDf = fullDyf.toDF()

if(fullDyf.count() > 0):
#5. Create Apache Iceberg Table from S3
    dropColumnList = ['Op','schema_name','table_name']
    
    current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    fullDf = fullDf.drop(*dropColumnList).withColumn('update_ts_dms',to_timestamp(col('update_ts_dms')))
    fullDf = fullDf.withColumn('last_applied_date',to_timestamp(lit(current_datetime)))
    fullDf.createOrReplaceTempView(f"{TABLE_NAME}_full")
    
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{DATABASE}")
    existing_tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{DATABASE};")

    df_existing_tables = existing_tables.select('tableName').rdd.flatMap(lambda x:x).collect()
    if f"{TABLE_NAME}_iceberg" not in df_existing_tables:
        print(f"Table {TABLE_NAME}_iceberg does not exist in Glue Catalog. Creating it now.")
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {CATALOG}.{DATABASE}.{TABLE_NAME}_iceberg
            USING iceberg 
            TBLPROPERTIES ('format-version'='2')
            PARTITIONED BY ({PARTITION})
            as (SELECT * from {TABLE_NAME}_full)""")
    else:
        print(f"Table {TABLE_NAME}_iceberg already exists")

#6. Read data from Apache Iceberg Table
    spark.sql(f"SELECT * FROM {CATALOG}.{DATABASE}.{TABLE_NAME}_iceberg limit 5").show()
    print(f"Total count of {TABLE_NAME}_Iceberg Table Results:\n")
    countDf = spark.sql(f"SELECT count(*) FROM {CATALOG}.{DATABASE}.{TABLE_NAME}_iceberg")
    print(f"{countDf.show()}")
    print(f"Iceberg data load is completed successfully.") 
else:
    print(f"No Data changed.")

print(f"Glue Job is completed successfully.")    
job.commit()
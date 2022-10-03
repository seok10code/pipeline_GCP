from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import numpy as np

from pyspark.sql.types import DateType, IntegerType, StringType, StructType, StructField, TimestampType
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import *
from pyspark.context import SparkContext




# crediential_path = "/home/seok10code/anaconda3/envs/google/DA/cloocus-da-w-2022-2-c333712e5a7b.json"
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = crediential_path


spark  = SparkSession\
    .builder\
    .master('yarn')\
    .appName("test")\
    .getOrCreate()

instaSchema = StructType([
    StructField("sid", IntegerType(), True),
    StructField("sid_profile", IntegerType(), True),
    StructField("post_id", StringType(), True),
    StructField("profile_id", IntegerType(), True),
    StructField("location_id", IntegerType(), True),
    StructField("cts", TimestampType(), True),
    StructField("post_type", IntegerType(), True),
    StructField("description", StringType(), True),
    StructField("numbr_like", IntegerType(), True),
    StructField("number_comments", IntegerType(), True)
                             ])
    
df = (
    spark.read
    .schema(instaSchema)
    .option("delimiter", "\t")
    .option("header", "true")
    .option("mode", "DROPMALFORMED")
    .csv("gs://team2-bucket/batch/instagram_posts.csv")
)


def deEmojify(inputString):
    return inputString.encode('ascii', 'ignore').decode('ascii')


# df_posts['cts'] = df_posts['cts'].astype('datetime64') # cts 데이경 타입 변경

# df_posts['description'] = df_posts['description'].str.replace(pat=r'[^\w]',repl= r' ',regex=True) #줄 바꿈
df.withColumn('description', regexp_replace(col("description"), "\n", " "))


# df_posts = df_posts.dropna(subset=['description']) # null값 전부 제거
# df.na.drop(subset=['description'])


# df_posts['description'] = df_posts['description'].apply(lambda x : deEmojify(x)) # 이모티콘, 이상한 문자열 제거
# deEmojifyUDF = udf(lambda z: deEmojify(z), StringType())
# df.withColumn("description", deEmojifyUDF(col("description")))
df.show()


# df_posts['count'] = df_posts['description'].apply(lambda x : len(x) if len(x) <=0 else len(x)) # description 문자 길이 count
# countUDF = udf(lambda x : len(x)) 
# df.withColumn("count", countUDF(col("descripiton")))

df.write.format('parquet')\
    .option("path", "gs://team2-bucket/batch_ETL/posts")\
    .mode('overwrite')\
    .option('encoding', 'UTF-8')\
    .save()

bigquery_client = bigquery.Client(project = 'cloocus-da-w-2022-2')
table_id = 'cloocus-da-w-2022-2.team2_table.instagramtable_posts'


job_config = bigquery.LoadJobConfig(
    schema = [
    bigquery.SchemaField('sid', 'INTEGER'),
    bigquery.SchemaField('sid_profile', 'INTEGER'),
    bigquery.SchemaField('post_id', 'STRING'),
    bigquery.SchemaField('profile_id', 'INTEGER'),
    bigquery.SchemaField('location_id', 'INTEGER'),
    bigquery.SchemaField('cts', 'TIMESTAMP'),
    bigquery.SchemaField('post_type', 'INTEGER'),
    bigquery.SchemaField('description', 'STRING'),
    bigquery.SchemaField('numbr_likes', 'INTEGER'),
    bigquery.SchemaField('number_comments', 'INTEGER')
    # bigquery.SchemaField('count', 'INTEGER')

    ],
        # skip_leading_rows=1,
        # source_format=bigquery.SourceFormat.CSV,
        # field_delimiter = '\t',
        # allow_quoted_newlines = True,
        # ignore_unknown_values = True
        source_format=bigquery.SourceFormat.PARQUET,

)
uri = "gs://team2-bucket/batch_ETL/*.parquet"


load_job = bigquery_client.load_table_from_uri(
    uri, table_id,  job_config=job_config
)
result = load_job.result()
destination_table = bigquery_client.get_table(table_id)  # Make an API request.
print("Loaded {} rows.".format(destination_table.num_rows))

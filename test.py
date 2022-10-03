from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import numpy as np
import os




# crediential_path = "/home/seok10code/anaconda3/envs/google/DA/cloocus-da-w-2022-2-c333712e5a7b.json"
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = crediential_path

# df_posts = pd.read_csv("gs://team2-bucket/batch/instagram_posts.csv", sep = '\t')
# df_posts = pd.read_csv("/home/seok10code/anaconda3/envs/google/DA/bquxjob_62352957_18316b84373.csv", )

df = pd.read_csv('gs://team2-bucket/batch/instagram_posts.csv', sep = '\t', iterator=True, chunksize=10000) 

df_posts = pd.concat(df, ignore_index = True)

# def deEmojify(inputString):
#     return inputString.encode('ascii', 'ignore').decode('ascii')



# for chunk in df_posts:
    
#     chunk['cts'] = chunk['cts'].astype('datetime64') # cts 데이경 타입 변경
#     chunk['description'] = chunk['description'].str.replace(pat=r'[^\w]',repl= r' ',regex=True) #줄 바꿈
#     chunk = chunk.dropna(subset=['description']) # null값 전부 제거
#     chunk['description'] = chunk['description'].apply(lambda x : deEmojify(x)) # 이모티콘, 이상한 문자열 제거
#     chunk['count'] = chunk['description'].apply(lambda x : len(x) if len(x) <=0 else len(x)) # description 문자 길이 count




# def deEmojify(inputString):
#     return inputString.encode('ascii', 'ignore').decode('ascii')


# df_posts['cts'] = df_posts['cts'].astype('datetime64') # cts 데이경 타입 변경
# df_posts['description'] = df_posts['description'].str.replace(pat=r'[^\w]',repl= r' ',regex=True) #줄 바꿈
# df_posts = df_posts.dropna(subset=['description']) # null값 전부 제거
# df_posts['description'] = df_posts['description'].apply(lambda x : deEmojify(x)) # 이모티콘, 이상한 문자열 제거
# df_posts['count'] = df_posts['description'].apply(lambda x : len(x) if len(x) <=0 else len(x)) # description 문자 길이 count

print(df_posts)

# storage_client = storage.Client(project = 'cloocus-da-w-2022-2')
# bucket = storage_client.get_bucket("team2-bucket")
# blob = bucket.get_blob("batch/instagram_posts.csv")
# bigquery_client = bigquery.Client(project = 'cloocus-da-w-2022-2')
# table_id = 'cloocus-da-w-2022-2.team2_table.instagramtable_posts'


# job_config = bigquery.LoadJobConfig(
#     schema = [
#     bigquery.SchemaField('sid', 'INTEGER'),
#     bigquery.SchemaField('sid_profile', 'INTEGER'),
#     bigquery.SchemaField('post_id', 'STRING'),
#     bigquery.SchemaField('profile_id', 'INTEGER'),
#     bigquery.SchemaField('location_id', 'INTEGER'),
#     bigquery.SchemaField('cts', 'TIMESTAMP'),
#     bigquery.SchemaField('post_type', 'INTEGER'),
#     bigquery.SchemaField('description', 'STRING'),
#     bigquery.SchemaField('numbr_likes', 'INTEGER'),
#     bigquery.SchemaField('number_comments', 'INTEGER'),
#     bigquery.SchemaField('count', 'INTEGER')

#     ],
#       skip_leading_rows=1,
#       source_format=bigquery.SourceFormat.CSV,
#       field_delimiter = '\t',
#       allow_quoted_newlines = True,
#       ignore_unknown_values = True

# )

# load_job = bigquery_client.load_table_from_dataframe(
#     df_posts, table_id,  job_config=job_config
# )
# result = load_job.result()
# print("Written {} rows to {}".format(result.output_rows, result.destination))
# print("Partitioning: {}".format(result.time_partitioning))








# # storage_client = storage.Client(project = 'cloocus-da-w-2022-2')
# # bucket = storage_client.get_bucket("team2-bucket")
# # blob = bucket.get_blob("batch/instagram_posts.csv")


# # df_posts = pd.read_csv("gs://team2-bucket/batch/instagram_posts.csv", sep = '\t')
# # df_posts = pd.read_csv("/home/seok10code/anaconda3/envs/google/DA/bquxjob_62352957_18316b84373.csv")








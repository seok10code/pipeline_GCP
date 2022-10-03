import datetime

from airflow import models
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocInstantiateWorkflowTemplateOperator,
)


q1 = """
        SELECT sid, sid_profile, post_id, profile_id, location_id, cts, post_type, REGEXP_REPLACE(description, r'[^0-9a-zA-Z]+',' ') AS description, numbr_likes, number_comments
        FROM `cloocus-da-w-2022-2.team2_table.instagramtable_posts` limit 1000
    """




project_id = "cloocus-da-w-2022-2"


default_args = {
    "start_date": days_ago(1),
    "project_id": project_id,
   
}

with models.DAG(
    "This_is_mine",
    default_args = default_args,
    schedule_interval = datetime.timedelta(days=1),
   
    
) as dag:
    
    p1 = DataprocInstantiateWorkflowTemplateOperator(
        task_id="mv_streaming_to_bq",
        template_id= "template-stream-gcs-to-bq",
        project_id=project_id,
        region="asia-northeast3"
        
    )
    
    p2 = DataprocInstantiateWorkflowTemplateOperator(
        task_id="mv_batch_to_bq",
        template_id= "template-csv-gcs-to-bq",
        project_id=project_id,
        region="asia-northeast3"
        
    )
    
    p3 = BigQueryOperator(
        task_id='ETL_remove_emoji',
        sql=q1, 
        use_legacy_sql=False,
        destination_dataset_table='cloocus-da-w-2022-2.team2_table.instagramtable_posts',
        write_disposition='WRITE_TRUNCATE'
        
    )
    
    
    # p4  = BigQueryOperator(
    #     task_id='ETL_',
    #     sql=q2, 
    #     use_legacy_sql=False,
    #     destination_dataset_table='cloocus-da-w-2022-2.team2_table.instagramtable_posts',
    #     write_disposition='WRITE_TRUNCATE'
        
    # )
    
    
    p1
    p2 >> p3
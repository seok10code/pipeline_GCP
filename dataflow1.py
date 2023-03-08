from __future__ import absolute_import
import argparse
import logging
import re
import apache_beam as beam
import json
import os

from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.pipeline import PipelineOptions
from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd


# word length code

class WordExtractingDoFn(beam.DoFn):
    def process(self, element):
        print('test')
        # splited = element.split(',')
        # writestring = ({'id': splited[0], 'price': splited[1], 'manufacturer': splited[2], 'condition': splited[3]})
        # #writestring = {'splited[0], splited[1], splited[2], splited[3]'}
        # return [writestring]

    # parser option code
    def run(argv=None, save_main_session=True):

        # parser = argparse.ArgumentParser()
        # parser.add_argument(
        #     '--input',dest='input',required=False,help='default'
        #     ,default='gs://gcs-cnflnt-bq-to-sql-us/flow-sg/batch.txt')
        # parser.add_argument(
        #     '--output',dest='output',required=False,help='default'
        #     ,default='lws-cloocus:nasa1515.batchtest')

        # known_args, pipeline_args = parser.parse_known_args(argv)
        pipeline_options = PipelineOptions(pipeline_args)

# pipline option

# google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
# google_cloud_options.project = 'soomineom-confluent-study'
# google_cloud_options.job_name = 'test-to-big'
# google_cloud_options.staging_location = 'gs://gcs-cnflnt-bq-to-sql-us/flow-sg/flow-staging'
# google_cloud_options.temp_location = 'gs://gcs-cnflnt-bq-to-sql-us/flow-sg/flow-temp'
# google_cloud_options.region = 'asia-northeast3'
# pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'

# conda update --all
# # test1

p = beam.Pipeline(options = PipelineOptions(pipeline_args))

with beam.Pipeline(options=pipeline_options) as p:

    table_schema = {
        'fields': [
            {"name": "id", "type": "STRING", "mode": "NULLABLE"}, 
            {"name": "price", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "manufacturer", "type": "STRING", "mode": "NULLABLE"},
            {"name": "condition", "type": "STRING", "mode": "NULLABLE"}
        ]
    }
    
    # (p 
    #     | 'Read Data' >> ReadFromText(known_args.input)

    #     | beam.ParDo(WordExtractingDoFn(WordExtractingDoFn))
    #     | 'write to BigQuery' >> beam.io.WriteToBigQuery(
    #         known_args.output,
    #         schema = table_schema,
    #         method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
    #         create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    #         write_disposition = beam.io.BigQueryDisposition.WRITE_TRUNCATE
    #     )
    # )

result = p.run()
result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
    
    
    
    
    
#     pipeline
#    .apply(KafkaIO.<Long, String>read()
#       .withBootstrapServers("broker_1:9092,broker_2:9092")
#       .withTopic("my_topic")  // use withTopics(List<String>) to read from multiple topics.
#       .withKeyDeserializer(LongDeserializer.class)
#       .withValueDeserializer(StringDeserializer.class)

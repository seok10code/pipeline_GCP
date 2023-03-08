from __future__ import absolute_import
import argparse

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, SetupOptions

class WordcountOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--input',
            default='gs://dataflow-samples/shakespeare/kinglear.txt',
            help='Path of the file to read from')
        parser.add_argument(
            '--output',
            required=True,
            help='Output file to write results to.')
pipeline_options = PipelineOptions(['--output', 'some/output_path'])
p = beam.Pipeline(options=pipeline_options)

wordcount_options = pipeline_options.view_as(WordcountOptions)
lines = p | 'read' >> ReadFromText(wordcount_options.input)
#     pipeline
#    .apply(KafkaIO.<Long, String>read()
#       .withBootstrapServers("broker_1:9092,broker_2:9092")
#       .withTopic("my_topic")  // use withTopics(List<String>) to read from multiple topics.
#       .withKeyDeserializer(LongDeserializer.class)
#       .withValueDeserializer(StringDeserializer.class)

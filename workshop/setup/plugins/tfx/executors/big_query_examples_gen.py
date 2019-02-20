"""Generic TFX bigquery examples_gen executor."""

import os
import random
import apache_beam as beam
import tensorflow as tf
from tfx.executors.base_executor import BaseExecutor
from tfx.utils import logging_utils
from tfx.utils import types
from google.cloud import bigquery

# Default file name for TFRecord output file prefix.
DEFAULT_FILE_NAME = 'data_tfrecord'


class _BigQueryConverter(object):
  """Help class for bigquery result row to tf example conversion."""

  def __init__(self, query):
    client = bigquery.Client()
    # Dummy query for get the type information for each field.
    query_job = client.query('SELECT * FROM ({}) LIMIT 0'.format(query))
    results = query_job.result()
    self._type_map = {}
    for field in results.schema:
      self._type_map[field.name] = field.field_type

  def _bytes_feature(self, value):
    """Returns a bytes_list from a string / byte."""
    if value is None:
      return tf.train.Feature(bytes_list=tf.train.BytesList())
    return tf.train.Feature(
        bytes_list=tf.train.BytesList(value=[tf.compat.as_bytes(value)]))

  def _float_feature(self, value):
    """Returns a float_list from a float / double."""
    if value is None:
      return tf.train.Feature(float_list=tf.train.FloatList())
    return tf.train.Feature(float_list=tf.train.FloatList(value=[value]))

  def _int64_feature(self, value):
    """Returns an int64_list from a bool / enum / int / uint."""
    if value is None:
      return tf.train.Feature(int64_list=tf.train.Int64List())
    return tf.train.Feature(int64_list=tf.train.Int64List(value=[value]))

  def row_to_example(self, instance):
    """Convert bigquery result row to tf example."""
    feature = {}
    for key, value in instance.items():
      data_type = self._type_map[key]
      if data_type == 'INTEGER':
        feature[key] = self._int64_feature(value)
      elif data_type == 'FLOAT':
        feature[key] = self._float_feature(value)
      elif data_type == 'STRING':
        feature[key] = self._bytes_feature(value)
      else:
        raise RuntimeError(
            'Unknown type in big query result: {}.'.format(data_type))
    example_proto = tf.train.Example(
        features=tf.train.Features(feature=feature))
    return example_proto.SerializeToString()


class BigQueryExamplesGen(BaseExecutor):
  """Generic TFX bigquery examples_gen executor."""

  # TODO(jyzhao): create base example gen, move functions to base class.
  def _partition_fn(self, record, num_partitions):
    # TODO(jyzhao): support custom split.
    # Splits data, train(partition=1) : eval(partition=0) = 2 : 1
    return 0 if random.randrange(3) == 0 else 1

  def do(self, inputs, outputs, exec_properties):
    logger = logging_utils.get_logger(exec_properties['log_root'], 'exec')
    self._log_startup(logger, inputs, outputs, exec_properties)

    training_tfrecord = types.get_split_uri(outputs['output'], 'train')
    eval_tfrecord = types.get_split_uri(outputs['output'], 'eval')

    if 'query' not in exec_properties:
      raise RuntimeError('Missing query.')
    query = exec_properties['query']

    logger.info('Generating examples from BigQuery.')
    with beam.Pipeline(options=self._get_beam_pipeline_options()) as pipeline:
      converter = _BigQueryConverter(query)
      example_splits = (
          pipeline
          | 'QueryTable' >> beam.io.Read(
              beam.io.BigQuerySource(query=query, use_standard_sql=True))
          | 'ToSerializedTFExample' >> beam.Map(converter.row_to_example)
          | 'ShuffleData' >> beam.transforms.Reshuffle()
          | 'SplitData' >> beam.Partition(self._partition_fn, 2))
      _ = (
          example_splits[1]
          | 'OutputTrainData' >> beam.io.WriteToTFRecord(
              os.path.join(training_tfrecord, DEFAULT_FILE_NAME),
              file_name_suffix='.gz'))
      _ = (
          example_splits[0]
          | 'OutputEvalData' >> beam.io.WriteToTFRecord(
              os.path.join(eval_tfrecord, DEFAULT_FILE_NAME),
              file_name_suffix='.gz'))
    logger.info('Examples generated.')

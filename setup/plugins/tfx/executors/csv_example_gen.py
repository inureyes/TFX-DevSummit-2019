"""Generic TFX CSV example gen executor."""
import hashlib
import os
import apache_beam as beam
import tensorflow_data_validation as tfdv
from tensorflow_transform import coders as tft_coders
from tensorflow_transform.tf_metadata import dataset_schema
from tensorflow_transform.tf_metadata import schema_utils
from tfx.executors.base_executor import BaseExecutor
from tfx.utils import io_utils
from tfx.utils import logging_utils
from tfx.utils import types

# Default file name for TFRecord output file prefix.
DEFAULT_FILE_NAME = 'data_tfrecord'


# TODO(jyzhao): BaseExampleGen for common stuff sharing.
class CsvExampleGen(BaseExecutor):
  """Generic TFX CSV example gen executor."""

  def _partition_fn(self, record, num_partitions):
    # TODO(jyzhao): support custom split.
    # Splits data, train(partition=0) : eval(partition=1) = 2 : 1
    return 1 if int(hashlib.sha256(record).hexdigest(), 16) % 3 == 0 else 0

  def _csv_to_tfrecord(self, csv_uri, schema, output_uri, output2_uri=None):
    with beam.Pipeline(options=self._pipeline.options) as pipeline:
      csv_coder = tft_coders.CsvCoder(
          io_utils.load_csv_column_names(csv_uri), schema)
      proto_coder = tft_coders.ExampleProtoCoder(schema)
      shuffled_data = (
          pipeline
          |
          'ReadFromText' >> beam.io.ReadFromText(csv_uri, skip_header_lines=1)
          | 'ParseCSV' >> beam.Map(csv_coder.decode)
          | 'ToSerializedTFExample' >> beam.Map(proto_coder.encode)
          # TODO(jyzhao): reshuffle is deprecated without replacement.
          # TODO(jyzhao): make reshuffle optional.
          | 'ShuffleData' >> beam.transforms.Reshuffle())

      if output2_uri is None:
        _ = (
            shuffled_data
            | 'WriteExamples' >> beam.io.WriteToTFRecord(
                os.path.join(output_uri, DEFAULT_FILE_NAME),
                file_name_suffix='.gz'))
        return

      example_splits = (
          shuffled_data
          | 'SplitData' >> beam.Partition(self._partition_fn, 2))
      _ = (
          example_splits[0]
          | 'OutputFirstSplit' >> beam.io.WriteToTFRecord(
              os.path.join(output_uri, DEFAULT_FILE_NAME),
              file_name_suffix='.gz'))
      _ = (
          example_splits[1]
          | 'OutputSecondSplit' >> beam.io.WriteToTFRecord(
              os.path.join(output2_uri, DEFAULT_FILE_NAME),
              file_name_suffix='.gz'))

  def _get_schema(self, raw_data):
    # TODO(jyzhao): duplication in schema_gen and statistics_gen for CSV.
    stats = tfdv.generate_statistics_from_csv(
        data_location=raw_data, pipeline_options=self._pipeline.options)
    schema = tfdv.infer_schema(statistics=stats)
    raw_feature_spec = schema_utils.schema_as_feature_spec(schema).feature_spec
    return dataset_schema.from_feature_spec(raw_feature_spec)

  def Do(self, inputs, outputs, exec_properties):
    logger = logging_utils.get_logger(exec_properties['log_root'], 'exec')
    self._log_startup(inputs, outputs, exec_properties)

    training_tfrecord = types.get_split_uri(outputs['output'], 'train')
    eval_tfrecord = types.get_split_uri(outputs['output'], 'eval')

    input_span = types.get_single_instance(inputs['input_data'])
    input_span_uri = input_span.uri

    logger.info('Generating examples.')

    presplit = input_span.artifact.custom_properties['presplit'].string_value
    if not presplit:
      raw_data = io_utils.get_only_uri_in_dir(input_span_uri)
      logger.info('No split {}.'.format(raw_data))

      schema = self._get_schema(raw_data)
      self._csv_to_tfrecord(raw_data, schema, training_tfrecord, eval_tfrecord)
    else:
      if set(presplit.split(',')) != set(types.DEFAULT_EXAMPLE_SPLITS):
        raise RuntimeError(
            'Custom presplit {} is not supported yet.'.format(presplit))

      training_raw_data = io_utils.get_only_uri_in_dir(
          os.path.join(input_span_uri, 'train'))
      logger.info('Train split {}.'.format(training_raw_data))
      eval_raw_data = io_utils.get_only_uri_in_dir(
          os.path.join(input_span_uri, 'eval'))
      logger.info('Eval split {}.'.format(eval_raw_data))

      schema = self._get_schema(training_raw_data)
      self._csv_to_tfrecord(training_raw_data, schema, training_tfrecord)
      self._csv_to_tfrecord(eval_raw_data, schema, eval_tfrecord)

    logger.info('Examples generated.')

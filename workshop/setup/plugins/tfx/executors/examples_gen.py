"""Generic TFX examples_gen executor."""
import os
import random
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


class ExamplesGen(BaseExecutor):
  """Generic TFX examples_gen executor."""

  def _get_raw_feature_spec(self, schema):
    """Returns feature spec from a schema."""
    return schema_utils.schema_as_feature_spec(schema).feature_spec

  def _make_proto_coder(self, schema):
    """Return a coder for tf.transform to read protos."""
    raw_feature_spec = self._get_raw_feature_spec(schema)
    raw_schema = dataset_schema.from_feature_spec(raw_feature_spec)
    return tft_coders.ExampleProtoCoder(raw_schema)

  def _make_csv_coder(self, schema, column_names):
    """Return a coder for tf.transform to read csv files."""
    raw_feature_spec = self._get_raw_feature_spec(schema)
    parsing_schema = dataset_schema.from_feature_spec(raw_feature_spec)
    return tft_coders.CsvCoder(column_names, parsing_schema)

  # TODO(jyzhao): move csv conversion and coder functions to utils.
  def _csv_to_tfrecord(self, csv, output_uri, schema):
    with beam.Pipeline(options=self._get_beam_pipeline_options()) as pipeline:
      csv_coder = self._make_csv_coder(schema,
                                       io_utils.load_csv_column_names(csv))
      proto_coder = self._make_proto_coder(schema)
      _ = (
          pipeline
          | 'ReadFromText' >> beam.io.ReadFromText(csv, skip_header_lines=1)
          | 'ShuffleData' >> beam.transforms.Reshuffle()
          | 'ParseCSV' >> beam.Map(csv_coder.decode)
          | 'ToSerializedTFExample' >> beam.Map(proto_coder.encode)
          | 'WriteExamples' >> beam.io.WriteToTFRecord(
              os.path.join(output_uri, DEFAULT_FILE_NAME),
              file_name_suffix='.gz'))

  def partition_fn(self, record, num_partitions):
    # TODO(jyzhao): support custom split.
    # Splits data, train(partition=1) : eval(partition=0) = 2 : 1
    return 0 if random.randrange(3) == 0 else 1

  def _csv_to_split_tfrecord(self, csv, train_output, eval_output, schema):
    with beam.Pipeline(options=self._get_beam_pipeline_options()) as pipeline:
      csv_coder = self._make_csv_coder(schema,
                                       io_utils.load_csv_column_names(csv))
      proto_coder = self._make_proto_coder(schema)
      raw_data = (
          pipeline
          | 'ReadFromText' >> beam.io.ReadFromText(csv, skip_header_lines=1)
          | 'ShuffleData' >> beam.transforms.Reshuffle()
          | 'SplitData' >> beam.Partition(self.partition_fn, 2))
      _ = (
          raw_data[1]
          | 'TrainDataDecode' >> beam.Map(csv_coder.decode)
          | 'TrainDataEncode' >> beam.Map(proto_coder.encode)
          | 'OutputTrainData' >> beam.io.WriteToTFRecord(
              os.path.join(train_output, DEFAULT_FILE_NAME),
              file_name_suffix='.gz'))
      _ = (
          raw_data[0]
          | 'EvalDataDecode' >> beam.Map(csv_coder.decode)
          | 'EvalDataEncode' >> beam.Map(proto_coder.encode)
          | 'OutputEvalData' >> beam.io.WriteToTFRecord(
              os.path.join(eval_output, DEFAULT_FILE_NAME),
              file_name_suffix='.gz'))

  def _get_schema(self, raw_data):
    # TODO(jyzhao): duplication in schema_gen and statistics_gen for CSV.
    #               Dags should be different when input are CSV vs TFRecord.
    stats = tfdv.generate_statistics_from_csv(
        data_location=raw_data,
        pipeline_options=self._get_beam_pipeline_options())
    return tfdv.infer_schema(statistics=stats)

  def do(self, inputs, outputs, exec_properties):
    logger = logging_utils.get_logger(exec_properties['log_root'], 'exec')
    self._log_startup(logger, inputs, outputs, exec_properties)

    # TODO(jyzhao): base examples-gen handles split and shuffle, multiple types
    #               of sub examples_gen for different type of input.

    training_tfrecord = types.get_split_uri(outputs['output'], 'train')
    eval_tfrecord = types.get_split_uri(outputs['output'], 'eval')

    logger.info('Generating examples.')
    span_input_data = types.get_single_instance(inputs['input_data'])
    span_input_uri = span_input_data.uri
    presplit = span_input_data.artifact.custom_properties[
        'presplit'].string_value
    if not presplit:
      raw_data = io_utils.get_only_uri_in_dir(span_input_uri)
      logger.info('No split {}.'.format(raw_data))
      schema = self._get_schema(raw_data)
      self._csv_to_split_tfrecord(raw_data, training_tfrecord, eval_tfrecord,
                                  schema)
    else:
      if set(presplit.split(',')) != set(types.DEFAULT_EXAMPLE_SPLITS):
        logger.warn('Custom presplit {} may not be fully supported yet'.format(
            presplit))
      training_raw_data = io_utils.get_only_uri_in_dir(
          os.path.join(span_input_uri, 'train'))
      logger.info('Train split {}.'.format(training_raw_data))
      eval_raw_data = io_utils.get_only_uri_in_dir(
          os.path.join(span_input_uri, 'eval'))
      logger.info('Eval split {}.'.format(eval_raw_data))
      schema = self._get_schema(training_raw_data)
      self._csv_to_tfrecord(training_raw_data, training_tfrecord, schema)
      self._csv_to_tfrecord(eval_raw_data, eval_tfrecord, schema)

    logger.info('Examples generated.')

"""Generic TFX examples_gen executor."""

import random
import apache_beam as beam
import tensorflow_data_validation as tfdv
from tfx.executors import io_utils
from tfx.executors.base_executor import BaseExecutor
from tfx.runtimes import tfx_logger
from tfx.tfx_types import CSVExampleSource


class ExamplesGen(BaseExecutor):
  """Generic TFX examples_gen executor."""

  # TODO(jyzhao): move csv conversion and coder functions to utils.
  def _csv_to_tfrecord(self, csv, output_uri, schema):
    with beam.Pipeline(options=self._get_beam_pipeline_options()) as pipeline:
      csv_coder = self._make_csv_coder(schema,
                                       CSVExampleSource.load_column_names(csv))
      proto_coder = self._make_proto_coder(schema)
      _ = (
          pipeline
          | 'ReadFromText' >> beam.io.ReadFromText(csv, skip_header_lines=1)
          | 'ShuffleData' >> beam.transforms.Reshuffle()
          | 'ParseCSV' >> beam.Map(csv_coder.decode)
          | 'ToSerializedTFExample' >> beam.Map(proto_coder.encode)
          | 'WriteExamples' >> beam.io.WriteToTFRecord(
              output_uri, file_name_suffix='.gz'))

  def partition_fn(self, record, num_partitions):
    # TODO(jyzhao): support custom split.
    # Splits data, train(partition=1) : eval(partition=0) = 2 : 1
    return 0 if random.randrange(3) == 0 else 1

  def _csv_to_split_tfrecord(self, csv, train_output, eval_output, schema):
    with beam.Pipeline(options=self._get_beam_pipeline_options()) as pipeline:
      csv_coder = self._make_csv_coder(schema,
                                       CSVExampleSource.load_column_names(csv))
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
              train_output, file_name_suffix='.gz'))
      _ = (
          raw_data[0]
          | 'EvalDataDecode' >> beam.Map(csv_coder.decode)
          | 'EvalDataEncode' >> beam.Map(proto_coder.encode)
          | 'OutputEvalData' >> beam.io.WriteToTFRecord(
              eval_output, file_name_suffix='.gz'))

  def _get_schema(self, raw_data):
    # TODO(jyzhao): duplication in schema_gen and statistics_gen for CSV.
    #               Dags should be different when input are CSV vs TFRecord.
    stats = tfdv.generate_statistics_from_csv(
        data_location=raw_data,
        pipeline_options=self._get_beam_pipeline_options())
    return tfdv.infer_schema(statistics=stats)

  def do(self, inputs, outputs, exec_properties):
    logger = tfx_logger.get_logger(exec_properties['log_root'], '.exec')
    self._log_startup(logger, inputs, outputs, exec_properties)

    input_data = inputs['input_data']

    # TODO(jyzhao): base examples-gen handles split and shuffle, multiple types
    #               of sub examples_gen for different type of input.
    # TODO(jyzhao): support csv output (probably in a different sub example-gen)
    #               as the following components might be able to take CSV.
    if (input_data.artifact.properties['subtype_name'].string_value !=
        'CSVExampleSource'):
      raise RuntimeError('Unsupported artifact type')

    training_tfrecord = outputs['output'].split_uri('train')
    eval_tfrecord = outputs['output'].split_uri('eval')

    logger.info('Generating examples.')

    if not input_data.split_uris:
      raw_data = io_utils.get_only_uri_in_dir(input_data.uri)
      logger.info('No split {}.'.format(raw_data))
      schema = self._get_schema(raw_data)
      self._csv_to_split_tfrecord(raw_data, training_tfrecord, eval_tfrecord,
                                  schema)
    else:
      training_raw_data = io_utils.get_only_uri_in_dir(
          input_data.split_uri('train'))
      logger.info('Train split {}.'.format(training_raw_data))
      eval_raw_data = io_utils.get_only_uri_in_dir(input_data.split_uri('eval'))
      logger.info('Eval split {}.'.format(eval_raw_data))
      schema = self._get_schema(training_raw_data)
      self._csv_to_tfrecord(training_raw_data, training_tfrecord, schema)
      self._csv_to_tfrecord(eval_raw_data, eval_tfrecord, schema)

    logger.info('Examples generated.')

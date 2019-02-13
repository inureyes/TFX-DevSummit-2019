"""Generic TFX transform executor."""

import apache_beam as beam
import tensorflow as tf
from tensorflow_data_validation.coders import tf_example_decoder
import tensorflow_transform.beam as tft_beam
from tensorflow_transform.coders import example_proto_coder
from tensorflow_transform.tf_metadata import dataset_metadata
from tensorflow_transform.tf_metadata import dataset_schema
from tfx.executors import io_utils
from tfx.executors.base_executor import BaseExecutor
from tfx.runtimes import tfx_logger


class Transform(BaseExecutor):
  """Generic TFX transform executor."""

  def do(self, inputs, outputs, exec_properties):
    """The main tf.transform method which analyzes and transforms data."""
    logger = tfx_logger.get_logger(exec_properties['log_root'], '.exec')
    self._log_startup(logger, inputs, outputs, exec_properties)

    input_data = inputs['input_data']
    if (input_data.artifact.properties['subtype_name'].string_value !=
        'TfExampleSource'):
      raise RuntimeError('Unknown input type {}'.format(input_data.artifact))

    train_data_uri = input_data.split_uri('train')
    eval_data_uri = input_data.split_uri('eval')
    schema_file = inputs['schema'].uri

    preprocessing_fn = io_utils.import_func(exec_properties['module_file'],
                                            'preprocessing_fn')

    transform_output = outputs['transform_output'].uri
    if tf.gfile.Exists(transform_output):
      io_utils.delete_dir(transform_output)
    transformed_examples = outputs['transformed_examples']
    transformed_train_output = transformed_examples.split_uri('train')
    if tf.gfile.Exists(transformed_train_output):
      io_utils.delete_dir(transformed_train_output)
    transformed_eval_output = transformed_examples.split_uri('eval')
    if tf.gfile.Exists(transformed_eval_output):
      io_utils.delete_dir(transformed_eval_output)


    schema = self._read_schema(schema_file)
    raw_feature_spec = self._get_raw_feature_spec(schema)
    raw_schema = dataset_schema.from_feature_spec(raw_feature_spec)
    raw_data_metadata = dataset_metadata.DatasetMetadata(raw_schema)

    with beam.Pipeline(options=self._get_beam_pipeline_options()) as pipeline:
      with tft_beam.Context(temp_dir=transform_output):
        logger.info('Analyzing train data and creating transform_fn.')
        raw_train_data = (
            pipeline
            | 'ReadTrainData' >> beam.io.ReadFromTFRecord(
                file_pattern=self._all_files_pattern(train_data_uri))
            | 'DecodeTrainData' >> tf_example_decoder.DecodeTFExample())

        transform_fn = (
            (raw_train_data, raw_data_metadata)
            | ('Analyze' >> tft_beam.AnalyzeDataset(preprocessing_fn)))

        (transformed_train_data, transformed_train_metadata) = (
            ((raw_train_data, raw_data_metadata), transform_fn)
            | 'TransformTrain' >> tft_beam.TransformDataset())

        coder = example_proto_coder.ExampleProtoCoder(
            transformed_train_metadata.schema)

        raw_eval_data = (
            pipeline
            | 'ReadEvalData' >> beam.io.ReadFromTFRecord(
                file_pattern=self._all_files_pattern(eval_data_uri))
            | 'DecodeEvalData' >> tf_example_decoder.DecodeTFExample())

        (transformed_eval_data, _) = (
            ((raw_eval_data, raw_data_metadata), transform_fn)
            | 'TransformEval' >> tft_beam.TransformDataset())

        _ = (
            transformed_train_data
            | 'SerializeTrainExamples' >> beam.Map(coder.encode)
            | 'WriteTrainExamples' >> beam.io.WriteToTFRecord(
                transformed_train_output, file_name_suffix='.gz'))

        _ = (
            transformed_eval_data
            | 'SerializeEvalExamples' >> beam.Map(coder.encode)
            | 'WriteEvalExamples' >> beam.io.WriteToTFRecord(
                transformed_eval_output, file_name_suffix='.gz'))

        _ = (
            transform_fn
            | ('WriteTransformFn' >> tft_beam.WriteTransformFn(transform_output)))

        logger.info(
            'Transformation complete. transform_fn can be found at {}.'
            .format(transform_output))

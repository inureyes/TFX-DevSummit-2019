"""Abstract TFX executor class."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import abc
import os
from future.utils import with_metaclass
import tensorflow as tf
from tensorflow.python.lib.io import file_io
from tensorflow_metadata.proto.v0 import schema_pb2
from tensorflow_transform import coders as tft_coders
from tensorflow_transform.tf_metadata import dataset_schema
from tensorflow_transform.tf_metadata import schema_utils
from google.protobuf import text_format


# TODO(b/122835564): Replace ALL runtime parameters to use runtime.get_param()
class BaseExecutor(with_metaclass(abc.ABCMeta, object)):
  """Abstract TFX executor class."""

  # TODO(b/122681505): Remove these once components can express multiple outputs
  # Until then, these need to exist for components consuming trainer output
  _EVAL_MODEL_DIR = 'eval_model_dir'
  _SERVING_MODEL_DIR = 'serving_model_dir'

  def __init__(self, runtime):
    self._runtime = runtime

  @abc.abstractmethod
  def do(self, inputs, outputs, exec_properties):
    """Execute underlying component implementation."""
    pass

  def _get_beam_pipeline_options(self):
    """Creates a default beam.PipelineOptions."""
    # TODO(zhitaoli): Implement this in runtime.
    return self._runtime.get_beam_pipeline_options()

  def _all_files_pattern(self, file_pattern):
    return '{}*'.format(file_pattern)

  def _eval_model_path(self, model_export_dir):
    return os.path.join(
        model_export_dir, self._EVAL_MODEL_DIR,
        tf.gfile.ListDirectory(
            os.path.join(model_export_dir, self._EVAL_MODEL_DIR))[-1])

  def _serving_model_path(self, model_export_dir):
    export_dir = os.path.join(model_export_dir, self._SERVING_MODEL_DIR,
                              'export')
    model_dir = os.path.join(export_dir, tf.gfile.ListDirectory(export_dir)[-1])
    model_name = tf.gfile.ListDirectory(model_dir)[-1]
    return os.path.join(model_dir, model_name)

  def _read_schema(self, path):
    """Reads a schema from the provided location.

    Args:
      path: The location of the file holding a serialized Schema proto.

    Returns:
      An instance of Schema or None if the input argument is None
    """
    result = schema_pb2.Schema()
    contents = file_io.read_file_to_string(path)
    text_format.Parse(contents, result)
    return result

  # Tf.Transform considers these features as "raw"
  def _get_raw_feature_spec(self, schema):
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

  def _log_startup(self, logger, inputs, outputs, exec_properties):
    """Log inputs, outputs, and executor properties in a standard format."""
    logger.info('Starting ' + self.__class__.__name__ + ' execution.')

    logger.info('Inputs for ' + self.__class__.__name__)
    for k, v in inputs.items():
      logger.info('{}:{}'.format(k, v))
    logger.info('Runtime parameters for ' + self.__class__.__name__)
    for k, v in exec_properties.items():
      logger.info('{}:{}'.format(k, v))
    logger.info('Outputs for ' + self.__class__.__name__)
    for k, v in outputs.items():
      logger.info('{}:{}'.format(k, v))

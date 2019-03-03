"""Generic TFX schema_gen executor."""

import os
import tensorflow_data_validation as tfdv
from tfx.executors.base_executor import BaseExecutor
from tfx.utils import io_utils
from tfx.utils import logging_utils
from tfx.utils import types

# Default file name for generate schema file.
DEFAULT_FILE_NAME = 'schema.pbtxt'


class SchemaGen(BaseExecutor):
  """Generic TFX schema_gen executor."""

  def Do(self, inputs, outputs, exec_properties):
    """Infers the schema using tfdv."""
    logger = logging_utils.get_logger(exec_properties['log_root'], 'exec')
    self._log_startup(inputs, outputs, exec_properties)

    train_stats_uri = io_utils.get_only_uri_in_dir(
        types.get_split_uri(inputs['stats'], 'train'))
    output_uri = os.path.join(
        types.get_single_uri(outputs['output']), DEFAULT_FILE_NAME)

    infer_feature_shape = False
    logger.info('Infering schema from statistics.')
    schema = tfdv.infer_schema(
        tfdv.load_statistics(train_stats_uri), infer_feature_shape)
    io_utils.write_pbtxt_file(output_uri, schema)
    logger.info('Schema written to {}.'.format(output_uri))

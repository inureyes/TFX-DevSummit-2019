"""Generic TFX schema_gen executor."""

import tensorflow_data_validation as tfdv
from tfx.executors import io_utils
from tfx.executors.base_executor import BaseExecutor
from tfx.runtimes import tfx_logger


class SchemaGen(BaseExecutor):
  """Generic TFX schema_gen executor."""

  def do(self, inputs, outputs, exec_properties):
    """Infers the schema using tfdv."""
    logger = tfx_logger.get_logger(exec_properties['log_root'], '.exec')
    self._log_startup(logger, inputs, outputs, exec_properties)

    infer_feature_shape = False
    logger.info('Infering schema from statistics.')
    schema = tfdv.infer_schema(
        tfdv.load_statistics(inputs['stats'].split_uri('train')),
        infer_feature_shape)
    io_utils.write_pbtxt_file(outputs['output'].uri, schema)
    logger.info('Schema written to {}.'.format(outputs['output']))

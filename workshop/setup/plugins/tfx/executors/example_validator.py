"""Generic TFX example validator executor."""

import os
import tensorflow_data_validation as tfdv
from tensorflow_metadata.proto.v0 import schema_pb2
from tfx.executors.base_executor import BaseExecutor
from tfx.utils import io_utils
from tfx.utils import logging_utils
from tfx.utils import types

# Default file name for anomalies output.
DEFAULT_FILE_NAME = 'anomalies.pbtxt'


class ExampleValidator(BaseExecutor):
  """Generic TFX example validator executor."""

  def Do(self, inputs, outputs, exec_properties):
    """Validate the statistics against the schema and materialize anomalies."""
    logger = logging_utils.get_logger(exec_properties['log_root'], 'exec')
    self._log_startup(inputs, outputs, exec_properties)

    logger.info('Validating schema against the computed statistics.')
    schema = io_utils.parse_pbtxt_file(
        io_utils.get_only_uri_in_dir(types.get_single_uri(inputs['schema'])),
        schema_pb2.Schema())
    stats = tfdv.load_statistics(
        io_utils.get_only_uri_in_dir(
            types.get_split_uri(inputs['stats'], 'eval')))
    output_uri = types.get_single_uri(outputs['output'])
    anomalies = tfdv.validate_statistics(stats, schema)
    io_utils.write_pbtxt_file(
        os.path.join(output_uri, DEFAULT_FILE_NAME), anomalies)
    logger.info(
        'Validation complete. Anomalies written to {}.'.format(output_uri))

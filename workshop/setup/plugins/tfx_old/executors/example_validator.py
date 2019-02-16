"""Generic TFX example validator executor."""

import tensorflow_data_validation as tfdv
from tfx.executors import io_utils
from tfx.executors.base_executor import BaseExecutor
from tfx.runtimes import tfx_logger


class ExampleValidator(BaseExecutor):
  """Generic TFX example validator executor."""

  def do(self, inputs, outputs, exec_properties):
    """Validate the statistics against the schema and materialize anomalies."""
    logger = tfx_logger.get_logger(exec_properties['log_root'], '.exec')
    self._log_startup(logger, inputs, outputs, exec_properties)

    logger.info('Validating schema against the computed statistics.')
    schema = self._read_schema(inputs['schema'].uri)
    stats = tfdv.load_statistics(inputs['stats'].split_uri('eval'))
    anomalies = tfdv.validate_statistics(stats, schema)
    io_utils.write_pbtxt_file(outputs['output'].uri, anomalies)
    logger.info('Validation complete. Anomalies written to {}.'.format(
        outputs['output']))

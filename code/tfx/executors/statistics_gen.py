"""Generic TFX statsgen executor."""

import tensorflow_data_validation as tfdv
from tfx.executors import io_utils
from tfx.executors.base_executor import BaseExecutor
from tfx.runtimes import tfx_logger


class StatisticsGen(BaseExecutor):
  """Generic TFX statsgen executor."""

  def do(self, inputs, outputs, exec_properties):
    """Computes stats using tfdv."""
    self._logger = tfx_logger.get_logger(exec_properties['log_root'], '.exec')
    self._log_startup(self._logger, inputs, outputs, exec_properties)

    for split, uri in inputs['input_data'].split_uris.items():
      self._logger.info('Generating statistics for split {}'.format(split))
      self._gen_statistics(uri, outputs['output'].split_uri(split))

  def _gen_statistics(self, input_data_uri, output_uri):
    self._logger.info('Generating statistics for {}'.format(input_data_uri))

    stats = tfdv.generate_statistics_from_tfrecord(
        data_location=self._all_files_pattern(input_data_uri),
        pipeline_options=self._get_beam_pipeline_options())
    io_utils.write_tfrecord_file(output_uri, stats)
    self._logger.info('Statistics written to {}.'.format(output_uri))

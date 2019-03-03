"""Generic TFX statsgen executor."""

import os
import tensorflow_data_validation as tfdv
from tfx.executors.base_executor import BaseExecutor
from tfx.utils import io_utils
from tfx.utils import logging_utils
from tfx.utils import types

# Default file name for stats generated for each split.
DEFAULT_FILE_NAME = 'stats_tfrecord'


class StatisticsGen(BaseExecutor):
  """Generic TFX statsgen executor."""

  def Do(self, inputs, outputs, exec_properties):
    """Computes stats using tfdv."""
    self._logger = logging_utils.get_logger(exec_properties['log_root'], 'exec')
    self._log_startup(inputs, outputs, exec_properties)

    split_to_instance = {x.split: x for x in inputs['input_data']}
    for split, instance in split_to_instance.items():
      self._logger.info('Generating statistics for split {}'.format(split))
      input_uri = io_utils.all_files_pattern(instance.uri)
      output_uri = types.get_split_uri(outputs['output'], split)
      tfdv.generate_statistics_from_tfrecord(
          data_location=input_uri,
          output_path=os.path.join(output_uri, DEFAULT_FILE_NAME),
          pipeline_options=self._pipeline.options)
      self._logger.info('Statistics written to {}.'.format(output_uri))

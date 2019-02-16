"""Generic TFX statsgen executor."""
import os
import tensorflow as tf
from tfx.executors.base_executor import BaseExecutor
from tfx.runtimes import tfx_logger


class Setup(BaseExecutor):
  """Generic TFX setup executor."""

  def do(self, inputs, outputs, exec_properties):
    """Create the root pipeline directory."""
    logger = tfx_logger.get_logger(exec_properties['log_root'], '.exec')
    self._log_startup(logger, inputs, outputs, exec_properties)

    project_path = exec_properties['project_path']
    if not tf.gfile.Exists(project_path):
      os.makedirs(project_path)
    logger.info('Setup complete.')

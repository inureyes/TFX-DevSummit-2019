"""Generic TFX statsgen executor."""
import tensorflow as tf
from tfx.executors.base_executor import BaseExecutor
from tfx.utils import logging_utils


class Setup(BaseExecutor):
  """Generic TFX setup executor."""

  def do(self, inputs, outputs, exec_properties):
    """Create the root pipeline directory."""
    logger = logging_utils.get_logger(exec_properties['log_root'], 'exec')
    self._log_startup(logger, inputs, outputs, exec_properties)

    project_path = exec_properties['project_path']
    if not tf.gfile.Exists(project_path):
      logger.info('Creating project path {}'.format(project_path))
      tf.gfile.MakeDirs(project_path)
    logger.info('Setup complete.')

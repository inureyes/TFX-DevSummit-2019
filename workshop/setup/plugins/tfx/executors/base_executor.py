"""Abstract TFX executor class."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import abc
from future.utils import with_metaclass


class BaseExecutor(with_metaclass(abc.ABCMeta, object)):
  """Abstract TFX executor class."""

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

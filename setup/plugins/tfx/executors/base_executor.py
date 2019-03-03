"""Abstract TFX executor class."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import abc
from future.utils import with_metaclass
import tensorflow as tf


class BaseExecutor(with_metaclass(abc.ABCMeta, object)):
  """Abstract TFX executor class."""

  def __init__(self, pipeline, additional_pipeline_args=None):
    self._pipeline = pipeline
    self._additional_pipeline_args = additional_pipeline_args or dict()

  @abc.abstractmethod
  def Do(self, inputs, outputs, exec_properties):
    """Execute underlying component implementation."""
    pass

  def _has_additional_pipeline_args(self, key):
    """Check whether given additional pipeline args was provided."""
    return key in self._additional_pipeline_args

  def _get_additional_pipeline_args(self, key=None):
    """Get additional pipeline args if specificed, or None otherwise."""
    return self._additional_pipeline_args.get(key)

  def _log_startup(self, inputs, outputs, exec_properties):
    tf.logging.info('Starting {} execution.'.format(self.__class__.__name__))
    tf.logging.info('Inputs for {} is: {}'.format(self.__class__.__name__,
                                                  inputs))
    tf.logging.info('Execution properties for {} is: {}'.format(
        self.__class__.__name__, exec_properties))
    tf.logging.info('Outputs for {} is: {}'.format(self.__class__.__name__,
                                                   outputs))

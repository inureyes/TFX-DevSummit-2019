"""Definition of TFX runner base class."""
import abc
from future.utils import with_metaclass


class TfxRunner(with_metaclass(abc.ABCMeta, object)):

  @abc.abstractmethod
  def run(self, pipeline):
    """Given logical TFX pipeline definition, runs the pipeline on the
    specified platform.
    
    Inputs:
      pipeline: logical TFX pipeline definition.
    Return: Platform-specific pipeline object.
    """
    pass

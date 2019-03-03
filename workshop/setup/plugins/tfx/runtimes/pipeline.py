"""Definition and related classes for TFX pipeline."""
import functools


class PipelineDecorator(object):
  """Pipeline decorator that has pipeline-level specification."""

  def __init__(self, **kwargs):
    self._pipeline = self._new_pipeline(**kwargs)

  # TODO(ruoyu): Come up with a better style to construct TFX pipeline.
  def __call__(self, func):

    @functools.wraps(func)
    def decorated():
      self._pipeline.components = func()
      return self._pipeline

    return decorated

  def _new_pipeline(self, **kwargs):
    return Pipeline(**kwargs)


class Pipeline(object):
  """Logical TFX pipeline object."""

  def __init__(self, **kwargs):
    self.kwargs = kwargs
    self._components = []

  @property
  def components(self):
    return self._components

  @components.setter
  def components(self, components):
    self._components = components

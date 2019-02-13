"""Abstracts the runtime environment from the executors."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from apache_beam.options.pipeline_options import PipelineOptions


class BaseRuntime(object):
  """Abstracts the runtime environment from the executors."""

  def __init__(self, pipeline_args=None):
    self._pipeline_options = PipelineOptions(
        pipeline_args or ['--runner=DirectRunner'])

  def get_beam_pipeline_options(self):
    """Return unified beam pipeline options."""
    return self._pipeline_options


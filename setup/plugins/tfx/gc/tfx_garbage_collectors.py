"""Garbage collection utils."""
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections

from tfx.runtimes.tfx_metadata import Metadata
from tfx.utils import logging_utils
from tfx.utils.types import ARTIFACT_STATE_MARKED_FOR_DELETION


class SpanBasedGarbageCollector(object):
  """Helper class to mark artifacts as garbage-collectable based on span."""

  def __init__(self,
               num_spans_to_keep,
               connection_config,
               log_root='/var/tmp/tfx'):
    super(SpanBasedGarbageCollector, self).__init__()
    self._connection_config = connection_config
    self._num_spans_to_keep = num_spans_to_keep
    self._logger = logging_utils.get_logger(log_root, 'gc.log')

  def _generate_artifact_group_key(self, artifact):
    return (
        artifact.properties['type_name'].string_value,
        artifact.properties['split'].string_value,
    )

  def _get_artifacts_to_gc_within_group(self, artifact_group):
    sorted_artifact = sorted(
        artifact_group,
        key=lambda artifact: artifact.properties['span'].int_value)
    return sorted_artifact[:len(sorted_artifact) - self._num_spans_to_keep]

  def do_garbage_collection(self):
    """Marks qualified artifacts as garbage-collectable.

    The artifacts marked will not be valid for use. However users should handle
    real file deletion after marking artifacts as 'MARKED_FOR_DELETION'.

    Returns:
      Artifacts that marked for deletion.
    """

    group_to_artifacts = collections.defaultdict(list)
    artifacts_to_gc = []
    with Metadata(self._connection_config) as m:
      all_artifacts = m.get_all_artifacts()
      for artifact in all_artifacts:
        group_key = self._generate_artifact_group_key(artifact)
        group_to_artifacts[group_key].append(artifact)

      # For every artifact group, gathers all artifacts that should be garbage
      # collected.
      for artifact_group in group_to_artifacts.values():
        artifacts_to_gc.extend(
            self._get_artifacts_to_gc_within_group(artifact_group))
      # For every artifact that should be garbage collected, marks them in
      # ml metadata.
      for artifact_to_be_gc in artifacts_to_gc:
        m.update_artifact_state(artifact_to_be_gc,
                                ARTIFACT_STATE_MARKED_FOR_DELETION)
        self._logger.info(
            'Artifact can be garbage collected: \n{}'.format(artifact_to_be_gc))
    return artifacts_to_gc

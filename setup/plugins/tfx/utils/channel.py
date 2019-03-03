"""Tfx Channel definition."""

import collections
from tfx.utils.types import TfxType


def as_channel(source):
  """Converts a source into a Channel."""

  if isinstance(source, Channel):
    return source
  elif isinstance(source, collections.Iterable) and source and isinstance(
      source[0], TfxType):
    return Channel(
      type_name=source[0].type_name,
      channel_name=None,
      static_artifact_collection=source)
  else:
    raise ValueError('Invalid source to be a channel: {}'.format(source))


class Channel(object):

  # TODO(b/124763842): Consider replace type_name with ArtifactType.
  def __init__(self,
               type_name,
               channel_name=None,
               static_artifact_collection=None):
    self.type_name = type_name
    self.channel_name = channel_name
    self._static_artifact_collection = static_artifact_collection or []
    self._validate_type()

  def __str__(self):
    return 'Channel<{}, {}, {}>'.format(self.type_name, self.channel_name,
                                        self._static_artifact_collection)

  def __repr__(self):
    return self.__str__()

  def _validate_type(self):
    for artifact in self._static_artifact_collection:
      if artifact.type_name != self.type_name:
        raise ValueError(
            'Static artifact collection with different artifact type than {}'
            .format(self.type_name))

  def get(self):
    # TODO: We plan to support dynamic query against a Channel point instead of
    # a static Artifact sets in the future.
    return self._static_artifact_collection

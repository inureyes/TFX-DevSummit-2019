"""TFX type definition."""
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

import abc
import json
import os
import builtins

from future.utils import with_metaclass

from google.protobuf.json_format import MessageToJson
from google.protobuf.json_format import Parse

from ml_metadata.proto import metadata_store_pb2
from tensorflow.python.lib.io import file_io

import tensorflow as tf

# Indicating there is an execution producing it.
ARTIFACT_STATE_PENDING = 'pending'
# Indicating artifact ready to be consumed.
ARTIFACT_STATE_PUBLISHED = 'published'
# Indicating no data in artifact uri although it's not marked as deleted.
ARTIFACT_STATE_MISSING = 'missing'
# Indicating artifact should be garbage collected.
ARTIFACT_STATE_DELETING = 'deleting'
# Indicating artifact being garbage collected.
ARTIFACT_STATE_DELETED = 'deleted'

# Default split of examples data.
DEFAULT_EXAMPLE_SPLITS = 'train,eval'
NO_SPLIT = ''


class TfxType(object):
  """Base Tfx Type.

  This is used for type checking and inter component communicating.
  """

  def __init__(self,
               type_name,
               uri='',
               default_file_name='',
               artifact_name='',
               splits=NO_SPLIT):
    artifact_type = metadata_store_pb2.ArtifactType()
    artifact_type.name = type_name
    artifact_type.properties['type_name'] = metadata_store_pb2.STRING
    artifact_type.properties['subtype_name'] = metadata_store_pb2.STRING
    # This is a temporary solution due to b/123435989.
    artifact_type.properties['name'] = metadata_store_pb2.STRING
    # This indicates the state of an artifact. A state can be any of the
    # followings: PENDING, PUBLISHED, MISSING, DELETING, DELETED
    # TODO(ruoyu): Maybe switch to artifact top-level state if it's supported.
    artifact_type.properties['state'] = metadata_store_pb2.STRING
    # Span number of an artifact. For the same artifact type produced by the
    # same executor, this number should always increase.
    artifact_type.properties['span'] = metadata_store_pb2.INT
    # Comma separated splits recognized. Empty string means artifact has no
    # split.
    artifact_type.properties['splits'] = metadata_store_pb2.STRING
    artifact_type.properties['default_file_name'] = metadata_store_pb2.STRING

    self.artifact_type = artifact_type

    artifact = metadata_store_pb2.Artifact()
    artifact.uri = uri or ''
    artifact.properties['type_name'].string_value = type_name
    artifact.properties['name'].string_value = artifact_name
    artifact.properties['splits'].string_value = splits
    self.artifact = artifact
    artifact.properties['default_file_name'].string_value = (
      default_file_name or '')

  def __str__(self):
    return '{}: {}.{}.{}'.format(self.artifact.properties['name'].string_value,
                                 self.artifact_type.name, self.uri, str(
                                     self.id))

  def __repr__(self):
    return self.__str__()

  @property
  def uri(self):
    return tf.compat.as_str(self.artifact.uri)

  @property
  def id(self):
    return self.artifact.id

  @property
  def span(self):
    return self.artifact.properties['span'].int_value

  @property
  def type_id(self):
    return self.artifact.type_id

  @property
  def type_name(self):
    return self.artifact_type.name

  @property
  def default_file_name(self):
    return self.artifact.properties['default_file_name'].string_value

  @property
  def state(self):
    return self.artifact.properties['state'].string_value

  @property
  def split_uris(self):
    return {
        split: tf.compat.as_str(os.path.join(self.artifact.uri, split))
        for split in filter(
            None, self.artifact.properties['splits'].string_value.split(','))
    }

  def split_uri(self, split):
    return self.split_uris[split]

  def set_artifact(self, artifact):
    self.artifact = artifact

  def set_artifact_type(self, artifact_type):
    self.artifact_type = artifact_type
    self.artifact.type_id = artifact_type.id

  def set_artifact_id(self, artifact_id):
    self.artifact.id = artifact_id

  def set_artifact_uri(self, uri):
    self.artifact.uri = uri

  def set_artifact_span(self, span):
    self.artifact.properties['span'].int_value = span

  def set_artifact_splits(self, splits):
    self.artifact.properties['splits'].string_value = splits

  def set_artifact_state(self, state):
    self.artifact.properties['state'].string_value = state

  def set_string_custom_property(self, key, value):
    self.artifact.custom_properties[key].string_value = value

  def set_int_custom_property(self, key, value):
    self.artifact.custom_properties[key].int_value = builtins.int(value)


# TODO(b/122682605): move CSVExampleSource into tfx_types
# tfx_example_source provides examples to TFX pipelines.
# TODO(b/123035573): Decide namespace hierarchy for our types.
class BaseExampleSource(with_metaclass(abc.ABCMeta, TfxType)):

  def __init__(self,
               type_name,
               uri='',
               default_file_name='',
               artifact_name='', splits=''):
    super(BaseExampleSource, self).__init__(type_name,
                                            uri=uri,
                                            default_file_name=default_file_name,
                                            artifact_name=artifact_name,
                                            splits=splits)
    self.artifact.properties['subtype_name'].string_value = type(self).__name__


class TfExampleSource(BaseExampleSource):
  """Example sources provided as a tfrecord file."""

  def __init__(self,
               uri=None,
               default_file_name='data.tfrecord',
               artifact_name='',
               splits=NO_SPLIT):
    super(TfExampleSource, self).__init__(
        'ExamplesPath',
        uri=uri,
        default_file_name=default_file_name,
        artifact_name=artifact_name,
        splits=splits)


class CSVExampleSource(BaseExampleSource):
  """Example sources provided as a CSV file."""

  # TODO(ruoyu): Substitute hard-coded type name with codegen constants once
  # available.
  def __init__(self,
               uri=None,
               default_file_name='data.csv',
               artifact_name='',
               presplit=False):
    splits = NO_SPLIT
    if presplit:
      splits = DEFAULT_EXAMPLE_SPLITS
    super(CSVExampleSource, self).__init__(
        'ExamplesPath',
        uri=uri,
        default_file_name=default_file_name,
        artifact_name=artifact_name,
        splits=splits)

  @classmethod
  def load_column_names(cls, csv_example_source):
    with file_io.FileIO(csv_example_source, 'r') as f:
      return f.readline().strip().split(',')

  # TODO(b/122685559): Figure out how to create a common Beam transform factory
  # from a csv_decoder_fn.


def parse_artifact_dict(json_str):
  """Parse a dict of artifact and type from its json format."""
  tfx_artifacts = {}
  for k, v in json.loads(json_str).items():
    artifact = metadata_store_pb2.Artifact()
    Parse(json.dumps(v['artifact']), artifact)
    artifact_type = metadata_store_pb2.ArtifactType()
    Parse(json.dumps(v['artifact_type']), artifact_type)
    tfx_artifacts[k] = TfxType(artifact_type.name, artifact.uri)
    tfx_artifacts[k].set_artifact_type(artifact_type)
    tfx_artifacts[k].set_artifact(artifact)
  return tfx_artifacts


def jsonify_tfx_type_dict(artifact_dict):
  """Serialize a dict of TfxType into json string."""
  d = {
      k: {
          'artifact': json.loads(MessageToJson(v.artifact)),
          'artifact_type': json.loads(MessageToJson(v.artifact_type)),
      } for k, v in artifact_dict.items()
  }
  return json.dumps(d)

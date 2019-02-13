"""TFX ml metadata library."""
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
import hashlib
from ml_metadata.metadata_store import metadata_store
from ml_metadata.proto import metadata_store_pb2
import tensorflow as tf
from tfx.tfx_types import ARTIFACT_STATE_PUBLISHED


# TODO(ruoyu): Figure out the story mutable UDFs. We should not reuse previous
# run when having different UDFs.
class Metadata(object):
  """Helper class to handle metadata I/O."""

  def __init__(self, connection_config):
    self._connection_config = connection_config

  def __enter__(self):
    # TODO(ruoyu): Establishing a connection pool instead of newing
    # a connection every time.
    self._store = metadata_store.MetadataStore(self._connection_config)
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    del self._store

  def _prepare_artifact_type(self, artifact_type):
    if artifact_type.id:
      return artifact_type
    type_id = self._store.put_artifact_type(artifact_type)
    artifact_type.id = type_id
    return artifact_type

  def get_artifact_by_id(self, artifact_id):
    [artifact] = self._store.get_artifacts_by_id([artifact_id])
    return artifact

  def update_artifact_state(self, artifact, new_state):
    if not artifact.id:
      raise ValueError('Artifact id missing for {}'.format(artifact))
    artifact.properties['state'].string_value = new_state
    self._store.put_artifacts([artifact])

  # This should be atomic. However this depends on ML metadata transaction
  # support.
  def check_artifact_state(self, artifact, expected_states):
    if not artifact.id:
      raise ValueError('Artifact id missing for {}'.format(artifact))
    [artifact_in_metadata] = self._store.get_artifacts_by_id([artifact.id])
    current_artifact_state = artifact_in_metadata.properties[
        'state'].string_value
    if not current_artifact_state in expected_states:
      raise RuntimeError(
          'Artifact state for {} is {}, but one of {} expected'.format(
              artifact_in_metadata, current_artifact_state, expected_states))

  # TODO(ruoyu): Make this transaction-based once b/123573724 is fixed.
  def publish_artifacts(self, raw_artifact_list,
                        state=ARTIFACT_STATE_PUBLISHED):
    """Publish a list of artifacts if any is not already published."""
    artifact_list = []
    for raw_artifact in raw_artifact_list:
      artifact_type = self._prepare_artifact_type(raw_artifact.artifact_type)
      raw_artifact.set_artifact_type(artifact_type)
      if not raw_artifact.artifact.id:
        raw_artifact.set_artifact_state(state)
        [artifact_id] = self._store.put_artifacts([raw_artifact.artifact])
        raw_artifact.set_artifact_id(artifact_id)
      artifact_list.append(raw_artifact.artifact)
    return artifact_list

  def get_all_artifacts(self):
    return self._store.get_artifacts()

  def _prepare_event(self, execution_id, artifact_id, is_input):
    """Commits a single event to the repository."""
    event = metadata_store_pb2.Event()
    event.artifact_id = artifact_id
    event.execution_id = execution_id
    if is_input:
      event.type = metadata_store_pb2.Event.DECLARED_INPUT
    else:
      event.type = metadata_store_pb2.Event.DECLARED_OUTPUT
    return event

  def _prepare_input_event(self, execution_id, artifact_id):
    return self._prepare_event(execution_id, artifact_id, True)

  def _prepare_output_event(self, execution_id, artifact_id):
    return self._prepare_event(execution_id, artifact_id, False)

  def _prepare_execution_type(self, type_name, exec_properties={}):
    """Get a execution type. Use existing type if available"""
    try:
      execution_type = self._store.get_execution_type(type_name)
      return execution_type.id
    except tf.errors.NotFoundError:
      execution_type = metadata_store_pb2.ExecutionType()
      execution_type.name = type_name
      for k, v in exec_properties.items():
        execution_type.properties[k] = metadata_store_pb2.STRING
      execution_type.properties['state'] = metadata_store_pb2.STRING
      # TODO(ruoyu): Find a better place / solution to the checksum logic.
      if 'module_file' in exec_properties:
        execution_type.properties['checksum_md5'] = metadata_store_pb2.STRING

      return self._store.put_execution_type(execution_type)

  def _prepare_execution(self, type_name, state, exec_properties={}):
    execution = metadata_store_pb2.Execution()
    execution.type_id = self._prepare_execution_type(type_name, exec_properties)
    for k, v in exec_properties.items():
      # We always convert execution properties to unicode.
      execution.properties[k].string_value = tf.compat.as_text(
          tf.compat.as_str_any(v))
    execution.properties['state'].string_value = tf.compat.as_text(state)
    # We also need to checksum UDF file to identify different binary being used.
    # Do we have a better way to checksum a file than hashlib.md5?
    # TODO(ruoyu): Find a better place / solution to the checksum logic.
    if 'module_file' in exec_properties and exec_properties[
        'module_file'] and tf.gfile.Exists(exec_properties['module_file']):
      execution.properties['checksum_md5'].string_value = tf.compat.as_text(
          tf.compat.as_str_any(
              hashlib.md5(open(
                  exec_properties['module_file']).read()).hexdigest()))
    return execution

  def _update_execution_state(self, execution, new_state):
    execution.properties['state'].string_value = tf.compat.as_text(new_state)
    self._store.put_executions([execution])

  def prepare_execution(self, type_name, exec_properties):
    execution = self._prepare_execution(type_name, 'new', exec_properties)
    [execution_id] = self._store.put_executions([execution])
    return execution_id

  def publish_execution(self, execution_id, input_dict, output_dict):
    """Publish an execution with input and output artifacts info.

    Args:
      execution_id: id of execution to be published.
      input_dict: inputs artifacts used by the execution with id ready.
      output_dict: output artifacts produced by the execution without id.

    Returns:
      Updated outputs with artifact ids.

    Raises:
      ValueError: If any output artifact already has id set.
    """
    [execution] = self._store.get_executions_by_id([execution_id])
    self._update_execution_state(execution, 'complete')

    tf.logging.info(
        'Publishing execution {}, with inputs {} and outputs {}'.format(
            execution, input_dict, output_dict))
    events = []
    if input_dict:
      for single_input in input_dict.values():
        if not single_input.artifact.id:
          raise ValueError(
              'input artifact {} has missing id'.format(single_input))
        events.append(self._prepare_input_event(
            execution_id, single_input.artifact.id))
    if output_dict:
      for single_output in output_dict.values():
        if single_output.artifact.id:
          raise ValueError(
              'output artifact {} already has an id'.format(single_output))
        [published_artifact] = self.publish_artifacts([single_output])  # pylint: disable=unbalanced-tuple-unpacking
        single_output.set_artifact(published_artifact)
        events.append(
            self._prepare_output_event(execution_id, published_artifact.id))
    if events:
      self._store.put_events(events)
    tf.logging.info(
        'Published execution with final outputs {}'.format(output_dict))
    return output_dict

  def previous_run(self, type_name, input_dict, exec_properties):
    """Gets previous run of same type that takes current set of input.

    Args:
      type_name: the type of run.
      input_dict: inputs used by the run.
      exec_properties: execution properties used by the run.

    Returns:
      Execution id of previous run that takes the input dict. None if not found.
    """

    tf.logging.info(
        'Checking previous run for execution_type_name {} and input_dict {}'
        .format(type_name, input_dict))

    if not input_dict:
      tf.logging.info(
          'No previous run for empty input_dict of type {}'.format(type_name))
      return None
    execution_ids_set = set()
    for input_artifact_id in [
        x.artifact.id for x in filter(None, input_dict.values())
    ]:
      current_execution_ids_set = set([
          e.execution_id
          for e in self._store.get_events_by_artifact_ids([input_artifact_id])
      ])
      tf.logging.info('Artifact id {} is used in executions {}'.format(
          input_artifact_id, current_execution_ids_set))
      if execution_ids_set:
        execution_ids_set = current_execution_ids_set.intersection(
            execution_ids_set)
      else:
        execution_ids_set = current_execution_ids_set

    tf.logging.info(
        'Following executions include all input artifacts: {}'.format(
            execution_ids_set))

    if execution_ids_set:
      expected_previous_execution = self._prepare_execution(
          type_name, 'complete', exec_properties)
      for execution in self._store.get_executions_by_id(execution_ids_set):
        expected_previous_execution.id = execution.id
        if execution == expected_previous_execution:
          tf.logging.info('Found previous execution: {}'.format(execution))
          return execution.id
    tf.logging.info('No execution matching type id and input artifacts found')
    return None

  def fetch_previous_result_artifacts(self, output_dict, execution_id):
    """Fetches output with artifact ids produced by a previous run.

    Args:
      output_dict: a dict of output BaseTfxPath objects.
      execution_id: the id of the execution that produced the outputs.

    Returns:
      Original output_dict with artifact id inserted.

    Raises:
      RuntimeError: path change without clean metadata.
    """

    name_to_artifacts = {}
    for event in self._store.get_events_by_execution_ids([execution_id]):
      if event.type == metadata_store_pb2.Event.DECLARED_OUTPUT:
        [artifact] = self._store.get_artifacts_by_id([event.artifact_id])
        name_to_artifacts[artifact.properties['name'].string_value] = artifact
    for output_name, output in output_dict.items():
      if output_name in name_to_artifacts:
        output.set_artifact(name_to_artifacts[output_name])
      else:
        raise RuntimeError('Unmatched output from previous execution.')
    return dict(output_dict)

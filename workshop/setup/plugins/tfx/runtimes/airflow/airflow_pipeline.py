"""TFX implementation of ml-pipelines++ SDK using Airflow."""
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
import collections
import os

from airflow.models import DAG
from ml_metadata.proto import metadata_store_pb2
import tensorflow as tf
from tfx.runtimes.airflow.airflow_component import Setup



class AirflowPipeline(DAG):
  """TFX Pipeline for airflow.

  This is a prototype of the TFX DSL syntax onto an airflow implementation.
  """
  # TODO(b/122681512): Currently using static class object to communicate
  # across scope. Need to come up with a better solution.
  pipelines = None

  def __init__(self, **kwargs):
    super(AirflowPipeline, self).__init__(
        dag_id=kwargs['pipeline_name'],
        schedule_interval=kwargs['schedule_interval'],
        start_date=kwargs['start_date'])
    self.tfx_cfg = self._init_pipeline_config(**kwargs)
    self.additional_pipeline_args = kwargs.get('additional_pipeline_args') or []
    self.docker_operator_cfg = kwargs.get('docker_operator_cfg', None)
    self.enable_cache = kwargs.get('enable_cache') or False
    # TODO(b/122677896): Add unid to log_root and run_name
    self.log_root = os.path.join(kwargs.get('log_root') or '/var/tmp/tfx/logs/',
                                 kwargs['pipeline_name'])
    self.run_name = kwargs['pipeline_name']
    self.metadata_connection_config = kwargs.get(
        'metadata_connection_config',
        self._get_default_metadata_connection_config(**kwargs))

    if AirflowPipeline.pipelines is None:
      AirflowPipeline.pipelines = {}
    if self.run_name not in AirflowPipeline.pipelines:
      AirflowPipeline.pipelines[self.run_name] = self

    # Add the set up job.
    self._setup_task = Setup(parent_dag=self, project_path=self.project_path)
    self._upstreams_map = collections.defaultdict(set)

  def _get_default_metadata_connection_config(self, **kwargs):
    db_uri = os.path.join(kwargs['metadata_db_root'], kwargs['pipeline_name'],
                          'metadata.db')
    tf.gfile.MakeDirs(os.path.dirname(db_uri))
    connection_config = metadata_store_pb2.ConnectionConfig()
    connection_config.sqlite.filename_uri = db_uri
    connection_config.sqlite.connection_mode =\
        metadata_store_pb2.SqliteMetadataSourceConfig.READWRITE_OPENCREATE
    return connection_config

  def _init_pipeline_config(self, **kwargs):
    """Creates a config object."""
    config = {'producers': {}, 'consumers': {}, 'dag': self}

    for key, value in kwargs.items():
      # don't lowercase the dag object; don't overwrite an existing key
      if (key.lower() != 'dag') and (key.lower() not in config):
        config[key.lower()] = value

    self.project_path = os.path.join(config['pipeline_root'],
                                     config['pipeline_name'])

    return config

  def add_node_to_graph(self, node, consumes, produces):
    """Build the dependency graph as nodes are defined."""

    if hasattr(self, '_setup_task'):
      node.set_upstream(self._setup_task)

    consumers = self.tfx_cfg['consumers']
    producers = self.tfx_cfg['producers']

    # Because the entire output list is consumed as a whole,
    for artifact_list in consumes or []:
      for artifact in artifact_list:
        # register worker as a consumer of artifact(s)
        if artifact in producers:
          for other_node in producers[artifact]:
            if artifact not in consumers or node not in consumers[artifact]:
              # we should add other_node -> node
              if other_node in self._upstreams_map[node]:
                continue
              self._upstreams_map[node].add(other_node)
              node.set_upstream(other_node)

        if artifact in consumers:
          consumers[artifact].add(node)
        else:
          consumers[artifact] = {node}

    for produce_list in produces or []:
      for artifact in produce_list:
        if artifact in producers:
          producers[artifact].add(node)
        else:
          producers[artifact] = {node}

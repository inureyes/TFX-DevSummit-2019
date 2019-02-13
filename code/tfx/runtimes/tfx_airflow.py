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
import base64
import collections
import json
import os
import types

from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from ml_metadata.proto import metadata_store_pb2
import tensorflow as tf
from tfx.base_component import BaseDecorator
from tfx.drivers.base_driver import BaseDriver
from tfx.drivers.base_driver import DriverOptions
from tfx.executors.base_runtime import BaseRuntime
from tfx.executors.setup import Setup as SetupExecutor
from tfx.runtimes import tfx_logger
from tfx.runtimes.tfx_metadata import Metadata
from tfx.tfx_types import jsonify_tfx_type_dict
from tfx.tfx_types import parse_artifact_dict
from google.protobuf.json_format import MessageToJson
from google.protobuf.json_format import Parse


class PipelineDecorator(BaseDecorator):

  def _new_pipeline(self, **kwargs):
    """Creates a pipeline runnable by airflow."""
    self._pipeline = Tfx(setup_task=SetupExecutor, **kwargs)
    return self._pipeline

  def _new_component(self, component_name, unique_name, driver, executor,
                     input_dict, output_dict, exec_properties):
    return Component(
        run_name=self._pipeline.run_name,
        component_name=component_name,
        unique_name=unique_name,
        driver=driver,
        executor=executor,
        input_dict=input_dict,
        output_dict=output_dict,
        exec_properties=exec_properties)


class MetadataExecutorAdaptor(object):
  """Execute executor based on decision from Metadata.."""

  def __init__(self, input_dict, output_dict, exec_properties, driver_options,
               driver_class, executor_class, beam_pipeline_args,
               metadata_connection_config):
    self._input_dict = dict((k, v) for k, v in input_dict.items() if v)
    self._output_dict = output_dict
    self._exec_properties = exec_properties
    self._driver_options = driver_options
    self._driver_class = driver_class
    self._executor_class = executor_class
    self._logger = tfx_logger.get_logger(exec_properties['log_root'], '.comp')
    self._execution_id = None
    self._beam_pipeline_args = beam_pipeline_args or []
    self._metadata_connection_config = metadata_connection_config

  def _update_input_dict_from_xcom(self, task_instance):
    """Determine actual input artifacts used from upstream tasks."""
    for single_input in self._input_dict.values():
      if not single_input.artifact.id and hasattr(
          single_input, 'source_key') and hasattr(single_input,
                                                  'source_component_id'):
        xcom_result = task_instance.xcom_pull(
            key=single_input.source_key,
            dag_id=single_input.source_component_id)
        if xcom_result:
          artifact_and_type = json.loads(xcom_result)
          Parse(
              json.dumps(artifact_and_type['artifact']), single_input.artifact)
          Parse(
              json.dumps(artifact_and_type['artifact_type']),
              single_input.artifact_type)

  def _publish_execution_to_metadata(self):
    with Metadata(self._metadata_connection_config) as m:
      return m.publish_execution(self._execution_id, self._input_dict,
                                 self._output_dict)

  def _publish_outputs_to_pipeline(self, task_instance, final_output):
    for o in final_output.values():
      xcom_value = json.dumps({
          'artifact': json.loads(MessageToJson(o.artifact)),
          'artifact_type': json.loads(MessageToJson(o.artifact_type)),
      })
      task_instance.xcom_push(key=o.source_key, value=xcom_value)

  def check_cache_and_maybe_prepare_execution(self, task_id, cached_branch,
                                              uncached_branch, **kwargs):
    """Depending on previous run status, run exec or skip."""
    # As the executor parameters affect the artifact, resolve all runtime
    # parameter overrides prior to checking the cache.
    for k, v in self._exec_properties.items():
      self._logger.info('Looking for {} override.'.format(k))
      x = self._resolve_runtime_parameter(
          prefix=task_id, param_name=k, context=kwargs, param_type=type(v))
      if x is not None:
        self._logger.info(
            'Parameter \'{}\' overridden by runtime value of {}.'.format(k, x))
        self._exec_properties[k] = x

    task_instance = kwargs['ti']
    self._update_input_dict_from_xcom(task_instance)

    with Metadata(self._metadata_connection_config) as m:
      driver = self._driver_class(log_root=self._exec_properties['log_root'],
                                  metadata_handler=m)
      execution_decision = driver.prepare_execution(
          self._input_dict, self._output_dict, self._exec_properties,
          self._driver_options)
      if not execution_decision.execution_id:
        self._logger.info(
            'All artifacts found. Publishing to pipeline and skipping executor.'
        )
        self._publish_outputs_to_pipeline(task_instance,
                                          execution_decision.output_dict)
        return cached_branch

      task_instance.xcom_push(
          key='_exec_inputs',
          value=jsonify_tfx_type_dict(execution_decision.input_dict))
      task_instance.xcom_push(
          key='_exec_outputs',
          value=jsonify_tfx_type_dict(execution_decision.output_dict))
      task_instance.xcom_push(
          key='_exec_properties',
          value=json.dumps(execution_decision.exec_properties))
      task_instance.xcom_push(
          key='_execution_id', value=execution_decision.execution_id)

      self._logger.info('No cached execution found. Starting executor.')
      return uncached_branch

  def python_exec(self, cache_task_name, **kwargs):
    """PythonOperator callable to invoke executor."""
    # This is executed in worker-space not runtime-space (i.e. with distributed
    # workers, this runs on the worker node not the controller node).
    task_instance = kwargs['ti']
    self._refresh_execution_args_from_xcom(task_instance, cache_task_name)
    executor = self._executor_class(
        BaseRuntime(pipeline_args=self._beam_pipeline_args))
    # Run executor
    executor.do(self._input_dict, self._output_dict, self._exec_properties)
    # Docker operator chooses 'return_value' so we try to be consistent.
    task_instance.xcom_push(
        key='return_value', value=jsonify_tfx_type_dict(self._output_dict))

  def _refresh_execution_args_from_xcom(self, task_instance, pushing_task_name):
    """Refresh inputs, outputs and exec_properties from xcom."""
    inputs_str = task_instance.xcom_pull(
        key='_exec_inputs', task_ids=pushing_task_name)
    input_dict = parse_artifact_dict(inputs_str)
    for k, v in input_dict.items():
      self._input_dict[k].set_artifact(v.artifact)
      self._input_dict[k].set_artifact_type(v.artifact_type)

    outputs_str = task_instance.xcom_pull(
        key='_exec_outputs', task_ids=pushing_task_name)
    output_dict = parse_artifact_dict(outputs_str)
    for k, v in output_dict.items():
      self._output_dict[k].set_artifact(v.artifact)
      self._output_dict[k].set_artifact_type(v.artifact_type)

    exec_properties_str = task_instance.xcom_pull(
        key='_exec_properties', task_ids=pushing_task_name)
    self._exec_properties = json.loads(exec_properties_str)

    self._execution_id = task_instance.xcom_pull(
        key='_execution_id', task_ids=pushing_task_name)

  def docker_operator(self, task_id, parent_dag, docker_operator_cfg,
                      pusher_task):
    """Creates a DockerOpertor to run executor in docker images."""
    volumes = [
        v if ':' in v else '{v}:{v}:rw'.format(v=v)
        for v in docker_operator_cfg.get('volumes', [])
    ]
    command = ' '.join([
        '--write-outputs-stdout',
        '--executor=%s' % self._executor_class.__name__,
        '--inputs-base64={{ ti.xcom_pull(key="_exec_inputs", '
        'task_ids="%s") | b64encode }}' % pusher_task,
        '--outputs-base64={{ ti.xcom_pull(key="_exec_outputs", '
        'task_ids="%s") | b64encode }}' % pusher_task,
        '--exec-properties-base64={{ ti.xcom_pull(key="_exec_properties", '
        'task_ids="%s") | b64encode }}' % pusher_task,
    ] + self._beam_pipeline_args)
    return DockerOperator(
        dag=parent_dag,
        task_id=task_id,
        command=command,
        volumes=volumes,
        xcom_push=True,
        image='tfx-executors-test:latest',
    )

  def publish_exec(self, cache_task_name, exec_task_name, **kwargs):
    """Publish artifacts produced in this execution to the pipeline."""
    task_instance = kwargs['ti']
    self._refresh_execution_args_from_xcom(task_instance, cache_task_name)

    # Overwrite outputs from cache with outputs produced by exec operator.
    outputs_str = task_instance.xcom_pull(
        key='return_value', task_ids=exec_task_name)
    output_dict = parse_artifact_dict(outputs_str)
    for k, v in output_dict.items():
      self._output_dict[k].set_artifact(v.artifact)
      self._output_dict[k].set_artifact_type(v.artifact_type)

    final_output = self._publish_execution_to_metadata()
    self._publish_outputs_to_pipeline(task_instance, final_output)

  def _resolve_runtime_parameter(self,
                                 prefix,
                                 param_name,
                                 context,
                                 param_type=types.StringType):
    """Implements support for airflow runtime parameters.

    Args:
      prefix: Namespace for the parameter lookup.
      param_name: The name of the parameter to be used.
      context: The workflow-specific context to go searching for the parameter.
        Specifically for airflow, this is the dag_run that is passed from the
        Component -> Operator.
      param_type: Used to cast the unicode response from Airflow as int or str.

    Returns:
      The runtime-override parameter value, or default if not set.

    Precedence is CLI > Airflow global variable > static value > default.

    CLI and Airflow variables are prefixed with the pipeline and
    component name (e.g. chicago_taxi_pipeline.trainer.eval_steps) but
    hardcoded parameters in the pipeline DAG config are not (as they
    are already properly scoped as part of the Component arguments).
    """
    # TODO(khaas): Replace all info logging with debug when config has loglevel
    full_param_name = prefix + '.' + param_name

    self._logger.debug('Looking for overrides of {}.'.format(full_param_name))
    # First look the variable in the CLI parameters (if present)
    if 'dag_run' in context:
      # conf only exists in dag_run on CLI-triggered runs
      if not isinstance(context['dag_run'].conf, types.NoneType):
        cli_params = context['dag_run'].conf
        if full_param_name in cli_params:
          self._logger.info('Found {}={} in CLI parameters.'.format(
              full_param_name, cli_params[full_param_name]))
          return cli_params[full_param_name]

    # If not found, look for Airflow global variable.  Default is DOESNOTEXIST
    # because airflow will throw an exception if the variable is not found and
    # the default is set to None.

    param_val = Variable.get(full_param_name, default_var='DOESNOTEXIST')
    # Airflow encodes variables as unicode when passed to executors so if we get
    # a unicode value and have a known type, cast the value to the expected
    # type.  Otherwise, cast to a string.
    if isinstance(param_val, unicode):
      self._logger.info('Found {}={} as Airflow variable.'.format(
          full_param_name, param_val))
      if param_type == types.IntType:
        return int(param_val)
      else:
        return str(param_val)

    # Not found as an override
    self._logger.debug('No override found for {}.'.format(full_param_name))
    return None


class Component(SubDagOperator):
  """Generic TFX component that consists of drivers, executors, and metadata."""

  class TfxWorker(DAG):
    """The airflow-specific implementation of TfxWorker."""

    def __init__(self, task_id, parent_dag, input_dict, output_dict,
                 exec_properties, driver_options, driver_class, executor_class,
                 beam_pipeline_args, metadata_connection_config):
      super(Component.TfxWorker, self).__init__(
          dag_id=task_id,
          schedule_interval=None,
          start_date=parent_dag.start_date,
          user_defined_filters={'b64encode': base64.b64encode})
      # TODO(b/123534176): Remove log_root from exec_properties
      adaptor = MetadataExecutorAdaptor(
          input_dict=input_dict,
          output_dict=output_dict,
          exec_properties=exec_properties,
          driver_options=driver_options,
          driver_class=driver_class,
          executor_class=executor_class,
          beam_pipeline_args=beam_pipeline_args,
          metadata_connection_config=metadata_connection_config,
      )
      # Before the executor runs, check if the artifact already exists
      checkcache_op = BranchPythonOperator(
          task_id=task_id + '.checkcache',
          provide_context=True,
          python_callable=adaptor.check_cache_and_maybe_prepare_execution,
          op_kwargs={
              'uncached_branch': task_id + '.exec',
              'cached_branch': task_id + '.publishcache',
              'task_id': task_id,
          },
          dag=self)
      if parent_dag.docker_operator_cfg:
        tfx_op = adaptor.docker_operator(
            task_id=task_id + '.exec',
            pusher_task=task_id + '.checkcache',
            parent_dag=self,
            docker_operator_cfg=parent_dag.docker_operator_cfg,
        )
      else:
        tfx_op = PythonOperator(
            task_id=task_id + '.exec',
            provide_context=True,
            python_callable=adaptor.python_exec,
            op_kwargs={
                'cache_task_name': task_id + '.checkcache',
            },
            dag=self)
      publishcache_op = DummyOperator(
          task_id=task_id + '.publishcache', dag=self)
      publishexec_op = PythonOperator(
          task_id=task_id + '.publishexec',
          provide_context=True,
          python_callable=adaptor.publish_exec,
          op_kwargs={
              'cache_task_name': task_id + '.checkcache',
              'exec_task_name': task_id + '.exec',
          },
          dag=self)

      tfx_op.set_upstream(checkcache_op)
      publishexec_op.set_upstream(tfx_op)
      publishcache_op.set_upstream(checkcache_op)

  class TfxComponentOutputs(object):
    """Helper class to wrap outputs from TFX components."""

    def __init__(self, d):
      self.__dict__ = d

  def _get_working_dir(self, base_dir, component_name, unique_name='DEFAULT'):
    return os.path.join(base_dir, component_name, unique_name, '')

  def __init__(self, run_name, component_name, unique_name, driver, executor,
               input_dict, output_dict, exec_properties):
    # Get parent dag.
    if run_name not in Tfx.pipelines:
      raise RuntimeError('Unknown run {}.'.format(run_name))
    parent_dag = Tfx.pipelines[run_name]

    # Prepare parameters to create TFX worker.
    if unique_name:
      worker_name = component_name + '.' + unique_name
    else:
      worker_name = component_name
    task_id = parent_dag.dag_id + '.' + worker_name

    # Create output object of appropriate type
    output_dir = self._get_working_dir(
        parent_dag.project_path,  # pylint: disable=protected-access
        component_name,
        unique_name or '')

    exec_properties['log_root'] = os.path.join(parent_dag.log_root, worker_name)
    driver_options = DriverOptions(
        worker_name=worker_name,
        base_output_dir=output_dir,
        enable_cache=parent_dag.enable_cache)

    worker = Component.TfxWorker(
        task_id=task_id,
        parent_dag=parent_dag,
        input_dict=input_dict,
        output_dict=output_dict,
        exec_properties=exec_properties,
        driver_options=driver_options,
        driver_class=driver,
        executor_class=executor,
        beam_pipeline_args=parent_dag.beam_pipeline_args,
        metadata_connection_config=parent_dag.metadata_connection_config)
    SubDagOperator.__init__(
        self, subdag=worker, task_id=worker_name, dag=parent_dag)

    # Update the output dict before providing to downstream components
    for k, v in output_dict.items():
      v.source_key = k
      v.source_component_id = worker.dag_id
    self._input = input_dict.values()
    self.outputs = Component.TfxComponentOutputs(output_dict)
    parent_dag.add_node_to_graph(
        node=self, consumes=self._input, produces=output_dict.values())


class Setup(Component):
  """For local, creates the output directory for this pipeline."""

  def __init__(self, run_name, project_path):
    component_name = 'setup'
    super(Setup, self).__init__(
        run_name=run_name,
        component_name=component_name,
        unique_name=None,
        driver=BaseDriver,
        executor=SetupExecutor,
        input_dict={},
        output_dict={},
        exec_properties={'project_path': project_path})


class Tfx(DAG):
  """TFX Pipeline for airflow.

  This is a prototype of the TFX DSL syntax onto an airflow implementation.
  """
  # TODO(b/122681512): Currently using static class object to communicate
  # across scope. Need to come up with a better solution.
  pipelines = None

  def __init__(self, **kwargs):
    super(Tfx, self).__init__(
        dag_id=kwargs['pipeline_name'],
        schedule_interval=kwargs['schedule_interval'],
        start_date=kwargs['start_date'])
    self.tfx_cfg = self._init_pipeline_config(**kwargs)
    self.beam_pipeline_args = kwargs.get('beam_pipeline_args') or []
    self.docker_operator_cfg = kwargs.get('docker_operator_cfg', None)
    self.enable_cache = kwargs.get('enable_cache') or False
    self.log_root = os.path.join(kwargs.get('log_root') or '/var/tmp/tfx/logs/',
                                 kwargs['pipeline_name'], kwargs['run_id'])
    self.run_name = kwargs['pipeline_name'] + '.' + kwargs['run_id']
    self.metadata_connection_config = kwargs.get(
        'metadata_connection_config',
        self._get_default_metadata_connection_config(**kwargs))

    if Tfx.pipelines is None:
      Tfx.pipelines = {}
    if self.run_name not in Tfx.pipelines:
      Tfx.pipelines[self.run_name] = self

    # Add the set up job.
    self._setup_task = Setup(
        run_name=self.run_name, project_path=self.project_path)

  def _get_default_metadata_connection_config(self, **kwargs):
    db_uri = os.path.join(kwargs['output_dir'], kwargs['pipeline_name'],
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

    self.project_path = os.path.join(config['output_dir'],
                                     config['pipeline_name'], config['run_id'])

    return config

  def add_node_to_graph(self, node, consumes, produces):
    """Build the dependency graph as nodes are defined."""

    if hasattr(self, '_setup_task'):
      node.set_upstream(self._setup_task)

    consumers = self.tfx_cfg['consumers']
    producers = self.tfx_cfg['producers']
    upstreams_map = collections.defaultdict(set)

    if consumes is not None:
      for artifact in consumes:
        # register worker as a consumer of artifact(s)
        if artifact in producers:
          for other_node in producers[artifact]:
            if artifact not in consumers or node not in consumers[artifact]:
              upstreams_map[node].add(other_node)

        if artifact in consumers:
          consumers[artifact].add(node)
        else:
          consumers[artifact] = {node}

    for node, upstreams in upstreams_map.items():
      node.set_upstream(list(upstreams))

    if produces is not None:
      for artifact in produces:
        if artifact in producers:
          producers[artifact].add(node)
        else:
          producers[artifact] = {node}

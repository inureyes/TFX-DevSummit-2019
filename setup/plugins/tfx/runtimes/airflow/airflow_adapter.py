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
import json
import types

import apache_beam as beam

from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator
from tfx.runtimes.tfx_metadata import Metadata
from tfx.utils import logging_utils
from tfx.utils.types import jsonify_tfx_type_dict
from tfx.utils.types import parse_tfx_type_dict
from tfx.utils.types import TfxType


class AirflowAdapter(object):
  """Execute executor based on decision from Metadata.."""

  def __init__(self, input_dict, output_dict, exec_properties, driver_options,
               driver_class, executor_class, additional_pipeline_args,
               metadata_connection_config):
    self._input_dict = dict((k, v) for k, v in input_dict.items() if v)
    self._output_dict = output_dict
    self._exec_properties = exec_properties
    self._driver_options = driver_options
    self._driver_class = driver_class
    self._executor_class = executor_class
    self._logger = logging_utils.get_logger(exec_properties['log_root'], 'comp')
    # Resolve source from input_dict and output_dict to decouple this earlier.
    self._input_source_dict = self._get_source_dict(self._input_dict)
    self._output_source_dict = self._get_source_dict(self._output_dict)
    self._execution_id = None
    self._additional_pipeline_args = additional_pipeline_args or {}
    self._metadata_connection_config = metadata_connection_config

  def _get_source_dict(self, inout_dict):
    """Capture name to source mapping from input/output dict."""
    source_dict = dict()
    for key, artifact_list in inout_dict.items():
      if not artifact_list or not getattr(artifact_list[0], 'source', None):
        continue
      source_dict[key] = artifact_list[0].source
    return source_dict

  def _update_input_dict_from_xcom(self, task_instance):
    """Determine actual input artifacts used from upstream tasks."""
    for key, input_list in self._input_dict.items():
      source = self._input_source_dict.get(key)
      if not source:
        continue
      xcom_result = task_instance.xcom_pull(
          key=source.key, dag_id=source.component_id)
      if not xcom_result:
        self._logger.warn('Cannot resolve xcom source from {}'.format(source))
        continue
      resolved_list = json.loads(xcom_result)
      for index, resolved_json_dict in enumerate(resolved_list):
        input_list[index] = TfxType.parse_from_json_dict(resolved_json_dict)

  def _publish_execution_to_metadata(self):
    with Metadata(self._metadata_connection_config) as m:
      return m.publish_execution(self._execution_id, self._input_dict,
                                 self._output_dict)

  def _publish_outputs_to_pipeline(self, task_instance, final_output):
    for key, output_list in final_output.items():
      source = self._output_source_dict[key]
      output_dict_list = [o.json_dict() for o in output_list]
      xcom_value = json.dumps(output_dict_list)
      task_instance.xcom_push(key=source.key, value=xcom_value)

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
        beam.Pipeline(
            argv=self._additional_pipeline_args.get('beam_pipeline_args', [])),
        self._additional_pipeline_args)
    # Run executor
    executor.Do(self._input_dict, self._output_dict, self._exec_properties)
    # Docker operator chooses 'return_value' so we try to be consistent.
    task_instance.xcom_push(
        key='return_value', value=jsonify_tfx_type_dict(self._output_dict))

  def _refresh_execution_args_from_xcom(self, task_instance, pushing_task_name):
    """Refresh inputs, outputs and exec_properties from xcom."""
    inputs_str = task_instance.xcom_pull(
        key='_exec_inputs', task_ids=pushing_task_name)
    self._input_dict = parse_tfx_type_dict(inputs_str)

    outputs_str = task_instance.xcom_pull(
        key='_exec_outputs', task_ids=pushing_task_name)
    self._output_dict = parse_tfx_type_dict(outputs_str)

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
    args = [
        '--write-outputs-stdout',
        '--executor=%s' % self._executor_class.__name__,
        '--inputs-base64={{ ti.xcom_pull(key="_exec_inputs", '
        'task_ids="%s") | b64encode }}' % pusher_task,
        '--outputs-base64={{ ti.xcom_pull(key="_exec_outputs", '
        'task_ids="%s") | b64encode }}' % pusher_task,
        '--exec-properties-base64={{ ti.xcom_pull(key="_exec_properties", '
        'task_ids="%s") | b64encode }}' % pusher_task]
    if 'beam_pipeline_args' in self._additional_pipeline_args:
      args += self._additional_pipeline_args['beam_pipeline_args']
    command = ' '.join(args)
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
    self._output_dict = parse_tfx_type_dict(outputs_str)
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

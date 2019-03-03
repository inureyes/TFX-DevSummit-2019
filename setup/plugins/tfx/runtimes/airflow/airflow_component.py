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
import os

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from tfx.drivers.base_driver import BaseDriver
from tfx.drivers.base_driver import DriverOptions
from tfx.executors.setup import Setup as SetupExecutor
from tfx.runtimes.airflow.airflow_adapter import AirflowAdapter


_OrchestrationSource = collections.namedtuple(
    '_OrchestrationSource',
    [
        'key',
        'component_id',
    ],
)


class TfxWorker(DAG):
  """The airflow-specific implementation of TfxWorker."""
  
  def __init__(self, task_id, parent_dag, input_dict, output_dict,
               exec_properties, driver_options, driver_class, executor_class,
               additional_pipeline_args, metadata_connection_config):
    super(TfxWorker, self).__init__(
      dag_id=task_id,
      schedule_interval=None,
      start_date=parent_dag.start_date,
      user_defined_filters={'b64encode': base64.b64encode})
    # TODO(b/123534176): Remove log_root from exec_properties
    adaptor = AirflowAdapter(
      input_dict=input_dict,
      output_dict=output_dict,
      exec_properties=exec_properties,
      driver_options=driver_options,
      driver_class=driver_class,
      executor_class=executor_class,
      additional_pipeline_args=additional_pipeline_args,
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


class Component(SubDagOperator):
  """Generic TFX component that consists of drivers, executors, and metadata."""

  def _get_working_dir(self, base_dir, component_name, unique_name='DEFAULT'):
    return os.path.join(base_dir, component_name, unique_name, '')

  def __init__(self, parent_dag, component_name, unique_name, driver, executor,
               input_dict, output_dict, exec_properties):
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

    # Update the output dict before providing to downstream components
    for k, output_list in output_dict.items():
      for single_output in output_list:
        single_output.source = _OrchestrationSource(key=k, component_id=task_id)

    exec_properties['log_root'] = os.path.join(parent_dag.log_root, worker_name)
    driver_options = DriverOptions(
        worker_name=worker_name,
        base_output_dir=output_dir,
        enable_cache=parent_dag.enable_cache)

    worker = TfxWorker(
        task_id=task_id,
        parent_dag=parent_dag,
        input_dict=input_dict,
        output_dict=output_dict,
        exec_properties=exec_properties,
        driver_options=driver_options,
        driver_class=driver,
        executor_class=executor,
        additional_pipeline_args=parent_dag.additional_pipeline_args,
        metadata_connection_config=parent_dag.metadata_connection_config)
    SubDagOperator.__init__(
        self, subdag=worker, task_id=worker_name, dag=parent_dag)

    parent_dag.add_node_to_graph(
        node=self, consumes=input_dict.values(), produces=output_dict.values())


class Setup(Component):
  """For local, creates the output directory for this pipeline."""

  def __init__(self, parent_dag, project_path):
    component_name = 'setup'
    super(Setup, self).__init__(
        parent_dag=parent_dag,
        component_name=component_name,
        unique_name=None,
        driver=BaseDriver,
        executor=SetupExecutor,
        input_dict={},
        output_dict={},
        exec_properties={'project_path': project_path})

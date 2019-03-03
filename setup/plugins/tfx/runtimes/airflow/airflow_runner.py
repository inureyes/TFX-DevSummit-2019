"""Definition of Airflow TFX runner."""
from tfx.runtimes.airflow.airflow_pipeline import AirflowPipeline
from tfx.runtimes.airflow.airflow_component import Component
from tfx.runtimes.tfx_runner import TfxRunner


class AirflowDAGRunner(TfxRunner):

  def _prepare_input_dict(self, input_dict):
    return dict((k, v.get()) for k, v in input_dict.items())

  def _prepare_output_dict(self, outputs):
    return dict((k, v.get()) for k, v in outputs.get_all().items())

  # TODO(b/124763328): Separate pipeline args and platform args.
  def run(self, pipeline):
    airflow_dag = AirflowPipeline(**pipeline.kwargs)

    # For every components in logical pipeline, add in real component.
    for component in pipeline.components:
      airflow_component = Component(
          airflow_dag,
          component_name=component.component_name,
          unique_name=component.unique_name,
          driver=component.driver,
          executor=component.executor,
          input_dict=self._prepare_input_dict(component.input_dict),
          output_dict=self._prepare_output_dict(component.outputs),
          exec_properties=component.exec_properties)

    return airflow_dag

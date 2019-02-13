"""TFX Evaluator Driver"""
from tfx.drivers.base_driver import BaseDriver


class Trainer(BaseDriver):
  """Driver for Trainer"""

  # TODO(ruoyu): This is a temporary solution since ML metadata support is not
  # ready to support b/122970393.
  def _fetch_latest_model(self):
    previous_models = [
        x for x in self._metadata_handler.get_all_artifacts()
        if x.properties['type_name'].string_value == 'ModelExportPath'
    ]
    if previous_models:
      latest_model = max(
          previous_models, key=lambda m: m.properties['span'].int_value)
      return latest_model.uri
    else:
      return None

  def prepare_execution(self, input_dict, output_dict, exec_properties,
                        driver_options):
    execution_decision = self._default_caching_handling(
        input_dict, output_dict, exec_properties, driver_options)

    # Fetch latest model dir for warms-tarting if needed.
    if execution_decision.execution_id and execution_decision.exec_properties[
      'warm_starting']:
      execution_decision.exec_properties[
          'warm_start_from'] = self._fetch_latest_model()
      self._logger.debug('Model directory to warm start from: {}'.format(
          execution_decision.exec_properties['warm_start_from']))

    return execution_decision

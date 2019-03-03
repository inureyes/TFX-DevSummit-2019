"""TFX Pusher Driver."""
from tfx.drivers.base_driver import BaseDriver


class Pusher(BaseDriver):
  """Driver for Pusher."""

  # TODO(jyzhao): This is a temporary solution since ML metadata support is not
  # ready to support b/122970393.
  def _fetch_latest_pushed_model(self):
    previous_pushed_models = [
        x for x in self._metadata_handler._store.get_artifacts()
        if (x.properties['type_name'].string_value == 'ModelPushPath' and
            x.custom_properties['pushed'].int_value == 1)
    ]
    if previous_pushed_models:
      latest_pushed_model = max(
          previous_pushed_models, key=lambda m: m.properties['span'].int_value)
      return latest_pushed_model.custom_properties['pushed_model'].string_value
    else:
      return None

  def prepare_execution(self, input_dict, output_dict, exec_properties,
                        driver_options):
    execution_decision = self._default_caching_handling(
        input_dict, output_dict, exec_properties, driver_options)

    if execution_decision.execution_id:
      execution_decision.exec_properties[
          'latest_pushed_model'] = self._fetch_latest_pushed_model()

    return execution_decision

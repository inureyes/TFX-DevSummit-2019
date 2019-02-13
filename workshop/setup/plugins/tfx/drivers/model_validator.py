"""TFX ModelValidator Driver."""
from tfx.drivers.base_driver import BaseDriver


class ModelValidator(BaseDriver):
  """Driver for ModelValidator."""

  # TODO(jyzhao): This is a temporary solution since ML metadata support is not
  # ready to support b/122970393.
  def _fetch_latest_blessed_model(self):
    previous_blessed_models = [
        x for x in self._metadata_handler._store.get_artifacts()
        if (x.properties['type_name'].string_value == 'ModelBlessingPath' and
            x.custom_properties['blessed'].int_value == 1)
    ]
    if previous_blessed_models:
      latest_blessed_model = max(
          previous_blessed_models, key=lambda m: m.properties['span'].int_value)
      return (
          latest_blessed_model.custom_properties['current_model'].string_value,
          latest_blessed_model.custom_properties['current_model_id'].int_value)
    else:
      return (None, None)

  def prepare_execution(self, input_dict, output_dict, exec_properties,
                        driver_options):
    # If current model got blessed, next run won't cache, so it will bless same
    # model twice with different model validation results.
    # TODO(jyzhao): latest blessed model as artifact.
    (exec_properties['latest_blessed_model'],
     exec_properties['latest_blessed_model_id']
    ) = self._fetch_latest_blessed_model()
    return self._default_caching_handling(input_dict, output_dict,
                                          exec_properties, driver_options)

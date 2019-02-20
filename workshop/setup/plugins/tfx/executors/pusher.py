"""Generic TFX pusher executor."""

import os
import tensorflow as tf
from tfx.executors.base_executor import BaseExecutor
from tfx.utils import io_utils
from tfx.utils import logging_utils
from tfx.utils import path_utils
from tfx.utils import types


class Pusher(BaseExecutor):
  """Generic TFX pusher executor."""

  def do(self, inputs, outputs, exec_properties):
    """Push model to target if blessed."""
    logger = logging_utils.get_logger(exec_properties['log_root'], 'exec')
    self._log_startup(logger, inputs, outputs, exec_properties)

    model_export = types.get_single_instance(inputs['model_export'])
    model_export_uri = model_export.uri
    model_blessing_uri = types.get_single_uri(inputs['model_blessing'])
    model_push = types.get_single_instance(outputs['model_push'])
    model_push_uri = model_push.uri
    latest_pushed_model = exec_properties['latest_pushed_model']

    # TODO(jyzhao): should this be in driver or executor.
    if tf.gfile.Exists(os.path.join(model_blessing_uri, 'BLESSED')):
      logger.info('Model pushing.')
      # TODO(jyzhao): support rpc push.
      model_path = path_utils.serving_model_path(model_export_uri)
      model_name = os.path.basename(model_path)
      io_utils.copy_dir(model_path, os.path.join(model_push_uri, model_name))
      logger.info('Model written to {}.'.format(model_push_uri))

      # Copied to a fixed outside path, which can be listened by model server.
      #
      # If model is already successfully copied to outside before, stop copying.
      # This is because model validator might blessed same model twice (check
      # mv driver) with different blessing output, we still want Pusher to
      # handle the mv output again to keep metadata tracking, but no need to
      # copy to outside path again..
      if model_export_uri != latest_pushed_model:
        serving_path = os.path.join(exec_properties['serving_model_dir'],
                                    model_name)
        # TF server won't load partial model, it will retry until fully copied.
        io_utils.copy_dir(model_path, serving_path)
        logger.info('Model written to serving path {}.'.format(serving_path))

      model_push.set_int_custom_property('pushed', 1)
      model_push.set_string_custom_property('pushed_model', model_export_uri)
      model_push.set_int_custom_property('pushed_model_id', model_export.id)
      logger.info('Model pushed.')
    else:
      model_push.set_int_custom_property('pushed', 0)
      logger.info('No blessed model found.')

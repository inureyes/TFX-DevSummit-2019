"""Generic TFX pusher executor."""

import os
import tensorflow as tf
from tfx.executors import io_utils
from tfx.executors.base_executor import BaseExecutor
from tfx.runtimes import tfx_logger


class Pusher(BaseExecutor):
  """Generic TFX pusher executor."""

  def do(self, inputs, outputs, exec_properties):
    """Push model to target if blessed."""
    logger = tfx_logger.get_logger(exec_properties['log_root'], '.exec')
    self._log_startup(logger, inputs, outputs, exec_properties)

    model_export = inputs['model_export'].uri
    model_blessing = inputs['model_blessing'].uri
    model_push = outputs['model_push'].uri
    latest_pushed_model = exec_properties['latest_pushed_model']

    # TODO(jyzhao): should this be in driver or executor.
    if tf.gfile.Exists(os.path.join(model_blessing, 'BLESSED')):
      logger.info('Model pushing.')
      # TODO(jyzhao): support rpc push.
      model_path = self._serving_model_path(model_export)
      model_name = os.path.basename(model_path)
      io_utils.copy_dir(model_path, os.path.join(model_push, model_name))
      logger.info('Model written to {}.'.format(model_push))

      # Copied to a fixed outside path, which can be listened by model server.
      #
      # If model is already successfully copied to outside before, stop copying.
      # This is because model validator might blessed same model twice (check
      # mv driver) with different blessing output, we still want Pusher to
      # handle the mv output again to keep metadata tracking, but no need to
      # copy to outside path again..
      if model_export != latest_pushed_model:
        serving_path = os.path.join(exec_properties['serving_model_dir'],
                                    model_name)
        # TF server won't load partial model, it will retry until fully copied.
        io_utils.copy_dir(model_path, serving_path)
        logger.info('Model written to serving path {}.'.format(serving_path))

      outputs['model_push'].set_int_custom_property('pushed', 1)
      outputs['model_push'].set_string_custom_property('pushed_model',
                                                       model_export)
      outputs['model_push'].set_int_custom_property('pushed_model_id',
                                                    inputs['model_export'].id)
      logger.info('Model pushed.')
    else:
      outputs['model_push'].set_int_custom_property('pushed', 0)
      logger.info('No blessed model found.')

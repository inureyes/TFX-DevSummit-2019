"""Generic TFX trainer executor."""

import os
import tensorflow as tf
from tensorflow_metadata.proto.v0 import schema_pb2
import tensorflow_model_analysis as tfma
from tfx.executors.base_executor import BaseExecutor
from tfx.utils import io_utils
from tfx.utils import logging_utils
from tfx.utils import path_utils
from tfx.utils import types


# TODO(ruoyu): Consider moving this out to a more common place. Internally we
# use a internal only DistributedTrainingSpec class. We might also want to add
# validation upon creation.
class TrainingSpec(object):
  __slots__ = 'estimator', 'train_spec', 'eval_spec', 'eval_input_receiver_fn'

  def __init__(self, estimator, train_spec, eval_spec, eval_input_receiver_fn):
    self.estimator = estimator
    self.train_spec = train_spec
    self.eval_spec = eval_spec
    self.eval_input_receiver_fn = eval_input_receiver_fn


class Trainer(BaseExecutor):
  """Generic TFX trainer executor."""

  _CHECKPOINT_FILE_NAME = 'checkpoint'

  @classmethod
  def _all_files_pattern(cls, file_pattern):
    return '{}*'.format(file_pattern)

  def do(self, inputs, outputs, exec_properties):
    """Method to train the model using the given trainer_fn."""
    logger = logging_utils.get_logger(exec_properties['log_root'], 'exec')
    self._log_startup(logger, inputs, outputs, exec_properties)

    trainer_fn = io_utils.import_func(exec_properties['module_file'],
                                      'trainer_fn')

    # Set up training parameters
    train_files = [
        Trainer._all_files_pattern(
            types.get_split_uri(inputs['transformed_examples'], 'train'))
    ]
    transform_output = types.get_single_uri(inputs['transform_output'])
    eval_files = Trainer._all_files_pattern(
        types.get_split_uri(inputs['transformed_examples'], 'eval'))
    schema_file = io_utils.get_only_uri_in_dir(
        types.get_single_uri(inputs['schema']))

    # TODO(zhitaoli): Make dynamic parameter from airflow CLI work again.
    train_steps = exec_properties['train_steps']
    eval_steps = exec_properties['eval_steps']
    # TODO(ruoyu): get verbosity from tfx logger once cl/230634501 is in.
    verbosity = 'INFO'

    output_path = types.get_single_uri(outputs['output'])
    serving_model_dir = path_utils.serving_model_dir(output_path)
    eval_model_dir = path_utils.eval_model_dir(output_path)

    # Assemble warm start path if needed.
    warm_start_from = None
    if exec_properties['warm_starting'] and exec_properties['warm_start_from']:
      previous_model_dir = os.path.join(exec_properties['warm_start_from'],
                                        path_utils.SERVING_MODEL_DIR)
      if previous_model_dir and tf.gfile.Exists(
          os.path.join(previous_model_dir, self._CHECKPOINT_FILE_NAME)):
        warm_start_from = previous_model_dir

    hparams = tf.contrib.training.HParams(
        train_files=train_files,
        transform_output=transform_output,
        output_dir=output_path,
        serving_model_dir=serving_model_dir,
        eval_files=eval_files,
        schema_file=schema_file,
        train_steps=train_steps,
        eval_steps=eval_steps,
        verbosity=verbosity,
        warm_start_from=warm_start_from)

    schema = io_utils.parse_pbtxt_file(schema_file, schema_pb2.Schema())

    training_spec = trainer_fn(hparams, schema)

    # Train the model
    logger.info('Training model.')
    tf.estimator.train_and_evaluate(training_spec.estimator,
                                    training_spec.train_spec,
                                    training_spec.eval_spec)
    logger.info(
        'Training complete.  Model written to {}'.format(serving_model_dir))

    # Export an eval savedmodel for TFMA
    logger.info('Exporting eval_savedmodel for TFMA.')
    tfma.export.export_eval_savedmodel(
        estimator=training_spec.estimator,
        export_dir_base=eval_model_dir,
        eval_input_receiver_fn=training_spec.eval_input_receiver_fn)

    logger.info('Exported eval_savedmodel to {}.'.format(eval_model_dir))

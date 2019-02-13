"""Generic TFX trainer executor."""

import os
import tensorflow as tf
import tensorflow_model_analysis as tfma
from tfx.executors import io_utils
from tfx.executors.base_executor import BaseExecutor
from tfx.runtimes import tfx_logger


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
    logger = tfx_logger.get_logger(exec_properties['log_root'], '.exec')
    self._log_startup(logger, inputs, outputs, exec_properties)

    trainer_fn = io_utils.import_func(exec_properties['module_file'],
                                      'trainer_fn')

    # Set up training parameters
    train_files = [
        Trainer._all_files_pattern(
            inputs['transformed_examples'].split_uri('train') + '-')
    ]
    transform_output = inputs['transform_output'].uri
    eval_files = Trainer._all_files_pattern(
        inputs['transformed_examples'].split_uri('eval') + '-')
    schema_file = inputs['schema'].uri

    # TODO(zhitaoli): Make dynamic parameter from airflow CLI work again.
    train_steps = exec_properties['train_steps']
    eval_steps = exec_properties['eval_steps']
    # TODO(ruoyu): get verbosity from tfx logger once cl/230634501 is in.
    verbosity = 'INFO'

    output_path = outputs['output'].uri
    serving_model_dir = os.path.join(output_path, self._SERVING_MODEL_DIR)
    eval_model_dir = os.path.join(output_path, self._EVAL_MODEL_DIR)

    # Assemble warm start path if needed.
    warm_start_from = None
    if exec_properties['warm_starting'] and exec_properties['warm_start_from']:
      previous_model_dir = os.path.join(exec_properties['warm_start_from'],
                                        self._SERVING_MODEL_DIR)
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

    schema = self._read_schema(schema_file)

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

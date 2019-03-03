"""Generic TFX trainer executor."""

import argparse
import datetime
import json
import os
import time
from googleapiclient import discovery
import tensorflow as tf
from tensorflow_metadata.proto.v0 import schema_pb2
import tensorflow_model_analysis as tfma
from tfx.executors.base_executor import BaseExecutor
from tfx.utils import io_utils
from tfx.utils import logging_utils
from tfx.utils import path_utils
from tfx.utils import types
from tfx.utils.types import jsonify_tfx_type_dict
from tfx.utils.types import parse_tfx_type_dict


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

  def _start_cmle_training(self, inputs, outputs, exec_properties):
    logger = logging_utils.get_logger(exec_properties['log_root'], 'exec')
    cloudml = discovery.build('ml', 'v1')
    json_inputs = jsonify_tfx_type_dict(inputs)
    json_outputs = jsonify_tfx_type_dict(outputs)
    json_exec_properties = json.dumps(exec_properties)

    # Create mutable CMLE args for this worker
    training_inputs = self._get_additional_pipeline_args('cmle_args').copy()

    # Configure CMLE job
    job_args = [
        '--inputs', json_inputs,
        '--outputs', json_outputs,
        '--exec-properties', json_exec_properties]
    training_inputs['args'] = job_args
    project_id = 'projects/{}'.format(training_inputs.pop('project'))
    job_name = 'tfx_' + datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    job_spec = {'jobId': job_name,
                'trainingInput': training_inputs}

    # Submit job to CMLE
    logger.info('Submitting job=\'{}\' to CMLE.'.format(job_name))
    request = cloudml.projects().jobs().create(body=job_spec,
                                               parent=project_id)
    request.execute()

    # Wait for CMLE job to finish
    job_id = '{}/jobs/{}'.format(project_id, job_name)
    request = cloudml.projects().jobs().get(name=job_id)
    response = request.execute()
    while response['state'] != 'SUCCEEDED' and response['state'] != 'FAILED':
      time.sleep(60)
      response = request.execute()

    if response['state'] == 'FAILED':
      err_msg = 'Job \'{}\' did not succeed.  Detailed response {}.'.format(
          job_name, response)
      logger.error(err_msg)
      raise RuntimeError(err_msg)

    # CMLE training complete
    logger.info('Job \'{}\' successful.'.format(job_name))

  def Do(self, inputs, outputs, exec_properties):
    """Method to train the model using the given trainer_fn."""
    logger = logging_utils.get_logger(exec_properties['log_root'], 'exec')
    self._log_startup(inputs, outputs, exec_properties)

    if self._has_additional_pipeline_args('cmle_args'):
      return self._start_cmle_training(inputs, outputs, exec_properties)

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


# TODO(b/124791851): move into executor_runner.py
if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--inputs',
      help='Inputs to {}, supplied as json.'.format(__file__),
      required=True)

  parser.add_argument(
      '--outputs',
      help='Outputs from {}, supplied as json.'.format(__file__),
      required=True)

  parser.add_argument(
      '--exec-properties',
      help='Additional properties for {}, supplied as json.'.format(__file__),
      required=True)

  args, other_args = parser.parse_known_args()
  args = args.__dict__

  inputs = parse_tfx_type_dict(args['inputs'])
  outputs = parse_tfx_type_dict(args['outputs'])
  exec_properties = json.loads(args['exec_properties'])

  # TODO(zhitaoli): Should we create a BaseRuntime here (and a dependency
  # between executors -> BaseRuntime) or leave None?
  trainer = Trainer(None)
  trainer.do(inputs, outputs, exec_properties)

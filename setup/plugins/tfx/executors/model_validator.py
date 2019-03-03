"""Generic TFX model validator executor."""

import os
import apache_beam as beam
import tensorflow_model_analysis as tfma
from tfx.executors.base_executor import BaseExecutor
from tfx.utils import io_utils
from tfx.utils import logging_utils
from tfx.utils import path_utils
from tfx.utils import types


# Path to store model eval results for validation.
CURRENT_MODEL_EVAL_RESULT_PATH = 'eval_results/current_model/'
BLESSED_MODEL_EVAL_RESULT_PATH = 'eval_results/blessed_model/'


class ModelValidator(BaseExecutor):
  """Generic TFX model validator executor."""

  def _run_model_analysis(self, model_dir, eval_examples_uri, slice_spec,
                          eval_output, model_name):
    """Run model analysis and generate eval results."""
    shared_model = tfma.default_eval_shared_model(
        eval_saved_model_path=model_dir)
    with self._pipeline as pipeline:
      _ = (
          pipeline
          | 'ReadData.' + model_name >> beam.io.ReadFromTFRecord(
              file_pattern=io_utils.all_files_pattern(eval_examples_uri))
          | 'ExtractEvaluateAndWriteResults.' + model_name >>
          tfma.ExtractEvaluateAndWriteResults(
              eval_shared_model=shared_model,
              slice_spec=slice_spec,
              output_path=eval_output))

    eval_result = tfma.load_eval_result(output_path=eval_output)
    return eval_result

  def _pass_threshold(self, eval_result):
    """Check threshold."""
    return True

  def _compare_eval_result(self, logger, current_model_eval_result,
                           latest_blessed_model_eval_result):
    """Compare accuracy of all metrics. return true if current is better."""
    for current_metric, blessed_metric in zip(
        current_model_eval_result.slicing_metrics,
        latest_blessed_model_eval_result.slicing_metrics):
      # slicing_metric is a tuple, index 0 is slice, index 1 is its value.
      if current_metric[0] != blessed_metric[0]:
        raise RuntimeError('EvalResult not match.')
      if (current_metric[1]['accuracy']['doubleValue'] <
          blessed_metric[1]['accuracy']['doubleValue']):
        logger.info(
            'Current model accuracy is worse than blessed model: {}'.format(
                current_metric[0]))
        return False
    return True

  def _generate_blessing_result(self, logger, eval_examples_uri, slice_spec,
                                current_model_dir, latest_blessed_model_dir,
                                results_path):
    # TODO(jyzhao): make it real when tfma verifier_lib.py is done.
    #   1. customized validation and threshold support.
    #   2. write verify results to ModelValidationPath.
    current_model_eval_result = self._run_model_analysis(
        model_dir=path_utils.eval_model_path(current_model_dir),
        eval_examples_uri=eval_examples_uri,
        slice_spec=slice_spec,
        eval_output=os.path.join(results_path, CURRENT_MODEL_EVAL_RESULT_PATH),
        model_name='current')
    if not self._pass_threshold(current_model_eval_result):
      logger.info('Current model does not pass threshold.')
      return False
    logger.info('Current model passes threshold.')

    if latest_blessed_model_dir is None:
      logger.info('No blessed model yet.')
      return True

    latest_blessed_model_eval_result = self._run_model_analysis(
        model_dir=path_utils.eval_model_path(latest_blessed_model_dir),
        eval_examples_uri=eval_examples_uri,
        slice_spec=slice_spec,
        eval_output=os.path.join(results_path, BLESSED_MODEL_EVAL_RESULT_PATH),
        model_name='latest_blessed')
    if (self._compare_eval_result(logger, current_model_eval_result,
                                  latest_blessed_model_eval_result)):
      logger.info('Current model better than latest blessed model.')
      return True

    return False

  def Do(self, inputs, outputs, exec_properties):
    """Validate current model against previously blessed model."""
    logger = logging_utils.get_logger(exec_properties['log_root'], 'exec')
    self._log_startup(inputs, outputs, exec_properties)

    eval_examples_uri = types.get_split_uri(inputs['examples'], 'eval')
    # TODO(b/125853306): support customized slice spec.
    slice_spec = [tfma.slicer.SingleSliceSpec()]
    # Current model.
    current_model = types.get_single_instance(inputs['model'])
    current_model_dir = current_model.uri
    blessing = types.get_single_instance(outputs['blessing'])
    blessing_dir = blessing.uri
    results = types.get_single_uri(outputs['results'])

    logger.info(
        'Using {} as current model for validation.'.format(current_model_dir))
    blessing.set_string_custom_property('current_model', current_model_dir)
    blessing.set_int_custom_property('current_model_id', current_model.id)

    # Latest blessed model.
    latest_blessed_model_dir = exec_properties['latest_blessed_model']
    latest_blessed_model_id = exec_properties['latest_blessed_model_id']
    logger.info(
        'Using {} as latest blessed model.'.format(latest_blessed_model_dir))
    if latest_blessed_model_dir:
      blessing.set_string_custom_property('previous_blessed_model',
                                          latest_blessed_model_dir)
      blessing.set_int_custom_property('previous_blessed_model_id',
                                       latest_blessed_model_id)

    logger.info('Validating model.')
    blessed = self._generate_blessing_result(
        logger=logger,
        eval_examples_uri=eval_examples_uri,
        slice_spec=slice_spec,
        current_model_dir=current_model_dir,
        latest_blessed_model_dir=latest_blessed_model_dir,
        results_path=results)

    if blessed:
      io_utils.write_string_file(os.path.join(blessing_dir, 'BLESSED'), '')
      blessing.set_int_custom_property('blessed', 1)
    else:
      io_utils.write_string_file(os.path.join(blessing_dir, 'NOT_BLESSED'), '')
      blessing.set_int_custom_property('blessed', 0)
    logger.info('Blessing result written to {}.'.format(blessing_dir))

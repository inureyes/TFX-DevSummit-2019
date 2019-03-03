"""Generic TFX model evaluator executor."""

import apache_beam as beam
import tensorflow_model_analysis as tfma
from tfx.executors.base_executor import BaseExecutor
from tfx.utils import io_utils
from tfx.utils import logging_utils
from tfx.utils import path_utils
from tfx.utils import types


class Evaluator(BaseExecutor):
  """Generic TFX model evaluator executor."""

  def Do(self, inputs, outputs, exec_properties):
    """Runs a batch job to evaluate the eval_model against the given input.

    Args:
      inputs: Input dict from input key to a list of Artifacts.
        - model_exports: exported model.
        - examples: examples for eval the model.
      outputs: Output dict from output key to a list of Artifacts.
        - output: model evaluation results.
      exec_properties: A dict of execution properties.

    Returns:
      None
    """
    if 'model_exports' not in inputs:
      raise ValueError('\'model_exports\' is missing in input dict.')
    if 'examples' not in inputs:
      raise ValueError('\'examples\' is missing in input dict.')
    if 'output' not in outputs:
      raise ValueError('\'output\' is missing in output dict.')

    logger = logging_utils.get_logger(exec_properties['log_root'], 'exec')
    self._log_startup(inputs, outputs, exec_properties)

    # Extract input artifacts
    model_exports_uri = types.get_single_uri(inputs['model_exports'])

    # TODO(b/125853306): support customized slice spec.
    slice_spec = [tfma.slicer.SingleSliceSpec()]

    output_uri = types.get_single_uri(outputs['output'])

    eval_model_path = path_utils.eval_model_path(model_exports_uri)

    logger.info('Using {} for model eval.'.format(eval_model_path))
    # TODO(khaas): Consider exposing add_metrics_callbacks to pipeline config
    eval_shared_model = tfma.default_eval_shared_model(
        eval_saved_model_path=eval_model_path)

    logger.info('Evaluating model.')
    with self._pipeline as pipeline:
      # pylint: disable=expression-not-assigned
      (pipeline
       | 'ReadData' >> beam.io.ReadFromTFRecord(
           file_pattern=io_utils.all_files_pattern(
               types.get_split_uri(inputs['examples'], 'eval')))
       |
       'ExtractEvaluateAndWriteResults' >> tfma.ExtractEvaluateAndWriteResults(
           eval_shared_model=eval_shared_model,
           slice_spec=slice_spec,
           output_path=output_uri))
    logger.info(
        'Evaluation complete. Results written to {}.'.format(output_uri))

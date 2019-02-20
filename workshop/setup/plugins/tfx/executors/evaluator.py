"""Generic TFX model evaluator executor."""

import json
import apache_beam as beam
import tensorflow_model_analysis as tfma
from tensorflow_model_analysis.slicer.slicer import SingleSliceSpec
from tfx.executors.base_executor import BaseExecutor
from tfx.utils import io_utils
from tfx.utils import logging_utils
from tfx.utils import path_utils
from tfx.utils import types


# TODO(b/123238652): Make tfma support json serialization of SingleSliceSpec.
class SingleSliceSpecEncoder(json.JSONEncoder):

  def default(self, o):
    if not isinstance(o, SingleSliceSpec):
      return json.JSONEncoder.default(self, o)

    return {
        'columns': list(o.__dict__['_columns']),
        'features': list(o.__dict__['_features']),
    }


class SingleSliceSpecDecoder(json.JSONDecoder):

  def __init__(self, *args, **kwargs):
    json.JSONDecoder.__init__(
        self, object_hook=self.object_hook, *args, **kwargs)

  def object_hook(self, dct):
    return SingleSliceSpec(columns=dct['columns'], features=dct['features'])


class Evaluator(BaseExecutor):
  """Generic TFX model evaluator executor."""

  def do(self, inputs, outputs, exec_properties):
    """Runs a batch job to evaluate the eval_model against the given input."""
    logger = logging_utils.get_logger(exec_properties['log_root'], 'exec')
    self._log_startup(logger, inputs, outputs, exec_properties)

    # Extract input artifacts
    model_exports_uri = types.get_single_uri(inputs['model_exports'])

    eval_spec = json.loads(
        exec_properties['eval_spec'], cls=SingleSliceSpecDecoder)

    output_uri = types.get_single_uri(outputs['output'])

    eval_model_path = path_utils.eval_model_path(model_exports_uri)

    logger.info('Using {} for model eval.'.format(eval_model_path))
    eval_shared_model = tfma.default_eval_shared_model(
        eval_saved_model_path=eval_model_path,
        # TODO(khaas): Consider exposing these to the pipeline config
        add_metrics_callbacks=[
            tfma.post_export_metrics.calibration_plot_and_prediction_histogram(
            ),
            tfma.post_export_metrics.auc_plots()
        ])

    logger.info('Evaluating model.')
    with beam.Pipeline(options=self._get_beam_pipeline_options()) as pipeline:
      _ = (
          pipeline
          | 'ReadData' >> beam.io.ReadFromTFRecord(
              file_pattern=io_utils.all_files_pattern(
                  types.get_split_uri(inputs['examples'], 'eval')))
          | 'ExtractEvaluateAndWriteResults' >>
          tfma.ExtractEvaluateAndWriteResults(
              eval_shared_model=eval_shared_model,
              slice_spec=eval_spec,
              output_path=output_uri))
    logger.info(
        'Evaluation complete. Results written to {}.'.format(output_uri))

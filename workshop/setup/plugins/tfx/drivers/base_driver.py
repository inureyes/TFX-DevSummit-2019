"""Abstract TFX driver class."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import tensorflow as tf
from tfx.runtimes import tfx_logger
from tfx.tfx_types import ARTIFACT_STATE_MISSING
from tfx.tfx_types import ARTIFACT_STATE_PUBLISHED


class ExecutionDecision(object):
  """ExecutionDecision records how executor should perform next execution.

  Note that an empty execution_id instructs (Airflow) orchestrator to skip
  the cache.
  """
  __slots__ = ['input_dict', 'output_dict', 'exec_properties', 'execution_id']

  def __init__(self,
               input_dict,
               output_dict,
               exec_properties,
               execution_id=None):
    self.input_dict = input_dict
    self.output_dict = output_dict
    self.exec_properties = exec_properties
    self.execution_id = execution_id


class DriverOptions(object):
  __slots__ = ['worker_name', 'base_output_dir', 'enable_cache']

  def __init__(self, worker_name, base_output_dir, enable_cache):
    self.worker_name = worker_name
    self.base_output_dir = base_output_dir
    self.enable_cache = enable_cache


class BaseDriver(object):

  def __init__(self, log_root, metadata_handler):
    self._metadata_handler = metadata_handler
    self._logger = tfx_logger.get_logger(log_root, '.driver')

  def _log_properties(self, input_dict, output_dict, exec_properties):
    """Log inputs, outputs, and executor properties in a standard format."""
    self._logger.info('Inputs for ' + self.__class__.__name__)
    self._logger.info(input_dict)
    self._logger.info('Runtime parameters for ' + self.__class__.__name__)
    self._logger.info(exec_properties)
    self._logger.info('Outputs for ' + self.__class__.__name__)
    self._logger.info(output_dict)

  def _get_output_from_previous_run(self, input_dict, output_dict,
                                    exec_properties, driver_options):
    """Returns outputs from previous identical execution if found."""
    previous_execution_id = self._metadata_handler.previous_run(
        type_name=driver_options.worker_name,
        input_dict=input_dict,
        exec_properties=exec_properties)
    if previous_execution_id:
      final_output = self._metadata_handler.fetch_previous_result_artifacts(
          output_dict, previous_execution_id)
      self._logger.info(
          'Reusing previous execution {} output artifacts {}'.format(
              previous_execution_id, final_output))
      return final_output
    else:
      return None

  # TODO(ruoyu): Add 'lock inputs' part.
  # TODO(ruoyu): Make this transaction-based once b/123573724 is fixed.
  def _verify_and_lock_inputs(self, input_dict):
    """Verify all inputs exist. Update input artifact state to in-use."""
    for single_input in input_dict.values():
      if not single_input.uri:
        raise RuntimeError('Input {} not available'.format(single_input))
      if not tf.gfile.Exists(os.path.dirname(single_input.uri)):
        self._metadata_handler.update_artifact_state(single_input.artifact,
                                                     ARTIFACT_STATE_MISSING)
        raise RuntimeError('Input {} is missing'.format(single_input))
      self._metadata_handler.check_artifact_state(
          artifact=single_input.artifact,
          expected_states=[ARTIFACT_STATE_PUBLISHED])

  def _default_caching_handling(self, input_dict, output_dict, exec_properties,
                                driver_options):
    """Check cache for desired and applicable identical execution."""
    enable_cache = driver_options.enable_cache
    base_output_dir = driver_options.base_output_dir
    worker_name = driver_options.worker_name

    # If caching is enabled, try to get previous execution results and directly
    # use as output.
    if enable_cache:
      output_result = self._get_output_from_previous_run(
          input_dict, output_dict, exec_properties, driver_options)
      if output_result:
        self._logger.info('Found cache from previous run.')
        return ExecutionDecision(
            input_dict=input_dict,
            output_dict=output_result,
            exec_properties=exec_properties)

    # Previous run is not available, prepare execution.
    # Registers execution in metadata.
    execution_id = self._metadata_handler.prepare_execution(
        worker_name, exec_properties)
    self._logger.info('Preparing new execution.')

    # Checks inputs exist and have valid states and locks them to avoid GC half
    # way
    self._verify_and_lock_inputs(input_dict)

    # Updates output.
    max_input_span = max([x.span for x in input_dict.values()
                         ]) if input_dict else 0
    for output_name, output_artifact in output_dict.items():
      # Updates outputs uri based on execution id.
      output_artifact.set_artifact_uri(
          os.path.join(base_output_dir, output_name, str(execution_id),
                       output_artifact.default_file_name))
      # Defaults to make the output span the max of input span.
      output_artifact.set_artifact_span(max_input_span)

    return ExecutionDecision(
        input_dict=input_dict,
        output_dict=output_dict,
        exec_properties=exec_properties,
        execution_id=execution_id)

  def prepare_execution(self, input_dict, output_dict, exec_properties,
                        driver_options):
    """Prepares inputs, outputs and execution properties for actual execution.

    This method could be overridden by custom drivers if they have a different
    logic. The default behavior is to check ml.metadata for an existing
    execution of same inputs and exec_properties, and use previous outputs
    instead of a new execution if found.

    Args:
      input_dict: key -> TfxType for inputs. One can expect every input already
        registered in ML metadata except ExamplesGen.
      output_dict: key -> TfxType for outputs. Uris of the outputs are not
        assigned. It's subclasses' responsibility to set the real output uris.
      exec_properties: Dict of other execution properties.
      driver_options: An instance of DriverOptions class.

    Returns:
      ExecutionDecision object. Include the following fields:
        execution_id: Registered execution_id for the upcoming execution. If
          None, then no execution needed.
        input_dict: Updated key -> TfxType for inputs that will be used by
          actual execution.
        output_dict: Updated key -> TfxType for outputs that will be used by
          actual execution.
        exec_properties: Updated dict of other execution properties that will be
          used by actual execution.

    """
    self._logger.info('Enter driver.')
    self._log_properties(input_dict, output_dict, exec_properties)
    execution_decision = self._default_caching_handling(
        input_dict, output_dict, exec_properties, driver_options)
    self._logger.info('Prepared execution.')
    self._log_properties(execution_decision.input_dict,
                         execution_decision.output_dict,
                         execution_decision.exec_properties)
    return execution_decision

"""Driver for ExamplesGen."""
import os
import re

import tensorflow as tf
from tfx.drivers.base_driver import BaseDriver


class ExamplesGen(BaseDriver):
  """Driver for ExamplesGen."""

  # TODO(ruoyu): Make this passed-in UDF.
  def _generate_span(self, uri):
    # Works no matter uri ends with '/' or not
    file_name = os.path.basename(os.path.dirname(os.path.join(uri, '')))
    matched = re.match(r'span_([0-9]+)', file_name)
    if not matched:
      return None
    else:
      return int(matched.group(1))

  def _generate_span_or_fail(self, uri):
    span = self._generate_span(uri)
    if not span:
      raise ValueError('unexpected file pattern: {}'.format(uri))
    return span

  # TODO(ruoyu): Currently we directly register external input in driver, it is
  # debatable whether or not driver should be read-only for metadata.
  def _import_artifact(self, single_input, uri):
    single_input.uri = uri
    single_input.span = self._generate_span_or_fail(uri)
    [artifact] = self._metadata_handler.publish_artifacts([single_input])  # pylint: disable=unbalanced-tuple-unpacking
    single_input.set_artifact(artifact)
    self._logger.debug('Registered new input: {}'.format(artifact))
    return artifact

  # Gets the next input for processing. If no new data found, returns the latest
  # span. We do not want to short-cut downstream components since they might
  # have different logics.
  def _prepare_input_for_processing(self, input_dict):
    try:
      registered_artifacts = self._metadata_handler.get_all_artifacts()
    except tf.errors.NotFoundError:
      registered_artifacts = []

    # Convenient lambdas for comparing.
    span_from_artifact = lambda artifact: self._generate_span_or_fail(artifact.
                                                                      uri)

    for input_list in input_dict.values():
      for single_input in input_list:
        input_dir = single_input.uri

        # If input dir is already a span, handle it directly as oneshot.
        if self._generate_span(input_dir) is not None:
          self._logger.info(
              'Processing input span {} directly.'.format(input_dir))
          matched_artifacts = [
              artifact for artifact in registered_artifacts
              if artifact.uri == input_dir
          ]
          if matched_artifacts:
            # If there are multiple matches, get the latest one for caching.
            latest_artifact = max(
                matched_artifacts, key=lambda artifact: artifact.id)
            single_input.set_artifact(latest_artifact)
          else:
            single_input.set_artifact(
                self._import_artifact(single_input, input_dir))
          continue

        # Gets all uris inside input folder. Raises error if invalid or folder.
        try:
          all_input_uris = [
              # TODO(b/123772894): sub folder support.
              os.path.join(input_dir, span_name)
              for span_name in tf.gfile.ListDirectory(input_dir)
          ]
        except (TypeError, tf.errors.NotFoundError) as e:
          raise RuntimeError('Error: {} for input: {}'.format(
              str(e), single_input))

        # Gets all uris in input directory that has been registered in metadata.
        # TODO(ruoyu): We might also want to checksum the file.
        registered_artifacts_in_dir = [
            artifact for artifact in registered_artifacts
            if artifact.uri in all_input_uris
        ]

        # If there are registered artifacts in the input folder, we need to use
        #   (1) a new uri that has the smallest span number that is larger than
        #       the largest span from registered artifacts in the input folder.
        #   (2) a registered artifact that has the largest span number if (1) is
        #       not available.
        # If there is not registered artifact, use a new uri that has the
        # smallest span number.
        if registered_artifacts_in_dir:
          # Gets the registered uri with the latest span number.
          registered_artifact_in_dir_with_latest_span = max(
              registered_artifacts_in_dir, key=span_from_artifact)
          largest_registered_span = span_from_artifact(
              registered_artifact_in_dir_with_latest_span)
          # Finds all uris in input folder that have span number larger than
          # the largest registered span.
          candidate_new_uris = [
              uri for uri in all_input_uris
              if self._generate_span_or_fail(uri) > largest_registered_span
          ]

          # For all uris that have span number larger than largest registered
          # span, use the smallest one as the next input.
          if candidate_new_uris:
            new_uri_to_process = min(
                candidate_new_uris, key=self._generate_span_or_fail)
            single_input.set_artifact(
                self._import_artifact(single_input, new_uri_to_process))
          else:
            single_input.set_artifact(
                registered_artifact_in_dir_with_latest_span)
            self._logger.info(
                'No valid new input, use previous input: {}'.format(
                    registered_artifact_in_dir_with_latest_span))
        else:
          new_uri_to_process = min(
              all_input_uris, key=self._generate_span_or_fail)
          single_input.set_artifact(
              self._import_artifact(single_input, new_uri_to_process))

    return input_dict

  def prepare_execution(self, input_dict, output_dict, exec_properties,
                        driver_options):
    updated_input_dict = self._prepare_input_for_processing(input_dict)
    return self._default_caching_handling(updated_input_dict, output_dict,
                                          exec_properties, driver_options)

#!/usr/bin/env python
# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Common entrypoint script for docker image shared by all TFX components."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import base64
import json

import apache_beam as beam
import tensorflow as tf

from tfx.executors.big_query_example_gen import BigQueryExampleGen
from tfx.executors.csv_example_gen import CsvExampleGen
from tfx.executors.evaluator import Evaluator
from tfx.executors.example_validator import ExampleValidator
from tfx.executors.model_validator import ModelValidator
from tfx.executors.pusher import Pusher
from tfx.executors.schema_gen import SchemaGen
from tfx.executors.setup import Setup
from tfx.executors.statistics_gen import StatisticsGen
from tfx.executors.trainer import Trainer
from tfx.executors.transform import Transform
from tfx.utils.types import jsonify_tfx_type_dict
from tfx.utils.types import parse_tfx_type_dict


_EXECUTORS = {
    cls.__name__: cls for cls in [
        Setup,
        Evaluator,
        BigQueryExampleGen,
        CsvExampleGen,
        ExampleValidator,
        ModelValidator,
        Pusher,
        SchemaGen,
        StatisticsGen,
        Trainer,
        Transform,
    ]
}


def main():
  parser = argparse.ArgumentParser()

  parser.add_argument('--write-outputs-stdout',
                      dest='write_outputs_stdout',
                      action='store_true',
                      help='Write outputs to last line of stdout, which will '
                      'be pushed to xcom in Airflow.')
  parser.add_argument('--executor',
                      type=str,
                      required=True,
                      help='Name of executor for current task, must be one of '
                      '{}'.format(_EXECUTORS.keys()))
  inputs_group = parser.add_mutually_exclusive_group(required=True)
  inputs_group.add_argument(
      '--inputs',
      type=str,
      help='json serialized dict of input artifacts.')
  inputs_group.add_argument(
      '--inputs-base64',
      type=str,
      help='base64 encoded json serialized dict of input artifacts.')

  outputs_group = parser.add_mutually_exclusive_group(required=True)
  outputs_group.add_argument(
      '--outputs',
      type=str,
      help='json serialized dict of output artifacts.')
  outputs_group.add_argument(
      '--outputs-base64',
      type=str,
      help='base64 encoded json serialized dict of output artifacts.')

  execution_group = parser.add_mutually_exclusive_group(required=True)
  execution_group.add_argument(
      '--exec-properties',
      type=str,
      help='json serialized dict of (non artifact) execution properties.')
  execution_group.add_argument(
      '--exec-properties-base64',
      type=str,
      help='json serialized dict of (non artifact) execution properties.')

  args, pipeline_args = parser.parse_known_args()

  (inputs_str, outputs_str, exec_properties_str) = (
      args.inputs or base64.b64decode(args.inputs_base64),
      args.outputs or base64.b64decode(args.outputs_base64),
      args.exec_properties or base64.b64decode(args.exec_properties_base64))

  inputs = parse_tfx_type_dict(inputs_str)
  outputs = parse_tfx_type_dict(outputs_str)
  exec_properties = json.loads(exec_properties_str)
  tf.logging.info(
      'Executor {} do: inputs: {}, outputs: {}, exec_properties: {}'.format(
          args.executor, inputs, outputs, exec_properties))

  pipeline = beam.Pipeline(argv=pipeline_args)
  executor = _EXECUTORS[args.executor](pipeline)
  executor.Do(inputs, outputs, exec_properties)

  # The last line of stdout will be pushed to xcom by Airflow.
  if args.write_outputs_stdout:
    print(jsonify_tfx_type_dict(outputs))


if __name__ == '__main__':
  main()

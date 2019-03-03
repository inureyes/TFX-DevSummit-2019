"""Chicago taxi example using TFX DSL."""
# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import datetime
import os
from tfx.runtimes.airflow.airflow_runner import AirflowDAGRunner as TfxRunner
from tfx.runtimes.pipeline import PipelineDecorator
from tfx.utils.dsl_utils import csv_inputs
from tfx.components import CsvExampleGen

# Directory and data locations
home_dir = os.path.join(os.environ['HOME'], 'airflow/')
pipeline_root = os.path.join(home_dir, 'data/tfx/pipelines/')
base_dir = os.path.join(home_dir, 'data/tfx_example/')
# Path to the model server directory. Pusher will output model here.
serving_model_dir = os.path.join(pipeline_root, 'serving_model/tfx_example')

@PipelineDecorator(
    pipeline_name='tfx_example',
    schedule_interval=None,
    start_date=datetime.datetime(2018, 1, 1),
    enable_cache=True,
    log_root='/var/tmp/tfx/logs',
    metadata_db_root=os.path.join(home_dir, 'data/tfx/metadata'),
    pipeline_root=pipeline_root)
def create_pipeline():
  """Implements the example pipeline with TFX."""
  examples = csv_inputs(os.path.join(base_dir, 'no_split/span_1'))
  example_gen = CsvExampleGen(input_data=examples)

  return [example_gen]

pipeline = TfxRunner().run(create_pipeline())
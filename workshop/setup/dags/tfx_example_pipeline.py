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
from tfx.runtimes.tfx_airflow import PipelineDecorator
from tfx.utils.dsl_utils import csv_inputs

# Directory and data locations
home_dir = os.path.join(os.environ['HOME'], 'airflow/')
base_dir = os.path.join(home_dir, 'data/tfx_example/')
output_dir = os.path.join(base_dir, 'pipelines/')

# Modules for trainer and transform
plugin_dir = os.path.join(home_dir, 'plugins/tfx_example/')
model = os.path.join(plugin_dir, 'model.py')
transforms = os.path.join(plugin_dir, 'transforms.py')

# Path which can be listened by model server. Pusher will output model here.
serving_model_dir = os.path.join(output_dir, 'tfx_example/serving_model')

@PipelineDecorator(
    pipeline_name='tfx_example_pipeline_DAG',
    schedule_interval=None,
    start_date=datetime.datetime(2019, 2, 1),
    enable_cache=True,
    run_id='tfx-example-local',
    log_root='/var/tmp/tfx/logs',
    output_dir=output_dir)
def create_pipeline():
  """Implements the example pipeline with TFX."""
  examples = csv_inputs(os.path.join(base_dir, 'no_split/span_1'))

pipeline = create_pipeline()  # pylint: disable=assignment-from-no-return

# Copyright 2019 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Chicago taxi example using TFX."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import datetime
import os
from tfx.utils.dsl_utils import csv_input
from tfx.components.example_gen.csv_example_gen.component import CsvExampleGen
# from tfx.components.statistics_gen.component import StatisticsGen # Step 3
# from tfx.components.schema_gen.component import SchemaGen # Step 3
# from tfx.components.example_validator.component import ExampleValidator # Step 3
# from tfx.components.transform.component import Transform # Step 4
# from tfx.components.trainer.component import Trainer # Step 5
# from tfx.components.evaluator.component import Evaluator # Step 6
# from tfx.components.model_validator.component import ModelValidator # Step 7
# from tfx.components.pusher.component import Pusher # Step 7

from tfx.orchestration.airflow.airflow_runner import AirflowDAGRunner as TfxRunner
from tfx.orchestration.pipeline import PipelineDecorator


# Directory and data locations
home_dir = os.path.join(os.environ['HOME'], 'airflow/')
pipeline_root = os.path.join(home_dir, 'data/tfx/pipelines/')
data_root = os.path.join(home_dir, 'data/taxi_data/')

# Python module file to inject customized logic into the TFX components. The
# Transform and Trainer both require user-defined functions to run successfully.
taxi_module_file = os.path.join(home_dir, 'dags/taxi_utils.py')

# Path which can be listened to by the model server.  Pusher will output the
# trained model here.
serving_model_dir = os.path.join(pipeline_root, 'serving_model/taxi')

# Airflow-specific configs; these will be passed directly to airflow
airflow_config = {
    'schedule_interval': None,
    'start_date': datetime.datetime(2019, 1, 1),
}


@PipelineDecorator(
    pipeline_name='taxi',
    enable_cache=True,
    metadata_db_root=os.path.join(home_dir, 'data/tfx/metadata'),
    pipeline_root=pipeline_root)
def create_pipeline():
  """Implements the chicago taxi pipeline with TFX."""
  examples = csv_input(data_root)

  # Brings data into the pipeline or otherwise joins/converts training data.
  example_gen = CsvExampleGen(input_base=examples)

  # Computes statistics over data for visualization and example validation.
#   statistics_gen = StatisticsGen(input_data=example_gen.outputs.examples) # Step 3

  # Generates schema based on statistics files.
#   schema_gen = SchemaGen(stats=statistics_gen.outputs.output) # Step 3

  # Performs anomaly detection based on statistics and data schema.
#   validate_stats = ExampleValidator( # Step 3
#       stats=statistics_gen.outputs.output, # Step 3
#       schema=schema_gen.outputs.output) # Step 3

  # Performs transformations and feature engineering in training and serving.
#   transform = Transform( # Step 4
#       input_data=example_gen.outputs.examples, # Step 4
#       schema=schema_gen.outputs.output, # Step 4
#       module_file=taxi_module_file) # Step 4

  # Uses user-provided Python function that implements a model using TF-Learn.
#   trainer = Trainer( # Step 5
#       module_file=taxi_module_file, # Step 5
#       transformed_examples=transform.outputs.transformed_examples, # Step 5
#       schema=schema_gen.outputs.output, # Step 5
#       transform_output=transform.outputs.transform_output, # Step 5
#       train_steps=10000, # Step 5
#       eval_steps=5000, # Step 5
#       warm_starting=True) # Step 5

  # Uses TFMA to compute a evaluation statistics over features of a model.
#   model_analyzer = Evaluator( # Step 6
#       examples=example_gen.outputs.examples, # Step 6
#       model_exports=trainer.outputs.output) # Step 6

  # Performs quality validation of a candidate model (compared to a baseline).
#   model_validator = ModelValidator( # Step 7
#       examples=example_gen.outputs.examples, model=trainer.outputs.output) # Step 7

  # Checks whether the model passed the validation steps and pushes the model
  # to a file destination if check passed.
#   pusher = Pusher( # Step 7
#       model_export=trainer.outputs.output, # Step 7
#       model_blessing=model_validator.outputs.blessing, # Step 7
#       serving_model_dir=serving_model_dir) # Step 7

  return [
      example_gen,
#       statistics_gen, schema_gen, validate_stats, # Step 3
#       transform, # Step 4
#       trainer, # Step 5
#       model_analyzer, # Step 6
#       model_validator, pusher # Step 7
  ]


pipeline = TfxRunner(airflow_config).run(create_pipeline())

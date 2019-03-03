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
from tfx.components import ExampleValidator
from tfx.components import SchemaGen
from tfx.components import StatisticsGen
from tfx.components import Transform
from tfx.components import Trainer
import tensorflow_model_analysis as tfma
from tfx.components import Evaluator
from tfx.components import ModelValidator
from tfx.components import Pusher

# Directory and data locations
home_dir = os.path.join(os.environ['HOME'], 'airflow/')
pipeline_root = os.path.join(home_dir, 'data/tfx/pipelines/')
base_dir = os.path.join(home_dir, 'data/tfx_example_solution/')
# Path to the model server directory. Pusher will output model here.
serving_model_dir = os.path.join(pipeline_root, 'serving_model/tfx_example_solution')

# Modules for trainer and transform
plugin_dir = os.path.join(home_dir, 'plugins/tfx_example_solution/')
model = os.path.join(plugin_dir, 'model.py')
transforms = os.path.join(plugin_dir, 'transforms.py')

@PipelineDecorator(
    pipeline_name='tfx_example_solution',
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
  statistics_gen = StatisticsGen(input_data=example_gen.outputs.output)
  infer_schema = SchemaGen(stats=statistics_gen.outputs.output)
  validate_stats = ExampleValidator(  # pylint: disable=unused-variable
      stats=statistics_gen.outputs.output,
      schema=infer_schema.outputs.output)
  transform = Transform(
      input_data=example_gen.outputs.output,
      schema=infer_schema.outputs.output,
      module_file=transforms)
  trainer = Trainer(
      module_file=model,
      transformed_examples=transform.outputs.transformed_examples,
      schema=infer_schema.outputs.output,
      transform_output=transform.outputs.transform_output,
      train_steps=10000,
      eval_steps=5000,
      warm_starting=True)
  model_analyzer = Evaluator(
      examples=example_gen.outputs.output,
      model_exports=trainer.outputs.output)
  model_validator = ModelValidator(
      examples=example_gen.outputs.output,
      model=trainer.outputs.output)
  pusher = Pusher(
      model_export=trainer.outputs.output,
      model_blessing=model_validator.outputs.blessing,
      serving_model_dir=serving_model_dir)

  return [
      example_gen, statistics_gen, infer_schema, validate_stats, transform,
      trainer, model_analyzer, model_validator, pusher
  ]

pipeline = TfxRunner().run(create_pipeline())
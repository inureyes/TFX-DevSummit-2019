# Add appropriate imports
import tensorflow_model_analysis as tfma
from tfx.components import Evaluator
from tfx.components import ExamplesGen
from tfx.components import ExampleValidator
from tfx.components import ModelValidator
from tfx.components import Pusher
from tfx.components import SchemaGen
from tfx.components import StatisticsGen
from tfx.components import Trainer
from tfx.components import Transform

# Add new specs
# For TFMA evaluation
taxi_eval_spec = [
    tfma.SingleSliceSpec(),
    tfma.SingleSliceSpec(columns=['trip_start_hour'])
]

# For model validation
taxi_mv_spec = [tfma.SingleSliceSpec()]

# Add components to the end of pipeline in create_pipeline()
examples_gen = ExamplesGen(input_data=examples)
statistics_gen = StatisticsGen(input_data=examples_gen.outputs.output)
infer_schema = SchemaGen(stats=statistics_gen.outputs.output)
validate_stats = ExampleValidator(  # pylint: disable=unused-variable
    stats=statistics_gen.outputs.output,
    schema=infer_schema.outputs.output)
transform = Transform(
    input_data=examples_gen.outputs.output,
    schema=infer_schema.outputs.output,
    module_file=taxi_pipeline_utils)
trainer = Trainer(
    module_file=taxi_pipeline_utils,
    transformed_examples=transform.outputs.transformed_examples,
    schema=infer_schema.outputs.output,
    transform_output=transform.outputs.transform_output,
    train_steps=10000,
    eval_steps=5000,
    warm_starting=True)
model_analyzer = Evaluator(  # pylint: disable=unused-variable
    examples=examples_gen.outputs.output,
    eval_spec=taxi_eval_spec,
    model_exports=trainer.outputs.output)
model_validator = ModelValidator(
    examples=examples_gen.outputs.output,
    model=trainer.outputs.output,
    eval_spec=taxi_mv_spec)
pusher = Pusher(  # pylint: disable=unused-variable
    model_export=trainer.outputs.output,
    model_blessing=model_validator.outputs.blessing,
    serving_model_dir=serving_model_dir)

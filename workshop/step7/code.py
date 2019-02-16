# Add appropriate imports
from tfx.components import ModelValidator
from tfx.components import Pusher

# Add new spec above create_pipeline()
# For model validation
taxi_mv_spec = [tfma.SingleSliceSpec()]

# Add components to the end of pipeline in create_pipeline()
model_validator = ModelValidator(
    examples=examples_gen.outputs.output,
    model=trainer.outputs.output,
    eval_spec=taxi_mv_spec)
pusher = Pusher(  # pylint: disable=unused-variable
    model_export=trainer.outputs.output,
    model_blessing=model_validator.outputs.blessing,
    serving_model_dir=serving_model_dir)

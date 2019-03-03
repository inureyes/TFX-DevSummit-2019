# Add appropriate imports
from tfx.components import ModelValidator
from tfx.components import Pusher

# Add components to the end of pipeline in create_pipeline()
  model_validator = ModelValidator(
      examples=example_gen.outputs.output,
      model=trainer.outputs.output)

  pusher = Pusher(
      model_export=trainer.outputs.output,
      model_blessing=model_validator.outputs.blessing,
      serving_model_dir=serving_model_dir)

# Add model_analyzer to return
  return [
      example_gen, statistics_gen, infer_schema, validate_stats, transform,
      trainer, model_analyzer, model_validator, pusher
  ]
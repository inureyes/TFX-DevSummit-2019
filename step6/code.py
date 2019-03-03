# Add appropriate imports
import tensorflow_model_analysis as tfma
from tfx.components import Evaluator

# Add components to the end of pipeline in create_pipeline()
  model_analyzer = Evaluator(
      examples=example_gen.outputs.output,
      model_exports=trainer.outputs.output)

# Add model_analyzer to return
  return [
      example_gen, statistics_gen, infer_schema, validate_stats, transform,
      trainer, model_analyzer
  ]
# Add appropriate imports
import tensorflow_model_analysis as tfma
from tfx.components import Evaluator

# Add new spec above create_pipeline()
# For TFMA evaluation
taxi_eval_spec = [
    tfma.SingleSliceSpec(),
    tfma.SingleSliceSpec(columns=['trip_start_hour'])
]

# Add components to the end of pipeline in create_pipeline()
model_analyzer = Evaluator(
    examples=examples_gen.outputs.output,
    eval_spec=taxi_eval_spec,
    model_exports=trainer.outputs.output)
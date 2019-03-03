# Copy model.py to ~/airflow/plugins/tfx_example

# Add appropriate imports
from tfx.components import Trainer

# Add components to the end of pipeline in create_pipeline()
  trainer = Trainer(
      module_file=model,
      transformed_examples=transform.outputs.transformed_examples,
      schema=infer_schema.outputs.output,
      transform_output=transform.outputs.transform_output,
      train_steps=10000,
      eval_steps=5000,
      warm_starting=True)

# Add trainer to return
  return [
      example_gen, statistics_gen, infer_schema, validate_stats, transform,
      trainer
  ]
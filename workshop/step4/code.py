# Create ~/airflow/plugins/tfx_example
# Copy __init__.py to ~/airflow/plugins/tfx_example
# Copy features.py to ~/airflow/plugins/tfx_example
# Copy transforms.py to ~/airflow/plugins/tfx_example

# Add appropriate imports
from tfx.components import Transform

# Add new modules above PipelineDecorator
# Modules for trainer and transform
plugin_dir = os.path.join(home_dir, 'plugins/tfx_example/')
model = os.path.join(plugin_dir, 'model.py')
transforms = os.path.join(plugin_dir, 'transforms.py')

# Add components to the end of pipeline in create_pipeline()
  transform = Transform(
      input_data=example_gen.outputs.output,
      schema=infer_schema.outputs.output,
      module_file=transforms)

# Add components to return
  return [
      example_gen, statistics_gen, infer_schema, validate_stats, transform
  ]

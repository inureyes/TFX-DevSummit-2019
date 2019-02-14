# Create ~/airflow/plugins/tfx_example
# Copy features.py to ~/airflow/plugins/tfx_example
# Copy transforms.py to ~/airflow/plugins/tfx_example

# Add appropriate imports
from tfx.components import Transform

# Add components to the end of pipeline in create_pipeline()
transform = Transform(
    input_data=examples_gen.outputs.output,
    schema=infer_schema.outputs.output,
    module_file=transforms)
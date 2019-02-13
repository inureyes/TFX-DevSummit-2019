# Add appropriate imports
from tfx.components import Transform

# Add components to the end of pipeline in create_pipeline()
transform = Transform(
    input_data=examples_gen.outputs.output,
    schema=infer_schema.outputs.output,
    module_file=transforms)
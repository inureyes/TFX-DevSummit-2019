# Add the following code to ~/airflow/dags/tfx_example_pipeline.py

# Add appropriate imports
from tfx.components import ExamplesGen
from tfx.components import ExampleValidator
from tfx.components import SchemaGen
from tfx.components import StatisticsGen

# Add components to the end of pipeline in create_pipeline()
examples_gen = ExamplesGen(input_data=examples)
statistics_gen = StatisticsGen(input_data=examples_gen.outputs.output)
infer_schema = SchemaGen(stats=statistics_gen.outputs.output)
validate_stats = ExampleValidator(  # pylint: disable=unused-variable
    stats=statistics_gen.outputs.output,
    schema=infer_schema.outputs.output)

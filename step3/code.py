# Add the following code to ~/airflow/dags/taxi_pipeline.py

# Add appropriate imports
from tfx.components import ExampleValidator
from tfx.components import SchemaGen
from tfx.components import StatisticsGen

# Add components to the end of pipeline in create_pipeline()
  statistics_gen = StatisticsGen(input_data=example_gen.outputs.output)
  infer_schema = SchemaGen(stats=statistics_gen.outputs.output)
  validate_stats = ExampleValidator(  # pylint: disable=unused-variable
      stats=statistics_gen.outputs.output,
      schema=infer_schema.outputs.output)

# Add components to return
  return [
      example_gen, statistics_gen, infer_schema, validate_stats
  ]
# TFX Developer Workshop

## Introduction

This workshop is designed to introduce TensorFlow Extended (TFX)
and help you learn to create your own machine learning
pipelines.  We'll follow a typical ML development process,
starting by examining the dataset, and end up with a complete
working pipeline.  Along the way we'll explore ways to debug
and update your pipeline, and measure performance.

## Step by Step

We'll gradually create our pipelines by working step by step,
following a typical ML developer process.  Here are the steps:

1. Setup your environment
1. Bring up initial pipeline skeleton
1. Dive into our data
1. Feature engineering
1. Training
1. Analyzing model performance
1. Deployment to production

## Prerequisites

* Linux
* Virtualenv
* Python 2.7
* Git

## Workshop Materials

The code is organized by the steps that we're working on, so
for each step you'll have the code you need and instructions
on what to do with it in the code.py file in the step directory.

## Step 1: Setup your environment

In a shell:

```bash
virtualenv tfx-workshop
source tfx-workshop/bin/activate
mkdir tfx; cd tfx
git clone <something>
cd <into clone>/workshop/setup
./setup_demo.sh
```

## Step 2: Bring up initial pipeline skeleton

In a shell:

```bash
# Open a new terminal window, and in that window ...
source tfx-workshop/bin/activate
airflow webserver

# Open another new terminal window, and in that window ...
source tfx-workshop/bin/activate
airflow scheduler

# Open a browser and go to http://127.0.0.1:8080
# Enable the tfx_example_pipeline_DAG
# Trigger the tfx_example_pipeline_DAG

# In the Airflow web UI, click on tfx_example_pipeline_DAG
# Click on Graph View
# Wait for the Setup component to turn dark green (~1 minutes)
# Use the refresh button on the right or refresh the page
```

* Return to DAGs list page in Airflow
* Trigger tfx_example_pipeline_DAG
* Wait for pipeline to complete
  * All dark green
  * Use refresh on right side or refresh page

## Step 3: Dive into our data

In an editor:

```python
# Add the following code to ~/airflow/dags/tfx_example_pipeline.py
# Add appropriate imports
from tfx.components import ExamplesGen
from tfx.components import ExampleValidator
from tfx.components import SchemaGen
from tfx.components import StatisticsGen

# Add components to pipeline in create_pipeline()
examples_gen = ExamplesGen(input_data=examples)

statistics_gen = StatisticsGen(
  input_data=examples_gen.outputs.output)
infer_schema = SchemaGen(stats=statistics_gen.outputs.output)
validate_stats = ExampleValidator(
  stats=statistics_gen.outputs.output,
  schema=infer_schema.outputs.output)
```

* Return to DAGs list page in Airflow
* Trigger tfx_example_pipeline_DAG
* Wait for pipeline to complete
  * All dark green
  * Use refresh on right side or refresh page

## Step 4: Feature engineering

In a shell:

```bash
mkdir ~/airflow/plugins/tfx_example
cp <repo>/setup/plugins/tfx_example/transforms.py ~/airflow/plugins/tfx_example
cp <repo>/setup/plugins/tfx_example/features.py ~/airflow/plugins/tfx_example
```

In an editor:

```python
# Add the following code to ~/airflow/dags/tfx_example_pipeline.py
# Add appropriate imports
from tfx.components import Transform

# Add components to pipeline in create_pipeline()
transform = Transform(
    input_data=examples_gen.outputs.output,
    schema=infer_schema.outputs.output,
    module_file=transforms)
```

* Return to DAGs list page in Airflow
* Trigger tfx_example_pipeline_DAG
* Wait for pipeline to complete
  * All dark green
  * Use refresh on right side or refresh page

## Step 5: Training

In a shell:

```bash
cp <repo>/setup/plugins/tfx_example/model.py ~/airflow/plugins/tfx_example
```

In an editor:

```python
# Add the following code to ~/airflow/dags/tfx_example_pipeline.py
# Add appropriate imports
from tfx.components import Trainer

# Add components to pipeline in create_pipeline()
trainer = Trainer(
    module_file=model,
    transformed_examples=
    transform.outputs.transformed_examples,
    schema=infer_schema.outputs.output,
    transform_output=
    transform.outputs.transform_output,
    train_steps=10000,
    eval_steps=5000,
    warm_starting=True)
```

* Return to DAGs list page in Airflow
* Trigger tfx_example_pipeline_DAG
* Wait for pipeline to complete
  * All dark green
  * Use refresh on right side or refresh page

## Step 6: Analyzing model performance

In an editor:

```python
# Add the following code to ~/airflow/dags/tfx_example_pipeline.py
# Add appropriate imports
import tensorflow_model_analysis as tfma
from tfx.components import Evaluator
# Add new spec above create_pipeline()
# For TFMA evaluation
taxi_eval_spec = [
    tfma.SingleSliceSpec(),
    tfma.SingleSliceSpec(
        columns=['trip_start_hour'])
    ]

# Add components to pipeline in create_pipeline()
model_analyzer = Evaluator(
    examples=examples_gen.outputs.output,
    eval_spec=taxi_eval_spec,
    model_exports=trainer.outputs.output)
```

* Return to DAGs list page in Airflow
* Trigger tfx_example_pipeline_DAG
* Wait for pipeline to complete
  * All dark green
  * Use refresh on right side or refresh page

## Step 7: Deployment to production

In an editor:

```python
# Add the following code to ~/airflow/dags/tfx_example_pipeline.py
# Add appropriate imports
from tfx.components import ModelValidator
from tfx.components import Pusher
# Add new spec above create_pipeline()
# For model validation
taxi_mv_spec = [tfma.SingleSliceSpec()]

# Add components to pipeline in create_pipeline()
model_validator = ModelValidator(
    examples=examples_gen.outputs.output,
    model=trainer.outputs.output,
    eval_spec=taxi_mv_spec)

pusher = Pusher(   
    model_export=trainer.outputs.output,
    model_blessing=
        Model_validator.outputs.blessing,
    serving_model_dir=serving_model_dir)
```

* Return to DAGs list page in Airflow
* Trigger tfx_example_pipeline_DAG
* Wait for pipeline to complete
  * All dark green
  * Use refresh on right side or refresh page
"""THIS IS CODEGEN."""

# TODO(b/122676789): Please update component_generator to generate components.

# DO NOT EDIT. Auto-generated code.

from tfx.base_component import BaseComponent
from tfx.base_component import ComponentOutputs
from tfx.drivers.base_driver import BaseDriver
from tfx.drivers.csv_example_gen import CsvExampleGen as CsvExampleGenDriver
from tfx.drivers.model_validator import ModelValidator as ModelValidatorDriver
from tfx.drivers.pusher import Pusher as PusherDriver
from tfx.drivers.trainer import Trainer as TrainerDriver
from tfx.executors.big_query_example_gen import BigQueryExampleGen as BigQueryExampleGenExecutor
from tfx.executors.csv_example_gen import CsvExampleGen as CsvExampleGenExecutor
from tfx.executors.evaluator import Evaluator as EvaluatorExecutor
from tfx.executors.example_validator import ExampleValidator as ExampleValidatorExecutor
from tfx.executors.model_validator import ModelValidator as ModelValidatorExecutor
from tfx.executors.pusher import Pusher as PusherExecutor
from tfx.executors.schema_gen import SchemaGen as SchemaGenExecutor
from tfx.executors.statistics_gen import StatisticsGen as StatisticsGenExecutor
from tfx.executors.trainer import Trainer as TrainerExecutor
from tfx.executors.transform import Transform as TransformExecutor
from tfx.utils.channel import as_channel
from tfx.utils.channel import Channel
from tfx.utils.types import DEFAULT_EXAMPLE_SPLITS
from tfx.utils.types import TfxType


# TODO(b/122677689): Let's make sure that code-gen generates Python Style
# TODO(ruoyu): Align with internal tfservice.proto.
class CsvExampleGen(BaseComponent):
  """CSV ExampleGen component class."""

  def __init__(self, input_data, name=None, outputs=None):
    component_name = 'csv_example_gen'
    driver = CsvExampleGenDriver
    executor = CsvExampleGenExecutor
    input_dict = {'input_data': as_channel(input_data)}
    exec_properties = {}
    super(CsvExampleGen, self).__init__(
        component_name=component_name,
        unique_name=name,
        driver=driver,
        executor=executor,
        input_dict=input_dict,
        outputs=outputs,
        exec_properties=exec_properties)

  def _create_outputs(self):
    output_artifact_collection = [
        TfxType(
            'ExamplesPath',
            split=split,
        ) for split in DEFAULT_EXAMPLE_SPLITS
    ]
    return ComponentOutputs({
        'output':
            Channel(
                type_name='ExamplesPath',
                channel_name='output',
                static_artifact_collection=output_artifact_collection)
    })

  def _type_check(self, input_dict, exec_properties):
    self._single_type_check(input_dict['input_data'], 'ExamplesPath')


class BigQueryExampleGen(BaseComponent):
  """BigQuery ExampleGen component class."""

  def __init__(self, query, name=None, outputs=None):
    component_name = 'big_query_example_gen'
    # TODO(jyzhao): if table checksum and query matches, should hit cache.
    driver = BaseDriver
    executor = BigQueryExampleGenExecutor
    input_dict = {}
    exec_properties = {'query': query}
    super(BigQueryExampleGen, self).__init__(
        component_name=component_name,
        unique_name=name,
        driver=driver,
        executor=executor,
        input_dict=input_dict,
        outputs=outputs,
        exec_properties=exec_properties)

  def _create_outputs(self):
    output_artifact_collection = [
        TfxType(
            'ExamplesPath',
            split=split,
        ) for split in DEFAULT_EXAMPLE_SPLITS
    ]
    return ComponentOutputs({
        'output':
            Channel(
                type_name='ExamplesPath',
                channel_name='output',
                static_artifact_collection=output_artifact_collection)
    })

  def _type_check(self, input_dict, exec_properties):
    pass


class StatisticsGen(BaseComponent):

  def __init__(self, input_data, name=None, outputs=None):
    component_name = 'statistics_gen'
    driver = BaseDriver
    executor = StatisticsGenExecutor
    input_dict = {'input_data': as_channel(input_data)}
    exec_properties = {}
    super(StatisticsGen, self).__init__(
        component_name=component_name,
        unique_name=name,
        driver=driver,
        executor=executor,
        input_dict=input_dict,
        outputs=outputs,
        exec_properties=exec_properties)

  def _create_outputs(self):
    output_artifact_collection = [
        TfxType(
            'ExampleStatisticsPath',
            split=split,
        ) for split in DEFAULT_EXAMPLE_SPLITS
    ]
    return ComponentOutputs({
        'output':
            Channel(
                type_name='ExampleStatisticsPath',
                channel_name='output',
                static_artifact_collection=output_artifact_collection)
    })

  def _type_check(self, input_dict, exec_properties):
    self._single_type_check(input_dict['input_data'], 'ExamplesPath')


class SchemaGen(BaseComponent):

  def __init__(self, stats, name=None, outputs=None):
    component_name = 'schema_gen'
    driver = BaseDriver
    executor = SchemaGenExecutor
    input_dict = {'stats': as_channel(stats)}
    exec_properties = {}
    super(SchemaGen, self).__init__(
        component_name=component_name,
        unique_name=name,
        driver=driver,
        executor=executor,
        input_dict=input_dict,
        outputs=outputs,
        exec_properties=exec_properties)

  def _create_outputs(self):
    output_artifact_collection = [TfxType('SchemaPath',)]
    return ComponentOutputs({
        'output':
            Channel(
                type_name='SchemaPath',
                channel_name='output',
                static_artifact_collection=output_artifact_collection)
    })

  def _type_check(self, input_dict, exec_properties):
    self._single_type_check(input_dict['stats'], 'ExampleStatisticsPath')


class ExampleValidator(BaseComponent):

  def __init__(self, stats, schema, name=None, outputs=None):
    component_name = 'example_validator'
    driver = BaseDriver
    executor = ExampleValidatorExecutor
    input_dict = {
        'stats': as_channel(stats),
        'schema': as_channel(schema)
    }
    exec_properties = {}
    super(ExampleValidator, self).__init__(
        component_name=component_name,
        unique_name=name,
        driver=driver,
        executor=executor,
        input_dict=input_dict,
        outputs=outputs,
        exec_properties=exec_properties)

  def _create_outputs(self):
    output_artifact_collection = [
        TfxType('ExampleValidationPath',)
    ]
    # TODO(b/122682980): Please codegen - replace SkewResultsPath with
    # ExampleValidationPath
    return ComponentOutputs({
        'output':
            Channel(
                type_name='ExampleValidationPath',
                channel_name='output',
                static_artifact_collection=output_artifact_collection)
    })

  def _type_check(self, input_dict, exec_properties):
    self._single_type_check(input_dict['stats'], 'ExampleStatisticsPath')
    self._single_type_check(input_dict['schema'], 'SchemaPath')


class Transform(BaseComponent):

  def __init__(self,
               input_data,
               schema,
               module_file,
               name=None,
               outputs=None):
    component_name = 'transform'
    driver = BaseDriver
    executor = TransformExecutor
    input_dict = {
        'input_data': as_channel(input_data),
        'schema': as_channel(schema),
    }
    exec_properties = {'module_file': module_file}
    super(Transform, self).__init__(
        component_name=component_name,
        unique_name=name,
        driver=driver,
        executor=executor,
        input_dict=input_dict,
        outputs=outputs,
        exec_properties=exec_properties)

  def _create_outputs(self):
    transform_output_artifact_collection = [
        TfxType('TransformPath',)
    ]
    transformed_examples_artifact_collection = [
        TfxType(
            'ExamplesPath',
            split=split,
        ) for split in DEFAULT_EXAMPLE_SPLITS
    ]
    return ComponentOutputs({
        'transform_output':
            Channel(
                type_name='TransformPath',
                channel_name='transform_output',
                static_artifact_collection=transform_output_artifact_collection
            ),
        'transformed_examples':
            Channel(
                type_name='ExamplesPath',
                channel_name='transformed_examples',
                static_artifact_collection=transformed_examples_artifact_collection
            ),
    })

  def _type_check(self, input_dict, exec_properties):
    self._single_type_check(input_dict['input_data'], 'ExamplesPath')
    self._single_type_check(input_dict['schema'], 'SchemaPath')
    # TODO(b/122685556): Typecheck preprocessing_fn


class Evaluator(BaseComponent):

  def __init__(self,
               examples,
               model_exports,
               name=None,
               outputs=None):
    component_name = 'evaluator'
    driver = BaseDriver
    executor = EvaluatorExecutor
    input_dict = {
        'examples': as_channel(examples),
        'model_exports': as_channel(model_exports),
    }
    exec_properties = {}
    super(Evaluator, self).__init__(
        component_name=component_name,
        unique_name=name,
        driver=driver,
        executor=executor,
        input_dict=input_dict,
        outputs=outputs,
        exec_properties=exec_properties)

  def _create_outputs(self):
    output_artifact_collection = [TfxType('ModelEvalPath',)]
    return ComponentOutputs({
        'output':
            Channel(
                type_name='ModelEvalPath',
                channel_name='output',
                static_artifact_collection=output_artifact_collection),
    })

  def _type_check(self, input_dict, exec_properties):
    self._single_type_check(input_dict['examples'], 'ExamplesPath')
    self._single_type_check(input_dict['model_exports'], 'ModelExportPath')


class ModelValidator(BaseComponent):
  """Model validator component class."""

  def __init__(self, examples, model, name=None, outputs=None):
    component_name = 'model_validator'
    driver = ModelValidatorDriver
    executor = ModelValidatorExecutor
    input_dict = {
        'examples': as_channel(examples),
        'model': as_channel(model),
    }
    exec_properties = {
        'latest_blessed_model': None,  # Set in driver.
        'latest_blessed_model_id': None,  # Set in driver.
    }
    super(ModelValidator, self).__init__(
        component_name=component_name,
        unique_name=name,
        driver=driver,
        executor=executor,
        input_dict=input_dict,
        outputs=outputs,
        exec_properties=exec_properties)

  def _create_outputs(self):
    blessing_artifact_collection = [
        TfxType('ModelBlessingPath',),
    ]
    results_artifact_collection = [
        TfxType('ModelValidationPath',),
    ]
    return ComponentOutputs({
        'blessing':
            Channel(
                type_name='ModelBlessingPath',
                channel_name='blessing',
                static_artifact_collection=blessing_artifact_collection),
        'results':
            Channel(
                type_name='ModelValidationPath',
                channel_name='results',
                static_artifact_collection=results_artifact_collection),
    })

  def _type_check(self, input_dict, exec_properties):
    self._single_type_check(input_dict['examples'], 'ExamplesPath')
    self._single_type_check(input_dict['model'], 'ModelExportPath')


class Trainer(BaseComponent):

  def __init__(self,
               module_file,
               transformed_examples,
               transform_output,
               schema,
               train_steps=None,
               eval_steps=None,
               warm_starting=False,
               name=None,
               outputs=None):
    component_name = 'trainer'
    driver = TrainerDriver
    executor = TrainerExecutor
    input_dict = {
        'transformed_examples': as_channel(transformed_examples),
        'transform_output': as_channel(transform_output),
        'schema': as_channel(schema),
    }
    exec_properties = {
        'train_steps': train_steps,
        'eval_steps': eval_steps,
        'module_file': module_file,
        'warm_starting': warm_starting,
        'warm_start_from': None,  # Set in driver.
    }
    super(Trainer, self).__init__(
        component_name=component_name,
        unique_name=name,
        driver=driver,
        executor=executor,
        input_dict=input_dict,
        outputs=outputs,
        exec_properties=exec_properties)

  def _create_outputs(self):
    output_artifact_collection = [
        TfxType('ModelExportPath',),
    ]
    return ComponentOutputs({
        'output':
            Channel(
                type_name='ModelExportPath',
                channel_name='output',
                static_artifact_collection=output_artifact_collection),
    })

  def _type_check(self, input_dict, exec_properties):
    self._single_type_check(input_dict['transformed_examples'], 'ExamplesPath')
    self._single_type_check(input_dict['transform_output'], 'TransformPath')
    self._single_type_check(input_dict['schema'], 'SchemaPath')


class Pusher(BaseComponent):
  """Pusher component class."""

  def __init__(self,
               model_export,
               model_blessing,
               serving_model_dir,
               name=None,
               outputs=None):
    component_name = 'pusher'
    driver = PusherDriver
    executor = PusherExecutor
    input_dict = {
        'model_export': as_channel(model_export),
        'model_blessing': as_channel(model_blessing),
    }
    exec_properties = {
        'serving_model_dir': serving_model_dir,
        'latest_pushed_model': None,  # Set in driver.
    }
    super(Pusher, self).__init__(
        component_name=component_name,
        unique_name=name,
        driver=driver,
        executor=executor,
        input_dict=input_dict,
        outputs=outputs,
        exec_properties=exec_properties)

  def _create_outputs(self):
    model_push_artifact_collection = [
        TfxType('ModelPushPath',),
    ]
    return ComponentOutputs({
        'model_push':
            Channel(
                type_name='ModelPushPath',
                channel_name='model_push',
                static_artifact_collection=model_push_artifact_collection),
    })

  def _type_check(self, input_dict, exec_properties):
    self._single_type_check(input_dict['model_export'], 'ModelExportPath')
    self._single_type_check(input_dict['model_blessing'], 'ModelBlessingPath')

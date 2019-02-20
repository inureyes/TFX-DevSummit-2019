"""THIS IS CODEGEN."""

# TODO(b/122676789): Please update component_generator to generate components.

# DO NOT EDIT. Auto-generated code.
import json

from tfx.base_component import BaseComponent
from tfx.drivers.base_driver import BaseDriver
from tfx.drivers.examples_gen import ExamplesGen as ExamplesGenDriver
from tfx.drivers.model_validator import ModelValidator as ModelValidatorDriver
from tfx.drivers.pusher import Pusher as PusherDriver
from tfx.drivers.trainer import Trainer as TrainerDriver
from tfx.executors.big_query_examples_gen import BigQueryExamplesGen as BigQueryExamplesGenExecutor
from tfx.executors.evaluator import Evaluator as EvaluatorExecutor
from tfx.executors.evaluator import SingleSliceSpecEncoder
from tfx.executors.example_validator import ExampleValidator as ExampleValidatorExecutor
from tfx.executors.examples_gen import ExamplesGen as ExamplesGenExecutor
from tfx.executors.model_validator import ModelValidator as ModelValidatorExecutor
from tfx.executors.pusher import Pusher as PusherExecutor
from tfx.executors.schema_gen import SchemaGen as SchemaGenExecutor
from tfx.executors.statistics_gen import StatisticsGen as StatisticsGenExecutor
from tfx.executors.trainer import Trainer as TrainerExecutor
from tfx.executors.transform import Transform as TransformExecutor
import tfx.utils.types


# TODO(b/122677689): Let's make sure that code-gen generates Python Style
# TODO(ruoyu): Align with internal tfservice.proto.
# TODO(b/122678186): Taking one csv file and split into two splits for ExamplesGen
class ExamplesGen(BaseComponent):

  def __new__(cls, input_data, name=None):
    component_name = 'examples_gen'
    driver = ExamplesGenDriver
    executor = ExamplesGenExecutor
    input_dict = {'input_data': input_data}
    exec_properties = {}
    return super(ExamplesGen, cls).__new__(
        cls,
        component_name=component_name,
        unique_name=name,
        driver=driver,
        executor=executor,
        input_dict=input_dict,
        exec_properties=exec_properties)

  @classmethod
  def _create_output_dict(cls):
    return {
        'output': [
            tfx.utils.types.TfxType(
                'ExamplesPath',
                split=split,
            ) for split in tfx.utils.types.DEFAULT_EXAMPLE_SPLITS
        ]
    }

  @classmethod
  def _type_check(cls, input_dict):
    cls._single_type_check(input_dict['input_data'], 'ExamplesPath')


class BigQueryExamplesGen(BaseComponent):
  """BigQuery ExamplesGen component class."""

  def __new__(cls, query, name=None):
    component_name = 'big_query_examples_gen'
    # TODO(jyzhao): if table checksum and query matches, should hit cache.
    driver = BaseDriver
    executor = BigQueryExamplesGenExecutor
    input_dict = {}
    exec_properties = {'query': query}
    return super(BigQueryExamplesGen, cls).__new__(
        cls,
        component_name=component_name,
        unique_name=name,
        driver=driver,
        executor=executor,
        input_dict=input_dict,
        exec_properties=exec_properties)

  @classmethod
  def _create_output_dict(cls):
    return {
        'output': [
            tfx.utils.types.TfxType(
                'ExamplesPath',
                split=split,
            ) for split in tfx.utils.types.DEFAULT_EXAMPLE_SPLITS
        ]
    }

  @classmethod
  def _type_check(cls, input_dict):
    pass


class StatisticsGen(BaseComponent):

  def __new__(cls, input_data, name=None):
    component_name = 'statistics_gen'
    driver = BaseDriver
    executor = StatisticsGenExecutor
    input_dict = {'input_data': input_data}
    exec_properties = {}
    return super(StatisticsGen, cls).__new__(
        cls,
        component_name=component_name,
        unique_name=name,
        driver=driver,
        executor=executor,
        input_dict=input_dict,
        exec_properties=exec_properties)

  @classmethod
  def _create_output_dict(cls):
    return {
        'output': [
            tfx.utils.types.TfxType(
                'ExampleStatisticsPath',
                split=split,
            ) for split in tfx.utils.types.DEFAULT_EXAMPLE_SPLITS
        ]
    }

  @classmethod
  def _type_check(cls, input_dict):
    cls._single_type_check(input_dict['input_data'], 'ExamplesPath')


class SchemaGen(BaseComponent):

  def __new__(cls, stats, executor=SchemaGenExecutor, name=None):
    component_name = 'schema_gen'
    driver = BaseDriver
    executor = SchemaGenExecutor
    input_dict = {'stats': stats}
    exec_properties = {}
    return super(SchemaGen, cls).__new__(
        cls,
        component_name=component_name,
        unique_name=name,
        driver=driver,
        executor=executor,
        input_dict=input_dict,
        exec_properties=exec_properties)

  @classmethod
  def _create_output_dict(cls):
    # TODO(b/122678513): Please codegen - appended 'schema.pbtxt'
    return {'output': [tfx.utils.types.TfxType('SchemaPath',)]}

  @classmethod
  def _type_check(cls, input_dict):
    cls._single_type_check(input_dict['stats'], 'ExampleStatisticsPath')


class ExampleValidator(BaseComponent):

  def __new__(cls, stats, schema, name=None):
    component_name = 'example_validator'
    driver = BaseDriver
    executor = ExampleValidatorExecutor
    input_dict = {'stats': stats, 'schema': schema}
    exec_properties = {}
    return super(ExampleValidator, cls).__new__(
        cls,
        component_name=component_name,
        unique_name=name,
        driver=driver,
        executor=executor,
        input_dict=input_dict,
        exec_properties=exec_properties)
  @classmethod
  def _create_output_dict(cls):
    # TODO(b/122683358): Please codegen - appended 'anomalies.pbtxt'
    # TODO(b/122682980): Please codegen - replace SkewResultsPath with
    # ExampleValidationPath
    return {'output': [tfx.utils.types.TfxType('ExampleValidationPath',)]}

  @classmethod
  def _type_check(cls, input_dict):
    cls._single_type_check(input_dict['stats'], 'ExampleStatisticsPath')
    cls._single_type_check(input_dict['schema'], 'SchemaPath')


class Transform(BaseComponent):

  def __new__(cls,
              input_data,
              schema,
              module_file,
              executor=TransformExecutor,
              name=None):
    component_name = 'transform'
    driver = BaseDriver
    executor = TransformExecutor
    input_dict = {
        'input_data': input_data,
        'schema': schema,
    }
    exec_properties = {
        'module_file': module_file
    }
    return super(Transform, cls).__new__(
        cls,
        component_name=component_name,
        unique_name=name,
        driver=driver,
        executor=executor,
        input_dict=input_dict,
        exec_properties=exec_properties)

  @classmethod
  def _create_output_dict(cls):
    # TODO(b/122683985): Please codegen - appended 'tfx.transformed'
    return {
        'transform_output': [tfx.utils.types.TfxType('TransformPath',),],
        'transformed_examples': [
            tfx.utils.types.TfxType(
                'ExamplesPath',
                split=split,
            ) for split in tfx.utils.types.DEFAULT_EXAMPLE_SPLITS
        ],
    }

  @classmethod
  def _type_check(cls, input_dict):
    cls._single_type_check(input_dict['input_data'], 'ExamplesPath')
    cls._single_type_check(input_dict['schema'], 'SchemaPath')
    # TODO(b/122685556): Typecheck preprocessing_fn


class Evaluator(BaseComponent):

  def __new__(cls, examples, eval_spec, model_exports, name=None):
    component_name = 'evaluator'
    driver = BaseDriver
    executor = EvaluatorExecutor
    input_dict = {
        'examples': examples,
        'model_exports': model_exports,
    }
    exec_properties = {
        'eval_spec':
            json.dumps(eval_spec or [], cls=SingleSliceSpecEncoder)}
    return super(Evaluator, cls).__new__(
        cls,
        component_name=component_name,
        unique_name=name,
        driver=driver,
        executor=executor,
        input_dict=input_dict,
        exec_properties=exec_properties)
  @classmethod
  def _create_output_dict(cls):
    return {
        'output': [tfx.utils.types.TfxType('ModelEvalPath',)],
    }

  @classmethod
  def _type_check(cls, input_dict):
    cls._single_type_check(input_dict['examples'], 'ExamplesPath')
    cls._single_type_check(input_dict['model_exports'], 'ModelExportPath')


class ModelValidator(BaseComponent):
  """Model validator component class."""

  def __new__(cls, examples, model, eval_spec, name=None):
    component_name = 'model_validator'
    driver = ModelValidatorDriver
    executor = ModelValidatorExecutor
    input_dict = {
        'examples': examples,
        'model': model,
    }
    exec_properties = {
        'eval_spec': json.dumps(eval_spec or [], cls=SingleSliceSpecEncoder),
        'latest_blessed_model': None,  # Set in driver.
        'latest_blessed_model_id': None,  # Set in driver.
    }
    return super(ModelValidator, cls).__new__(
        cls,
        component_name=component_name,
        unique_name=name,
        driver=driver,
        executor=executor,
        input_dict=input_dict,
        exec_properties=exec_properties)

  @classmethod
  def _create_output_dict(cls):
    return {
        'blessing': [tfx.utils.types.TfxType('ModelBlessingPath',),],
        'results': [tfx.utils.types.TfxType('ModelValidationPath',),],
    }

  @classmethod
  def _type_check(cls, input_dict):
    cls._single_type_check(input_dict['examples'], 'ExamplesPath')
    cls._single_type_check(input_dict['model'], 'ModelExportPath')


class Trainer(BaseComponent):

  def __new__(
      cls,
      # TODO(ruoyu): Please add 'module_file'(?) to proto, codegen
      module_file,
      transformed_examples,
      transform_output,
      schema,
      train_steps=None,
      eval_steps=None,
      warm_starting=False,
      name=None):
    component_name = 'trainer'
    driver = TrainerDriver
    input_dict = {
        'transformed_examples': transformed_examples,
        'transform_output': transform_output,
        'schema': schema
    }
    executor = TrainerExecutor
    exec_properties = {
        'train_steps': train_steps,
        'eval_steps': eval_steps,
        'module_file': module_file,
        'warm_starting': warm_starting,
        'warm_start_from': None,  # Set in driver.
    }
    return super(Trainer, cls).__new__(
        cls,
        component_name=component_name,
        unique_name=name,
        driver=driver,
        executor=executor,
        input_dict=input_dict,
        exec_properties=exec_properties)

  @classmethod
  def _create_output_dict(cls):
    return {
        'output': [tfx.utils.types.TfxType('ModelExportPath',),],
    }

  @classmethod
  def _type_check(cls, input_dict):
    cls._single_type_check(input_dict['transformed_examples'], 'ExamplesPath')
    cls._single_type_check(input_dict['transform_output'], 'TransformPath')
    cls._single_type_check(input_dict['schema'], 'SchemaPath')
    # TODO(b/122685557): Typecheck trainer_fn?


class Pusher(BaseComponent):
  """Pusher component class."""

  def __new__(cls, model_export, model_blessing, serving_model_dir, name=None):
    component_name = 'pusher'
    driver = PusherDriver
    executor = PusherExecutor
    input_dict = {
        'model_export': model_export,
        'model_blessing': model_blessing
    }
    exec_properties = {
        'serving_model_dir': serving_model_dir,
        'latest_pushed_model': None,  # Set in driver.
    }
    return super(Pusher, cls).__new__(
        cls,
        component_name=component_name,
        unique_name=name,
        driver=driver,
        executor=executor,
        input_dict=input_dict,
        exec_properties=exec_properties)

  @classmethod
  def _create_output_dict(cls):
    return {
        'model_push': [tfx.utils.types.TfxType('ModelPushPath',),],
    }

  @classmethod
  def _type_check(cls, input_dict):
    cls._single_type_check(input_dict['model_export'], 'ModelExportPath')
    cls._single_type_check(input_dict['model_blessing'], 'ModelBlessingPath')

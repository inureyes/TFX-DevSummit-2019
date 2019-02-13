from proto.tfservice_pb2 import Service
from proto.spec_pb2 import spec
import six
import itertools

# TODO(b/122677898): P0: Wrap this in a class and allow default values.
# TODO(b/122677727): P0: Support multi-output cases.
# TODO(b/122679163): P2: Explore ast solution for class and function creation.

import_template = """
# DO NOT EDIT. Auto-generated code.

from tfx_airflow import BaseTfxPath
from tfx_airflow import Component
"""

component_cls_template = """
class {class_name}(Component):
{init_func}
{create_output_func}
{type_check_func}"""

type_cls_template = """
class {class_name}(BaseTfxPath):
  def __init__(self, path):
    super({class_name}, self).__init__(path)
"""

init_func_template = """
  def __init__(self, task, {arg_list}tag=None, **kwargs):
    component_name = "{display_name}"
    super({class_name}, self).__init__(
        component_name=component_name,
        worker_callable=task,
        worker_exec='python',
        input_list={input_list},
        unique_name=tag,
        {arg_assignments}**kwargs)"""

# TODO(b/122677727): We should allow more than one output.
create_output_func_template = """
  def _create_output(self, output_path):
    return {output_type}(output_path)"""

type_check_func_template = """
  def _type_check(self, input_list):
{type_check_rules}
"""

single_type_check_template = """    self._single_type_check(input_list['{input_label}'], {expected_type})"""


def generate_tfx_component():
  gen_code = generate_code()
  if six.PY2:
    import imp
    module = imp.new_module('tfx_components')
  else:
    import types
    module = types.ModuleType('tfx_components')

  import sys
  sys.modules['tfx_components'] = module
  exec gen_code in module.__dict__

  return module


def generate_code():
  import_code_blocks = [import_template]
  component_code_blocks, tfx_io_types = generate_components_code()
  type_code_blocks = generate_type_code(tfx_io_types)
  return '\n'.join(
      itertools.chain(import_code_blocks, type_code_blocks,
                      component_code_blocks))


def generate_type_code(tfx_io_types):
  return [type_cls_template.format(class_name=t) for t in tfx_io_types]


def generate_components_code():
  component_code_blocks = []
  tfx_io_type_set = set()
  for field in Service.DESCRIPTOR.fields:
    msg_type = field.message_type
    cls_name = msg_type.name
    cls_spec = msg_type.GetOptions().Extensions[spec]
    cls_args = [fd.name for fd in msg_type.fields]
    component_code_blocks.append(generate_class(cls_name, cls_args, cls_spec))
    for i in itertools.chain(cls_spec.inputs, cls_spec.outputs):
      tfx_io_type_set.add(i.type)
  return component_code_blocks, tfx_io_type_set


def generate_class(cls_name, cls_args, cls_spec):
  return component_cls_template.format(
      class_name=cls_name,
      init_func=generate_init_func(cls_name, cls_args, cls_spec),
      create_output_func=generate_create_output_func(cls_spec),
      type_check_func=generate_type_check_func(cls_spec))


def generate_init_func(cls_name, cls_args, cls_spec):

  def camel_to_snake(text):
    import re
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2',
                  re.sub('(.)([A-Z][a-z]+)', r'\1_\2', text)).lower()

  input_arg_list = [input.label for input in cls_spec.inputs]
  input_list = '{' + ', '.join(
      ["\'{t}\' : {t}".format(t=input) for input in input_arg_list]) + '}'
  arg_list = ', '.join(itertools.chain(input_arg_list, cls_args)) + ',' if len(
      cls_args) > 0 or len(input_arg_list) > 0 else ''
  arg_assignments = ', '.join(['{}={}'.format(arg, arg) for arg in cls_args
                              ]) + ',' if len(cls_args) > 0 else ''
  snake_type_name = camel_to_snake(cls_name)

  return init_func_template.format(
      class_name=cls_name,
      display_name=snake_type_name,
      arg_list=arg_list,
      input_list=input_list,
      arg_assignments=arg_assignments)


# TODO(ruoyu): Allow more than one outputs.
def generate_create_output_func(cls_spec):
  output_type = cls_spec.outputs[-1].type if len(
      cls_spec.outputs) > 0 else 'DefaultPath'
  return create_output_func_template.format(output_type=output_type)


def generate_type_check_func(cls_spec):
  type_checks = []
  for input in cls_spec.inputs:
    type_checks.append(
        single_type_check_template.format(
            input_label=input.label, expected_type=input.type))
  return type_check_func_template.format(type_check_rules='\n'.join(
      type_checks) if len(type_checks) > 0 else '    pass')


print(generate_code())

"""TFX base component definition."""
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import functools


# TODO(ruoyu): Make BaseComponent and BaseDecorator ABC.
class BaseComponent(object):
  """Base TFX component."""

  def __new__(cls, component_name, unique_name, driver, executor, input_dict,
              exec_properties):
    cls._type_check(input_dict)
    output_dict = cls._create_output_dict()
    return cls._new_component(component_name, unique_name, driver, executor,
                              input_dict, output_dict, exec_properties)

  @classmethod
  def _single_type_check(cls, target, expected_type_name):
    if target.type_name != expected_type_name:
      raise TypeError('Expects {} but found {}'.format(expected_type_name,
                                                       str(target.type_name)))

  @classmethod
  def _create_output_dict(cls):
    pass

  @classmethod
  def _type_check(cls, input_dict):
    pass

  # TODO(ruoyu): Find a way to make this classmethod instead of staticmethod.
  @staticmethod
  def _new_component(component_name, unique_name, driver, executor, input_dict,
                     output_dict, exec_properties):
    pass


class BaseDecorator(object):
  """Base TFX pipeline decorator."""

  def __init__(self, **kwargs):
    self._pipeline = self._new_pipeline(**kwargs)
    self._kwargs = kwargs

  def __call__(self, func):

    @functools.wraps(func)
    def decorated():
      BaseComponent._new_component = staticmethod(self._new_component)  # pylint: disable=protected-access
      func()
      return self._pipeline

    return decorated

  def _new_pipeline(self, **kwargs):
    pass

  def _new_component(self, component_name, unique_name, driver, executor,
                     input_dict, output_dict, exec_properties):
    pass

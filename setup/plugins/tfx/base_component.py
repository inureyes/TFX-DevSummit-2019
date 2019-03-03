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
import abc
from future.utils import with_metaclass


class ComponentOutputs(object):
  """Helper class to wrap outputs from TFX components."""

  def __init__(self, d):
    self.__dict__ = d

  def get_all(self):
    return self.__dict__


class BaseComponent(with_metaclass(abc.ABCMeta, object)):
  """Base TFX component.

  This is the parent class of any TFX component.

  Attributes:
    component_name: Name of the component, should be unique per component class.
    unique_name: Unique name for every component class instance.
    driver: Driver class to handle pre-execution behaviors in a component.
    executor: Executor class to do the real execution work.
    input_dict: A [Text -> Channel] dict serving as the inputs to the component.
    exec_properties: A [Text -> Any] dict serving as additional properties
        needed for execution.
    outputs: Optional Channel destinations of the component.
  """

  def __init__(self, component_name, unique_name, driver, executor, input_dict,
               outputs, exec_properties):
    self.component_name = component_name
    self.unique_name = unique_name
    self.driver = driver
    self.executor = executor
    self.input_dict = input_dict
    self.exec_properties = exec_properties
    self.outputs = outputs or self._create_outputs()
    self._type_check(self.input_dict, self.exec_properties)

  def _single_type_check(self, target, expected_type_name):
    if target.type_name != expected_type_name:
      raise TypeError('Expects {} but found {}'.format(expected_type_name,
                                                       str(target.type_name)))

  def __str__(self):
    return """
{
  component_name: {},
  unique_name: {},
  driver: {},
  executor: {},
  input_dict: {},
  outputs: {},
  exec_properties: {}
}
    """.format(self.component_name, self.unique_name, self.driver,
               self.executor, self.input_dict, self.outputs,
               self.exec_properties)

  def __repr__(self):
    return self.__str__()

  @abc.abstractmethod
  def _create_outputs(self):
    """Create outputs placeholder for components.

    return: ComponentOutputs object containing the dict of [Text -> Channel]
    """
    raise NotImplementedError

  @abc.abstractmethod
  def _type_check(self, input_dict, exec_properties):
    """Do type checking for the inputs and exec_properties."""
    raise NotImplementedError

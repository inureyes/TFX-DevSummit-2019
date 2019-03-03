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
"""TFX type definition."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from tfx.utils.types import DEFAULT_EXAMPLE_SPLITS
from tfx.utils.types import TfxType


def csv_inputs(uri, presplit=False):
  """Helper function to declare inputs for csv_example_gen component."""
  instance = TfxType(type_name='ExamplesPath')
  instance.uri = uri
  if presplit:
    instance.set_string_custom_property(
        'presplit', ','.join(DEFAULT_EXAMPLE_SPLITS))
  return [instance]

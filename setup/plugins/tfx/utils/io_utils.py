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
"""io_utils provides utilities functions commonly shared by executors."""

import os
import six
import tensorflow as tf
from tensorflow.python.lib.io import file_io
from google.protobuf import text_format


def import_func(module_path, fn_name):
  """Import a function from a module provided as source file."""
  if module_path is None:
    return None
  _, fileext = os.path.splitext(module_path)
  assert fileext in ['.py', '.pyc']

  # If a GCS bucket (gs://...), download to local filesystem first as
  # importlib can't import from GCS
  if module_path.startswith('gs://'):
    module_filename = os.path.basename(module_path)
    copy_file(module_path, module_filename, True)
    module_path = module_filename

  if six.PY2:
    import imp  # pylint: disable=g-import-not-at-top
    user_module = imp.load_source('user_module', module_path)
  else:
    import importlib.util  # pylint: disable=g-import-not-at-top
    spec = importlib.util.spec_from_file_location('user_module', module_path)
    user_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(user_module)
  return getattr(user_module, fn_name)


def copy_file(src, dst, overwrite=False):
  if overwrite and tf.gfile.Exists(dst):
    tf.gfile.Remove(dst)
  dst_dir = os.path.dirname(dst)
  tf.gfile.MakeDirs(dst_dir)
  tf.gfile.Copy(src, dst, overwrite=overwrite)


def copy_dir(src, dst):
  """Copy directory."""
  if tf.gfile.Exists(dst):
    tf.gfile.DeleteRecursively(dst)
  tf.gfile.MakeDirs(dst)

  for dir_name, sub_dirs, leaf_files in tf.gfile.Walk(src):
    for leaf_file in leaf_files:
      leaf_file_path = os.path.join(dir_name, leaf_file)
      new_file_path = os.path.join(dir_name.replace(src, dst, 1), leaf_file)
      tf.gfile.Copy(leaf_file_path, new_file_path)

    for sub_dir in sub_dirs:
      tf.gfile.MakeDirs(os.path.join(dst, sub_dir))


def get_only_uri_in_dir(dir_path):
  files = tf.gfile.ListDirectory(dir_path)
  if len(files) != 1:
    raise RuntimeError(
        'Only one file per dir is supported: {}.'.format(dir_path))
  return os.path.join(dir_path, files[-1])


def delete_dir(path):
  if tf.gfile.Exists(os.path.dirname(path)):
    tf.gfile.DeleteRecursively(path)


def write_string_file(file_name, string_value):
  tf.gfile.MakeDirs(os.path.dirname(file_name))
  file_io.write_string_to_file(file_name, string_value)


def write_pbtxt_file(file_name, proto):
  write_string_file(file_name, text_format.MessageToString(proto))


def write_tfrecord_file(file_name, proto):
  tf.gfile.MakeDirs(os.path.dirname(file_name))
  with tf.python_io.TFRecordWriter(file_name) as writer:
    writer.write(proto.SerializeToString())


def parse_pbtxt_file(file_name, message):
  contents = file_io.read_file_to_string(file_name)
  text_format.Parse(contents, message)
  return message


def load_csv_column_names(csv_file):
  """Parse the first line of a csv file as column names."""
  with file_io.FileIO(csv_file, 'r') as f:
    return f.readline().strip().split(',')


def all_files_pattern(file_pattern):
  """Returns file pattern suitable for beam to locate multiple files."""
  return '{}*'.format(file_pattern)

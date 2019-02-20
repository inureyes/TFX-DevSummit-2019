"""Example of garbage collection example."""
from absl import app
from absl import flags

from ml_metadata.proto import metadata_store_pb2
import tensorflow as tf
from tfx.gc.tfx_garbage_collectors import SpanBasedGarbageCollector

FLAGS = flags.FLAGS

flags.DEFINE_integer(
    'num_span_to_keep', default=None, help='umber of spans to keep')
flags.DEFINE_string(
    'metadata_db_path',
    default='/var/tmp/metadata.db',
    help='Path to metadata database')
flags.DEFINE_string(
    'log_path', default='/var/tmp/tfx', help='Path to base log directory')


def _get_default_metadata_connection_config(db_uri):
  if not tf.gfile.Exists(db_uri):
    raise RuntimeError('Database uri does not exist: {}'.format(str(db_uri)))
  connection_config = metadata_store_pb2.ConnectionConfig()
  connection_config.sqlite.filename_uri = db_uri
  connection_config.sqlite.connection_mode = \
    metadata_store_pb2.SqliteMetadataSourceConfig.READWRITE_OPENCREATE
  return connection_config


def main(argv):
  del argv

  mlmd_connection_config = _get_default_metadata_connection_config(
      FLAGS.metadata_db_path)
  gc = SpanBasedGarbageCollector(FLAGS.num_span_to_keep, mlmd_connection_config,
                                 FLAGS.log_path)
  gc.do_garbage_collection()


if __name__ == '__main__':
  flags.mark_flag_as_required('num_span_to_keep')
  app.run(main)

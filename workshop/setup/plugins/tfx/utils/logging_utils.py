"""Utils for TFX-specific logger."""

import logging
import os
import tensorflow as tf


def get_logger(log_dir, log_file_name, logging_level=logging.INFO):
  """Create and configure a TFX-specific logger.

  Args:
    log_dir: log directory path, will create if not exists.
    log_file_name: name of log file under above directory.
    logging_level: logger's level, default to INFO.

  Returns:
    A logger that outputs to log_dir/log_file_name.

  Raises:
    RuntimeError: if log dir exists as a file.

  """
  log_path = os.path.join(log_dir, log_file_name)
  logger = logging.getLogger(log_path)
  logger.setLevel(logging_level)

  if not tf.gfile.Exists(log_dir):
    tf.gfile.MakeDirs(log_dir)
  if not tf.gfile.IsDirectory(log_dir):
    raise RuntimeError('Log dir exists as a file: {}'.format(log_dir))

  # Create logfile handler.
  fh = logging.FileHandler(log_path)
  # Define logmsg format.
  formatter = logging.Formatter(
      '%(asctime)s - %(filename)s:%(lineno)s - %(levelname)s: %(message)s')
  fh.setFormatter(formatter)
  # Add handler to logger.
  logger.addHandler(fh)

  return logger

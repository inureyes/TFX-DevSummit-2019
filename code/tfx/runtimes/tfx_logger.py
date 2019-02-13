"""Creates and configure a TFX-specific logger."""


import logging
import os
import tensorflow as tf


def get_logger(log_path, suffix):
  """Create and configure a TFX-specific logger."""
  # create logfile handler
  # TODO(khaas): set log level from pipeline config
  log_path = log_path + suffix
  logger = logging.getLogger(log_path)

  if not tf.gfile.IsDirectory(log_path):
    tf.gfile.MakeDirs(log_path)
  fh = logging.FileHandler(os.path.join(log_path, 'tfx.log'))
  fh.setLevel(logging.DEBUG)

  # define logmsg format
  formatter = logging.Formatter(
      '%(asctime)s - %(filename)s:%(lineno)s - %(levelname)s: %(message)s')
  fh.setFormatter(formatter)

  # add handler to logger
  logger.addHandler(fh)
  return logger

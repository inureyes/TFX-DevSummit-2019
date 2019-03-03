#!/bin/bash
#
# Use this to completely nuke the pypi libraries that TFX requires
# and start with a 'clean' environment.  This will uninstall TF/TFX
# libraries and airflow libraries.
#
# It will not delete any directories.  You'll want to delete
# ~/airflow/dags/tfx and ~/airflow/dags/taxi on your own.
#

pip uninstall tensorflow
pip uninstall tensorflow-model-analysis
pip uninstall tensorflow-data-validation
pip uninstall tensorflow-metadata
pip uninstall tensorflow-transform
pip uninstall apache-airflow

echo ''
echo 'Remember to delete ~/airflow/dags/taxi_pipeline*, ~/airflow/plugins/*,'
echo '~/airflow/data/taxi_data, ~/airflow/data/tfx, and /var/tmp/tfx/logs/.'
echo ''

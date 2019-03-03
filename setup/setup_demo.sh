#!/bin/bash
# Set up the environment for the TFX workshop

printf "Installing TFX workshop\n\n"

# TF/TFX prereqs
printf "Installing TensorFlow\n"
pip install tensorflow
printf "Installing TFX\n"
pip install tfx==0.12.0rc0
# pip install tensorflow-data-validation
# printf "Installing tensorflow-model-analysis\n"
# pip install tensorflow-model-analysis
printf "${YELLOW}Installing Google API Client${NC}\n"
pip install google-api-python-client
# printf "Installing ml-metadata\n"
# pip install ml_metadata
printf "'Fixing' the Jupyter version\n"
pip install --upgrade notebook==5.7.2
jupyter nbextension install --py --symlink --sys-prefix tensorflow_model_analysis
jupyter nbextension enable --py --sys-prefix tensorflow_model_analysis

# Docker images
printf "Installing docker\n"
pip install docker
docker build -t ..

# Airflow
# set this to avoid the GPL version; no functionality difference either way
printf "Configuring airflow\n"
export SLUGIFY_USES_TEXT_UNIDECODE=yes
printf "Installing airflow\n"
pip install apache-airflow
printf "Initializing airflow db (sqllite)\n"
airflow initdb

# Adjust configuration
sed -i'.orig' 's/dag_dir_list_interval = 300/dag_dir_list_interval = 1/g' ~/airflow/airflow.cfg
sed -i'.orig' 's/job_heartbeat_sec = 5/job_heartbeat_sec = 1/g' ~/airflow/airflow.cfg
sed -i'.orig' 's/scheduler_heartbeat_sec = 5/scheduler_heartbeat_sec = 1/g' ~/airflow/airflow.cfg
sed -i'.orig' 's/dag_default_view = tree/dag_default_view = graph/g' ~/airflow/airflow.cfg
sed -i'.orig' 's/load_examples = True/load_examples = False/g' ~/airflow/airflow.cfg

airflow resetdb --yes
airflow initdb

# Copy Dag to ~/airflow/dags
mkdir -p ~/airflow/dags
cp dags/taxi_pipeline.py ~/airflow/dags/
cp dags/taxi_utils.py ~/airflow/dags/
cp dags/taxi_pipeline_solution.py ~/airflow/dags/
cp dags/taxi_utils_solution.py ~/airflow/dags/

# # Copy pipeline code to ~/airflow/plugins
# mkdir -p ~/airflow/plugins
# cp -R plugins/tfx ~/airflow/plugins
# cp -R plugins/tfx_example ~/airflow/plugins
# cp -R plugins/tfx_example_solution ~/airflow/plugins

# Copy data to ~/airflow/data
cp -R data ~/airflow

# Copy data to ~/airflow/data
# mkdir -p ~/airflow/data/tfx_example_solution
# cp -R data/tfx_example/* ~/airflow/data/tfx_example_solution/

printf "\nTFX workshop installed.\n"

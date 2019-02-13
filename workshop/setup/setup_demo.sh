#!/bin/bash
# Set up the environment for the TFX workshop

printf "Installing TFX workshop\n\n"

# TF/TFX prereqs
printf "Installing tensorflow\n"
pip install tensorflow
printf "Installing tensorflow-data-validation\n"
pip install tensorflow-data-validation
printf "Installing tensorflow-model-analysis\n"
pip install tensorflow-model-analysis
printf "Installing ml-metadata\n"
pip install ml_metadata
printf "'Fixing' the jupyter version\n"
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

# Copy Dag to ~/airflow/dags
mkdir -p ~/airflow/dags
cp dags/tfx_example_pipeline.py ~/airflow/dags/

# Copy pipeline code to ~/airflow/plugins
mkdir -p ~/airflow/plugins
cp -r plugins/tfx ~/airflow/plugins

# Copy data to ~/airflow/data
mkdir -p ~/airflow/data/tfx_example
cp -r data/tfx_example/* ~/airflow/data/tfx_example/

printf "\nTFX workshop installed.\n"

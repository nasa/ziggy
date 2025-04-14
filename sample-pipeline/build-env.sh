#! /bin/bash
#
# Shell script that builds the Python environment for the sample pipeline. 
#
# The environment variable PIPELINE_CONFIG_PATH contains the path to
# the pipeline configuration file. This script uses that path to
# derive all the paths it needs.
#
# Author: PT
# Author: Bill Wohler

etc_dir="$(dirname "$PIPELINE_CONFIG_PATH")"
sample_root="$(dirname "$etc_dir")"
sample_home="$sample_root/build"
python_env=$sample_home/env

# Put the build directory next to the env directory in the directory tree.
mkdir -p $python_env

# Create and populate the data receipt directory from the sample data
data_receipt_dir=$sample_home/pipeline-results/data-receipt
mkdir -p $data_receipt_dir
cp -r $sample_root/data/* $data_receipt_dir

# Copy the Python source to the build directory.
mkdir -p $sample_home/src/main
cp -r $sample_root/src/main/python $sample_home/src/main

python3 -m venv $python_env/pipeline

# We're about to activate the environment, so we should make sure that the environment
# gets deactivated at the end of script execution.
trap 'deactivate' EXIT

source $python_env/pipeline/bin/activate

# Build the environment with the needed packages.
pip3 install $sample_home/src/main/python/sample_pipeline $ZIGGY_HOME/src/main/python/ziggy

# Generate version information.
$ZIGGY_HOME/bin/ziggy generate-build-info --home $sample_home

exit 0

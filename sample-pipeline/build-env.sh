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
ziggy_root="$(dirname "$sample_root")"

# Put the build directory next to the env directory in the directory tree.
mkdir -p $python_env

# Create and populate the data receipt directory from the sample data
data_receipt_dir=$sample_home/pipeline-results/data-receipt
mkdir -p $data_receipt_dir
cp -r $sample_root/data/* $data_receipt_dir

# Build the bin directory in build.
bin_dir=$sample_home/bin
mkdir -p $bin_dir
bin_src_dir=$sample_root/src/main/sh

# Copy the shell scripts from src to build.
install -m a+rx  $bin_src_dir/permuter.sh $bin_dir/permuter
install -m a+rx  $bin_src_dir/flip.sh $bin_dir/flip
install -m a+rx  $bin_src_dir/averaging.sh $bin_dir/averaging

python3 -m venv $python_env

# We're about to activate the environment, so we should make sure that the environment
# gets deactivated at the end of script execution.
trap 'deactivate' EXIT

source $python_env/bin/activate

# Build the environment with the needed packages.
pip3 install h5py Pillow numpy

# Get the location of the environment's site packages directory
site_pkgs=$(python3 -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")

# Copy the pipeline major_tom package to the site-packages location.
cp -r $ziggy_root/sample-pipeline/src/main/python/major_tom $site_pkgs

# Copy the Ziggy components to the site-packages location.
cp -r $ziggy_root/src/main/python/hdf5mi $site_pkgs
cp -r $ziggy_root/src/main/python/zigutils $site_pkgs

exit 0

#! /bin/bash
#
# Shell script that tears down the Python environment for the sample pipeline. 
#
# The environment variable PIPELINE_CONFIG_PATH contains the path to
# the pipeline configuration file. This script uses that path to
# derive all the paths it needs.
#
# Author: PT
# Author: Bill Wohler

etc_dir="$(dirname "$PIPELINE_CONFIG_PATH")"
sample_root="$(dirname "$etc_dir")"
sample_home=$sample_root/build

chmod -R u+w $sample_home
rm -rf $sample_home

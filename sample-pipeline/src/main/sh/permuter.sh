#! /bin/bash
#
# Shell script that runs the color-permuting pipeline module. 
#
# The default assumption is that the PIPELINE_CONFIG_PATH environment variable points
# at the etc/sample.properties file in the sample-pipeline directory. In this case, 
# the known location of that file allows the script to find everything else it needs
# relative to that location. 
#
# Alternately, the user may specify two environment variables:
#      SAMPLE_PIPELINE_PYTHON_ENV specifies the location of the Python
#          environment constructed by build-env.sh. 
#      ZIGGY_HOME specifies the top-level Ziggy directory. 
#
# Note that if you point PIPELINE_CONFIG_PATH at a different file (if, for example, 
# you copy the file that ships with Ziggy to a different location so you can edit it
# without running afoul of Git change tracking), you will need to define ZIGGY_HOME
# and SAMPLE_PIPELINE_PYTHON_ENV variables!
#
# Author: PT

# Check for a SAMPLE_PIPELINE_PYTHON_ENV.
if [ -n "$SAMPLE_PIPELINE_PYTHON_ENV" ]; then
    if [ -z "$ZIGGY_HOME" ]; then
        echo "SAMPLE_PIPELINE_PYTHON_ENV set but ZIGGY_HOME not set!"
        exit 1
    fi
else
    etc_dir="$(dirname "$PIPELINE_CONFIG_PATH")"
    sample_home="$(dirname "$etc_dir")"
    ZIGGY_HOME="$(dirname "$sample_home")"
    SAMPLE_PIPELINE_PYTHON_ENV=$sample_home/build/env
fi

# We're about to activate the environment, so we should make sure that the environment
# gets deactivated at the end of script execution.
trap 'deactivate' EXIT

source $SAMPLE_PIPELINE_PYTHON_ENV/bin/activate

# Get the location of the environment's site packages directory.
SITE_PKGS=$(python3 -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")

# Use the environment's Python to run the permuter Python script.
python3 $SITE_PKGS/major_tom/permuter.py

# Capture the Python exit code and pass it to the caller as the script's exit code.
exit $?
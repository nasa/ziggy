#! /bin/bash
#
# Shell script that runs Python algorithms. 
#
# The script looks for three environment variables:
#    ZIGGY_PYTHON_MODULE, which is the package and module name to be used
#    ZIGGY_PYTHON_FUNCTION, which is the function in the module to be used
#    ZIGGY_VIRT_ENV, which is the path to the virtual environment (can be null).
#
# The script activates the Python environment, then constructs a command line that
# is passed to Python. The command line imports the necessary module and runs the
# specified function in the module. 
#
# Author: PT

# Activate the environment, if any.
if [ -z $ZIGGY_VIRT_ENV ]; then
	echo "No virtual environment specified"
else
	if [ ! -d $ZIGGY_VIRT_ENV ]; then
		echo "$ZIGGY_VIRT_ENV is not a directory"
		exit 1
	fi
	
	# Check to see what kind of environment we're talking about.
	if [ -d $ZIGGY_VIRT_ENV/conda-meta ]; then
		echo "Activiating Conda environment $ZIGGY_VIRT_ENV"
		source conda activate $ZIGGY_VIRT_ENV
		trap 'source deactivate' EXIT
	else
		echo "Activating venv environment $ZIGGY_VIRT_ENV"
		source $ZIGGY_VIRT_ENV/bin/activate
		trap 'deactivate' EXIT
	fi
	echo "Environment $ZIGGY_VIRT_ENV activated"
fi

# Construct the Python command line that starts and runs ziggy.py.
python_command="python3 -m ziggytools.ziggy $ZIGGY_PYTHON_MODULE $ZIGGY_PYTHON_FUNCTION"

# Run the Python command.
$python_command

# capture the Python exit code and pass it to the caller as the script's exit code
exit $?
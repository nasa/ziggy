'''
Module that wraps a Python pipeline algorithm in a try-except block so that the correct
return value is produced and, in the event of an exception, a correct stack trace file is
produced. The inputs file is also read into Python and passed as an argument to the algorithm
function.

@author: PT
'''
import importlib
import sys
from ziggytools.hdf5 import Hdf5AlgorithmInterface
from ziggytools.stacktrace import ZiggyErrorWriter
from ziggytools.pidfile import write_pid_file
from ziggytools.fileutils import algorithm_step_name

def run_module(python_function):

    try:

        # Generate the PID file.
        write_pid_file()

        # Read the inputs.
        inputs_file_name = algorithm_step_name() + "-inputs.h5"
        inputs = Hdf5AlgorithmInterface().read_file(inputs_file_name)
        python_function(inputs)

        exit(0)

    except Exception:
        ZiggyErrorWriter()
        exit(1)

if __name__ == "__main__":

    # Capture the module and function information from command line arguments.
    module_name = sys.argv[1]
    function_name = sys.argv[2]

    # Perform import.
    module = importlib.import_module(module_name)
    function = getattr(module, function_name)

    # Execute the function.
    run_module(function)
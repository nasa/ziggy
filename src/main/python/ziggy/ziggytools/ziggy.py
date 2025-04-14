'''
Module that wraps a Python pipeline algorithm in a try-except block so that the correct
return value is produced and, in the event of an exception, a correct stack trace file is
produced. The inputs file is also read into Python and passed as an argument to the algorithm
function.

@author: PT
'''

from .hdf5 import Hdf5AlgorithmInterface
from .stacktrace import ZiggyErrorWriter
from .pidfile import write_pid_file
from .fileutils import algorithm_step_name

def run_module(python_function_name):

    try:

        # Generate the PID file.
        write_pid_file()

        # Read the inputs.
        inputs_file_name = algorithm_step_name() + "-inputs.h5"
        inputs = Hdf5AlgorithmInterface().read_file(inputs_file_name)
        python_function_name(inputs)

        exit(0)

    except Exception:
        ZiggyErrorWriter()
        exit(1)

'''
Wraps the permute_color Python function in the Ziggy elements needed for things like input/output
management and exception handling. 

@author: PT
'''

from ziggytools.stacktrace import ZiggyErrorWriter
from ziggytools.hdf5 import Hdf5ModuleInterface
from ziggytools.pidfile import write_pid_file
from sys import exit
from major_tom import permute_color
import os
import time

# Define the HDF5 read/write class as a global variable.
hdf5_module_interface = Hdf5ModuleInterface()

if __name__ == '__main__':
    try:

        # Generate the PID file.
        write_pid_file()
        
        # Read inputs: note that the inputs contain the names of all the files
        # that are to be used in this process, as well as model names and 
        # parameters. All files are in the working directory. 
        inputs = hdf5_module_interface.read_file("permuter-inputs.h5")
        data_file = inputs['dataFilenames']
        parameters = inputs['moduleParameters']['Algorithm_Parameters']
        models = inputs['modelFilenames']
        
        # Handle the parameter values that can cause an error or cause 
        # execution to complete without generating output.
        dir_name = os.path.basename(os.getcwd())
        if dir_name == "st-0":
            throw_exception = parameters['throw_exception_subtask_0']
        else:
            throw_exception = False
            
        if dir_name == "st-1":
            produce_output = parameters['produce_output_subtask_1']
        else:
            produce_output = True
            
        # Run the color permuter. The permute_color function will produce
        # the output with the correct filename.
        permute_color(data_file, throw_exception, produce_output)
        
        # Sleep for a user-specified interval. This is here just so the
        # user can watch execution run on the pipeline console.
        time.sleep(parameters['execution_pause_seconds'])
        exit(0)

    except Exception:
        ZiggyErrorWriter()
        exit(1)


'''
Wraps the left_right_flip and up_down_flip Python function in the Ziggy elements needed for things like 
input/output management and exception handling. 

@author: PT
'''
from zigutils.stacktrace import ZiggyErrorWriter
from hdf5mi.hdf5 import Hdf5ModuleInterface
from zigutils.pidfile import write_pid_file
from sys import exit

from major_tom import left_right_flip, up_down_flip
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
        inputs = hdf5_module_interface.read_file("flip-inputs.h5")
        data_file = inputs.dataFilenames
        parameters = inputs.moduleParameters.Algorithm_Parameters
        models = inputs.modelFilenames

        # Run the two flippers and allow them to save their outputs
        print("Start with left-right flip")
        left_right_flip(data_file)
        print("Moving on to up-down flip")
        up_down_flip(data_file)
        
        # Sleep for a user-specified interval. This is here just so the
        # user can watch execution run on the pipeline console.
        time.sleep(parameters.execution_pause_seconds)
        print("Flip pipeline module completed")
        exit(0)

    except Exception:
        ZiggyErrorWriter()
        exit(1)

'''
Wraps the image-averaging Python function in the Ziggy elements needed for things like 
input/output management and exception handling. 

@author: PT
'''
from ziggytools.stacktrace import ZiggyErrorWriter
from ziggytools.hdf5 import Hdf5ModuleInterface
from ziggytools.pidfile import write_pid_file
from sys import exit

from major_tom import average_images
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
        inputs = hdf5_module_interface.read_file("averaging-inputs.h5")
        data_files = inputs['dataFilenames']
        parameters = inputs['moduleParameters']['Algorithm_Parameters']
        models = inputs['modelFilenames']

        # Run the averaging function.
        average_images(data_files)
                
        # Sleep for a user-specified interval. This is here just so the
        # user can watch execution run on the pipeline console.
        time.sleep(parameters['execution_pause_seconds'])
        print("Flip pipeline module completed")
        exit(0)

    except Exception:
        ZiggyErrorWriter()
        exit(1)

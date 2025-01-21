'''
Generates information in the format expected by Ziggy when an exception occurs
in Python. See Java classes MatlabErrorReturn and MatlabStack in gov.nasa.ziggy.module.io.matlab
for more details. 
Created on Nov 19, 2020

@author: PT
'''
from sys import exc_info, stdout
from inspect import getmodule
from os import getcwd, sep
from os.path import normpath
if __package__ is None or __package__ == '':
    from hdf5 import Hdf5ModuleInterface
else:
    from .hdf5 import Hdf5ModuleInterface
from traceback import print_exception

class ZiggyErrorReturn():
    
    def __init__(self):
        
        # capture the contents of the exception and its stack trace
        exception_type, exception_value, exception_traceback = exc_info()
                
        # populate the message and identifier
        self.message = exception_value.args[0]
        if self.message is None or not self.message:
            self.message = str(exception_value)
        self.identifier = exception_type.__name__
        
        # generate the stack trace
        self.stack=[]
        current_traceback = exception_traceback
        while current_traceback is not None:
            self.stack.append(ZiggyStack(current_traceback))
            current_traceback = current_traceback.tb_next
        self.exception_type = exception_type
        self.exception_value = exception_value
        self.exception_traceback = exception_traceback
        
'''
Captures one step in a stack trace in the format expected by Ziggy when an exception occurs.
See Java class MatlabStack in gov.nasa.ziggy.module.io.matlab for more details.

@author: PT
'''
class ZiggyStack:
    
    def __init__(self, traceback):
        frame = traceback.tb_frame
        self.file = getmodule(frame).__name__
        self.name = frame.f_code.co_name
        self.line = traceback.tb_lineno
        
'''
Writes a Ziggy-formatted HDF5 file to the subtask directory. The user supplies an optional
"sequence number," which defaults to zero. The result is a file named "<csci-name>-error-<seq-num>.h5"
The worker later reads this and adds its contents to the log for the subtask.
The standard pattern for writing a pipeline algorithm call in Python is the following:

    try:
        algorithm_call(args)
    except:
        ZiggyErrorWriter()
        
When multiple inputs per subtask are used, the sequence number of the current input should be
supplied as an argument to the ZiggyErrorWriter constructor. 

@author PT
'''
class ZiggyErrorWriter:
    
    def __init__(self, seq_num=0):
        
        # get the module name
        working_dir = normpath(getcwd())
        dir_parts = working_dir.split(sep)
        num_parts = len(dir_parts)
        task_dir = dir_parts[num_parts-2]
        task_dir_tokens = task_dir.split("-")
        module_name = task_dir_tokens[2]
        
        if (not isinstance(seq_num, int)):
            seq_num = 0;
        
        # construct the error file name
        error_file_name = module_name + "-error.h5"
        
        # get the information for the error file
        error_return = ZiggyErrorReturn()

        # write a stack trace to stdout, which will get put into the log
        (print_exception(error_return.exception_type, 
                         error_return.exception_value, 
                         error_return.exception_traceback, 
                         None, stdout))
        
        # Remove fields from the error_return that must not be present
        # when attempting to write to HDF5
        del(error_return.exception_type)
        del(error_return.exception_value)
        del(error_return.exception_traceback)
                 
        # write to HDF5 file
        hdf5_module_interface = Hdf5ModuleInterface()
        hdf5_module_interface.write_file(error_file_name, error_return)
        

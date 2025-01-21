'''
Tests that the ZiggyErrorReturn class produces a correctly formatted set of information on any
Python failures so that the information can be returned to the pipeline worker. 
Created on Nov 19, 2020

@author: PT
'''
import unittest
from ..stacktrace import ZiggyErrorReturn, ZiggyErrorWriter
from .throwexception import ExceptionGenerator
from ..hdf5 import Hdf5ModuleInterface
from tempfile import TemporaryDirectory
from os import makedirs, chdir
from os.path import isfile

class StackTraceTest(unittest.TestCase):
    
    # Tests the ZiggyErrorReturn class.
    def test_generate_stack_trace(self):
        try:
            
            # Cause an exception to be thrown
            ExceptionGenerator()
            self.assertTrue(False,"No exception thrown by ExceptionGenerator")
        except:
            error_return = ZiggyErrorReturn()
            
            # check the contents of the ZiggyErrorReturn object
            self.check_error(error_return, "test_generate_stack_trace", 23)

    # tests the ZiggyErrorWriter use-case in which the user specifies a sequence number
    def test_write_error_file(self):
        
        temp_dir = TemporaryDirectory()
        sub_task_dir = temp_dir.name + "/50-100-cal/st-0"
        makedirs(sub_task_dir)
        chdir(sub_task_dir)
        try:
            ExceptionGenerator()
            self.assertTrue(False, "No exception thrown by ExceptionGenerator")
        except:
            ZiggyErrorWriter(5)
            error_file = "cal-error.h5"
            self.assertTrue(isfile(error_file))
            hdf5_module_interface = Hdf5ModuleInterface()
            hdf5_module_interface.set_reconstitute_struct_array(True)
            error_file_contents = hdf5_module_interface.read_file(error_file)
            
            # check the contents of the error file
            self.check_error(error_file_contents, "test_write_error_file", 39)           

    # tests the ZiggyErrorWriter use-case in which the user does not specify a sequence number
    def test_write_error_file_implicit_seqnum(self):
        
        temp_dir = TemporaryDirectory()
        sub_task_dir = temp_dir.name + "/50-100-cal/st-0"
        makedirs(sub_task_dir)
        chdir(sub_task_dir)
        try:
            ExceptionGenerator()
            self.assertTrue(False, "No exception thrown by ExceptionGenerator")
        except:
            ZiggyErrorWriter()
            error_file = "cal-error.h5"
            self.assertTrue(isfile(error_file))
            hdf5_module_interface = Hdf5ModuleInterface()
            hdf5_module_interface.set_reconstitute_struct_array(True)
            error_file_contents = hdf5_module_interface.read_file(error_file)
            
            # check the contents of the error file
            self.check_error(error_file_contents, "test_write_error_file_implicit_seqnum", 60)           
        
    # Performs checks of the contents of a given error objecgt
    def check_error(self, error_contents, stack_name0, stack_lineno0):   

        if hasattr(error_contents, '__dict__'):
            error_contents = error_contents.__dict__

        # check the top-level contents of the error
        self.assertTrue('message' in error_contents)
        self.assertTrue('identifier' in error_contents)
        self.assertTrue('stack' in error_contents)

        self.assertEqual("division by zero", error_contents['message'])
        self.assertEqual("ZeroDivisionError", error_contents['identifier'])
        stack = error_contents['stack']
        self.assertEqual(4, len(stack))
        
        # the first entry in the stack varies depending on caller
        stack0 = stack[0]
        if hasattr(stack0, '__dict__'):
            stack0 = stack0.__dict__
        self.assertEqual("ziggy.ziggytools.tests.stacktracetests", stack0['file'])
        self.assertEqual(stack_name0, stack0['name'])
        self.assertEqual(stack_lineno0, stack0['line'])
            
        # the remaining entries are the same for all callers
        stack1 = stack[1]
        if hasattr(stack1, '__dict__'):
            stack1 = stack1.__dict__
        self.assertEqual("ziggy.ziggytools.tests.throwexception", stack1['file'])
        self.assertEqual("__init__", stack1['name'])
        self.assertEqual(13, stack1['line'])
            
        stack2 = stack[2]
        if hasattr(stack2, '__dict__'):
            stack2 = stack2.__dict__
        self.assertEqual("ziggy.ziggytools.tests.throwexception", stack2['file'])
        self.assertEqual("call1", stack2['name'])
        self.assertEqual(16, stack2['line'])
           
        stack3 = stack[3]
        if hasattr(stack3, '__dict__'):
            stack3 = stack3.__dict__
        self.assertEqual("ziggy.ziggytools.tests.throwexception", stack3['file'])
        self.assertEqual("call2", stack3['name'])
        self.assertEqual(19, stack3['line'])
            
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
    
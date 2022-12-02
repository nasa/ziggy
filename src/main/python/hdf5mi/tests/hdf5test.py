'''
hdf5moduleinterface_test : unit tests for the Hdf5ModuleInterface class.  
Created on Oct 5, 2020

@author: PT
'''
import unittest
import tempfile
import os
import numpy
from hdf5 import Hdf5ModuleInterface, StructTemplate

class Hdf5ModuleInterfaceTest(unittest.TestCase):

    def setUp(self):
        self._temporary_directory = tempfile.TemporaryDirectory();
        self.h = Hdf5ModuleInterface()
        self.hdf5_file_name = os.path.join(self._temporary_directory.name, "test-file.h5")
        pass


    def tearDown(self):
        pass

    # Tests the ability to read and write a simple object. A simple object in this
    # context means one that has just a couple of scalar numeric fields. 
    def test_write_and_read_simple_object(self):
        test_object = Hdf5SimpleTestObject()
        self.h.write_file(self.hdf5_file_name, test_object)
        obj_from_file = self.h.read_file(self.hdf5_file_name)
        self.assertTrue(hasattr(obj_from_file, "real_scalar"))
        self.assertEqual(-105.3, obj_from_file.real_scalar)
        self.assertTrue(hasattr(obj_from_file, "int_scalar"))
        self.assertEqual(50, obj_from_file.int_scalar)
        
    # Tests the ability to write and read "parallelizable" arrays, that is to say, an
    # array of objects or dictionaries in which each entry in each object/dictionary
    # is a scalar value. These are stored as parallel arrays that are generated from
    # the scalar values. In this test, the original array of structs is not rebuilt,
    # and the retrieved object is a struct of arrays.
    def test_read_and_write_parallelizable_array_no_reconstitute(self):
        test_object = Hdf5ObjectWithParallelizableArray()
        self.h.write_file(self.hdf5_file_name, test_object)
        obj_from_file = self.h.read_file(self.hdf5_file_name)
        self.assertTrue(hasattr(obj_from_file, "parallelizable_array"))
        parallelizable_array = obj_from_file.parallelizable_array
        self.assertTrue(isinstance(parallelizable_array, StructTemplate))
        self.assertTrue(hasattr(parallelizable_array, "a") \
                        and isinstance(parallelizable_array.a, numpy.ndarray))
        self.assertEqual((2,2), parallelizable_array.a.shape)
        self.assertTrue(hasattr(parallelizable_array, "b") \
                        and isinstance(parallelizable_array.b, numpy.ndarray))
        self.assertEqual((2,2), parallelizable_array.b.shape)
        self.assertTrue(hasattr(parallelizable_array, "c") \
                        and isinstance(parallelizable_array.c, numpy.ndarray))
        self.assertEqual((2,2), parallelizable_array.c.shape)
        self.assertTrue(hasattr(parallelizable_array, "d") \
                        and isinstance(parallelizable_array.d, numpy.ndarray))
        self.assertEqual((2,2), parallelizable_array.d.shape)
        for i in (0,1):
            for j in (0,1):
                k = 4 * j + 8 * i
                self.assertEqual(k+1, parallelizable_array.a[i][j])
                self.assertEqual(k+2, parallelizable_array.b[i][j])
                self.assertEqual(k+3, parallelizable_array.c[i][j])
                self.assertEqual(k+4, parallelizable_array.d[i][j])

    # Tests the ability to read and write parallelizable struct arrays and to
    # reconstitute the struct array at the end. 
    def test_read_and_write_parallelizable_array_reconstitute(self):
        test_object = Hdf5ObjectWithParallelizableArray()
        self.h.write_file(self.hdf5_file_name, test_object)
        self.h.set_reconstitute_struct_array(True)
        obj_from_file = self.h.read_file(self.hdf5_file_name)
        parallelizable_array = obj_from_file.parallelizable_array
        self.assertTrue(isinstance(parallelizable_array, numpy.ndarray))
        self.assertEquals((2,2), parallelizable_array.shape)
        for i in (0,1):
            for j in (0,1):
                k = 4 * j + 8 * i
                array_element = parallelizable_array[i][j]
                self.assertTrue(isinstance(array_element, StructTemplate))
                self.assertTrue(hasattr(array_element, "a"))
                self.assertEquals(1+k, array_element.a)
                self.assertTrue(hasattr(array_element, "b"))
                self.assertEquals(2+k, array_element.b)
                self.assertTrue(hasattr(array_element, "c"))
                self.assertEquals(3+k, array_element.c)
                self.assertTrue(hasattr(array_element, "d"))
                self.assertEquals(4+k, array_element.d)
                
    def test_read_and_write_parallelizable_string_array(self):
        test_object = Hdf5WithParallelizableStringArray()
        self.h.write_file(self.hdf5_file_name, test_object)
        obj_from_file = self.h.read_file(self.hdf5_file_name)
        parallelizable_array = obj_from_file.parallelizable_array
        self.assertTrue(isinstance(parallelizable_array, StructTemplate))
        self.assertTrue(hasattr(parallelizable_array, "a") \
                        and isinstance(parallelizable_array.a, numpy.ndarray))
        self.assertEqual((2,2), parallelizable_array.a.shape)
        self.assertEqual("the", parallelizable_array.a[0,0])
        self.assertEqual("calling", parallelizable_array.a[0,1])
        self.assertEqual("back", parallelizable_array.a[1,0])
        self.assertEqual("in", parallelizable_array.a[1,1])
        self.assertTrue(hasattr(parallelizable_array, "b") \
                        and isinstance(parallelizable_array.b, numpy.ndarray))
        self.assertEqual((2,2), parallelizable_array.b.shape)
        self.assertEqual("wild", parallelizable_array.b[0,0])
        self.assertEqual("on", parallelizable_array.b[0,1])
        self.assertEqual("from", parallelizable_array.b[1,0])
        self.assertEqual("august", parallelizable_array.b[1,1])
        self.assertTrue(hasattr(parallelizable_array, "c") \
                        and isinstance(parallelizable_array.c, numpy.ndarray))
        self.assertEqual((2,2), parallelizable_array.c.shape)
        self.assertEqual("boys", parallelizable_array.c[0,0])
        self.assertEqual("their", parallelizable_array.c[0,1])
        self.assertEqual("the", parallelizable_array.c[1,0])
        self.assertEqual("moon", parallelizable_array.c[1,1])
        self.assertTrue(hasattr(parallelizable_array, "d") \
                        and isinstance(parallelizable_array.d, numpy.ndarray))
        self.assertEqual((2,2), parallelizable_array.d.shape)
        self.assertEqual("were", parallelizable_array.d[0,0])
        self.assertEqual("way", parallelizable_array.d[0,1])
        self.assertEqual("fire", parallelizable_array.d[1,0])
        self.assertEqual("surrender", parallelizable_array.d[1,1])
        

    # tests the ability to read and write structs that cannot be "parallelized."
    # In the process it also exercises the ability to read and write structs with
    # sub-structs
    def test_read_and_write_non_parallelizable_array(self):
        test_object = Hdf5ObjectWithNonParallelizableArray()
        self.h.write_file(self.hdf5_file_name, test_object)
        obj_from_file = self.h.read_file(self.hdf5_file_name)
        non_parallelizable_array = obj_from_file.non_parallelizable_array
        self.assertTrue(isinstance(non_parallelizable_array, numpy.ndarray))
        self.assertEquals((2,2), non_parallelizable_array.shape)
        for i in (0,1):
            for j in (0,1):
                k = 4 * j + 8 * i
                array_element = non_parallelizable_array[i][j]
                self.assertTrue(isinstance(array_element, StructTemplate))
                self.assertTrue(hasattr(array_element, "a"))
                self.assertEquals(1+k, array_element.a)
                self.assertTrue(hasattr(array_element, "b"))
                self.assertEquals(2+k, array_element.b)
                self.assertTrue(hasattr(array_element, "c"))
                self.assertEquals(3+k, array_element.c)
                self.assertTrue(hasattr(array_element, "d"))
                self.assertTrue(numpy.array_equal(numpy.array([4+k, 4+k]), array_element.d))
        
    # Tests the ability to read and write boolean values, which need to be stored.
    # Note that while the values to be written may be Python bools (which are a subclass
    # of int), they are read as numpy bools (which are totally different).
    def test_read_and_write_booleans(self):   
        test_object = Hdf5BooleanTestObject()
        self.h.write_file(self.hdf5_file_name, test_object)
        obj_from_file = self.h.read_file(self.hdf5_file_name)
        self.assertTrue(hasattr(obj_from_file, "bool_scalar"))
        self.assertTrue(hasattr(obj_from_file, "bool_list"))
        self.assertTrue(hasattr(obj_from_file, "bool_array"))
        self.assertEqual(numpy.bool_, type(obj_from_file.bool_scalar))
        self.assertEqual(numpy.True_, obj_from_file.bool_scalar)
        self.assertTrue(numpy.array_equal(
            numpy.array([numpy.True_, numpy.False_, numpy.True_]), 
            obj_from_file.bool_list))
        self.assertTrue(numpy.array_equal(
            numpy.array([[numpy.False_, numpy.False_], [numpy.True_, numpy.True_]]),
            obj_from_file.bool_array))
    
    # Tests the ability to read and write strings and arrays of strings 
    def test_read_and_write_strings(self):
        test_object = Hdf5StringTestObject()
        self.h.write_file(self.hdf5_file_name, test_object)
        obj_from_file = self.h.read_file(self.hdf5_file_name)
        self.assertTrue(hasattr(obj_from_file, "scalar_string"))
        self.assertTrue(hasattr(obj_from_file, "string_array"))
        self.assertEqual("String value!", obj_from_file.scalar_string)
        self.assertTrue(numpy.array_equal(test_object.string_array, 
                obj_from_file.string_array))
    
    # Tests the ability to read a single group from an HDF5 file
    def test_read_single_group(self):
        test_object = Hdf5SimpleTestObject()
        self.h.write_file(self.hdf5_file_name, test_object)
        obj_from_file = self.h.read_file(self.hdf5_file_name, "real_scalar")
        self.assertEqual(-105.3, obj_from_file)
        
    # Tests the field ordering options -- either the order in which the fields were
    # written to the file, or alphabetical order
    def test_field_ordering(self):
        test_object = Hdf5SimpleTestObject()
        self.h.write_file(self.hdf5_file_name, test_object)
        obj_from_file = self.h.read_file(self.hdf5_file_name)
        prop_list = list(obj_from_file.__dict__.keys())
        self.assertEqual("real_scalar", prop_list[0])
        self.h.set_preserve_field_order(False)
        obj_from_file = self.h.read_file(self.hdf5_file_name)
        prop_list = list(obj_from_file.__dict__.keys())
        self.assertEqual("int_scalar", prop_list[0])
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
    
# end of Hdf5ModuleInterfaceTest class
    
'''
Object that can be written to disk as an HDF5 file and read back
'''
class Hdf5SimpleTestObject:
    
    def __init__(self):
        
        # numeric scalars
        self.real_scalar = -105.3
        self.int_scalar = 50
                
# end of Hdf5TestObject class

'''
Object that contains boolean (which have to be written as bytes and then converted back
to boolean on retrieval).
'''
class Hdf5BooleanTestObject:
    def __init__(self):
        self.bool_scalar = True
        self.bool_list = [True, False, True]
        self.bool_array = numpy.array([[False, False], [True, True]])
# end of Hdf5BooleanTestObject class

'''
Scalar object that can be an attribute of the Hdf5TestObject. Also contains test cases for scalar
string, multi-dimensional array of strings, and parallelizable object arrays
'''
class Hdf5StringTestObject:
    
    def __init__(self):
        
        # string
        self.scalar_string = "String value!"
        
        # string array
        self.string_array = numpy.array([["Alice", "in", "her", "party", "dress"], ["she", "thanks", "you", "kindly", "so"]])
                
# end of Hdf5ScalarObject class

'''
Scalar object that contains an array of structs that can be parallelized.
'''
class Hdf5ObjectWithParallelizableArray:
    
    def __init__(self):
        # multi-dimensional array of parallelizable objects
        self.parallelizable_array = numpy.array(
            [[Hdf5ParallelizableObject(1, 2, 3, 4), Hdf5ParallelizableObject(5, 6, 7, 8)], 
             [Hdf5ParallelizableObject(9, 10, 11, 12), Hdf5ParallelizableObject(13, 14, 15, 16)]]
            )
 
# end of Hdf5ObjectWithParallelizableArray class

class Hdf5WithParallelizableStringArray:
    
    def __init__(self):
        self.parallelizable_array = numpy.array(
            [[Hdf5ParallelizableObject("the", "wild", "boys", "were"), 
              Hdf5ParallelizableObject("calling", "on", "their", "way")], 
             [Hdf5ParallelizableObject("back", "from", "the", "fire"), 
              Hdf5ParallelizableObject("in", "august", "moon", "surrender")]]
            )
'''
Scalar object that contains an array of structs that cannot be parallelized
'''       
class Hdf5ObjectWithNonParallelizableArray:
    def __init__(self):
        self.non_parallelizable_array = numpy.array(
            [[Hdf5ParallelizableObject(1, 2, 3, [4, 4]), Hdf5ParallelizableObject(5, 6, 7, [8, 8])], 
             [Hdf5ParallelizableObject(9, 10, 11, [12, 12]), Hdf5ParallelizableObject(13, 14, 15, [16, 16])]]
            )
# end of Hdf5ObjectWithNonParallelizableArray
        
class Hdf5ParallelizableObject:
    
    def __init__(self, a, b, c, d):
        
        self.a = a
        self.b = b
        self.c = c
        self.d = d 
        
# end of Hdf5ParallelizableObject class
        
        
        
        
        
        
        
        
'''
hdf5moduleinterface_test : unit tests for the Hdf5ModuleInterface class.  
Created on Oct 5, 2020

@author: PT
'''
import unittest
import tempfile
import os
import numpy as np
from ..hdf5 import Hdf5ModuleInterface

class Hdf5ModuleInterfaceTest(unittest.TestCase):

    def setUp(self):
        self._temporary_directory = tempfile.TemporaryDirectory()
        self.h = Hdf5ModuleInterface()
        self.hdf5_file_name = os.path.join(self._temporary_directory.name, "test-file.h5")
        pass


    def tearDown(self):
        pass

    # Tests the ability to read and write a simple dictionary.
    # A simple dictionary in this context means one that has just a
    # couple of scalar numeric fields.
    def test_write_and_read_simple_dict(self):
        test_object = _hdf5_simple_test_dict()
        self.h.write_file(self.hdf5_file_name, test_object)
        dict_from_file = self.h.read_file(self.hdf5_file_name)
        self.assertTrue("real_scalar" in dict_from_file)
        self.assertEqual(-105.3, dict_from_file['real_scalar'])
        self.assertTrue("int_scalar" in dict_from_file)
        self.assertEqual(50, dict_from_file['int_scalar'])

    def test_read_and_write_1d_dict_array(self):
        test_object = dict()
        inner_dictionary = dict()
        inner_dictionary['field1'] = _hdf5_numeric_array()
        inner_dictionary['field2'] = _hdf5_numeric_array()
        test_object['dictionary'] = (inner_dictionary, inner_dictionary)
        self.h.write_file(self.hdf5_file_name, test_object)
        dict_from_file = self.h.read_file(self.hdf5_file_name)
        self.assertTrue('dictionary' in dict_from_file)
        self.assertTrue(isinstance(dict_from_file['dictionary'], tuple))
        dictionary_array = dict_from_file['dictionary']
        self.assertEqual(2, len(dictionary_array))
        for inner_dictionary in dictionary_array:
            self.assertTrue(isinstance(inner_dictionary, dict))
            self.assertTrue('field1' in inner_dictionary)
            self.assertEqual((1, 2, 3, 4, 5), inner_dictionary['field1'])
            self.assertTrue('field2' in inner_dictionary)
            self.assertEqual((1, 2, 3, 4, 5), inner_dictionary['field2'])

    # Tests the ability to write and read "parallelizable" arrays, that is to say, an
    # array of dictionaries in which each entry in each dictionary
    # is a scalar value. These are stored as parallel arrays that are generated from
    # the scalar values. In this test, the original array of structs is not rebuilt,
    # and the retrieved object is a dictionary of arrays.
    def test_read_and_write_parallelizable_array_no_reconstitute(self):
        test_dict = {"parallelizable_array":_hdf5_parallelizable_array()}
        self.h.write_file(self.hdf5_file_name, test_dict)
        dict_from_file = self.h.read_file(self.hdf5_file_name)
        self.assertTrue("parallelizable_array" in dict_from_file)
        parallelizable_array = dict_from_file['parallelizable_array']
        self.assertTrue(isinstance(parallelizable_array, dict))
        self.assertTrue("a" in parallelizable_array \
                        and isinstance(parallelizable_array['a'], tuple))
        self.assertTrue("b" in parallelizable_array \
                        and isinstance(parallelizable_array['b'], tuple))
        self.assertTrue("c" in parallelizable_array \
                        and isinstance(parallelizable_array['c'], tuple))
        self.assertTrue("d" in parallelizable_array \
                        and isinstance(parallelizable_array['d'], tuple))
        for i in (0,1):
            for j in (0,1):
                k = 4 * j + 8 * i
                self.assertEqual(k+1, parallelizable_array['a'][i][j])
                self.assertEqual(k+2, parallelizable_array['b'][i][j])
                self.assertEqual(k+3, parallelizable_array['c'][i][j])
                self.assertEqual(k+4, parallelizable_array['d'][i][j])

    # Tests the ability to read and write parallelizable struct arrays and to
    # reconstitute the struct array at the end. 
    def test_read_and_write_parallelizable_array_reconstitute(self):
        test_dict = {"parallelizable_array":_hdf5_parallelizable_array()}
        self.h.write_file(self.hdf5_file_name, test_dict)
        self.h.set_reconstitute_struct_array(True)
        dict_from_file = self.h.read_file(self.hdf5_file_name)
        parallelizable_array = dict_from_file['parallelizable_array']
        self.assertTrue(isinstance(parallelizable_array, tuple))
        for i in (0,1):
            for j in (0,1):
                k = 4 * j + 8 * i
                array_element = parallelizable_array[i][j]
                self.assertTrue(isinstance(array_element, dict))
                self.assertTrue("a" in array_element)
                self.assertEqual(1+k, array_element['a'])
                self.assertTrue("b" in array_element)
                self.assertEqual(2+k, array_element['b'])
                self.assertTrue("c" in array_element)
                self.assertEqual(3+k, array_element['c'])
                self.assertTrue("d" in array_element)
                self.assertEqual(4+k, array_element['d'])
                
    def test_read_and_write_parallelizable_string_array(self):
        test_dict = {"parallelizable_array":_hdf5_parallelizable_string_array()}
        self.h.write_file(self.hdf5_file_name, test_dict)
        dict_from_file = self.h.read_file(self.hdf5_file_name)
        parallelizable_array = dict_from_file['parallelizable_array']
        self.assertTrue(isinstance(parallelizable_array, dict))
        self.assertTrue("a" in parallelizable_array \
                        and isinstance(parallelizable_array['a'], tuple))
        self.assertEqual("the", parallelizable_array['a'][0][0])
        self.assertEqual("calling", parallelizable_array['a'][0][1])
        self.assertEqual("back", parallelizable_array['a'][1][0])
        self.assertEqual("in", parallelizable_array['a'][1][1])
        self.assertTrue("b" in parallelizable_array \
                        and isinstance(parallelizable_array['b'], tuple))
        self.assertEqual("wild", parallelizable_array['b'][0][0])
        self.assertEqual("on", parallelizable_array['b'][0][1])
        self.assertEqual("from", parallelizable_array['b'][1][0])
        self.assertEqual("august", parallelizable_array['b'][1][1])
        self.assertTrue("c" in parallelizable_array \
                        and isinstance(parallelizable_array['c'], tuple))
        self.assertEqual("boys", parallelizable_array['c'][0][0])
        self.assertEqual("their", parallelizable_array['c'][0][1])
        self.assertEqual("the", parallelizable_array['c'][1][0])
        self.assertEqual("moon", parallelizable_array['c'][1][1])
        self.assertTrue("d" in parallelizable_array \
                        and isinstance(parallelizable_array['d'], tuple))
        self.assertEqual("were", parallelizable_array['d'][0][0])
        self.assertEqual("way", parallelizable_array['d'][0][1])
        self.assertEqual("fire", parallelizable_array['d'][1][0])
        self.assertEqual("surrender", parallelizable_array['d'][1][1])
        

    # tests the ability to read and write structs that cannot be "parallelized."
    # In the process it also exercises the ability to read and write structs with
    # sub-structs
    def test_read_and_write_non_parallelizable_array(self):
        test_dict = {"non_parallelizable_array": _hdf5_non_parallelizable_array()}
        self.h.write_file(self.hdf5_file_name, test_dict)
        dict_from_file = self.h.read_file(self.hdf5_file_name)
        non_parallelizable_array = dict_from_file['non_parallelizable_array']
        self.assertTrue(isinstance(non_parallelizable_array, tuple))
        for i in (0,1):
            for j in (0,1):
                k = 4 * j + 8 * i
                array_element = non_parallelizable_array[i][j]
                self.assertTrue(isinstance(array_element, dict))
                self.assertTrue("a" in array_element)
                self.assertEqual(1+k, array_element['a'])
                self.assertTrue("b" in array_element)
                self.assertEqual(2+k, array_element['b'])
                self.assertTrue("c" in array_element)
                self.assertEqual(3+k, array_element['c'])
                self.assertTrue("d" in array_element)
                self.assertEqual(4+k, array_element['d'][0])
                self.assertEqual(4+k, array_element['d'][1])

    # Tests the ability to read and write boolean values, which need to be stored.
    # Note that while the values to be written may be Python bools (which are a subclass
    # of int), they are read as numpy bools (which are totally different).
    def test_read_and_write_booleans(self):
        test_dict = _hdf5_boolean_test_dict()
        self.h.write_file(self.hdf5_file_name, test_dict)
        dict_from_file = self.h.read_file(self.hdf5_file_name)
        self.assertTrue("bool_scalar" in dict_from_file)
        self.assertTrue("bool_list" in dict_from_file)
        self.assertTrue(isinstance(dict_from_file["bool_list"], tuple))
        self.assertTrue("bool_array" in dict_from_file)
        self.assertTrue(isinstance(dict_from_file["bool_array"], tuple))
        self.assertTrue(dict_from_file['bool_scalar'])
        self.assertTrue(dict_from_file['bool_list'][0])
        self.assertFalse(dict_from_file['bool_list'][1])
        self.assertTrue(dict_from_file['bool_list'][2])
        self.assertFalse(dict_from_file['bool_array'][0][0])
        self.assertFalse(dict_from_file['bool_array'][0][1])
        self.assertTrue(dict_from_file['bool_array'][1][0])
        self.assertTrue(dict_from_file['bool_array'][1][1])

    # Tests the ability to read and write strings and arrays of strings 
    def test_read_and_write_strings(self):
        test_dict = _hdf5_string_test_dict()
        self.h.write_file(self.hdf5_file_name, test_dict)
        dict_from_file = self.h.read_file(self.hdf5_file_name)
        self.assertTrue("scalar_string" in dict_from_file)
        self.assertTrue("string_array" in dict_from_file)
        self.assertEqual("String value!", dict_from_file['scalar_string'])
        self.assertEqual("Alice", dict_from_file['string_array'][0][0])
        self.assertEqual("in", dict_from_file['string_array'][0][1])
        self.assertEqual("her", dict_from_file['string_array'][0][2])
        self.assertEqual("party", dict_from_file['string_array'][0][3])
        self.assertEqual("dress", dict_from_file['string_array'][0][4])
        self.assertEqual("she", dict_from_file['string_array'][1][0])
        self.assertEqual("thanks", dict_from_file['string_array'][1][1])
        self.assertEqual("you", dict_from_file['string_array'][1][2])
        self.assertEqual("kindly", dict_from_file['string_array'][1][3])
        self.assertEqual("so", dict_from_file['string_array'][1][4])

    # Tests the ability to read a single group from an HDF5 file
    def test_read_single_group(self):
        test_dict = _hdf5_simple_test_dict()
        self.h.write_file(self.hdf5_file_name, test_dict)
        group_from_file = self.h.read_file(self.hdf5_file_name, "real_scalar")
        self.assertEqual(-105.3, group_from_file)
        
    # Tests the field ordering options -- either the order in which the fields were
    # written to the file, or alphabetical order
    def test_field_ordering(self):
        test_dict = _hdf5_simple_test_dict()
        self.h.write_file(self.hdf5_file_name, test_dict)
        dict_from_file = self.h.read_file(self.hdf5_file_name)
        self.assertEqual("real_scalar", list(dict_from_file)[0])
        self.h.set_preserve_field_order(False)
        dict_from_file = self.h.read_file(self.hdf5_file_name)
        self.assertEqual("int_scalar", list(dict_from_file)[0])
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
    
# end of Hdf5ModuleInterfaceTest class

'''
Returns a simple dictionary with 2 entries, each of which is a 
numeric scalar. 
'''
def _hdf5_simple_test_dict():
    return {"real_scalar": -105.3, "int_scalar": 50}

'''
Returns a dictionary in which all entries are combinations of boolean values. 
'''
def _hdf5_boolean_test_dict():
    return {"bool_scalar": True, "bool_list": (True, False, True),
            "bool_array": ((False, False), (True, True))}

'''
Returns a dictionary in which all entries are combinations of string values. 
'''
def _hdf5_string_test_dict():
    return {"scalar_string": "String value!",
            "string_array": (("Alice", "in", "her", "party", "dress"),
                             ("she", "thanks", "you", "kindly", "so"))}

'''
Returns a dictionary that can be parallelized (i.e., all dictionaries returned from this function
have the same keys). 
'''
def _hdf5_parallelizable_dict(a, b, c, d):
    return {"a": a, "b":b, "c":c, "d": d}

'''
Returns a parallelizable multi-dimensional list of numeric values. 
'''
def _hdf5_parallelizable_array():
    return ((_hdf5_parallelizable_dict(1, 2, 3, 4),
              _hdf5_parallelizable_dict(5, 6, 7, 8)),
             (_hdf5_parallelizable_dict(9, 10, 11, 12),
              _hdf5_parallelizable_dict(13, 14, 15, 16)))

'''
Returns a parallelizable multi-dimensional list of strings. 
'''
def _hdf5_parallelizable_string_array():
    return ((_hdf5_parallelizable_dict("the", "wild", "boys", "were"),
              _hdf5_parallelizable_dict("calling", "on", "their", "way")),
            (_hdf5_parallelizable_dict("back", "from", "the", "fire"),
              _hdf5_parallelizable_dict("in", "august", "moon", "surrender")))

'''
Returns a multi-dimensional list that cannot be parallelized (not all values in the list are
scalars).
'''
def _hdf5_non_parallelizable_array():
    return ((_hdf5_parallelizable_dict(1, 2, 3, (4, 4)),
              _hdf5_parallelizable_dict(5, 6, 7, (8, 8))),
            (_hdf5_parallelizable_dict(9, 10, 11, (12, 12)),
              _hdf5_parallelizable_dict(13, 14, 15, (16, 16))))

def _hdf5_numeric_array():
    t = (1, 2, 3, 4, 5)
    return t
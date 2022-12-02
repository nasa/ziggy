"""
HDF5 module interface

Provides import and export services for HDF5 files that conform to the HDF5 Persistable
standard. HDF5 files are read into a Python dictionary; similarly, Python dictionaries
can be written to HDF5 files. The necessary metadata and organization for the HDF5
Persistable standard can thus be applied or interpreted as necessary. 

This is based in large part on the MATLAB hdf5ConverterClass.

@author PT

"""

import h5py
import numpy
import numbers
from numpy import int8

class StructTemplate:
        pass
    
class Hdf5ModuleInterface:
    
    def __init__(self):
        
        # Compression level for arrays
        self._compression_level = 0
        
        # Smallest array size that will be compressed -- because compression
        # involves some overhead size to the resulting file, this prevents 
        # files with compressed arrays from winding up larger than the uncompressed
        # version
        self._compression_min_elements = 0
        
        # Struct arrays of scalars are stored in HDF5 Persistable files as a struct
        # with parallel arrays. When _reconstituteStructArray is True, the struct
        # will be converted back to an array of structs in Python
        self._reconstitute_struct_array = False
        
        # The natural retrieval order for HDF5 groups is alphabetical, but in the
        # Persistable HDF5 specification the groups have an attribute that shows the
        # original order of fields in the data struct that produced the HDF5 file. 
        # When _preserve_field_order is True, the object produced by the HDF5 reader
        # returns the fields in that order. When False, they are returned in alphabetical
        # order. 
        self._preserve_field_order = True
        
        self._TYPE_ATTRIBUTE_MAP = self._type_attribute_map()
        
        self._HDF5_GROUP_DELIMITER = "/"
        
    # returns the constant mapping from HDF5 native types to the value stored in the
    # type attribute of an HDF5 array. This mapping must match what is used in all other
    # HDF5 module interface packages (currently MATLAB, C++, and Java). Someday mayble
    # this will be converted to a set of constants defined in a single place that all
    # HDF5 module interface implementations can share. 
    def _type_attribute_map(self):
        
        type_attribute_map = {"int8"    : 2,
                              "int16"   : 3,
                              "int32"   : 4,
                              "int64"   : 5, 
                              "float32" : 6, 
                              "float64" : 7,
                              numpy.str_    : 8,
                              "H5T_OPAQUE"  : 9}
        return type_attribute_map
    
    def set_compression_level(self, compression_level):
        self._compression_level = int(compression_level);
        
    def set_compression_min_elements(self, compression_min_elements):
        self._compression_min_elements = int(compression_min_elements)
        
    def set_reconstitute_struct_array(self, reconstitute_struct_array):
        if not isinstance(reconstitute_struct_array, bool):
            raise ValueError("argument {} is not bool", reconstitute_struct_array)
        self._reconstitute_struct_array = reconstitute_struct_array
        
    def set_preserve_field_order(self, preserve_field_order):
        if not isinstance(preserve_field_order, bool):
            raise ValueError("argument {} is not bool", preserve_field_order)
        self._preserve_field_order = preserve_field_order
        
    # read an HDF5 file into a data dictionary. Dictionary keys are the names
    # of the structs / groups; dictionary values are the content of same. If 
    # a struct_name argument is supplied, the contents of the corresponding 
    # group will be returned. Note that struct_name must use the HDF5 naming
    # convention, which uses filesep to indicate sub-structrures; thus structA.structB
    # would translate to "/structA/structB".
    def read_file(self, file_name, group_name = ""):  
        
        file = h5py.File(file_name, "r")
        no_match = False
        if not group_name:
            hdf5_contents = self._read_groups(file)
        else:
            group = self._find_group(file, group_name)
            if group:
                hdf5_contents = self._read_group(group)
            else:
                no_match = True
        
        file.close()
        
        if no_match:
            raise NameError("Group with name {} not found", group_name)
            
        return hdf5_contents
    
    # recursively searches for a named sub-group in a parent group. If found,
    # that group is returned, otherwise an empty string is returned. 
    def _find_group(self, group, group_name):
        
        return_value = ""
        # if the group_name starts with the HDF5 group delimiter, lop it off
        # now
        
        if group_name[0] == self._HDF5_GROUP_DELIMITER:
            split_group_name = group_name.split(self._HDF5_GROUP_DELIMITER, 1)
            group_name = split_group_name[1]
        
        # split on the HDF5 group delimiter, which is /
        split_group_name = group_name.split(self._HDF5_GROUP_DELIMITER, 1)
        sub_group = split_group_name[0]
                
        # is the named sub-group even a member of the group? If so, is it also
        # a group?
        
        if sub_group in group and isinstance(group[sub_group], h5py.Group):
            if len(split_group_name) > 1:
                return_value = self._find_group(sub_group, split_group_name[1])
            else:
                return_value = group[sub_group]
                
        return return_value
    
    # Reads a set of HDF5 groups into a dictionary. Note that the Persistable standard
    # for HDF5 files requires that a group have either sub-groups or a dataset but
    # never both, so it is not necessary for this method to check for datasets -- the
    # fact that the multi-group reader has been called means that no datasets will be
    # found
    def _read_groups(self, group):
        
        return_value = dict()
        group_keys = self._get_group_keys(group)
        for k in group_keys:
            j = self._read_group(group[k])
            return_value.update({k : j})
            
        q = StructTemplate()
        q.__dict__ = return_value
        return q
    
    # Returns an ordered set of subgroups for a group. If the _preserve_field_order
    # attribute is True, the order of the keys will be the order which they were created
    # in the HDF5 file, which generally corresponds to their order in the object used to
    # generate the file; otherwise, they are returned in HDF5's natural alphabetical order.
    def _get_group_keys(self, group):
        
        group_keys = group.keys()
        if not (self._preserve_field_order):
            returned_keys = group_keys
        else:
            keys_dict = dict()
            for k in group_keys:
                order = group[k].attrs["FIELD_ORDER"]
                keys_dict.update({order : k})
            returned_keys = list()
            num_keys = len(group_keys)
            for i in range(num_keys):
                returned_keys.append(keys_dict.get(i))
        
        return returned_keys
    
    # Reads the contents of a single HDF5 group. The contents of a single group can
    # be either one or more groups, or a dataset. If the group contains sub-groups, 
    # these can be "independent" sub-groups or they can be a struct array.           
    def _read_group(self, group):
        
        return_value = ""
        # Read out: options are:
        #    parallel array
        #    struct array
        #    plain old struct with fields
        #    dataset
        #    empty
        # Note that we expect that group.keys() may have only 1 element, the
        # dataset, thus we want to convert to a list for ease of handling 
        k = [*group.keys(),]
        if self._is_parallel_array(group):
            return_value = self._read_parallel_array(group)
        elif self._is_struct_array(group):
            return_value = self._read_struct_array(group)
        elif len(k) > 1:
            return_value = self._read_groups(group)
        elif len(k) == 1:
            if isinstance(group[k[0]], h5py.Dataset):
                return_value = self._read_dataset(group)
        else:
            return_value = ""
        return return_value
        
    # Determines whether a group was originally an array of structs / objects that were
    # converted to a scalar struct of parallel arrays of primitives.
    def _is_parallel_array(self, group): 
        return "PARALLEL_ARRAY" in group.attrs
    
    # Determines whether a group contains a struct array
    def _is_struct_array(self, group):
        return "STRUCT_OBJECT_ARRAY" in group.attrs
    
    # Reads the contents of a dataset from an HDF5 array. If the dataset was originally
    # a boolean array that was cast to bytes for HDF5 storage, the resulting array is
    # cast back to bool. 
    def _read_dataset(self, group):
        
        k = [*group.keys(),]
        return_value = group[k[0]][()]
        if self._is_bool_array(group):
            return_value = return_value.astype(bool)
        
        data_type = group.attrs["DATA_TYPE"]
        if data_type == self._TYPE_ATTRIBUTE_MAP[numpy.str_]:
            return_value = self._to_strings(return_value)
            
        # special case of scalar value
        if numpy.ndim(return_value) == 1 and len(return_value) == 1:
            return_value = return_value[0]
        
        
        return return_value
    
    # Determines whether a group's contents are a booolean array that was cast to bytes
    # for HDF5 storage
    def _is_bool_array(self, group):
        return "LOGICAL_BOOLEAN_ARRAY" in group.attrs 
    
    # Converts any bytearrays in a collection of strings to Python strings
    def _to_strings(self, return_value):
        
        original_shape = return_value.shape
        return_value = return_value.flatten()
        for i in range(return_value.size):
            if hasattr(return_value[i], 'decode'):   
                return_value[i] = return_value[i].decode()
        return_value = return_value.reshape(original_shape)
        return return_value
    
    # Reads into memory a struct of parallel arrays that was originally a struct array of
    # primitives. If _reconstitute_struct_array is true, the original struct array is 
    # rebuilt from the contents of the paralell arrays into a multi-dimensional list of
    # dictionaries. 
    def _read_parallel_array(self, group):
        
        return_value = ""
        struct0 = self._read_groups(group)
        
        # Do we need to convert back to a struct array? In this case it will be an array
        # of dictionaries, each of which will have scalar primitive contents. Note that
        # this is a really painful thing to have to do, so should only be done if there's
        # a very good reason for it!
        if self._reconstitute_struct_array:
            
            # determine the shape that's needed eventually:
            struct0_dict = struct0.__dict__
            fields = list(struct0_dict.keys())
            shape = struct0_dict[fields[0]].shape
            
            # convert all the parallel arrays to 1-dimensional
            for field in fields:
                struct0_dict[field] = struct0_dict[field].flatten()
            
            # construct 1-d list to hold the results
            n_elem = len(struct0_dict[fields[0]])
            list_1_d = [None] * n_elem
            
            # populate the list -- loop over elements and within that over fields
            for i_elem in range(n_elem):
                d = dict()
                for field in fields:
                    d.update({field : struct0_dict[field][i_elem]})
                    
                q = StructTemplate()
                q.__dict__ = d
                list_1_d[i_elem] = q
                
            # reshape to desired n-dimensional shape
            list_n_d = numpy.reshape(list_1_d, shape)
            return_value = list_n_d
            
        else:
            return_value = struct0
            
        return return_value
    
    # Reads a struct array from an HDF5 file. The resulting array is returned as an
    # n-dimensional list of dictionaries
    def _read_struct_array(self, group):
        
        # get the dimensions of the array from the appropriate attribute
        array_dims = group.attrs["STRUCT_OBJECT_ARRAY_DIMS"]
        
        # get the number of elements in the array
        n_elem = 1
        for dim in array_dims:
            n_elem *= dim
        
        # define the 1-d list of struct array members
        list_1_d = [None] * n_elem
        
        # loop over groups in the array
        for i_group in group.keys():
            
            # get the content and the location of the group
            content = self._read_group(group[i_group])
            location = self._struct_array_location(i_group, array_dims)
            list_1_d[location] = content
           
        # reshape as needed        
        list_n_d = numpy.reshape(list_1_d, array_dims)
        
        return list_n_d
    
    # Converts an array location to an index. The array location is encoded in 
    # the name of the corresponding HDF5 group, i.e., <groupName>-#-#-#-... .
    # the index is the ordinal position of the array location when converted to
    # a 1-d array in a row-major manner (i.e., last index changes fastest). Both
    # the subscripts and the index are zero-based. 
    def _struct_array_location(self, group_name, array_dims): 
        
        # get the zero-based subscripts out of the group name
        name_split = group_name.split("-")
        name_split.pop(0)
        subscript = list()
        for subscript_string in name_split:
            subscript.append(int(subscript_string))
        
        # convert to an index using row-major indexing: this means that 
        # (0,0,0) is followed by (0,0,1) ... (0,0,M-1); then (0,1,0), 
        # (0,1,1) ... (0,1,M) ... (0,N,M), (1,0,0), (1,0,1)...
        
        ind = 0
        for i in range(len(subscript)):
            if i > 0:
                ind *= array_dims[i-1]
            ind += subscript[i]
            
        return ind
    
    # Writes an instance of a Python class (or a Python dictionary) to an 
    # HDF5 file. The write process supports all the functionality needed for
    # the HDF5 Persistable specification.
    def write_file(self, file_name, data_object):
        
        file = h5py.File(file_name, "w")
        self._write_scalar_struct(data_object, file)
        file.close()
        
    # writes a scalar "struct" (in this case an object or a dictionary) to
    # an HDF5 file. 
    def _write_scalar_struct(self, data_object, group):
        
        if hasattr(data_object, "__dict__"):
            data_object = data_object.__dict__
            
        if not isinstance(data_object, dict):
            raise ValueError("_write_scalar_struct takes a class instance or dict as argument")
        
        group.attrs.create("DATA_TYPE", numpy.array(self._TYPE_ATTRIBUTE_MAP["H5T_OPAQUE"], dtype=numpy.int8))
        
        # loop over the contents of the dictionary
        
        i_field_order = 0
        for k in data_object.keys():
            new_group = group.create_group(k)
            
            # write the ordering of the field as an attribute
            new_group.attrs.create("FIELD_ORDER", int(i_field_order))
            i_field_order = i_field_order + 1
            
            # There are a number of different forms of content that the current value could
            # represent, and even within that there are a number of different ways that the
            # information might be represented in Python. Here we attempt to address all of those!
            value = data_object[k]
            np_value = numpy.atleast_1d(value)
            
            # empty field
            if value is None or (isinstance(value, str) and len(value) == 0):
                new_group.attrs.create("EMPTY_FIELD", numpy.array([0], dtype=numpy.int8))
                
            # object -- recursive call to this method
            elif hasattr(value, "__dict__"):
                self._write_scalar_struct(value, new_group)
            
            # dictionary -- recursive call to this method
            elif isinstance(value, dict):
                self._write_scalar_struct(value, new_group)
                
            # numeric
            elif numpy.issubdtype(np_value.dtype, numpy.number):
                self._write_numeric_array(np_value, new_group, k)
        
            # boolean 
            elif str(np_value.dtype) == "bool":
                self._write_bool_array(np_value, new_group, k)
                
            # text strings
            elif self._is_string_array(np_value):
                self._write_string_array(np_value, new_group, k)
                
            # object list, set or array:
            elif self._is_dict_or_object_array(np_value):
                self._write_struct_array(value, new_group, k)
                
            # something not supported
            else:
                raise ValueError("Data object {} is something unsupported", k)
    
    # determines whether a data object is a numpy array of strings
    def _is_string_array(self, value):
        
        fvalue = value.flatten()
        return all(isinstance(v, str) for v in fvalue)
    
    # determines whether a data object is a numpy array of objects or dictionaries
    
    def _is_dict_or_object_array(self, value):
        
        fvalue = value.flatten()
        is_struct_array = True;
        for v in fvalue:
            is_struct = hasattr(v, "__dict__") or isinstance(v, dict)
            is_struct_array = is_struct_array and is_struct
        return is_struct_array
    
    # Writes a numeric array to an HDF5 group. Here we assume that the value has been
    # converted to a numpy array prior to being passed as an argument to this method.         
    def _write_numeric_array(self, value, group, name):
        
        # write the type attribute
        array_dtype = str(value.dtype)
            
        group.attrs.create("DATA_TYPE", numpy.array([self._TYPE_ATTRIBUTE_MAP[array_dtype]], \
                dtype=numpy.int8))
        
        # create and write the dataset
        if self._compression_level > 0 and value.size > self._compression_min_elements:
            group.create_dataset(name, shape=value.shape, dtype=array_dtype, data=value, \
                    compression="gzip", compression_opts=self._compression_level)
        else:
            group.create_dataset(name, shape=value.shape, dtype=array_dtype, data=value)
    
    # Writes a boolean array to an HDF5 group, first converting it to a byte array    
    def _write_bool_array(self, value, group, name):
        
        # add the attribute that says that this should be treated as booleans, not as int8
        group.attrs.create("LOGICAL_BOOLEAN_ARRAY", numpy.array([0], dtype=numpy.int8))
        self._write_numeric_array(numpy.array(value, dtype=int8), group, name) 
        
    # writes an array of strings to an HDF5 group
    def _write_string_array(self, value, group, name):  
        
        # write the correct type attribute
        group.attrs.create("DATA_TYPE", numpy.array(self._TYPE_ATTRIBUTE_MAP[numpy.str_], \
                dtype=numpy.int8)) 
        
        # write the array (but first it has to be converted to UTF-8)
        value = self._to_utf8(value)
        group.create_dataset(name, shape=value.shape, dtype=h5py.string_dtype(), \
                data=value)
        
    # Converts an array of strings in any encoding to the UTF-8 encoding desired by 
    # HDF5's variable-width string array infrastructure
    def _to_utf8(self, value):
        
        value = numpy.atleast_1d(value)        
        if value.size == 1:
            value = numpy.atleast_1d(value[0].encode("UTF-8"))
        else:
            original_shape = value.shape
            value = value.flatten()
            value_list = list()
            for i in range(value.size):
                value_list.append(value[i].encode("UTF-8"))
#                value[i] = value[i].encode("UTF-8")
            
            value = numpy.atleast_1d(value_list)
            value = value.reshape(original_shape)
            
        return value
          
    # Writes an array of structs to HDF5. Under the top-level group, there is a group for
    # each struct in the array. The name of the group specifies its location in the struct
    # array, so for example, in a 3-d array, <structName>-0-0-0 would be the [0][0][0] 
    # element of the array, etc. 
    def _write_struct_array(self, value, group, name):
        
        # special case: struct array in which each field of each struct in the array contains
        # only primitive scalars. In this case, write the struct array in the special, 
        # struct-of-parallel-arrays format
        if self._is_parallelizable(value):
            self._write_struct_as_parallel_array(value, group)
            
        else:
            
            # write some group information
            
            group.attrs.create("STRUCT_OBJECT_ARRAY", numpy.array([0], dtype=numpy.int8))
            group.attrs.create("STRUCT_OBJECT_ARRAY_DIMS", \
                    numpy.array(value.shape, dtype=numpy.int64))
            # make a numpy array out of the struct array, since it could be a list or something
            # like that; also flatten it but capture the original size
            npvalue = numpy.array(value)
            original_shape = npvalue.shape
            npvalue = npvalue.flatten()
            
            for j in range(npvalue.size):
                group_name = self._name_from_index(name, j, original_shape)
                new_group = group.create_group(group_name)
                self._write_scalar_struct(npvalue[j], new_group)
            
    # determine whether a struct array can be written as a scalar struct of parallel arrays
    # of primitives
    def _is_parallelizable(self, value):
        
        # convert the value to an array and flatten it
        fvalue = numpy.array(value).flatten()
        sample_dict = fvalue[0];
        if hasattr(sample_dict, "__dict__"):
            sample_dict = sample_dict.__dict__
        
        is_parallelizable = True
        for k in sample_dict.keys():
            is_scalar, field_value = self._is_scalar(sample_dict[k])
            if is_scalar:
                is_primitive = self._is_primitive(field_value)
            else:
                is_primitive = False
            is_parallelizable = is_parallelizable and is_scalar \
                and is_primitive
            
        return is_parallelizable
    
    # determines whether a value is a scalar, where a scalar can be a number, a boolean,
    # a string, a list with 1 element, a set with 1 element, or a Numpy array with 1 element
    # (either 0 dimensional or 1 dimensional). Returns a boolean indicating whether the value
    # is a scalar, and the "unpacked" value if the scalar was contained in a list, set, or
    # array. 
    def _is_scalar(self, value):
        
        is_scalar = False
        scalar_value = None
        if isinstance(value, numbers.Number):
            is_scalar = True
            scalar_value = value
        elif isinstance(value, str):
            is_scalar = True
            scalar_value = value
        elif isinstance(value, bool):
            is_scalar = True
            scalar_value = value
        elif isinstance(value, list):
            if len(value) == 1:
                is_scalar = True
                scalar_value = value[0]
        elif isinstance(value, set):
            if len(value) == 1:
                is_scalar = True
                list_value = list(value)
                scalar_value = list_value[0]
        elif isinstance(value, numpy.array):
            if value.ndim == 0:
                is_scalar = True
                scalar_value = value[()]
            elif value.ndim == 1 and value.size == 1:
                is_scalar = True
                scalar_value = value[0]
                
        return is_scalar, scalar_value
    
    # determines whether its argument is a number, bool, or string. It is assumed that the
    # caller has already ensured that the argument is a scalar.
    def _is_primitive(self, field_value):
        
        is_number = isinstance(field_value, numbers.Number)
        is_bool = isinstance(field_value, bool)
        is_str = isinstance(field_value, str)
        return is_number or is_bool or is_str
        
    # Writes a struct array as a scalar struct of parallel arrays. 
    def _write_struct_as_parallel_array(self, value, group):   
        
        # mark the group appropriately
        group.attrs.create("PARALLEL_ARRAY", numpy.array([0], dtype=numpy.int8))
        # convert to a numpy array and capture the shape
        npvalue = numpy.array(value)
        original_shape = npvalue.shape
        
        # flatten and convert to dictionaries
        npvalue = npvalue.flatten()
        for n in range(npvalue.size):
            if hasattr(npvalue[n], "__dict__"):
                npvalue[n] = npvalue[n].__dict__
        
        # set up a new dictionary
        pvalue = dict();
        
        npvalue0 = npvalue[0]
        npvalue_shape = npvalue.shape
        for k in npvalue0.keys():

            # there really has to be a better way to get a dtype for a scalar            
            rv = self._is_scalar(npvalue0[k])
            array_for_dtype = numpy.array(rv[1])
            if isinstance(rv[1], str):
                array_dtype = h5py.special_dtype(vlen=str)
            else:
                array_dtype = array_for_dtype.dtype
            empty_array = numpy.zeros(shape=npvalue_shape, dtype=array_dtype)
            pvalue.update({k : empty_array})
            for j in range(npvalue.size):
                pvalue[k][j] = npvalue[j][k]
                
        # reshape the arrays
        for k in pvalue.keys():
            pvalue[k] = numpy.reshape(pvalue[k], original_shape)
            
        # the pvalue object is a scalar struct and can be processed as such
        self._write_scalar_struct(pvalue, group)
       
    # determines the name of a struct array member's HDF5 group from its index in the flattened
    # version of the array and the original shape of the array. 
    def _name_from_index(self, name, index, original_shape):
        
        # determine the "size" of each dimension -- that is to say, how many elements we
        # have to go before the dimension's index can imcrement. Example: for an array of
        # original dimensions 3 x 4 x 5, we need to be on the 20th element for the first
        # index to change, on the 5th element for the 2nd index to change, but the last index
        # can change for every step through the array
        
        array_size = list()
        n_dims = len(original_shape)
        for i in range(n_dims):
            this_size = 1
            if i < n_dims - 1:
                for j in range(i+1,n_dims):
                    this_size = this_size * original_shape[j]
            array_size.append(int(this_size))
        
        # find the value of each subscript by taking the index and dividing by the 
        # array_size for that subscript. Then compute the remainder so that the same
        # calculation will work for the next subscript. 
        specific_name = name
        for i in range(n_dims):
            subscript = index // array_size[i]
            index = index - subscript * array_size[i]
            specific_name = specific_name + "-" + str(subscript)
            
        return specific_name
            
        
        
        
        
        
        
        
        
        
        
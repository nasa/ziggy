classdef hdf5ConverterClass < handle
%
% hdf5ConverterClass -- performs conversion between MATLAB structs and HDF5 files
%
% The hdf5Converter class reads pipeline data files into MATLAB structs, and writes MATLAB
% structs to HDF5 pipeline files. This facilitates the exchange of data between the
% pipeline infrastructure and the MATLAB applications. 
%
% The MATLAB object that is written to the HDF5 file must be a scalar struct or scalar
% object. The fields / properties of the object can be of the following types:
%
% ==> array or scalar of non-complex numeric type
% ==> array or scalar of logicals
% ==> text (scalar string or 1-d char array)
% ==> text array (string array or cellstring array)
% ==> array or scalar of struct or object
%

%=========================================================================================

properties (Constant)
    OBJ_CONV_WARN = 'matlab:structOnObject' ;
    TYPE_ATTRIBUTE_MAP = containers.Map( ...
        {'H5T_NATIVE_INT8', 'H5T_NATIVE_INT16', 'H5T_NATIVE_INT32', ...
        'H5T_NATIVE_INT64', 'H5T_NATIVE_FLOAT', 'H5T_NATIVE_DOUBLE', ...
        'H5T_C_S1', 'H5T_OPAQUE'}, ...
        int32([2, 3, 4, 5, 6, 7, 8, 9])) ;
end

properties (GetAccess = 'public', SetAccess = 'protected')

    convertNumericToDouble  = false;  % preserve numeric original types
    convertTextToStrings    = false;  % use char arrays for text
    compressionLevel        = 0    ;  % some compression
    compressionMinElements  = 200  ;  % min array size to apply compression
    preserveFieldOrder      = true ;  % don't sort fields alphabetically
    structArrayColumnVector = true ;  % 1-d struct array is a column vector by default
    reconstituteStructArray = false ; % don't convert parallel arrays back to struct array
    
end
    
methods
    function obj = hdf5ConverterClass() % trivial constructor
    end

    %   Setters
    
    function set_compression_level(object, compLevel)
        compLevelInt = trunc(compLevel) ;
        if compLevelInt ~= compLevel || compLevel < -1 || compLevel > 9
            error('ziggy:hdf5ConverterClass:set_compression_level:compressionLevelInvalid', ...
                'set_compression_level: value must be integer between -1 and 9') ;
        end
        object.compressionLevel = compLevel ;
    end
    
    function text_as_strings(object)
        object.convertTextToStrings = true ;
    end
    function text_as_char_arrays(object)
        object.convertTextToStrings = false ;
    end
    function preserve_precision(object)
        object.convertNumericToDouble = false ;
    end
    function convert_to_double(object)
        object.convertNumericToDouble = true ;
    end
    function set_compression_min_elements(object, minElem)
        if ~isnumeric(minElem) || ~isreal(minElem) || ~isscalar(minElem)
            error('ziggy:hdf5ConverterClass:set_compression_min_elements:invalidArgument', ...
                'set_compression_min_elements: argument must be scalar real value') ;
        end
        object.compressionMinElements = minElem ;
    end
    function set_original_field_ordering(object)
        object.preserveFieldOrder = true ;
    end
    function set_alpha_field_ordering(object)
        object.preserveFieldOrder = false ;
    end
    function set_struct_array_column_vector(object)
        object.structArrayColumnVector = true ;
    end
    function set_struct_array_row_vector(object)
        object.structArrayColumnVector = false;
    end
    function set_struct_arrays(object)
        object.reconstituteStructArray = true;
    end
    function set_parallel_arrays(object)
        object.reconstituteStructArray = false;
    end
    
%=========================================================================================

%   methods that live in the directory rather than on here

    write_file( object, filename, dataStruct )
    dataStruct = read_file( object, filename, structName )
    
end

%=========================================================================================

methods (Access = 'protected', Static)
    fieldType = get_field_type( fieldValue )
end

end % classdef


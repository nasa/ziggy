function write_file( object, filename, dataStruct )
%
% write_file -- write a MATLAB struct to a file in HDF5 format
%
% hdf5ConverterObject.write_file( filename, dataStruct ) writes the MATLAB scalar struct
%     dataStruct to the file filename. The dataStruct argument must be a scalar and its
%     fields may only be in a few categories, use hdf5ConverterClass help for details. 
%
% Arrays will be written to HDF5 arrays with their shape preserved, i.e., an M x N MATLAB
%    array is written to an M x N HDF5 array. As MATLAB stores arrays in column-major
%    order and HDF5 uses row-major, this means that the value ordering in HDF5 is
%    different from that in MATLAB.
%

%=========================================================================================

%   check that the inputStruct is a scalar struct 

    if ~isscalar(dataStruct) || ( ~isstruct(dataStruct) && ~isobject(dataStruct) )
        error('ziggy:hdf5ConverterClass:write_file:notAStruct', ...
            'write_struct_as_hdf5: input is not a scalar struct or MATLAB object');
    end
    
%   create the file; delete it if it already exists

    fileId = H5F.create(filename, 'H5F_ACC_TRUNC', 'H5P_DEFAULT', 'H5P_DEFAULT') ;
    
%   we may need to do object to struct, but don't want a ton of warnings on it

    warnState = warning('query', object.OBJ_CONV_WARN) ;
    warning('off', object.OBJ_CONV_WARN) ;
    
%   write the struct

    subGroups = write_scalar_struct( object, dataStruct, fileId ) ;
    nOpenObjects = H5F.get_obj_count(fileId,'H5F_OBJ_ALL');
    if (nOpenObjects > 1)
        disp('Not all HDF5 objects are closed, summary follows');
        disp(['Number open groups: ',num2str(H5F.get_obj_count(fileId, 'H5F_OBJ_GROUP'))]);
        disp(['Number open datasets: ',num2str(H5F.get_obj_count(fileId, 'H5F_OBJ_DATASET'))]);
        disp(['Number open datatypes: ',num2str(H5F.get_obj_count(fileId, 'H5F_OBJ_DATATYPE'))]);
        disp(['Number open attributes: ',num2str(H5F.get_obj_count(fileId, 'H5F_OBJ_ATTR'))]);
        disp(['Number open objects (except for file): ', ...
            num2str(H5F.get_obj_count(fileId,'H5F_OBJ_ALL')-1)]);
    else
        disp('No open objects detected') ;
    end
    H5F.close(fileId) ;
    
    warning(warnState.state, object.OBJ_CONV_WARN) ;

return

%=========================================================================================

%   subfunction that writes a scalar struct to HDF5; the caller must provide the group ID,
%   and must close that group after writing

function subGroups = write_scalar_struct( object, dataStruct, groupId )

    subGroups = [] ;
%   convert object to struct, if need be

    if isobject(dataStruct)
        dataStruct = struct(dataStruct) ;
    end
        
    fieldNames = fieldnames(dataStruct) ;
    
    iFieldOrder = int32(0) ;
    for iField = fieldNames(:)'
        fieldName = iField{1} ;
        fieldValue = dataStruct.(fieldName) ;
        subSubGroups = [] ;
        
%       construct a group for the field

        fieldGroupId = H5G.create( groupId, fieldName, 'H5P_DEFAULT', ...
                'H5P_DEFAULT', 'H5P_DEFAULT' ) ;
            
%       store the group's order in the struct as an attribute

        scalarSpace = H5S.create('H5S_SCALAR') ;
        orderAttribute = H5A.create(fieldGroupId, 'FIELD_ORDER', 'H5T_NATIVE_INT32', ...
            scalarSpace, 'H5P_DEFAULT') ;
        H5A.write( orderAttribute, 'H5ML_DEFAULT', iFieldOrder ) ;
        H5A.close(orderAttribute) ;
        H5S.close(scalarSpace) ;
        iFieldOrder = iFieldOrder + 1 ;
        
        subGroups = [subGroups ; fieldGroupId] ;
        
%       get the kind of value in the field

        fieldType = hdf5ConverterClass.get_field_type( fieldValue ) ;
        
        switch fieldType
            case 'numeric'
                write_numeric_array( object, fieldName, fieldValue, fieldGroupId ) ;
            case 'logical'
                write_logical_array( object, fieldName, fieldValue, fieldGroupId ) ;
            case {'struct', 'object'}
                [isParallelizable, structTable] = is_parallelizable( fieldValue ) ;
                if (isParallelizable) 
                    subSubGroups = write_struct_as_parallel_array( object, structTable, ...
                        size(fieldValue), fieldGroupId ) ;
                else
                    subSubGroups = write_struct( object, fieldName, fieldValue, ...
                        fieldGroupId ) ;
                end
            case 'cell'
                    subSubGroups = write_struct( object, fieldName, fieldValue, ...
                        fieldGroupId ) ;
            case 'text'
                write_text( object, fieldName, fieldValue, fieldGroupId ) ;
            case 'empty'
                write_empty_attribute( fieldGroupId ) ;
            case 'invalid'
                error('ziggy:hdf5ConverterClass:write_file:invalidFieldType', ...
                    ['field "',fieldName,'" type is not valid for HDF5 conversion']) ;
        end
        H5G.close(fieldGroupId);
        subGroups = [subGroups ; subSubGroups(:)] ;
        
    end
return

%=========================================================================================

%   subfunction that writes an attribute that indicates an empty field

function write_empty_attribute( fieldGroupId ) 

    orderAttributeSpace = H5S.create('H5S_SCALAR');
    orderAttributeId = H5A.create(fieldGroupId, 'EMPTY_FIELD', 'H5T_NATIVE_INT8', ...
        orderAttributeSpace, 'H5P_DEFAULT', 'H5P_DEFAULT') ;
    H5A.close(orderAttributeId);
    H5S.close(orderAttributeSpace); 

return

%=========================================================================================

function write_data_type_attribute( object, fieldGroupId, fieldType )

    typeAttributeSpace = H5S.create('H5S_SCALAR') ;
    typeAttributeId = H5A.create(fieldGroupId, 'DATA_TYPE', 'H5T_NATIVE_INT32', ...
        typeAttributeSpace, 'H5P_DEFAULT', 'H5P_DEFAULT') ;
    H5A.write(typeAttributeId, 'H5ML_DEFAULT', object.TYPE_ATTRIBUTE_MAP(fieldType)) ;
    H5A.close(typeAttributeId) ;
    H5S.close(typeAttributeSpace); 
    
return

%=========================================================================================

%   subfunction that writes a numeric array

function write_numeric_array( object, fieldName, fieldValue, fieldGroupId )

%   create the dataspace for the array -- note that we are using the same shape in MATLAB
%   and in Java, C++, etc. This means that the dataspace size agrees with the MATLAB size,
%   but that the array must be transposed when saved

    dataspace = create_dataspace( fieldValue ) ;
    
%   detect the class

    h5Class = detect_hdf5_numeric_class( fieldValue ) ;
    write_data_type_attribute( object, fieldGroupId, h5Class ) ;
    if length(fieldValue) > object.compressionMinElements && object.compressionLevel > 0
        deflateProperty = H5P.create('H5P_DATASET_CREATE') ;
%        H5P.set_chunk(deflateProperty,length(fieldValue(:))) ;
        H5P.set_chunk(deflateProperty,original_size(fieldValue)) ;
        H5P.set_deflate(deflateProperty,object.compressionLevel) ;
    else
         deflateProperty = 'H5P_DEFAULT' ;
    end
    dataset = H5D.create( fieldGroupId, fieldName, h5Class, dataspace, ...
        deflateProperty ) ;
    H5D.write( dataset, 'H5ML_DEFAULT', 'H5S_ALL', 'H5S_ALL', 'H5P_DEFAULT', ...
        permute(fieldValue,fliplr([1:ndims(fieldValue)])) ) ;
    H5D.close(dataset) ;
    H5P.close(deflateProperty) ;
    H5S.close(dataspace) ;

return

%=========================================================================================

%   determine the HDF5 numeric class based on the MATLAB numeric class -- we use the HDF5
%   "native" classes for everything

function h5Class = detect_hdf5_numeric_class( arrayData )

    h5Class = 'H5T_NATIVE_' ;
    
    if (isinteger(arrayData))
        h5Suffix = upper(class(arrayData)) ;
    elseif isa(arrayData,'single')
        h5Suffix = 'FLOAT' ;
    elseif isa(arrayData,'double')
        h5Suffix = 'DOUBLE' ;
    else
        error('ziggy:write_struct_as_hdf5:invalidNumericClass', ...
            'write_struct_as_hdf5: invalid numeric class') ;
    end
    
    h5Class = [h5Class h5Suffix] ;
    
return

%=========================================================================================

%   subfunction that writes an array of logicals -- this means that the array has to be
%   converted to int8, and an attribute needs to be added to the group to indicate that it
%   should be reconstituted as a logical / boolean and not treated as int8 

function write_logical_array( object, fieldName, fieldValue, fieldGroupId ) 

    fieldValueInt8 = int8(fieldValue) ;
    
    scalarSpace = H5S.create('H5S_SCALAR') ;
    typeAttribute = H5A.create(fieldGroupId, 'LOGICAL_BOOLEAN_ARRAY', 'H5T_NATIVE_INT8', ...
        scalarSpace, 'H5P_DEFAULT') ;
    H5A.close(typeAttribute) ;
    H5S.close(scalarSpace) ;
    write_numeric_array( object, fieldName, fieldValueInt8, fieldGroupId ) ;
    
return

%=========================================================================================

%   subfunction that writes a struct into an HDF5 group. This function also has to handle
%   the case of a struct array, a MATLAB object, or an object array

function subSubGroups = write_struct( object, fieldName, fieldValue, fieldGroupId )

    subSubGroups = [] ;
    originalSize = original_size(fieldValue) ;
    fieldValue = fieldValue(:) ;
    indices = cell(1, length(originalSize)) ;
    
%   if this is a struct array, make a note of that in the fieldGroupId, and include the
%   dimensions for downstream users
    
    if length(fieldValue) > 1
        scalarSpace = H5S.create('H5S_SCALAR') ;
        typeAttribute = H5A.create(fieldGroupId, 'STRUCT_OBJECT_ARRAY', 'H5T_NATIVE_INT8', ...
            scalarSpace, 'H5P_DEFAULT') ;
        H5A.close(typeAttribute) ;
        H5S.close(scalarSpace) ;
        
        write_data_type_attribute(object, fieldGroupId, 'H5T_OPAQUE') ;
        arraySpace = H5S.create_simple(1,length(originalSize),[]);
        dimsAttribute = H5A.create(fieldGroupId,'STRUCT_OBJECT_ARRAY_DIMS', ...
            'H5T_NATIVE_LONG', arraySpace, 'H5P_DEFAULT', 'H5P_DEFAULT');
        H5A.write(dimsAttribute, 'H5ML_DEFAULT', int64(originalSize));
        H5A.close(dimsAttribute); 
    end
    
%   loop over structs
    
    for iStruct = 1:length( fieldValue )
        
        thisField = fieldValue(iStruct) ;
        if (isobject(thisField))
            thisField = struct(thisField) ;
        elseif (iscell(thisField))
            thisField = thisField{1};
        end
        
%       if this is a struct array, we need to construct a new group for each array member;
%       the group name will be <structName>-<i>-<j> ... where i, j, etc are the zero-based
%       subscripts of the array member (note that since a hyphen is not an allowed
%       character in a MATLAB variable name or function name, using hyphen this way makes
%       the identification of the subscripts unambiguous)

        if length(fieldValue) > 1
            [indices{:}] = ind2sub(originalSize,iStruct) ;
            structName = fieldName ;
            for iIndex = 1:length(indices)
                structName = [structName, '-', num2str(indices{iIndex}-1)] ;
            end
            groupId = H5G.create( fieldGroupId, structName, 'H5P_DEFAULT', ...
                'H5P_DEFAULT', 'H5P_DEFAULT' ) ;
            write_data_type_attribute(object, groupId, 'H5T_OPAQUE') ;
            subSubGroups = [subSubGroups ; groupId] ;
        else
            groupId = fieldGroupId ;
        end
        evenLowerSubGroups = write_scalar_struct( object, thisField, groupId ) ;
        subSubGroups = [subSubGroups ; evenLowerSubGroups(:)] ;
        if groupId ~= fieldGroupId
            H5G.close(groupId);
        end
        
    end

return

%=======================ST==================================================================

function write_text( object, fieldName, fieldValue, fieldGroupId )

%   there is an ambiguity in the case where Java writes a scalar string and a string array
%   or list of length 1. On the MATLAB side it's the ambiguity between a char array and a
%   cell array of char array where the cell array has length 1. Address that now.

    if iscellstr(fieldValue)
        scalarSpace = H5S.create('H5S_SCALAR') ;
        typeAttribute = H5A.create(fieldGroupId, 'STRING_ARRAY', 'H5T_NATIVE_INT8', ...
            scalarSpace, 'H5P_DEFAULT') ;
        H5A.close(typeAttribute) ;
    end
    
    write_data_type_attribute(object,fieldGroupId, 'H5T_C_S1') ;
    fieldValue = cellstr(fieldValue) ;
    dataspace = create_dataspace( fieldValue ) ;
    dataType = H5T.copy('H5T_C_S1') ;
    H5T.set_size(dataType,'H5T_VARIABLE');
    dataset = H5D.create( fieldGroupId, fieldName, dataType, dataspace, 'H5P_DEFAULT' ) ;
    H5D.write( dataset, dataType, 'H5S_ALL', 'H5S_ALL', 'H5P_DEFAULT', ...
        permute(fieldValue,fliplr([1:ndims(fieldValue)])) ) ;
    H5D.close(dataset);
    H5T.close(dataType) ;
    H5S.close(dataspace) ;

return

%=========================================================================================

%   subfunction that creates a valid dataspace for data. The issue here is that a MATLAB
%   scalar is actually a 1 x 1 array, with 2 dimensions each of which has length 1. In
%   Java, this is interpreted as an array[1][1], which is not correct.

function dataSpace = create_dataspace( fieldValue ) 

    dims = original_size(fieldValue) ;
    nDims = length(dims) ;
    dataSpace = H5S.create_simple( nDims, dims, [] ) ;
   
return

%=========================================================================================

%   subfunction that resolves the issue of 2-d arrays where one array dimension is 1. For
%   most languages, an array[1][1] has 1 element, at array[0][0], so it is is identical to
%   array[1] and indeed to a scalar. For Java, however, array[0] is an array of 1-d
%   arrays, so we need to NOT do this. 

function arraySize = original_size( fieldValue )
    
    arraySize = size(fieldValue) ;
    if length(arraySize) == 2 && any(arraySize == 1)
        arraySize = numel(fieldValue);
    end
    
return

%=========================================================================================

%   subfunction that determines whether a struct array can be represented as a parallel
%   array of "primitive" values (real numerics, logicals, strings). This permits such a
%   struct to be stored in HDF5 in a more compact fashion. Also returns the table
%   generated in the testing process, which can then be used in the actual writing of the
%   struct array to HDF5

function [parallelizable, structAsTable] = is_parallelizable(struct0)

    parallelizable = true;
    structAsTable = [];
    
%   parallelizable structs have > 1 element    
    
    struct0 = struct0(:) ;
    parallelizable = parallelizable && numel(struct0) > 1 ;
    
%   parallelizable structs can be converted to a table
    
    try
        structAsTable = struct2table(struct0) ;
    catch exceptionObject
        if strcmp(exceptionObject.identifier, 'MATLAB:struct2table:UnequalFieldLengths') || ...
                strcmp(exceptionObject.identifier, 'MATLAB:table:UnequalFieldLengths')
            parallelizable = false;
            return
        else
            rethrow(exceptionObject);
        end
    end
    
%   each entry in each column of the resulting table should be a scalar, and one of the
%   following classes: numeric real, logical, cellstr 

    fieldNames = fieldnames(struct0) ;
    for fieldName = fieldNames(:)'
        parallelizable = parallelizable && numel(structAsTable.(fieldName{1})) == ...
            numel(struct0) ;
        goodClass = isreal(structAsTable.(fieldName{1})) || ...
            islogical(structAsTable.(fieldName{1})) || ...
            iscellstr(structAsTable.(fieldName{1})) ;
        parallelizable = parallelizable && goodClass ;
    end
    
return

%=========================================================================================

%   subfunction that writes a struct array in the form of parallel arrays of the struct
%   members; in this case the "struct array" has been pre-processed into a table to ease
%   the conversion

function subGroupIds = write_struct_as_parallel_array( object, structTable, ...
        originalSize, structGroupId )

    subGroupIds = [] ;
    
%   write the attribute that indicates a parallel array representation of a struct array

    scalarSpace = H5S.create('H5S_SCALAR') ;
    typeAttribute = H5A.create(structGroupId, 'PARALLEL_ARRAY', 'H5T_NATIVE_INT8', ...
        scalarSpace, 'H5P_DEFAULT') ;
    H5A.close(typeAttribute) ;
    H5S.close(scalarSpace) ;

%   write the attribute that indicates that the group contains a struct of some sort

    write_data_type_attribute(object, structGroupId, 'H5T_OPAQUE') ;
    
%   loop over fields

    fieldNames = structTable.Properties.VariableNames ;
    
    iFieldOrder = int32(0) ;
    for iField = fieldNames(:)'
        fieldName = iField{1} ;
        fieldValue = structTable.(fieldName) ;
       
%       restore the original shape of the struct array       
       
        fieldValue = reshape(fieldValue, originalSize) ;
        
%       construct the group for it and set the ordering

        fieldGroupId = H5G.create( structGroupId, fieldName, 'H5P_DEFAULT', ...
                'H5P_DEFAULT', 'H5P_DEFAULT' ) ;
        scalarSpace = H5S.create('H5S_SCALAR') ;
        orderAttribute = H5A.create(fieldGroupId, 'FIELD_ORDER', 'H5T_NATIVE_INT32', ...
            scalarSpace, 'H5P_DEFAULT') ;
        H5A.write( orderAttribute, 'H5ML_DEFAULT', iFieldOrder ) ;
        H5A.close(orderAttribute) ;
        H5S.close(scalarSpace) ;
        iFieldOrder = iFieldOrder + 1 ;
        
%       get the kind of value in the field

        fieldType = hdf5ConverterClass.get_field_type( fieldValue ) ;
        
%       write the array        
        switch fieldType
            case 'numeric'
                write_numeric_array( object, fieldName, fieldValue, fieldGroupId ) ;
            case 'logical'
                write_logical_array( object, fieldName, fieldValue, fieldGroupId ) ;
            case 'text'
                write_text( object, fieldName, fieldValue, fieldGroupId ) ;
            case 'empty'
                write_empty_attribute( fieldGroupId ) ;
            case 'invalid'
                error('ziggy:hdf5ConverterClass:write_file:invalidFieldType', ...
                    ['field "',fieldName,'" type is not valid for HDF5 conversion']) ;
        end
        H5G.close(fieldGroupId);
        subGroupIds = [subGroupIds ; fieldGroupId] ;
        
    end

return
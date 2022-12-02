function dataStruct = read_file( object, filename, structName )
%
% read_file -- read a module interface HDF5-formatted file into a MATLAB struct
%
% dataStruct = hdf5ConverterObject.read_file( filename ) reads the HDF5 file specified by
%    filename into a MATLAB struct. The HDF5 file has to conform to the standards for
%    module interface files, see hdf5ConverterClass help for details.
%
% data = hdf5ConverterObject.read_file( filename, structName ) searches for a particular
%    sub-structure in the HDF5 file, and returns it if found. The structName has to be
%    expressed in HDF5-style "path" formalism, i.e., if you want to load the
%    structA.structB sub-structure of the main struct, structName should be
%    '/structA/structB'. 
%

%=========================================================================================

%   get the information about the file

    h5FileInfo = h5info( filename ) ;
    
%   open the file

    fileId = H5F.open( filename ) ;
    
%   use the groups reader to produce the top-level struct

    noMatch = false ;
    if ~exist('structName', 'var') || isempty(structName)
        dataStruct = read_groups( object, fileId, h5FileInfo.Groups ) ;
    else
        groupInfo = find_group( object, h5FileInfo.Groups, structName ) ;
        if ~isempty(groupInfo)
            dataStruct = read_group( object, fileId, groupInfo ) ;
        else
            noMatch = true ;
        end
    end
    
    H5F.close(fileId) ;
    
    if (noMatch)
        error('ziggy:Hdf5ConverterClass:read_file:UnmatchedStructName', ...
            ['read_file: no HDF5 group found with name "', structName, '"']) ;
    end
    
return

%=========================================================================================

%   subfunction that reads a list of groups and returns a map that connects the group
%   names and their intended orders

function groupMap = construct_group_map( object, groupInfo )

%   loop over the groups and put them into an ordering Map

    groupMap = containers.Map('KeyType', 'int32', 'ValueType', 'any') ;
    
    for iGroup = 1:length(groupInfo)
        thisGroup = groupInfo(iGroup) ;
        if object.preserveFieldOrder
            fieldOrder = get_field_order_from_group( thisGroup ) ;
        else
            fieldOrder = iGroup ;
        end
        groupMap(fieldOrder) = thisGroup ;
    end

return

%=========================================================================================

%   subfunction that iteratively searches for a specified group / sub-struct

function groupInfo = find_group( object, parentGroupInfo, structName )

    %   look for an exact match
    exactMatch = false(length(parentGroupInfo), 1) ;
    for iGroup = 1:length(parentGroupInfo)
        exactMatch(iGroup) = strcmp( structName, parentGroupInfo(iGroup).Name ) ;
    end
    if any(exactMatch)
        groupInfo = parentGroupInfo(exactMatch) ;
        
    %   if that doesn't happen, look for a match between the groups and the start of the
    %   struct name
    else
        startMatch = false(length(parentGroupInfo), 1) ;
        for iGroup = 1:length(parentGroupInfo)
            startMatch(iGroup) = startsWith( structName, parentGroupInfo(iGroup).Name, ...
                'IgnoreCase', true ) ;
        end
        if any(startMatch)
            groupInfo = find_group( object, parentGroupInfo(iGroup), structName ) ;
        else
            
        %   if we get this far, it means there is no match
            groupInfo = [] ;
        end
    end

return

%=========================================================================================

%   subfunction that reads a list of groups into a scalar struct

function dataStruct = read_groups( object, fileId, groupInfo )

%   loop over the groups and put them into an ordering Map

    groupMap = construct_group_map( object, groupInfo ) ;
    
%   build the struct

    dataStruct = [] ;
    
    for iGroup = 1:length(groupInfo)
        fieldName = get_field_name( groupMap(int32(iGroup)) ) ;
        
        dataStruct.(fieldName) = read_group( object, fileId, groupMap(iGroup) ) ;
    end
        
return

%=========================================================================================

%   subfunction that retrieves the original order of the field

function fieldOrder = get_field_order_from_group( groupStruct )

    attributes = groupStruct.Attributes ;
    orderAttribute = attributes( strcmp( 'FIELD_ORDER', {attributes.Name} ) ) ;
    fieldOrder = orderAttribute.Value+1 ;
    
return

%=========================================================================================

%   subfunction that gets the name of a struct field from the name of the corresponding
%   HDF5 group

function fieldName = get_field_name( groupStruct )

    groupName = groupStruct.Name ;
    fieldName = fliplr(strtok(fliplr(groupName),'/')) ;

return

%=========================================================================================

%   subfunction that returns the contents of a single group as a MATLAB struct field

function fieldValue = read_group( object, fileId, groupStruct )

%   The group can have one of 3 flavors of contents:
%   ==> it can have a dataset, in which case this group should be translated to a MATLAB
%       array of numerics (or logicals)
%   ==> it can have a list of sub-groups in which case it is a struct
%   ==> it can have a list of sub-groups and an attribute that indicates that it is a 
%       struct array 

    if ~isempty(groupStruct.Datasets)
        fieldValue = read_data_array( object, fileId, groupStruct ) ;
    elseif is_struct_array(groupStruct)
        fieldValue = read_struct_array( object, fileId, groupStruct ) ;
    elseif is_parallel_array(groupStruct)
        fieldValue = read_parallel_array_struct( object, fileId, groupStruct ) ;
    else
        fieldValue = read_groups( object, fileId, groupStruct.Groups ) ;
    end
        
return

%=========================================================================================

%   subfunction that returns the contents of a single group as a MATLAB array of logical,
%   numerics, or char arrays

function dataArray = read_data_array( object, fileId, groupStruct )

    groupId = H5G.open( fileId, groupStruct.Name ) ;
    dataSetId = H5D.open( groupId, groupStruct.Datasets.Name ) ;
    dataArray = H5D.read(dataSetId) ;
    H5D.close(dataSetId) ;
    H5G.close(groupId) ;
    
%   If the array is > 1d, transpose so that the MATLAB size is the same as the HDF5 size

    dataArrayDims = size(dataArray) ;
    if (length(dataArrayDims) > 1 && length(find(dataArrayDims > 1)) > 1) 
        dataArray = permute(dataArray,fliplr([1:ndims(dataArray)]));
    end
    
%   if this is supposed to be a logical, handle that now

    if is_logical( groupStruct )
        dataArray = logical(dataArray) ;
    end
    
%   precision conversion

    if isnumeric(dataArray) && object.convertNumericToDouble
        dataArray = double(dataArray) ;
    end
    
%   text conversion

    if iscellstr(dataArray) 
        if object.convertTextToStrings
            dataArray = string(dataArray) ;
        elseif length(dataArray) == 1 && ~is_string_array( groupStruct )
            dataArray = char(dataArray) ;
            if isempty(dataArray)
                dataArray = '' ;
            end
        end
    end
    
return

%=========================================================================================

%   subfunction that determines whether a group is the parent group for a struct array

function isStructArray = is_struct_array( groupStruct )

    attributes = groupStruct.Attributes ;
    if isempty(attributes)
        isStructArray = false ;
    else
        structArrayIndicator = strcmp( 'STRUCT_OBJECT_ARRAY', {attributes.Name} ) ;
        isStructArray = any(structArrayIndicator(:)) ;
    end
    
return

%=========================================================================================

%   subfunction that determines whether a group is intended to contain a struct array that
%   has been represented as parallel arrays of primitive types

function isParallelArray = is_parallel_array( groupStruct )

    attributes = groupStruct.Attributes;
    if isempty(attributes)
        isParallelArray = false;
    else
        parallelArrayIndicator = strcmp( 'PARALLEL_ARRAY', {attributes.Name} ) ;
        isParallelArray = any(parallelArrayIndicator(:)) ;
    end

return

%=========================================================================================

%   subfunction that determines whether a group is intended to contain a logical array (as
%   opposed to numeric)

function isLogical = is_logical( groupStruct )

    attributes = groupStruct.Attributes ;
    logicalIndicator = strcmp( 'LOGICAL_BOOLEAN_ARRAY', {attributes.Name} ) ;
    isLogical = any(logicalIndicator(:)) ;
    
return

%=========================================================================================

%   subfunction that determines whether a group is intended to be a scalar string vs a
%   string array of length 1

function isStringArray = is_string_array( groupStruct )
    attributes = groupStruct.Attributes ;
    if isempty(attributes)
        isStringArray = false ;
    else
        stringArrayIndicator = strcmp( 'STRING_ARRAY', {attributes.Name} ) ;
        isStringArray = any(stringArrayIndicator(:)) ;
    end
return

%=========================================================================================

%   subfunction that reads a struct array

function structArray = read_struct_array( object, fileId, groupStruct )

%   determine the intended dimensions

    structArrayDims = get_struct_array_dimensions( groupStruct ) ;
    
%   loop over struct members

    structArray = struct([]) ;
    for iGroup = groupStruct.Groups(:)'
        
%       put the array together as a vector

        subscripts = get_subscripts( iGroup ) ;
        if length(subscripts) == 2
            structIndex = sub2ind(structArrayDims, subscripts(1), subscripts(2)) ;
        else
            structIndex = sub_2_ind_unknown_dimensions( structArrayDims, subscripts ) ;
        end
        scalarStruct = read_group( object, fileId, iGroup ) ;
        if isempty(structArray)
%            structArray = repmat(scalarStruct, length(groupStruct.Groups), 1) ;
            structArray = cell(length(groupStruct.Groups),1);
            structArray{1} = scalarStruct;
        else
            structArray{structIndex} = scalarStruct ;
        end
    end
    
%   if all the structs have the same fields, the cell array can be converted to a struct
%   array

    structArray = convert_to_struct_array_if_possible(structArray);
%   reshape to intended shape 

    structArray = reshape(structArray, structArrayDims) ;
    
%   if a 1-d struct array is supposed to be a row vector, handle that now

    if (~object.structArrayColumnVector && length(find(structArrayDims~=1)) == 1)
        structArray = structArray(:)' ;
    end
    
return

%=========================================================================================

%   Converts a cell array of structs to a struct array if all the structs have the same
%   fields in the same order 

function structArrayOut = convert_to_struct_array_if_possible( structArrayIn )

    templateStruct = structArrayIn{1};
    templateFieldnames = fieldnames(templateStruct);
    allStructsMatch = true;
    
    for iStruct = structArrayIn(:)'
        iiStruct = iStruct{1};
        allStructsMatch = allStructsMatch && isequal(templateFieldnames, ...
            fieldnames(iiStruct)) ;
    end
    
    if allStructsMatch
        structArrayOut = repmat(templateStruct, length(structArrayIn), 1);
        for iStruct = 1:length(structArrayIn)
            structArrayOut(iStruct) = structArrayIn{iStruct} ;
        end
    else 
        structArrayOut = structArrayIn;
    end

return

%=========================================================================================


%   subfunction that returns dimensions of a struct array

function structArrayDims = get_struct_array_dimensions( groupStruct )

%   get the number of dimensions

    subsDummy = get_subscripts( groupStruct.Groups(1) ) ;
    nDims = length(subsDummy) ;
    
    structArrayDims = zeros(1,nDims) ;
    
%   loop over the array members and get their subscripts

    for iGroup = groupStruct.Groups(:)'
        
        subs = get_subscripts( iGroup ) ;
        structArrayDims = max([structArrayDims ; subs]) ;
        
    end
    
return

%=========================================================================================

%   subfunction that gets the one-based subscripts from a struct array member

function subs = get_subscripts( groupStruct )

    groupName = get_field_name( groupStruct ) ;    
    subs = [] ;
    [~,remainder] = strtok( groupName, '-' ) ;
    while ~isempty(remainder)
        [token,remainder] = strtok( remainder, '-' ) ;
        subs = [subs str2num(token)] ;
    end
    subs = subs + 1 ;
    if (length(subs) == 1)
        subs = [subs 1] ;
    end
    
return

%=========================================================================================

%   subfunction that performs sub2ind for an unknown # of dimensions > 2

function index = sub_2_ind_unknown_dimensions( sizeArray, subscriptsArray )

%   sadly, Mathworks doesn't seem to give me any better way to do this...

    subIndExpression = 'sub2ind([' ;
    for iSize = 1:length(sizeArray)
        subIndExpression = [subIndExpression,num2str(sizeArray(iSize))] ;
        if iSize < length(sizeArray)
            subIndExpression = [subIndExpression,', '] ;
        end
    end
    subIndExpression = [subIndExpression,'], '] ;
    for iDims = 1:length(subscriptsArray)
        subIndExpression = [subIndExpression,num2str(subscriptsArray(iDims))] ;
        if iDims < length(subscriptsArray)
            subIndExpression = [subIndExpression, ', '] ;
        end
    end
    subIndExpression = [subIndExpression,')'] ;
    index = eval(subIndExpression) ;
return

%=========================================================================================

%   subfunction that converts parallel primitive arrays back to a struct array

function parallelStructArray = read_parallel_array_struct( object, fileId, groupStruct )

%   read the parallel arrays into a scalar struct

    struct0 = read_groups( object, fileId, groupStruct.Groups ) ;
    
    if object.reconstituteStructArray
    
%       capture the desired final shape

        fieldNames = fieldnames(struct0) ;
        finalShape = size(struct0.(fieldNames{1})) ;
    
%       convert the arrays into column vectors

        for fieldName = fieldNames(:)'
            struct0.(fieldName{1}) = struct0.(fieldName{1})(:) ;
        end
    
%       convert the resulting scalar struct to a table

        table0 = struct2table(struct0) ;
    
%       convert the table back to a non-scalar struct

        struct1 = table2struct(table0) ;
    
%       reshape to desired final shape

        parallelStructArray = reshape(struct1, finalShape) ;
    
    else
        parallelStructArray = struct0;
    end

return
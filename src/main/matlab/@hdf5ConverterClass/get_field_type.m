function fieldType = get_field_type( fieldValue )
%
% get_field_type -- determine the type of a field, in the context of HDF5 conversion
%
% fieldType = hdf5ConverterClass.get_field_type( fieldValue ) determines the field type
% of the MATLAB object fieldValue. 
%

%=========================================================================================

    fieldType = 'invalid' ;
    
    if isempty(fieldValue)
        fieldType = 'empty' ;
    elseif isnumeric(fieldValue) && isreal(fieldValue)
        fieldType = 'numeric' ;
    elseif islogical(fieldValue)
        fieldType = 'logical' ;
    elseif isstruct(fieldValue)
        fieldType = 'struct' ;
    elseif isobject(fieldValue)
        fieldType = 'object' ;
    else
        try
            cellstr(fieldValue);
            fieldType = 'text' ;
        catch
            % do nothing
        end
    end
    
return


function tf = is_int_valued(v)
%
% is_int_valued -- determines whether a MATLAB array contains all integer values
% 
% tf = is_int_valued(v) returns true if v is an array in which all the entries are
% integer-typed, or are reals that have integer value, and false otherwise. 
%

%=========================================================================================

    tf = isnumeric(v) && isreal(v) && all(v(:) == round(v(:)));
    
return


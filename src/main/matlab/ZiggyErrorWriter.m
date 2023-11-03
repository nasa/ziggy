classdef ZiggyErrorWriter < handle
%
% ZiggyErrorWriter -- writes an HDF5 error file that can be read by Ziggy. The file
% contains the error type and message, plus a stack trace.
%
% The entirety of the class' capabilities is in its constructor. 
%

%=========================================================================================

properties (SetAccess = 'private', GetAccess = 'public')
    message    = [];
    identifier = [];
    stack      = [];
end % properties

%=========================================================================================
    
methods
    
    % constructor
    function obj = ZiggyErrorWriter(lastError)
        if ~exist('lastError', 'var') || isempty(lastError) 
            lastError = lasterror;
        end
        
        % get the module name from the directory
        [filePath, ~, ~] = fileparts(pwd);
        [~, taskDir, ~] = fileparts(filePath);
        taskDirParts = split(taskDir, '-');
        moduleName = taskDirParts{3};
        
        errorFileName = [moduleName, '-error.h5'];
        
        % populate the members of this object
        obj.message = lastError.message;
        obj.identifier = lastError.identifier;
        obj.stack = lastError.stack;
        
        % write the error file
        h = hdf5ConverterClass();
        h.write_file(errorFileName, obj);
        
    end % constructor

end % methods

%=========================================================================================

end % classdef


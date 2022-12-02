classdef ZiggyErrorWriter < handle
%
% ZiggyErrorWriter -- writes an HDF5 error file that can be read by Ziggy. The file
% contains the error type and message, plus a stack trace.
%
% The entirety of the class' capabilities is in its constructor. The constructor can take
% an optional integer argument which is the sequence number to be used in the file name of
% the error file. If no sequence number is supplied (which is the more typical use-case),
% a value of 0 will be used. The resulting file will be <module-name>-error-<seq-num>.h5.
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
    function obj = ZiggyErrorWriter(sequenceNumber)
        if ~exist('sequenceNumber', 'var') || isempty(sequenceNumber)
            sequenceNumber = 0;
        end
        if ~is_int_valued(sequenceNumber) || ~isscalar(sequenceNumber)
            sequenceNumber = 0;
        end
        
        % get the module name from the directory
        [filePath, ~, ~] = fileparts(pwd);
        [~, taskDir, ~] = fileparts(filePath);
        taskDirParts = split(taskDir, '-');
        moduleName = taskDirParts{1};
        
        errorFileName = [moduleName, '-error-', num2str(sequenceNumber),'.h5'];
        
        % populate the members of this object
        lastError = lasterror;
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


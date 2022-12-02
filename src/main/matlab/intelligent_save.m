%% function intelligent_save (varargin)
%
% This function is called in exactly the same way as the built-in Matlab
% 'save' function. Therefore one needs only search and replace existing
% calls to save() in order to make code more robust.
%
% Attempts to save large variables to Matlab binary files have caused
% failures in the pipeline. It is therefore desirable to recognize and
% act to prevent such failures when possible. Only the HDF5 file format
% (specified with the '-v7.3' option) will support saving variables larger
% than 2GB. This function adds a layer between the user and the Matlab
% save() function, which examines the argument list and will automatically
% attempt to save in HDF5 format under the following conditions:
%
% (1) Neither the '-v7.3' nor the '-append' options are present in the
%     argument list.
% (2) One of the variables named in the argument list is >= 2GB in size, or
%     a failed attempt was made to save without the -v7.3 option.
%
% To clarify the first of these conditions, if the '-append' option is
% supplied, then the format is restricted to that of the existing file. If
% the file doesn't already exist, then the inclusion of the '-append'
% option will cause an error. If the '-v7.3' was specified in the argument
% list, then we are already saving in HDF5 format and nothing more can be
% done to ensure success.
%
% INPUTS
%     varargin : A valid argument list for save().
%
% OUTPUTS
%     (none)
%
% NOTES
%   - If you'll be appending variables to a file and it's even *possible*
%     that one may be larger than 2GB, it's very important to create that
%     file using the -v7.3 option. This function can do nothing to remedy
%     the situation in which you need to append a large variable to a
%     .mat file that is not already in HDF5 format. 
%   - Unlike the built-in save(), which produces only a warning, this
%     function will throw an exception on MATLAB:save:sizeTooBigForMATFile
%     if it can't recover by way of its "intelligence".
%   - If you modify this function, use the class testIntelligentSaveClass
%     to run an extensive battery of tests.
%   - See the following Wikipedia entry for more information about HDF5
%     format: https://en.wikipedia.org/wiki/Hierarchical_Data_Format
%%*************************************************************************
function intelligent_save(varargin)
    GB = 1024 ^ 3;
    HDF5_ARG = '-v7.3'; % Use the HDF5 format to allow variables >= 2GB.       
    SIZE_TOO_BIG_MSGID = 'MATLAB:save:sizeTooBigForMATFile';
    
    usingAppendMode =  any(strcmpi(varargin, '-append'));
    usingStructMode =  any(strcmpi(varargin, '-struct'));
              
    %----------------------------------------------------------------------
    % Examine arguments.
    %----------------------------------------------------------------------
    % Determine output file name. 
    if isempty(varargin)
        saveFileName = 'matlab.mat';
    else
        saveFileName = varargin{1};
    end
    
    % Scan varargin for version options. 
    versionStr = get_version_string(varargin);
    
    % Determine variable sizes. If the '-struct' option was passed, then we
    % skip this step and rely on post-analysis of warning message to
    % decide what to do in the event of a failure.
    maxVarSizeBytes = 0;
    if ~usingStructMode
        for iArg = 1:numel(varargin)
            argStr = varargin{iArg};
            if iArg > 1 % First arg is always the filename.
                if ~isempty(argStr) && argStr(1) ~= '-' ...
                        && evalin('caller', ['exist(''', argStr,''', ''var'')'])
                    s = evalin('caller', ['whos(''', argStr, ''')']);
                    if s.bytes > maxVarSizeBytes;
                        maxVarSizeBytes = s.bytes;
                    end
                end
            end
        end
    end

    % If max variable size is greater than 2GB and we are NOT in append
    % mode, then save with the -v7.3 option. If -v7.3 was already specified
    % in the argument list, then do nothing.
    if ~usingAppendMode && ~strcmp(versionStr, HDF5_ARG) && maxVarSizeBytes > 2*GB
        display('intelligent_save: One or more variables >= 2GB. Saving with -v7.3.');
        versionStr = HDF5_ARG;
    end
    
    %----------------------------------------------------------------------
    % Save 
    %----------------------------------------------------------------------
    s = warning; % Save the current warning state.
    
    % Promote this particular warning to error status. Note that this is
    % undocumented Matlab functionality.
    warning('error', SIZE_TOO_BIG_MSGID);
    
    commandStr = construct_command_str(varargin, versionStr);
    try
        evalin('caller', commandStr);
    catch exception        
        % Check to see whether the last save operation caused a
        % sizeTooBigForMATFile error. If so, then try v7.3 if it was not
        % used in the first attempt.
        if strcmpi(exception.identifier, SIZE_TOO_BIG_MSGID) ...
                && ~strcmp(versionStr, HDF5_ARG) && ~usingAppendMode
            fprintf('intelligent_save: Retrying with %s option.\n', HDF5_ARG);
            commandStr = construct_command_str(varargin, HDF5_ARG);
            try
                evalin('caller', commandStr);
            catch exception
                exception_handler(exception, s, saveFileName);
            end
        else
            exception_handler(exception, s, saveFileName);
        end
    end
    
    % restore warning state
    warning(s);
end

%%*************************************************************************
function commandStr = construct_command_str(argList, versionStr)

    % If a versionStr argument was supplied, use it to replace any version
    % specification in argList.
    if exist('versionStr', 'var') && ~isempty(versionStr)
        [~, versionArgIndex] = get_version_string(argList);
        if isempty(versionArgIndex)
            argList{end+1} = versionStr;
        else
            argList{versionArgIndex} = versionStr;
        end
    end
    
    argList = strcat({''''}, argList, {''''});
    argStr = sprintf(', %s', argList{:});
    commandStr = sprintf('save(%s)', argStr(3:end) ); %trim leading comma from argStr.
end

%%*************************************************************************
% Find the first valid version option in the argument list. Return the
% empty string if none are found.
function [versionStr, versionArgIndex] = get_version_string(argList)
    VALID_VERSION_OPTIONS = {'-v4','-v6','-v7','-v7.3'};
    versionStr = ''; % Empty versionStr allows save() to use the default.
   
    isVersionArg = ismember(lower(argList), VALID_VERSION_OPTIONS);
    versionArgIndex = find(isVersionArg, 1, 'first');
    if ~isempty(versionArgIndex)
        versionStr = argList{versionArgIndex};
    end
end

%%*************************************************************************
% Print info, reset warning state, aned rethrow exception.
function exception_handler(exception, warnState, saveFileName)
    fprintf('intelligent_save: Unable to save file %s.\n', saveFileName);
    fprintf('exception.identifier = %s\n', exception.identifier);
    fprintf('exception.message    = %s\n', exception.message);
    fprintf('stack trace:\n------------\n');    
    for i = 1:numel(exception.stack)
        disp(exception.stack(i));
    end
    warning(warnState); 
    rethrow(exception);
end

%********************************** EOF ***********************************

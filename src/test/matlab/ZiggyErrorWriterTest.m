classdef ZiggyErrorWriterTest < matlab.unittest.TestCase
%
% ZiggyErrorWriterTest -- unit test class for ZiggyErrorWriter class
%

%=========================================================================================

properties (GetAccess = 'public', SetAccess = 'private')
    origDir    = [];
    taskDir    = [];
    subTaskDir = [];
    tempDir    = [];
end % properties

%=========================================================================================

methods (TestMethodSetup)
    
    % Create the temp dir, task dir, and subtask dir, and cd into the subtask dir
    function get_temp_directories(obj)
        obj.tempDir = tempdir();
        obj.taskDir = fullfile(obj.tempDir, 'cal-50-100');
        mkdir(obj.taskDir);
        obj.subTaskDir = fullfile(obj.taskDir, 'st-0');
        mkdir(obj.subTaskDir);
        obj.origDir = pwd;
        cd(obj.subTaskDir);
    end
    
end % TestMethodSetup methods

%=========================================================================================

methods (TestMethodTeardown)
    
    % cd back to the original dir and delete the temp dirs
    function delete_temp_directories(obj) 
        cd(obj.origDir);
        rmdir(obj.subTaskDir);
        rmdir(obj.taskDir);
    end
    
end % TestMethodTeardown methods

%=========================================================================================

methods % general methods -- used as part of the error generation
    
    function f1(obj) 
        f2(obj);
    end
    function f2(obj)
        f3(obj);
    end
    function f3(obj) 
        error('ZiggyErrorWriterTest:f3:exampleException', ...
            'f3: example exception');
    end
    
    % Performs the checks on the contents of an error file
    function perform_error_file_checks(obj, expectedFileName, highestCaller, ...
            highestCallerLineNumber)
        
        expectedFile = fullfile(obj.subTaskDir, expectedFileName);
        obj.verifyEqual( exist(expectedFile, 'file'), 2, ...
            'Error file does not exist in sub task directory!' );
        h = hdf5ConverterClass();
        h.set_struct_arrays();
        errorFileContents = h.read_file(expectedFile);
        obj.verifyTrue(isfield(errorFileContents, 'message'), ...
            'No message field!');
        obj.verifyTrue( contains(errorFileContents.message, ...
            'f3: example exception'), 'Error message not as expected!' );
        obj.verifyTrue(isfield(errorFileContents, 'identifier'), ...
            'No identifier field!');
        obj.verifyEqual( errorFileContents.identifier, ...
            'ZiggyErrorWriterTest:f3:exampleException', 'Error identifier not as expected!' );
        obj.verifyTrue(isfield(errorFileContents, 'stack'), ...
            'No stack field!');
        stack = errorFileContents.stack;
        obj.verifyTrue(isstruct(stack) && length(stack) >= 4 && isfield(stack, 'file') ...
            && isfield(stack, 'name') && isfield(stack, 'line'), ...
            'Stack field not as expected!');

        obj.verifyEqual( ZiggyErrorWriterTest.file_name(stack(1).file), ...
            'ZiggyErrorWriterTest.m', 'stack(1) file not as expected!' );
        obj.verifyEqual( stack(1).name, 'ZiggyErrorWriterTest.f3', ...
            'stack(1) name not as expected!' );
        obj.verifyEqual( stack(1).line, 56, 'stack(1) line not as expected!' );

        obj.verifyEqual( ZiggyErrorWriterTest.file_name(stack(2).file), ...
            'ZiggyErrorWriterTest.m', 'stack(2) file not as expected!' );
        obj.verifyEqual( stack(2).name, 'ZiggyErrorWriterTest.f2', ...
            'stack(2) name not as expected!' );
        obj.verifyEqual( stack(2).line, 53, 'stack(2) line not as expected!' );

        obj.verifyEqual( ZiggyErrorWriterTest.file_name(stack(3).file), ...
            'ZiggyErrorWriterTest.m', 'stack(3) file not as expected!' );
        obj.verifyEqual( stack(3).name, 'ZiggyErrorWriterTest.f1', ...
            'stack(3) name not as expected!' );
        obj.verifyEqual( stack(3).line, 50, 'stack(3) line not as expected!' );

        obj.verifyEqual( ZiggyErrorWriterTest.file_name(stack(4).file), ... 
            'ZiggyErrorWriterTest.m', 'stack(4) file not as expected!' );
        obj.verifyEqual( stack(4).name, highestCaller, ...
            'stack(4) name not as expected!' );
        obj.verifyEqual( stack(4).line, highestCallerLineNumber, ...
            'stack(4) line not as expected!' );

        delete(expectedFile)
        
    end
    
end % general methods

%=========================================================================================

methods (Static)
    
    function fn = file_name( fullFileSpec )
        [~, name, ext] = fileparts(fullFileSpec);
        fn = [name, ext];
    end
    
end % static methods

%=========================================================================================

methods (Test)
    
    % tests the case in which the caller supplies a sequence number
    function test_error_with_seq_num(obj)
        try
            obj.f1();
            obj.verifyTrue(false, 'No exception thrown!');
        catch
            ZiggyErrorWriter(5);
            obj.perform_error_file_checks( 'cal-error-5.h5', ...
                'ZiggyErrorWriterTest.test_error_with_seq_num', 134 );
        end % try-catch block 
    end % test_error_with_seq_num method
    
    % Tests the case in which the caller does not supply a sequence number
    function test_error_no_seq_num(obj)
       try
            obj.f1();
            obj.verifyTrue(false, 'No exception thrown!');
        catch
            ZiggyErrorWriter();
            obj.perform_error_file_checks( 'cal-error-0.h5', ...
                'ZiggyErrorWriterTest.test_error_no_seq_num', 146 );
        end % try-catch block 
    end % test_error_no_seq_num method

    % Tests the case in which the caller supplies an invalid seq num
    function test_error_invalid_seq_num(obj)
       try
            obj.f1();
            obj.verifyTrue(false, 'No exception thrown!');
        catch
            ZiggyErrorWriter('string');
            obj.perform_error_file_checks( 'cal-error-0.h5', ...
                'ZiggyErrorWriterTest.test_error_invalid_seq_num', 158 );
        end % try-catch block 
    end % test_error_no_seq_num method

end % test methods
    
end % classdef


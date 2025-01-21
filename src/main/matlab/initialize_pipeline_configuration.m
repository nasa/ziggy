function initialize_pipeline_configuration( csciNamesToSkip ) 
%
% initialize_pipeline_configuration -- sets up the necessary MATLAB paths and etc. for use
% with a Ziggy-based pipeline.
%
% initialize_pipeline_configuration() adds the Ziggy third-party runtime JAR files to the
% MATLAB Java class path; it adds the pipeline and Ziggy MATLAB source file directories
% to the MATLAB search path; and finally it adds the pipeline and Ziggy JAR files to the
% MATLAB Java class path, if those are present (i.e., if they have been compiled already).
% The function assumes the existence of an environment variable, ZIGGY_CODE_ROOT, which
% is pointed to the top-level directory of the Ziggy directory tree. 
%
% During the Ziggy build, Ziggy copies this and other MATLAB files to its build directory:
% ziggy/build/src/main/matlab. This function is written with the assumption that the user
% will run from the build location and not from ziggy/src/main/matlab. 
%
% If argument csciNamesToSkip is defined, it must be a cell array of names of CSCIs that
% are not to be added to the MATLAB search path. 
%

%=========================================================================================

%   We should only do this if we are running interactively or are in the compiler

    if (~isdeployed || ismcc)
        
        warning('OFF', 'MATLAB:javaclasspath:jarAlreadySpecified') ;
        
%       get the Ziggy code root env var

%       get the location of this file (it's in ziggy/build/src/main/matlab, so once we
%       find this file we know where the libs directory is)

        thisFile = strsplit(mfilename('fullpath'), filesep) ;
        thisFile{1} = filesep ;
        
%       need to lop off the last 4 parts of the path to this file: the filename, "matlab",
%       "main", "src"

        nPathSteps = length(thisFile) - 4 ;
        buildDir = fullfile(thisFile{1:nPathSteps}, 'build');
        thisLocation = fullfile(thisFile{1:length(thisFile)-1}) ;
        
%       add this location to the path

        addpath(thisLocation) ;
        
        if exist(fullfile(buildDir, 'libs', 'pipeline-classpath.jar'), 'file') ~= 0
            javaaddpath(fullfile(buildDir, 'libs', 'pipeline-classpath.jar')) ;
        else
            jarlist = dir(fullfile(buildDir, 'libs', '*.jar')) ;
            for iJar = jarlist(:)'
                javaaddpath(fullfile(buildDir, 'libs', iJar.name)) ;
            end
        end
        
%       construct pipeline path object and pipeline properties object

        pipelineProperties = pipelinePropertiesClass();
        pipelinePathObject = pipelinePathClass() ;
        
%       add CSCI paths to the MATLAB path

        csciNames = pipelinePathObject.get_csci_names ;
        csciNames = csciNames(:)' ;
        
        for iCsci = csciNames
            if ~exist('csciNamesToSkip', 'var') || ...
                    ~any(ismember(csciNamesToSkip, iCsci{1}))
                csciMatlabDirs = pipelinePathObject.get_csci_paths( iCsci{1} ) ;
                pipelinePathClass.addpath_cell_array( csciMatlabDirs ) ;
                csciMexDirs = pipelinePathObject.get_csci_mex_paths( iCsci{1} ) ;
                pipelinePathClass.addpath_cell_array( csciMexDirs ) ;
            end
        end

%       log4j configuration

        ziggyHomeDir = pipelineProperties.get_property('ziggy.home.dir') ;
        if (ziggyHomeDir(end) == filesep)
            ziggyHomeDir = ziggyHomeDir(1:end-1) ;
        end
        log4jConfigFile = fullfile(ziggyHomeDir, 'etc', ...
            'log4j2.xml') ;
        if (exist(log4jConfigFile, 'file'))
            disp(['Setting log4j2 configuration file to ' log4jConfigFile]);
            java.lang.System.setProperty('log4j2.configurationFile', log4jConfigFile);
            log4jDestination = fullfile(pipelineProperties.get_property( ...
                'ziggy.pipeline.home.dir'), 'logs', 'matlab.log');
            disp(['Setting log4j log file to: ', log4jDestination]);
            java.lang.System.setProperty('ziggy.logFile', log4jDestination);
        else
            disp('No log4j config file found') ;
        end

        warning('ON', 'MATLAB:javaclasspath:jarAlreadySpecified') ;

    end % if ~isdeployed || ismcc
return

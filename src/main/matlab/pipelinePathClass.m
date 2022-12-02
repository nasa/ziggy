classdef pipelinePathClass < handle
%
%   pipelinePathClass -- MATLAB class that dynamically locates and provides path
%   information on directories that are of use to MATLAB in the context of pipeline
%   development and execution. This class relies upon the pipelinePropertiesClass to
%   obtain pipeline configuration properties relevant to the directory locations. 

%=========================================================================================


%   NB: haven't decided yet how or even whether to handle test data or release test data
%   in this context
properties (GetAccess = 'public', SetAccess = 'protected')
    csciPathStruct      = [] ;
%    testDataRoot        = [] ;
%    releaseTestDataRoot = [] ;
end


methods
    
%=========================================================================================

% constructor

function object = pipelinePathClass()
    
%   get the properties

    propertiesObject = pipelinePropertiesClass() ;
    
%   get the code directories, assuming they are 1 level above the build directories, which in
%   turn are in the properties

    pipelineHomeDir = propertiesObject.get_property('pipeline.home.dir') ;
    ziggyHomeDir   = propertiesObject.get_property('ziggy.home.dir') ;
    if (pipelineHomeDir(end) == filesep)
        pipelineHomeDir = pipelineHomeDir(1:end-1) ;
    end
    if (ziggyHomeDir(end) == filesep)
        ziggyHomeDir = ziggyHomeDir(1:end-1) ;
    end
    pipelineCodeDir = fileparts(pipelineHomeDir) ;
    ziggyCodeDir   = fileparts(ziggyHomeDir) ;
    
%   dynamically construct the CSCI list and populate it with everything

    object.csciPathStruct = object.set_csci_paths( {pipelineCodeDir, ziggyCodeDir} ) ;
    
end % constructor

%=========================================================================================

% get MATLAB paths for the csci

function paths = get_csci_paths( object, csciName )
    paths = object.csciPathStruct( object.look_up_csci( csciName ) ).mFilePaths ;
end

% get MEX paths for the CSCI

function paths = get_csci_mex_paths( object, csciName )
    paths = object.csciPathStruct( object.look_up_csci( csciName ) ).mexFilePaths ;
end

% get unit test paths for the CSCI

function paths = get_test_paths( object, csciName )
    paths = object.csciPathStruct( object.look_up_csci( csciName ) ).unitTestPaths ;
end

% get unit test data paths for the CSCI

function paths = get_test_data_paths( object, csciName )
    paths = object.csciPathStruct( object.look_up_csci( csciName ) ).unitTestDataPaths ;
end

% get release test paths for the CSCI

function paths = get_release_test_paths( object, csciName )
    paths = object.csciPathStruct( object.look_up_csci(csciName) ).releaseTestPaths ;
end

% get release test data paths for the CSCI

function paths = get_release_test_data_paths( object, csciName )
    paths = object.csciPathStruct( object.look_up_csci(csciName) ).releaseTestDataPaths ;
end

% get CSCI names

function csciNames = get_csci_names( object )
    csciNames = { object.csciPathStruct.csciName } ;
end

% set unit test path environment variable

% function set_unit_test_data_root( object, pathName )
%     
% %   set the environment variable -- we need to do this so that if another
% %   spocMatlabPathclass object is later instantiated, it picks up this value, rather than
% %   forcing the user to set it again
% 
%     spocMatlabPathClass.set_unit_test_data_path( pathName ) ;
%     object.testDataRoot = getenv( object.dataRootEnvVar ) ;
%     
% %   populate the path information
%     
%     object.set_csci_test_data_paths(true) ;
%     
% end
% 
% % set release test path environment variable
% 
% function set_release_test_data_root( object, pathName )
%     
% %   use the static method to set the environment variable -- we need to do this so that if
% %   another instance of spocMatlabPathClass is instantiated, it picks up this value,
% %   rather than forcing the user to set it again
%     
%     spocMatlabPathClass.set_release_test_data_path( pathName ) ;
%     object.releaseTestDataRoot = getenv( object.releaseTestDataRootEnvVar ) ;
%     
% end

%=========================================================================================


end % public methods

methods (Access = 'protected')

% dynamically build the CSCI list and get information about its mexfiles    
    
function csciPathStruct1 = set_csci_paths( object, topDirs )
    
%   eliminate duplicates from topDirs, but preserve the ordering

    [~,iA] = unique(topDirs) ;
    iA = sort(iA) ;
    topDirs = topDirs(iA) ;
    
%   define the struct template

    csciPathStructTemplate = struct( 'csciName', [], 'mFilePaths', [], ...
        'mexFilePaths', [], 'unitTestPaths', [], 'unitTestDataPaths', [], ...
        'releaseTestPaths', [], 'releaseTestDataPaths', [] ) ;
    
    csciPathStruct1 = struct([]) ;
    
%   loop over topDirs entries

    for iTopDir = topDirs(:)'
        topDir = iTopDir{1} ;
    
%       get the directory at the code root level, removing . and .. entries

        topLevelDir = pipelinePathClass.search_for_subdirs_and_mfiles( topDir ) ;
    
%       make one struct entry for each of the surviving directories at this level

        csciPathStruct0 = repmat(csciPathStructTemplate, length(topLevelDir), 1 ) ;
        isCsci = true(size(csciPathStruct0)) ;
    
%       loop over CSCIs

        for iCsci = 1:length(csciPathStruct0)
            csciPathStruct0(iCsci).csciName = topLevelDir(iCsci).name ;
        
%           look for a MATLAB path or a build matlab path, if neither one is there move on to 
%           the next directory

            csciRoot = fullfile( topDir, csciPathStruct0(iCsci).csciName ) ;
            matlabPathRoot = fullfile( csciRoot, 'src', 'main', 'matlab' ) ;
            buildPathRoot = fullfile( csciRoot, 'build', 'src', 'main', 'matlab' ) ;
            if ~exist( matlabPathRoot, 'dir' ) && ~exist( buildPathRoot, 'dir' )
                isCsci(iCsci) = false ;
                continue ;
            end
        
%           recursively search down in the matlabPathRoot to find every bottom point in the
%           directory tree; those are the MATLAB paths

            if exist( matlabPathRoot, 'dir' )
                csciPathStruct0(iCsci).mFilePaths = pipelinePathClass.recursively_find_bottom_dirs( ...
                    matlabPathRoot ) ;
            end
        
%           add anything in the $CSCI_NAME/build/src/main/matlab folder, if it exists

            if exist( buildPathRoot, 'dir' )
                csciPathStruct0(iCsci).mFilePaths = [...
                    csciPathStruct0(iCsci).mFilePaths ; ...
                    pipelinePathClass.recursively_find_bottom_dirs( buildPathRoot )] ;
            end
        
%           recursively search down src/test/matlab for test directories on this CSCI

            matlabTestRoot = fullfile( csciRoot, 'src', 'test', 'matlab' ) ;
            if exist( matlabTestRoot, 'dir' )
                csciPathStruct0(iCsci).unitTestPaths = pipelinePathClass.recursively_find_bottom_dirs( ...
                    matlabTestRoot ) ;
            
                % Also add to the main file path.
                csciPathStruct0(iCsci).mFilePaths = [ ...
                    csciPathStruct0(iCsci).mFilePaths ; ...
                    csciPathStruct0(iCsci).unitTestPaths ] ;
            end
        
% %       similar process for the release test directories
% 
%         matlabReleaseTestRoot = fullfile( csciRoot, 'src', 'release-test', 'matlab' ) ;
%         if exist( matlabReleaseTestRoot, 'dir' )
%             csciPathStruct(iCsci).releaseTestPaths = ...
%                 spocMatlabPathClass.recursively_find_bottom_dirs( ...
%                 matlabReleaseTestRoot ) ;
%         end
        
%           mexfile paths are relatively easy, since they go into the build/lib directory
%           under the given CSCI

            mexPathRoot = fullfile( csciRoot, 'build', 'lib' ) ;
            if exist( mexPathRoot, 'dir' )
                libsDir = dir(mexPathRoot) ;
                mexSuffix = regexp( {libsDir.name}, '(\.mex)' ) ;
                mexSuffix = cell2mat(mexSuffix) ;
                if ~isempty(mexSuffix)
                    csciPathStruct0(iCsci).mexFilePaths = {mexPathRoot} ;
                end
            end
                
        end % loop over potential CSCIs
        
%       now -- some of these things are NOT CSCI's, and the sign of this is that they
%       contain no matlab path. Those cases were marked using the isCsci logical array
%       earlier. Trim away the ones which aren't CSCIs.

        csciPathStruct0 = csciPathStruct0(isCsci) ;
        
%       capture the current dir's CSCIs        
        
        csciPathStruct1 = [csciPathStruct1 ; csciPathStruct0] ;
        
    end % loop over topDirs
    
    
end % set_csci_paths function

% function to look up a CSCI name

function csciIndex = look_up_csci( object, csciName )
    allCsciNames = {object.csciPathStruct.csciName} ;
    csciIndex = strcmpi( csciName, allCsciNames ) ;
end

% function to populate the CSCI test data directories

% function set_csci_test_data_paths( object, displayMissing )
%     
% %   default value of displayMissing is false
% 
%     if ~exist('displayMissing', 'var') || isempty( displayMissing )
%         displayMissing = false ;
%     end
%     
% %   loop over CSCIs 
%     
%     for iCsci = 1:length( object.csciPathStruct )
%         thisCsci = object.csciPathStruct(iCsci).csciName ;
%         
% %       look for UNIT_TEST_DATA_ROOT/csciName/test, which is the expected directory for
% %       unit test data for this CSCI
% 
%         csciTestDataDir = fullfile( object.testDataRoot, thisCsci, 'test' ) ;
%         if exist( csciTestDataDir, 'dir' )
%             object.csciPathStruct(iCsci).unitTestDataPaths = {csciTestDataDir} ;
%         else
%             if displayMissing
%                 disp(['No unit test data directory for CSCI ',thisCsci]) ;
%             end
%             object.csciPathStruct(iCsci).unitTestDataPaths = [] ;
%         end
%         
% %       same sort of deal for release test data
% 
%         csciTestDataDir = fullfile( object.testDataRoot, thisCsci, 'release-test' ) ;
%         if exist( csciTestDataDir, 'dir' )
%             object.csciPathStruct(iCsci).releaseTestDataPaths = {csciTestDataDir} ;
%         else
%             if displayMissing
%                 disp(['No release test data directory for CSCI ',thisCsci]) ;
%             end
%             object.csciPathStruct(iCsci).releaseTestDataPaths = [] ;
%         end
%         
%     end
%         
% end

end % protected methods

%=========================================================================================

methods (Access = 'protected', Static)

% given a directory location, search that directory for sub-directories which are not '.',
% '..', or '@*'; return the names of those subdirs. Also return a logical indicating
% whether any of the non-directory files are of the form '*.m'.

function [nonTrivialDir,containsMfiles] = search_for_subdirs_and_mfiles( location )
    topLevelDir = dir(location) ;
    ordinaryFiles = topLevelDir(~[topLevelDir.isdir]) ;
    topLevelDir(~[topLevelDir.isdir]) = [] ;
    topLevelName = {topLevelDir.name} ;
    selfReferentialDir = strncmp('.',topLevelName,1) ;
    matlabClassDir     = strncmp('@',topLevelName,1) ;
    topLevelDir(selfReferentialDir | matlabClassDir) = [] ;
    nonTrivialDir = topLevelDir ;
    mfiles = regexp({ordinaryFiles.name},'(\.m)') ;
    mfiles = cell2mat(mfiles) ;
    containsMfiles = ~isempty(mfiles) ;
end

% recursively search a specified directory to find all the subdirs which are either the
% bottom of the tree (ie, no subdirs themselves, other than '@*') or else contain 

function foundDirs = recursively_find_bottom_dirs( topOfPath )
    foundDirs = [] ;
    [nextLevelDir,mFilesPresent] = pipelinePathClass.search_for_subdirs_and_mfiles( ...
        topOfPath ) ;
    if mFilesPresent || isempty(nextLevelDir)
        foundDirs = [foundDirs ; {topOfPath}] ;
    end
    if ~isempty( nextLevelDir )
        for iNext = 1:length(nextLevelDir)
            moreDirs = pipelinePathClass.recursively_find_bottom_dirs( fullfile( ...
                topOfPath, nextLevelDir(iNext).name ) ) ;
            if ~isempty( moreDirs )
                foundDirs = [foundDirs ; moreDirs] ;
            end
        end
    end 
end % recursively find bottom dirs

end % protected static methods
    
%=========================================================================================

methods (Static)
    
function addpath_cell_array( cellArray )
    cellArray = cellArray(:)' ;
    for iArray = cellArray
        addpath( iArray{1} ) ;
    end
end

% function set_unit_test_data_path( pathName )
%     
%     setenv( pipelinePathClass.dataRootEnvVar, pathName ) ;
% 
% end
% 
% function set_release_test_data_path( pathName )
%     
%     setenv( pipelinePathClass.releaseTestDataRootEnvVar, pathName ) ;
%     
% end
    
end % static methods
end


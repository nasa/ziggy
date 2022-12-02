classdef pipelinePropertiesClass < handle
%
%   pipelinePropertiesClass -- MATLAB class that provides read-only access to the
%   properties used to configure a Ziggy-based pipeline.
%
%   This class wraps a handful of Java classes that do all the actual work of handling
%   system properties, pipeline properties, and Ziggy properties. The class assumes that
%   the environment variable PIPELINE_CONFIG_PATH contains the location of the pipeline's
%   properties file, and that the pipeline properties file in turn contains the location
%   of the Ziggy properties file. 
%

%=========================================================================================

properties (GetAccess = 'public', SetAccess = 'protected')
    pipelineConfigPath  = [] ;
    ziggyConfigPath    = [] ;
    configurationObject = [] ;
end
    
%=========================================================================================

methods
    
%   Constructor

function obj = pipelinePropertiesClass()

    import org.apache.commons.configuration.CompositeConfiguration;
    import org.apache.commons.configuration.SystemConfiguration;
    import org.apache.commons.configuration.PropertiesConfiguration;
    import java.io.File ;
    
    obj.pipelineConfigPath = getenv('PIPELINE_CONFIG_PATH') ;
    
%   construct the configuration object and populate it with the system configuration

    configurationObject = CompositeConfiguration() ;
    systemConfiguration = SystemConfiguration() ;
    configurationObject.addConfiguration(systemConfiguration) ;
    
%   get the pipeline configuration

    pipelineConfigFile = File(obj.pipelineConfigPath) ;
    pipelineConfiguration = PropertiesConfiguration(pipelineConfigFile) ;
    configurationObject.addConfiguration(pipelineConfiguration) ;
    
%   get the location of the Ziggy configuration

    obj.ziggyConfigPath = char(configurationObject.getString('ziggy.config.path')) ;
    if ~isempty(obj.ziggyConfigPath) && exist(obj.ziggyConfigPath,'file') ~= 0
        ziggyConfigFile = File(obj.ziggyConfigPath) ;
        ziggyConfiguration = PropertiesConfiguration(ziggyConfigFile) ;
        configurationObject.addConfiguration(ziggyConfiguration) ;
    end
    obj.configurationObject = configurationObject ;
     
end

%   Returns the value of the named property, if it is present in the properties object;
%   the value is returned as a string.

function prop = get_property( obj, propName )
    
    prop = char(obj.configurationObject.getString(propName)) ;
    
end % get_property function

end % public methods

%=========================================================================================

end


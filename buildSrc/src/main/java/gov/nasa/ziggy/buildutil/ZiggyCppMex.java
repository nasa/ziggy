package gov.nasa.ziggy.buildutil;

import java.io.File;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.OutputFiles;
import org.gradle.api.tasks.TaskAction;

import gov.nasa.ziggy.buildutil.ZiggyCppPojo.BuildType;

/**
 * Performs mexfile builds for Gradle. The sequence of build steps is as follows:
 * 1. The C/C++ files are compiled with a MATLAB_MEX_FILE compiler directive.
 * 2. The object files from (1) are linked into a shared object library.
 * 3. The actual mexfiles are built from the appropriate object files and the 
 *     shared object library.
 * The user specifies the following:
 * 1. The path to the C/C++ files
 * 2. Compiler and linker options, including libraries, library paths, include 
 *     file paths, optimization flags.
 * 3. The names of the desired mexfiles.
 * 4. Optionally, a name for the shared library (otherwise a default name is generated
 *     from the C++ source file path).
 * 
 * Because it is effectively impossible to unit test any class that extends DefaultTask,
 * the actual workings of the ZiggyCppMex class are in a separate class, ZiggyCppMexPojo,
 * which has appropriate unit testing. This class provides a thin interface between 
 * Gradle and the ZiggyCppMexPojo class.
 * 
 * @author PT
 *
 */
public class ZiggyCppMex extends DefaultTask {

    private static final Logger log = LoggerFactory.getLogger(ZiggyCppMex.class);
    private ZiggyCppMexPojo ziggyCppMexObject = new ZiggyCppMexPojo();

    /**
     * Default constructor. This constructor populates the 3 project directories needed by the
     * ZiggyCppMexPojo class (build, root, and project), and populates the default compile and link
     * options, and the default release and debug optimization options, if these are set as extra
     * properties in the Project object.
     */
    public ZiggyCppMex() {
        Project project = getProject();
        ziggyCppMexObject.setBuildDir(project.getBuildDir());
        ziggyCppMexObject.setRootDir(ZiggyCpp.pipelineRootDir(project));
        ziggyCppMexObject.setProjectDir(project.getProjectDir());
        if (project.hasProperty(ZiggyCppMexPojo.DEFAULT_COMPILE_OPTIONS_GRADLE_PROPERTY)) {
            ziggyCppMexObject.setCompileOptions(ZiggyCppPojo.gradlePropertyToList(
                project.property(ZiggyCppMexPojo.DEFAULT_COMPILE_OPTIONS_GRADLE_PROPERTY)));
        }
        if (project.hasProperty(ZiggyCppMexPojo.DEFAULT_LINK_OPTIONS_GRADLE_PROPERTY)) {
            ziggyCppMexObject.setLinkOptions(ZiggyCppPojo.gradlePropertyToList(
                project.property(ZiggyCppMexPojo.DEFAULT_LINK_OPTIONS_GRADLE_PROPERTY)));
        }
        if (project.hasProperty(ZiggyCppMexPojo.DEFAULT_RELEASE_OPTS_GRADLE_PROPERTY)) {
            ziggyCppMexObject.setReleaseOptimizations(ZiggyCppPojo.gradlePropertyToList(
                project.findProperty(ZiggyCppMexPojo.DEFAULT_RELEASE_OPTS_GRADLE_PROPERTY)));
        }
        if (project.hasProperty(ZiggyCppMexPojo.DEFAULT_DEBUG_OPTS_GRADLE_PROPERTY)) {
            ziggyCppMexObject.setDebugOptimizations(ZiggyCppPojo.gradlePropertyToList(
                project.findProperty(ZiggyCppMexPojo.DEFAULT_DEBUG_OPTS_GRADLE_PROPERTY)));
        }
        setMatlabPath();
    }

    /** Provides access to the ZiggyCppMexPojo method action() for Gradle. */
    @TaskAction
    public void action() {
        ziggyCppMexObject.action();
    }

    /** Specifies that the C/C++ source files are the input files for this Gradle task. */
    @InputFiles
    public List<File> getCppFiles() {
        return ziggyCppMexObject.getCppFiles();
    }

    /** Specifies that the mexfiles are the output files for this Gradle task. */
    @OutputFiles
    public List<File> getMexfiles() {
        return ziggyCppMexObject.getMexfiles();
    }

    /** Specifies that the shared object library is also an output file for this Gradle task. */
    @OutputFile
    public File getBuiltFile() {
        return ziggyCppMexObject.getBuiltFile();
    }

    // Below are setters and getters for the ZiggyCppMexPojo members that must be mutated
    // by ZiggyCppMex tasks in gradle. In principle only setters are needed, but in the
    // interest of sanity getters are also provided. Note that not all ZiggyCppMexPojo
    // members need to be set by ZiggyCppMex, there are a number that are used internally
    // and are not set as part of a task.
    //
    // Note that the setters take Object and List<Object> rather than String and List<String>.
    // This is because Gradle allows its text string objects to be either Java String class or
    // Groovy GString class. Consequently, we pass everything to ZiggyCppMexPojo as Objects, and
    // ZiggyCppMexPojo uses the toString() methods to convert everything to Java Strings.

    // Path to the C++ source files
    public void setCppFilePath(Object cppFilePath) {
        ziggyCppMexObject.setCppFilePath(cppFilePath);
    }

    public String getCppFilePath() {
        return ziggyCppMexObject.getCppFilePaths().get(0);
    }

    // Paths for include files
    public void setIncludeFilePaths(List<? extends Object> includeFilePaths) {
        ziggyCppMexObject.setIncludeFilePaths(includeFilePaths);
    }

    public List<String> getIncludeFilePaths() {
        return ziggyCppMexObject.getIncludeFilePaths();
    }

    // paths for libraries that must be linked in
    public void setLibraryPaths(List<? extends Object> libraryPaths) {
        ziggyCppMexObject.setLibraryPaths(libraryPaths);
    }

    public List<String> getLibraryPaths() {
        return ziggyCppMexObject.getLibraryPaths();
    }

    // Libraries that must be linked in
    public void setLibraries(List<? extends Object> libraries) {
        ziggyCppMexObject.setLibraries(libraries);
    }

    public List<String> getLibraries() {
        return ziggyCppMexObject.getLibraries();
    }

    // compiler options
    public void setCompileOptions(List<? extends Object> compileOptions) {
        ziggyCppMexObject.setCompileOptions(compileOptions);
        ;
    }

    public List<String> getCompileOptions() {
        return ziggyCppMexObject.getCompileOptions();
    }

    // linker options
    public void setLinkOptions(List<? extends Object> linkOptions) {
        ziggyCppMexObject.setLinkOptions(linkOptions);
    }

    public List<String> getLinkOptions() {
        return ziggyCppMexObject.getLinkOptions();
    }

    // output type (executable, shared library, static library)
    public void setOutputType(Object outputType) {
        ziggyCppMexObject.setOutputType(outputType);
    }

    public BuildType getOutputType() {
        return ziggyCppMexObject.getOutputType();
    }

    // Name of shared library file to be produced
    public void setOutputName(Object name) {
        ziggyCppMexObject.setOutputName(name);
    }

    public String getOutputName() {
        return ziggyCppMexObject.getOutputName();
    }

    // Names of mexfiles to be produced
    public void setMexfileNames(List<? extends Object> mexfileNames) {
        ziggyCppMexObject.setMexfileNames(mexfileNames);
    }

    public List<String> getMexfileNames() {
        return ziggyCppMexObject.getMexfileNames();
    }

    /**
     * Sets the path to the MATLAB executable. This searches the following options in the following
     * order: 1. If there is a project extra property, matlabPath, use that. 2. If no matlabPath
     * extra property, use the PATH environment variable to find the first path that includes both
     * MATLAB (case-insensitive) and "bin" (case-sensitive). Use that. 3. If neither the path env
     * var nor the project have the needed information, use the MATLAB_HOME env var. 4. If all of
     * the above fail, throw a GradleException.
     */
    public void setMatlabPath() {
        String matlabPath = null;
        Project project = getProject();
        if (project.hasProperty(ZiggyCppMexPojo.MATLAB_PATH_PROJECT_PROPERTY)) {
            matlabPath = project.findProperty(ZiggyCppMexPojo.MATLAB_PATH_PROJECT_PROPERTY)
                .toString();
            log.info("MATLAB path set from project extra property: " + matlabPath);
        }
        if (matlabPath == null) {
            String systemPath = System.getenv("PATH");
            if (systemPath != null) {
                String[] systemPaths = systemPath.split(":");
                for (String path : systemPaths) {
                    String pathLower = path.toLowerCase();
                    if (pathLower.contains("matlab") && path.endsWith("bin")) {
                        matlabPath = path.substring(0, path.length() - 4);
                        log.info("MATLAB path set from PATH environment variable: " + matlabPath);
                        break;
                    }
                }
            }
        }
        if (matlabPath == null) {
            String matlabHome = System.getenv(ZiggyCppMexPojo.MATLAB_PATH_ENV_VAR);
            if (matlabHome != null) {
                matlabPath = matlabHome;
                log.info("MATLAB path set from MATLAB_HOME environment variable: " + matlabPath);
            }
        }
        if (matlabPath == null) {
            throw new GradleException(
                "Unable to find MATLAB path in Gradle, PATH env var, or MATLAB_HOME env var");
        }
        ziggyCppMexObject.setMatlabPath(matlabPath);
    }
}

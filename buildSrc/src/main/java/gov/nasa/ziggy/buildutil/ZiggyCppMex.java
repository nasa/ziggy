package gov.nasa.ziggy.buildutil;

import java.io.File;
import java.util.List;

import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.OutputFiles;
import org.gradle.api.tasks.TaskAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.buildutil.ZiggyCppPojo.BuildType;

/**
 * Performs mexfile builds for Gradle. The sequence of build steps is as follows:
 * <ol>
 * <li>The C/C++ files are compiled with a MATLAB_MEX_FILE compiler directive. The resulting object
 * files are saved in $buildDir/obj .
 * <li>The object files from (1) are linked into a shared object library. The shared object library
 * is saved in $buildDir/lib .
 * <li>The actual mexfiles are built from the appropriate object files and the shared object
 * library. The mexfile is saved in $buildDir/mex .
 * </ol>
 * <p>
 * The user specifies the following:
 * <ol>
 * <li>The path to the C/C++ files
 * <li>Compiler and linker options, including libraries, library paths, include file paths,
 * optimization flags.
 * <li>The names of the desired mexfiles.
 * <li>Optionally, a name for the shared library (otherwise a default name is generated from the C++
 * source file path).
 * </ol>
 * <p>
 * Usage example: assume that mexfile main functions are in files foo.cpp and bar.cpp, which are in
 * the src/cpp/foo subdirectory of $projectDir, along with header files and potentially additional
 * C++ files. The user wishes to build mexfiles from these functions, which depend on additional
 * libraries elsewhere on the system. The task
 *
 * <pre>
 * task mexFoo(type : ZiggyCppMex) {
 *     sourceFilePath   = "$projectDir/src/cpp/foo"
 *     includeFilePaths = ["$projectDir/src/cpp/foo", "$projectDir/include"]
 *     libraryPaths     = ["$buildDir/lib", "$rootDir/project2/build/lib"]
 *     libraries        = ["blas", "lapack"]
 *     mexfileNames     = ["foo", "bar"]
 *     compileOptions   = ["std=c++11", "O2"]
 *     outputName       = "baz"
 * }
 * </pre>
 *
 * will compile the contents of $projectDir/src/cpp/mex into object files in $buildDir/obj, and link
 * them into library libbaz.(so, dylib) in $buildDir/lib; mexfiles foo.(mexa64, mexmaci64,
 * mexmaca64) and bar.(mexa64, mexmaci64, mexmaca64) are then generated in $buildDir/mex by linking
 * against libraries libblas and liblapack, which are located in the directories specified by the
 * libraryPaths property. Options "-std=c++11" and "-O2" will be applied at compile time; link
 * options can be specified with the linkOptions property (not shown).
 * <p>
 * Note that the user does not need to specify the locations for MATLAB include files, the locations
 * or names of MATLAB libraries, or indeed the location of MATLAB's mex executable. The MATLAB
 * top-level directory comes from either the $matlabHome Gradle variable or the $MATLAB_HOME
 * environment variable; from this location, the operating system, and the CPU architecture, all of
 * the MATLAB libraries, library directories, and mexfile suffix are automatically determined.
 * <p>
 * By default, object files produced by {@link ZiggyCppMex} are saved in $buildDir/obj, libraries in
 * $buildDir/lib, and mexfiles in $buildDir/mex. This behavior can be overridden. If the property
 * outputDir is specified, then all 3 types of files will be stored in the location specified by
 * outputDir. If the property outputDirParent is specified, then the 3 file types will be saved in
 * <outputDirParent>/obj, <outputDirParent>/lib, and <outputDirParent>/mex, respectively. If both
 * outputDir and outputDirParent are specified, the outputDir value will be used and the
 * outputDirParent value ignored.
 * <p>
 * Because it is effectively impossible to unit test any class that extends DefaultTask, the actual
 * workings of the ZiggyCppMex class are in a separate class, ZiggyCppMexPojo, which has appropriate
 * unit testing. This class provides a thin interface between Gradle and the ZiggyCppMexPojo class.
 *
 * @author PT
 */
public class ZiggyCppMex extends DefaultTask {

    private static final Logger log = LoggerFactory.getLogger(ZiggyCppMex.class);

    private static final String DEFAULT_COMPILE_OPTIONS_GRADLE_PROPERTY = "defaultCppMexCompileOptions";
    private static final String DEFAULT_LINK_OPTIONS_GRADLE_PROPERTY = "defaultCppMexLinkOptions";
    private static final String DEFAULT_RELEASE_OPTS_GRADLE_PROPERTY = "defaultCppMexReleaseOptimizations";
    private static final String DEFAULT_DEBUG_OPTS_GRADLE_PROPERTY = "defaultCppMexDebugOptimizations";
    private static final String MATLAB_PATH_PROJECT_PROPERTY = "matlabHome";
    private static final String MATLAB_PATH_ENV_VAR = "MATLAB_HOME";

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
        if (project.hasProperty(DEFAULT_COMPILE_OPTIONS_GRADLE_PROPERTY)) {
            ziggyCppMexObject.setcppCompileOptions(ZiggyCppPojo
                .gradlePropertyToList(project.property(DEFAULT_COMPILE_OPTIONS_GRADLE_PROPERTY)));
        }
        if (project.hasProperty(DEFAULT_LINK_OPTIONS_GRADLE_PROPERTY)) {
            ziggyCppMexObject.setLinkOptions(ZiggyCppPojo
                .gradlePropertyToList(project.property(DEFAULT_LINK_OPTIONS_GRADLE_PROPERTY)));
        }
        if (project.hasProperty(DEFAULT_RELEASE_OPTS_GRADLE_PROPERTY)) {
            ziggyCppMexObject.setReleaseOptimizations(ZiggyCppPojo
                .gradlePropertyToList(project.findProperty(DEFAULT_RELEASE_OPTS_GRADLE_PROPERTY)));
        }
        if (project.hasProperty(DEFAULT_DEBUG_OPTS_GRADLE_PROPERTY)) {
            ziggyCppMexObject.setDebugOptimizations(ZiggyCppPojo
                .gradlePropertyToList(project.findProperty(DEFAULT_DEBUG_OPTS_GRADLE_PROPERTY)));
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
    public List<File> getSourceFiles() {
        return ziggyCppMexObject.getSourceFiles();
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

    // Path to the source files
    public void setSourceFilePath(Object cppFilePath) {
        ziggyCppMexObject.setSourceFilePath(cppFilePath);
    }

    @Internal
    public String getSourceFilePath() {
        return ziggyCppMexObject.getSourceFilePaths().get(0);
    }

    // Paths for include files
    public void setIncludeFilePaths(List<? extends Object> includeFilePaths) {
        ziggyCppMexObject.setIncludeFilePaths(includeFilePaths);
    }

    @Internal
    public List<String> getIncludeFilePaths() {
        return ziggyCppMexObject.getIncludeFilePaths();
    }

    // Paths for libraries that must be linked in.
    public void setLibraryPaths(List<? extends Object> libraryPaths) {
        ziggyCppMexObject.setLibraryPaths(libraryPaths);
    }

    @Internal
    public List<String> getLibraryPaths() {
        return ziggyCppMexObject.getLibraryPaths();
    }

    // Libraries that must be linked in
    public void setLibraries(List<? extends Object> libraries) {
        ziggyCppMexObject.setLibraries(libraries);
    }

    @Internal
    public List<String> getLibraries() {
        return ziggyCppMexObject.getLibraries();
    }

    // compiler options
    public void setcppCompileOptions(List<? extends Object> compileOptions) {
        ziggyCppMexObject.setcppCompileOptions(compileOptions);
    }

    @Internal
    public List<String> getcppCompileOptions() {
        return ziggyCppMexObject.getcppCompileOptions();
    }

    public void setcCompileOptions(List<? extends Object> compileOptions) {
        ziggyCppMexObject.setcCompileOptions(compileOptions);
    }

    @Internal
    public List<String> getcCompileOptions() {
        return ziggyCppMexObject.getcCompileOptions();
    }

    // linker options
    public void setLinkOptions(List<? extends Object> linkOptions) {
        ziggyCppMexObject.setLinkOptions(linkOptions);
    }

    @Internal
    public List<String> getLinkOptions() {
        return ziggyCppMexObject.getLinkOptions();
    }

    // output type (executable, shared library, static library)
    public void setOutputType(Object outputType) {
        ziggyCppMexObject.setOutputType(outputType);
    }

    @Internal
    public BuildType getOutputType() {
        return ziggyCppMexObject.getOutputType();
    }

    // Name of shared library file to be produced
    public void setOutputName(Object name) {
        ziggyCppMexObject.setOutputName(name);
    }

    @Internal
    public String getOutputName() {
        return ziggyCppMexObject.getOutputName();
    }

    // Names of mexfiles to be produced
    public void setMexfileNames(List<? extends Object> mexfileNames) {
        ziggyCppMexObject.setMexfileNames(mexfileNames);
    }

    @Internal
    public List<String> getMexfileNames() {
        return ziggyCppMexObject.getMexfileNames();
    }

    // Directory to use for build product
    public void setOutputDir(Object outputDir) {
        ziggyCppMexObject.setOutputDir(outputDir);
    }

    @Input
    @Optional
    public String getOutputDir() {
        return ziggyCppMexObject.getOutputDir();
    }

    // Parent directory for build product (i.e., build products go in this location + "/obj",
    // "/bin", or "/lib")

    public void setOutputDirParent(Object outputDirParent) {
        ziggyCppMexObject.setOutputDirParent(outputDirParent);
    }

    @Input
    @Optional
    public String getOutputDirParent() {
        return ziggyCppMexObject.getOutputDirParent();
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
        if (project.hasProperty(MATLAB_PATH_PROJECT_PROPERTY)) {
            matlabPath = project.findProperty(MATLAB_PATH_PROJECT_PROPERTY).toString();
            log.info("MATLAB path set from project extra property {}", matlabPath);
        }
        if (matlabPath == null) {
            String systemPath = System.getenv("PATH");
            if (systemPath != null) {
                String[] systemPaths = systemPath.split(":");
                for (String path : systemPaths) {
                    String pathLower = path.toLowerCase();
                    if (pathLower.contains("matlab") && path.endsWith("bin")) {
                        matlabPath = path.substring(0, path.length() - 4);
                        log.info("MATLAB path set from PATH environment variable {}", matlabPath);
                        break;
                    }
                }
            }
        }
        if (matlabPath == null) {
            String matlabHome = System.getenv(MATLAB_PATH_ENV_VAR);
            if (matlabHome != null) {
                matlabPath = matlabHome;
                log.info("MATLAB path set from MATLAB_HOME environment variable {}", matlabPath);
            }
        }
        if (matlabPath == null) {
            throw new GradleException(
                "Unable to find MATLAB path in Gradle, PATH env var, or MATLAB_HOME env var");
        }
        ziggyCppMexObject.setMatlabPath(matlabPath);
    }
}

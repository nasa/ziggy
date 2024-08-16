package gov.nasa.ziggy.buildutil;

import java.io.File;
import java.util.List;

import org.gradle.api.DefaultTask;
import org.gradle.api.Project;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

import gov.nasa.ziggy.buildutil.ZiggyCppPojo.BuildType;

/**
 * Performs C++ builds for Gradle. Ziggy and its pipelines use this instead of the standard Gradle
 * C++ task classes because it provides only the options we actually need without many that we can't
 * use, and also because it allows us to use the CXX environment variable to define the location of
 * the C++ compiler. This allows us to use the same compiler as some of the third party libraries
 * used by Ziggy (they also use CXX to select their compiler).
 * <p>
 * Usage example: assume that the user wishes to construct a shared object library, libfoo.(so,
 * dylib) from the contents of directories $projectDir/foo and $projectDir/bar, with additional
 * include files in $projectDir/include and $rootDir/project2/include. The task
 *
 * <pre>
 * task fooLib(type : ZiggyCpp) {
 *     outputName        = "foo"
 *     sourceFilePaths   = ["$projectDir/foo", "$projectDir/bar"]
 *     includeFilePaths  = ["$projectDir/include", "$rootDir/project2/include"]
 *     outputType        = "shared"
 *     cppCompileOptions = ["std=c++11", "O2"]
 *     cCompileOptions   = ["O2"]
 *     maxCompileThreads = 10
 * }
 * </pre>
 *
 * will compile the contents of the specified C++ file paths, placing the resulting object files in
 * $buildDir/obj, and then link them into shared object library libfoo.(so, dylib), placed in
 * $buildDir/lib. Options "-std=c++11" and "-O2" will be applied at compile time. During the compile
 * phase, up to 10 threads will run in order to compile source files in parallel. If
 * maxCompileThreads is not specified, a default value equal to the number of cores on the system
 * will be used.
 * <p>
 * If a static library had been desired in the prior example, the outputType would be changed to
 * "static", and file libfoo.a would be saved to $buildDir/lib. If a standalone executable was
 * needed, the outputType would be changed to "executable" and the resulting file foo would be
 * placed in $buildDir/bin. In this latter case, the user can specify additional libraries and
 * library paths via additional options:
 *
 * <pre>
 * task fooProgram(type : ZiggyCpp) {
 *     outputName        = "foo"
 *     sourceFilePaths   = ["$projectDir/foo", "$projectDir/bar"]
 *     includeFilePaths  = ["$projectDir/include", "$rootDir/project2/include"]
 *     outputType        = "executable"
 *     cppCompileOptions = ["std=c++11", "O2"]
 *     cCompileOptions   = ["O2"]
 *     libraryPaths      = ["$buildDir/lib", "$rootDir/project2/build/lib"]
 *     libraries         = ["math1", "spelling2"]
 * }
 * </pre>
 *
 * would link using libmath1 and libmath2, located in $buildDir/lib and/or
 * $rootDir/project2/build/lib.
 * <p>
 * The user can override the default directories used to store build products, which are:
 * $buildDir/lib, $buildDir/obj, and $buildDir/bin, respectively, for libraries, object files, and
 * executables. There are two ways to override: the first is to specify a directory that will be
 * used for all output products using the property name "outputDir". The other is to specify a
 * parent directory for the output directories using the property name "outputDirParent". In this
 * latter case, outputs will go to <outputDirParent>/lib, <outputDirParent>/obj, and
 * <outputDirParent>/bin, respectively, for libraries, object files, and executables. If both
 * outputDir and outputDirParent are specified, outputDir will take precedence.
 * <p>
 * Because Gradle classes that extend DefaultTask are effectively impossible to unit test, all of
 * the actual data and work are managed by the ZiggyCppPojo class (which does have unit tests),
 * while this class simply provides access to the ZiggyCppPojo class for Gradle.
 *
 * @author PT
 */
public class ZiggyCpp extends DefaultTask {

    private static final String DEFAULT_CPP_COMPILE_OPTIONS_GRADLE_PROPERTY = "defaultCppCompileOptions";
    private static final String DEFAULT_C_COMPILE_OPTIONS_GRADLE_PROPERTY = "defaultCCompileOptions";
    private static final String DEFAULT_LINK_OPTIONS_GRADLE_PROPERTY = "defaultLinkOptions";
    private static final String DEFAULT_RELEASE_OPTS_GRADLE_PROPERTY = "defaultReleaseOptimizations";
    private static final String DEFAULT_DEBUG_OPTS_GRADLE_PROPERTY = "defaultDebugOptimizations";
    private static final String PIPELINE_ROOT_DIR_PROP_NAME = "pipelineRootDir";

    /**
     * Data and methods used to perform the C++ compile and link.
     */
    private ZiggyCppPojo ziggyCppPojo = new ZiggyCppPojo();

    /**
     * Default constructor. Provides the ZiggyCppPojo object with directories for the project and
     * default options, if set
     */
    public ZiggyCpp() {
        Project project = getProject();
        ziggyCppPojo.setBuildDir(project.getBuildDir());
        ziggyCppPojo.setRootDir(pipelineRootDir(project));
        if (project.hasProperty(DEFAULT_CPP_COMPILE_OPTIONS_GRADLE_PROPERTY)) {
            ziggyCppPojo.setcppCompileOptions(ZiggyCppPojo.gradlePropertyToList(
                project.property(DEFAULT_CPP_COMPILE_OPTIONS_GRADLE_PROPERTY)));
        }
        if (project.hasProperty(DEFAULT_C_COMPILE_OPTIONS_GRADLE_PROPERTY)) {
            ziggyCppPojo.setcCompileOptions(ZiggyCppPojo
                .gradlePropertyToList(project.property(DEFAULT_C_COMPILE_OPTIONS_GRADLE_PROPERTY)));
        }
        if (project.hasProperty(DEFAULT_LINK_OPTIONS_GRADLE_PROPERTY)) {
            ziggyCppPojo.setLinkOptions(ZiggyCppPojo
                .gradlePropertyToList(project.property(DEFAULT_LINK_OPTIONS_GRADLE_PROPERTY)));
        }
        if (project.hasProperty(DEFAULT_RELEASE_OPTS_GRADLE_PROPERTY)) {
            ziggyCppPojo.setReleaseOptimizations(ZiggyCppPojo
                .gradlePropertyToList(project.findProperty(DEFAULT_RELEASE_OPTS_GRADLE_PROPERTY)));
        }
        if (project.hasProperty(DEFAULT_DEBUG_OPTS_GRADLE_PROPERTY)) {
            ziggyCppPojo.setDebugOptimizations(ZiggyCppPojo
                .gradlePropertyToList(project.findProperty(DEFAULT_DEBUG_OPTS_GRADLE_PROPERTY)));
        }
    }

    /**
     * Returns the root directory of the pipeline
     *
     * @param project Current project
     * @return rootDir if the pipelineRootDir project property is not set; if that property is set,
     * the contents of the pipelineRootDir property are returned as a File
     */
    public static File pipelineRootDir(Project project) {
        File pipelineRootDir = null;
        if (project.hasProperty(PIPELINE_ROOT_DIR_PROP_NAME)) {
            pipelineRootDir = new File(project.property(PIPELINE_ROOT_DIR_PROP_NAME).toString());
        } else {
            pipelineRootDir = project.getRootDir();
        }
        return pipelineRootDir;
    }

    /** Provides access to the ZiggyCppPojo action() method for Gradle. */
    @TaskAction
    public void action() {
        ziggyCppPojo.action();
    }

    /**
     * Provides access to the ZiggyCppPojo list of C++ files for Gradle, and specifies for gradle
     * that those files are the inputs for this task.
     *
     * @return List of C++ files found in the C++ source file directory.
     */
    @InputFiles
    public List<File> getSourceFiles() {
        return ziggyCppPojo.getSourceFiles();
    }

    /**
     * Provides Gradle with access to the ZiggyCppPojo File that will be the final product of the
     * build. Also specifies that this file is the output for this task.
     *
     * @return File containing the target product for a task.
     */
    @OutputFile
    public File getBuiltFile() {
        return ziggyCppPojo.getBuiltFile();
    }

    // Below are setters and getters for the ZiggyCppPojo members that must be mutated
    // by ZiggyCpp tasks in gradle. In principle only setters are needed, but in the
    // interest of sanity getters are also provided. Note that not all ZiggyCppPojo
    // members need to be set by ZiggyCpp, there are a number that are used internally
    // and are not set as part of a task.
    //
    // Note that the setters take Object and List<Object> rather than String and List<String>.
    // This is because Gradle allows its text string objects to be either Java String class or
    // Groovy GString class. Consequently, we pass everything to ZiggyCppPojo as Objects, and
    // ZiggyCppPojo uses the toString() methods to convert everything to Java Strings.

    // Path to the source files
    public void setSourceFilePaths(List<Object> sourceFilePaths) {
        ziggyCppPojo.setSourceFilePaths(sourceFilePaths);
    }

    @Input
    public List<String> getSourceFilePaths() {
        return ziggyCppPojo.getSourceFilePaths();
    }

    // Paths for include files
    public void setIncludeFilePaths(List<? extends Object> includeFilePaths) {
        ziggyCppPojo.setIncludeFilePaths(includeFilePaths);
    }

    @Input
    public List<String> getIncludeFilePaths() {
        return ziggyCppPojo.getIncludeFilePaths();
    }

    // Paths for libraries that must be linked in.
    public void setLibraryPaths(List<? extends Object> libraryPaths) {
        ziggyCppPojo.setLibraryPaths(libraryPaths);
    }

    @Input
    public List<String> getLibraryPaths() {
        return ziggyCppPojo.getLibraryPaths();
    }

    // Libraries that must be linked in
    public void setLibraries(List<? extends Object> libraries) {
        ziggyCppPojo.setLibraries(libraries);
    }

    @Input
    public List<String> getLibraries() {
        return ziggyCppPojo.getLibraries();
    }

    // compiler options
    public void setcppCompileOptions(List<? extends Object> compileOptions) {
        ziggyCppPojo.setcppCompileOptions(compileOptions);
    }

    @Input
    public List<String> getcppCompileOptions() {
        return ziggyCppPojo.getcppCompileOptions();
    }

    public void setcCompileOptions(List<? extends Object> compileOptions) {
        ziggyCppPojo.setcCompileOptions(compileOptions);
    }

    @Input
    public List<String> getcCompileOptions() {
        return ziggyCppPojo.getcCompileOptions();
    }

    // linker options
    public void setLinkOptions(List<? extends Object> linkOptions) {
        ziggyCppPojo.setLinkOptions(linkOptions);
    }

    @Input
    public List<String> getLinkOptions() {
        return ziggyCppPojo.getLinkOptions();
    }

    // output type (executable, shared library, static library)
    public void setOutputType(Object outputType) {
        ziggyCppPojo.setOutputType(outputType);
    }

    @Input
    public BuildType getOutputType() {
        return ziggyCppPojo.getOutputType();
    }

    // Name of file to be produced
    public void setOutputName(Object name) {
        ziggyCppPojo.setOutputName(name);
    }

    @Input
    public String getOutputName() {
        return ziggyCppPojo.getOutputName();
    }

    // Directory to use for build product
    public void setOutputDir(Object outputDir) {
        ziggyCppPojo.setOutputDir(outputDir);
    }

    @Input
    @Optional
    public String getOutputDir() {
        return ziggyCppPojo.getOutputDir();
    }

    // Parent directory for build product (i.e., build products go in this location + "/obj",
    // "/bin", or "/lib")

    public void setOutputDirParent(Object outputDirParent) {
        ziggyCppPojo.setOutputDirParent(outputDirParent);
    }

    @Input
    @Optional
    public String getOutputDirParent() {
        return ziggyCppPojo.getOutputDirParent();
    }

    public void setMaxCompileThreads(int maxCompileThreads) {
        ziggyCppPojo.setMaxCompileThreads(maxCompileThreads);
    }

    @Internal
    public int getMaxCompileThreads() {
        return ziggyCppPojo.getMaxCompileThreads();
    }
}

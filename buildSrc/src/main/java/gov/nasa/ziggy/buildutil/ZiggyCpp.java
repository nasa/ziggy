package gov.nasa.ziggy.buildutil;

import java.io.File;
import java.util.List;

import org.gradle.api.DefaultTask;
import org.gradle.api.Project;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.Input;


import gov.nasa.ziggy.buildutil.ZiggyCppPojo.BuildType;

/**
 * Performs C++ builds for Gradle. Ziggy and its pipelines use this instead of the 
 * standard Gradle C++ task classes because it provides only the options we actually 
 * need without many that we can't use, and also because it allows us to use the CXX
 * environment variable to define the location of the C++ compiler. This allows us to
 * use the same compiler as some of the third party libraries used by Ziggy (they also
 * use CXX to select their compiler). 
 * 
 * Because Gradle classes that extend DefaultTask are effectively impossible to unit test,
 * all of the actual data and work are managed by the ZiggyCppPojo class (which does have
 * unit tests), while this class simply provides access to the ZiggyCppPojo class for Gradle. 
 * 
 * @author PT
 *
 */
public class ZiggyCpp extends DefaultTask {

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
        if (project.hasProperty(ZiggyCppPojo.DEFAULT_COMPILE_OPTIONS_GRADLE_PROPERTY)) {
            ziggyCppPojo.setCompileOptions(ZiggyCppPojo.gradlePropertyToList(
                project.property(ZiggyCppPojo.DEFAULT_COMPILE_OPTIONS_GRADLE_PROPERTY)));
        }
        if (project.hasProperty(ZiggyCppPojo.DEFAULT_LINK_OPTIONS_GRADLE_PROPERTY)) {
            ziggyCppPojo.setLinkOptions(ZiggyCppPojo.gradlePropertyToList(
                project.property(ZiggyCppPojo.DEFAULT_LINK_OPTIONS_GRADLE_PROPERTY)));
        }
        if (project.hasProperty(ZiggyCppPojo.DEFAULT_RELEASE_OPTS_GRADLE_PROPERTY)) {
            ziggyCppPojo.setReleaseOptimizations(ZiggyCppPojo.gradlePropertyToList(
                project.findProperty(ZiggyCppPojo.DEFAULT_RELEASE_OPTS_GRADLE_PROPERTY)));
        }
        if (project.hasProperty(ZiggyCppPojo.DEFAULT_DEBUG_OPTS_GRADLE_PROPERTY)) {
            ziggyCppPojo.setDebugOptimizations(ZiggyCppPojo.gradlePropertyToList(
                project.findProperty(ZiggyCppPojo.DEFAULT_DEBUG_OPTS_GRADLE_PROPERTY)));
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
        if (project.hasProperty(ZiggyCppPojo.PIPELINE_ROOT_DIR_PROP_NAME)) {
            pipelineRootDir = new File(
                project.property(ZiggyCppPojo.PIPELINE_ROOT_DIR_PROP_NAME).toString());
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
    public List<File> getCppFiles() {
        return ziggyCppPojo.getCppFiles();
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

    // Path to the C++ source files
    public void setCppFilePaths(List<Object> cppFilePaths) {
        ziggyCppPojo.setCppFilePaths(cppFilePaths);
    }

    @Input
    public List<String> getCppFilePaths() {
        return ziggyCppPojo.getCppFilePaths();
    }

    // Paths for include files
    public void setIncludeFilePaths(List<? extends Object> includeFilePaths) {
        ziggyCppPojo.setIncludeFilePaths(includeFilePaths);
    }

    @Input
    public List<String> getIncludeFilePaths() {
        return ziggyCppPojo.getIncludeFilePaths();
    }

    // paths for libraries that must be linked in
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
    public void setCompileOptions(List<? extends Object> compileOptions) {
        ziggyCppPojo.setCompileOptions(compileOptions);
        ;
    }

    @Input
    public List<String> getCompileOptions() {
        return ziggyCppPojo.getCompileOptions();
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
}

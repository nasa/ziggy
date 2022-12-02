package gov.nasa.ziggy.buildutil;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.gradle.api.GradleException;
import org.gradle.internal.os.OperatingSystem;

/**
 * Performs compilation and linking of C++ code for Ziggy and for pipelines based on Ziggy.
 * The command line options, source directory, include paths, library paths, library names, and
 * type of build (executable, shared library, static library) and output file name
 * must be specified in the Gradle task that makes use of this class. The compiler is determined 
 * from the CXX environment variable, thus is compatible with third-party packages that use the same 
 * convention. Actual source file names are deduced from listing the source directory, object file 
 * information is generated during the compile and saved for use in the link. 
 * 
 * If Gradle's JVM has cppdebug set to true as a system property, the compile and link commands will
 * have appropriate options (-g and -Og). These will be placed after the compile / link options, thus
 * will override any optimization options supplied to Gradle. 
 * 
 * NB: this is a POJO that makes minimal use of the Gradle API. In particular, it does not extend
 * DefaultTask. This is because classes that extend DefaultTask are de facto impossible to unit test
 * in Java. The ZiggyCpp class embeds a ZiggyCpp class to perform its actions and store its members.
 * 
 * @author PT
 *
 */

public class ZiggyCppPojo {

    private static final Logger log = LoggerFactory.getLogger(ZiggyCppPojo.class);

    /** Type of build: shared object, static library, or standalone program */
    enum BuildType {
        SHARED, STATIC, EXECUTABLE
    }

    public static final String CPP_COMPILER_ENV_VAR = "CXX";
    public static final String[] CPP_FILE_TYPES = { ".c", ".cpp" };
    public static final String CPP_DEBUG_PROPERTY_NAME = "cppdebug";

    public static final String DEFAULT_COMPILE_OPTIONS_GRADLE_PROPERTY = "defaultCppCompileOptions";
    public static final String DEFAULT_LINK_OPTIONS_GRADLE_PROPERTY = "defaultCppLinkOptions";
    public static final String DEFAULT_RELEASE_OPTS_GRADLE_PROPERTY = "defaultCppReleaseOptimizations";
    public static final String DEFAULT_DEBUG_OPTS_GRADLE_PROPERTY = "defaultCppDebugOptimizations";
    public static final String PIPELINE_ROOT_DIR_PROP_NAME = "pipelineRootDir";

    /** Path to the C++ files to be compiled */
    private List<String> cppFilePaths = null;

    /** Paths to the include files */
    private List<String> includeFilePaths = new ArrayList<>();

    /** Paths to the libraries needed in linking */
    private List<String> libraryPaths = new ArrayList<>();

    /** Libraries needed for linking (minus the "lib" prefix and all file type suffixes) */
    private List<String> libraries = new ArrayList<>();

    /** compile options (minus the initial hyphen) */
    private List<String> compileOptions = new ArrayList<>();

    /** linker options (minus the initial hyphen) */
    private List<String> linkOptions = new ArrayList<>();

    /** Optimizations, if any, desired for a build without cppdebug=true system property */
    private List<String> releaseOptimizations = new ArrayList<>();

    /** Optimizations, if any, desired for a build with cppdebug=true system property */
    private List<String> debugOptimizations = new ArrayList<>();

    /** Caller-selected build type */
    private BuildType outputType = null;

    /** Name of the output file (with no "lib" prefix or file type suffix) */
    private String name = null;

    /** C++ files found in the cppFilePath directory */
    private List<File> cppFiles = new ArrayList<>();

    /** Object files built from the C++ files */
    private List<File> objectFiles = new ArrayList<>();

    /** Desired output (executable or library) as a File */
    private File builtFile = null;

    /** Desired Gradle build directory, as a File */
    private File buildDir = null;

    /** Root directory for the parent Gradle project, as a File */
    private File rootDir = null;

    /** C++ compiler command including path to same */
    private String cppCompiler = null;

    /** Default executor used only for testing, do not use for real execution! */
    private DefaultExecutor defaultExecutor = null;

    /** Operating system, needed to set options and names for the linker command */
    private OperatingSystem operatingSystem = OperatingSystem.current();

    // stores logger warning messages. Used only for testing.
    private List<String> loggerWarnings = new ArrayList<>();

    /**
     * Converts a list of arguments to a single string that can be used in a command line compiler
     * call
     *
     * @param argList list of arguments
     * @param prefix prefix for each argument ("-I", "-L", etc.)
     * @return the list of arguments converted to a string, and with the prefix added to each
     */
    public String argListToString(List<String> argList, String prefix) {
        StringBuilder argStringBuilder = new StringBuilder();
        for (String arg : argList) {
            argStringBuilder.append(prefix + arg + " ");
        }
        return argStringBuilder.toString();
    }

    File objDir() {
        return new File(buildDir, "obj");
    }

    File libDir() {
        return new File(buildDir, "lib");
    }

    File binDir() {
        return new File(buildDir, "bin");
    }

    /**
     * Search the specified file path for C and C++ files, and populate the cppFiles list with same.
     * If the file path is not set or does not exist, a GradleException will be thrown.
     */
    private void populateCppFiles() {

        // check that the path is set and exists
        if (cppFilePaths == null) {
            throw new GradleException("C++ file path is null");
        }

        // clear any existing files, and also handle the null pointer case
        // neither of these should ever occur in real life, but why risk it?
        if (cppFiles == null || !cppFiles.isEmpty()) {
            cppFiles = new ArrayList<>();
        }

        for (String cppFilePath : cppFilePaths) {
            File cppFileDir = new File(cppFilePath);
            if (!cppFileDir.exists()) {
                String w = "C++ file path " + cppFilePath + " does not exist";
                log.warn(w);
                addLoggerWarning(w);

            } else {

                // find all C and C++ files and add them to the cppFiles list
                for (String fileType : CPP_FILE_TYPES) {
                    File[] cFiles = cppFileDir.listFiles(new FilenameFilter() {
                        public boolean accept(File dir, String name) {
                            return name.endsWith(fileType);
                        }
                    });
                    for (File file : cFiles) {
                        cppFiles.add(file);
                    }
                }
            }
        }

        if (!cppFiles.isEmpty()) {
            Collections.sort(cppFiles);
            // write the list of files to the log if info logging is set
            StringBuilder fileListBuilder = new StringBuilder();
            for (File file : cppFiles) {
                fileListBuilder.append(file.getName());
                fileListBuilder.append(" ");
            }
            log.info("List of C/C++ files in directory " + cppFilePaths + ": "
                + fileListBuilder.toString());
        }
    }

    /**
     * Populate the builtFile member based on the name of the file to be built, its type, and the
     * OS. Throws a GradleException if the name isn't defined, the output type isn't defined, or the
     * OS is' something other than Mac OS, Linux, or Unix.
     */
    protected void populateBuiltFile() {

        // handle error cases
        if (name == null || outputType == null) {
            throw new GradleException("Both output name and output type must be specified");
        }

        String outputDirectory = null;
        String prefix = null;
        String fileType = null;
        // determine output directory
        if (outputType == BuildType.EXECUTABLE) {
            outputDirectory = binDir().getAbsolutePath();
            prefix = "";
            fileType = "";
        } else {
            outputDirectory = libDir().getAbsolutePath();
            prefix = "lib";
            if (outputType == BuildType.STATIC) {
                fileType = ".a";
            } else {
                if (operatingSystem.isMacOsX()) {
                    fileType = ".dylib";
                } else if (operatingSystem.isLinux()) {
                    fileType = ".so";
                } else {
                    throw new GradleException(
                        "ZiggyCpp class does not support OS " + operatingSystem.getName());
                }
            }
        }
        String outputFile = prefix + name + fileType;
        builtFile = new File(outputDirectory, outputFile);

    }

    /**
     * Determines the name of the object file that is generated when compiling a given source file.
     *
     * @param sourceFile source C/C++ file
     * @return name of the source file with original type stripped off and replaced with ".o"
     */
    public static String objectNameFromSourceFile(File sourceFile) {
        String sourceName = sourceFile.getName();
        String strippedName = null;
        for (String fileType : CPP_FILE_TYPES) {
            if (sourceName.endsWith(fileType)) {
                strippedName = sourceName.substring(0, sourceName.length() - fileType.length());
                break;
            }
        }
        return strippedName + ".o";
    }

    /**
     * Generates the command to compile a single source file
     *
     * @param sourceFile File of the C/C++ source that is to be compiled
     * @return the compile command as a single string. This command will include the include files
     * and command line options specified in the object, and will route the output to the correct
     * output directory (specifically $buildDir/obj). It will also take care of setting options
     * correctly for a debug build if the JVM has cppdebug=true set as a system property.
     */
    public String generateCompileCommand(File sourceFile) {
        return generateCompileCommand(sourceFile, null, null);
    }

    /**
     * Generates the command to compile a single source file, with additional options that are
     * needed for mexfiles
     *
     * @param sourceFile File of the C/C++ source that is to be compiled
     * @param matlabIncludePath String that indicates the location of MATLAB include files, can be
     * null
     * @param matlabCompilerDirective String that contains the MATLAB compiler directive, can be
     * null
     * @return the compile command as a single string. This command will include the include files
     * and command line options specified in the object, and will route the output to the correct
     * output directory (specifically $buildDir/obj). It will also take care of setting options
     * correctly for a debug build if the JVM has cppdebug=true set as a system property.
     */
    public String generateCompileCommand(File sourceFile, String matlabIncludePath,
        String matlabCompilerDirective) {

        StringBuilder compileStringBuilder = new StringBuilder();

        // compiler executable
        compileStringBuilder.append(getCppCompiler() + " ");

        // compile only flag
        compileStringBuilder.append("-c ");

        // define the output file
        compileStringBuilder.append(
            "-o " + objDir().getAbsolutePath() + "/" + objectNameFromSourceFile(sourceFile) + " ");

        // add the include paths
        compileStringBuilder.append(argListToString(includeFilePaths, "-I"));

        // If there is a MATLAB include path, handle that now
        if (matlabIncludePath != null && !matlabIncludePath.isEmpty()) {
            compileStringBuilder.append("-I" + matlabIncludePath + " ");
        }

        // add the command line options
        compileStringBuilder.append(argListToString(compileOptions, "-"));

        // if there is a MATLAB compiler directive, handle that now
        if (matlabCompilerDirective != null && !matlabCompilerDirective.isEmpty()) {
            compileStringBuilder.append("-" + matlabCompilerDirective + " ");
        }

        // depending on whether there is a cppdebug system property set to true, we either set up
        // for debugging, or -- not.
        boolean debug = false;
        if (System.getProperty(CPP_DEBUG_PROPERTY_NAME) != null) {
            debug = Boolean.getBoolean(CPP_DEBUG_PROPERTY_NAME);
        }
        if (debug) {
            compileStringBuilder.append(argListToString(debugOptimizations, "-"));
        } else {
            compileStringBuilder.append(argListToString(releaseOptimizations, "-"));
        }
        compileStringBuilder.append(sourceFile.getAbsolutePath());

        // send the results of this to the log if info mode is selected
        log.info(compileStringBuilder.toString());

        return compileStringBuilder.toString();

    }

    /**
     * Generates a linker command line. The line takes into account the linker options, the desired
     * output type (executable, static library, or shared object), and library paths and names.
     *
     * @return Linker command line as a String.
     */
    public String generateLinkCommand() {
        return generateLinkCommand(null);
    }

    /**
     * Generates a linker command line. The line takes into account the linker options, the desired
     * output type (executable, static library, or shared object), and library paths and names.
     *
     * @param matlabLibPath String that indicates the path to MATLAB shared objects, can be null
     * @return Linker command line as a String.
     */
    public String generateLinkCommand(String matlabLibPath) {

        StringBuilder linkStringBuilder = new StringBuilder();

        // start with the actual command, which is either the compiler or the archive builder
        if (outputType == BuildType.STATIC) {
            linkStringBuilder.append("ar rs ");
        } else {
            linkStringBuilder.append(getCppCompiler() + " -o ");
        }

        // add the name of the desired output file
        linkStringBuilder.append(getBuiltFile().getAbsolutePath() + " ");

        // if this is an executable or shared object, add the linker options
        // and library paths
        if (outputType != BuildType.STATIC) {
            linkStringBuilder.append(argListToString(libraryPaths, "-L"));
            if (matlabLibPath != null && !matlabLibPath.isEmpty()) {
                linkStringBuilder.append("-L" + matlabLibPath + " ");
            }

        }

        // add release or debug options
        if (outputType == BuildType.EXECUTABLE) {
            linkStringBuilder.append(argListToString(linkOptions, "-"));
            if (System.getProperty(CPP_DEBUG_PROPERTY_NAME) != null
                && Boolean.getBoolean(CPP_DEBUG_PROPERTY_NAME)) {
                linkStringBuilder.append(argListToString(debugOptimizations, "-"));
            } else {
                linkStringBuilder.append(argListToString(releaseOptimizations, "-"));
            }
        }

        // if this is to be a shared object, put in the "-shared" option

        if (outputType == BuildType.SHARED) {
            linkStringBuilder.append("-shared ");
        }

        // if the OS is Mac OS, set the install name. The install name assumes that the library
        // will be installed in the build/lib directory under the root directory.

        if (operatingSystem.isMacOsX() && outputType == BuildType.SHARED) {
            linkStringBuilder.append("-install_name " + getRootDir().getAbsolutePath()
                + "/build/lib/" + getBuiltFile().getName() + " ");
        }

        // add the object files
        for (File objectFile : objectFiles) {
            linkStringBuilder.append(objectFile.getName() + " ");
        }

        // Add library names. These have come after the object files due to a positional
        // dependence in the Linux linker
        if (outputType != BuildType.STATIC) {
            linkStringBuilder.append(argListToString(libraries, "-l"));
            if (matlabLibPath != null && !matlabLibPath.isEmpty()) {
                linkStringBuilder.append("-lmex -lmx -lmat ");
            }
        }

        log.info(linkStringBuilder.toString());
        return linkStringBuilder.toString();

    }

    /**
     * Returns a DefaultExecutor object. For normal execution, this always returns a new object, but
     * if the defaultExecutor member is non-null, that is what is returned. This latter case should
     * only happen in testing, when a mocked DefaultExecutor object is stored in defaultExecutor.
     *
     * @return a new DefaultExecutor (normal execution), or a mocked one (testing).
     */
    protected DefaultExecutor getDefaultExecutor() {
        if (defaultExecutor == null) {
            return new DefaultExecutor();
        }
        return defaultExecutor;
    }

    /**
     * Main action of the class. This method compiles the files and captures information about the
     * resulting object files, then performs whatever linking / library building action is required.
     * If any compile or the final link / library build command fails, a GradleException is thrown.
     * Files in the include directories that end in .h or .hpp are copied to the build directory's
     * include subdir.
     */
    public void action() {

        log.info(String.format("%s.action()\n", this.getClass().getSimpleName()));

        // compile the source files
        compileAction();

        // perform the linker / archiver step
        linkAction();
    }

    protected void compileAction() {

        // create the obj directory

        File objDir = objDir();
        if (!objDir.exists()) {
            log.info("mkdir: " + objDir.getAbsolutePath());
            objDir.mkdirs();
        }
        // loop over source files, compile them and add the object file to the object file list
        for (File file : getCppFiles()) {
            DefaultExecutor compilerExec = getDefaultExecutor();
            compilerExec.setWorkingDirectory(new File(cppFilePaths.get(0)));
            try {
                int returnCode = compilerExec
                    .execute(new CommandLineComparable(generateCompileCommand(file)));

                if (returnCode != 0) {
                    throw new GradleException("Compilation of file " + file.getName() + " failed");
                }
                objectFiles.add(new File(objDir, objectNameFromSourceFile(file)));
            } catch (IOException e) {
                throw new GradleException(
                    "IOException occurred when attempting to compile " + file.getName(), e);
            }
        }
    }

    protected void linkAction() {

        File objDir = objDir();
        DefaultExecutor linkExec = getDefaultExecutor();
        linkExec.setWorkingDirectory(objDir);
        File destDir = null;
        if (outputType.equals(BuildType.EXECUTABLE)) {
            destDir = binDir();
        } else {
            destDir = libDir();
        }
        if (!destDir.exists()) {
            log.info("mkdir: " + destDir.getAbsolutePath());
            destDir.mkdirs();
        }
        try {
            int returnCode = linkExec.execute(new CommandLineComparable(generateLinkCommand()));
            if (returnCode != 0) {
                throw new GradleException(
                    "Link / library construction of " + getBuiltFile().getName() + " failed");
            }
        } catch (IOException e) {
            throw new GradleException("IOException occurred during link / library construction of "
                + getBuiltFile().getName(), e);
        }

        // copy the files from each of the include directories to buildDir/include
        File includeDest = new File(buildDir, "include");
        for (String include : includeFilePaths) {
            File[] includeFiles = new File(include).listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return (name.endsWith(".h") || name.endsWith(".hpp"));
                }
            });
            for (File includeFile : includeFiles) {
                try {
                    FileUtils.copyFileToDirectory(includeFile, includeDest);
                } catch (IOException e) {
                    throw new GradleException("Unable to copy include files from" + include + " to "
                        + includeDest.getAbsoluteFile(), e);
                }
            }
        }

    }

    /**
     * Converts a list of Objects to a list of Strings, preserving their order.
     *
     * @param libraries2 List of objects to be converted.
     * @return ArrayList of strings obtained by taking toString() of the objects in the objectList.
     */
    static List<String> objectListToStringList(List<? extends Object> libraries2) {
        List<String> stringList = new ArrayList<>();
        for (Object obj : libraries2) {
            stringList.add(obj.toString());
        }
        return stringList;
    }

    /**
     * Converts a Gradle property to a list of Strings. The property can be a scalar or a list, Java
     * Strings or Groovy GStrings.
     *
     * @param gradleProperty property to be converted.
     * @return contents of gradleProperty as a list of Java Strings.
     */
    @SuppressWarnings("unchecked")
    static List<String> gradlePropertyToList(Object gradleProperty) {
        if (gradleProperty instanceof List<?>) {
            return objectListToStringList((List<? extends Object>) gradleProperty);
        } else {
            List<Object> gradlePropertyList = new ArrayList<>();
            gradlePropertyList.add(gradleProperty);
            return objectListToStringList((List<? extends Object>) gradlePropertyList);
        }
    }

//	setters and getters 

    public void setCppFilePath(Object cppFilePath) {
        this.cppFilePaths = new ArrayList<>();
        cppFilePaths.add(cppFilePath.toString());
    }

    public List<String> getCppFilePaths() {
        return cppFilePaths;
    }

    public void setCppFilePaths(List<Object> cppFilePaths) {
        this.cppFilePaths = objectListToStringList(cppFilePaths);
    }

    public List<String> getIncludeFilePaths() {
        return includeFilePaths;
    }

    public void setIncludeFilePaths(List<? extends Object> includeFilePaths) {
        this.includeFilePaths = new ArrayList<>();
        this.includeFilePaths.addAll(objectListToStringList(includeFilePaths));
    }

    public List<String> getLibraryPaths() {
        return libraryPaths;
    }

    public void setLibraryPaths(List<? extends Object> libraryPaths) {
        this.libraryPaths = new ArrayList<>();
        this.libraryPaths.addAll(objectListToStringList(libraryPaths));
    }

    public List<String> getLibraries() {
        return libraries;
    }

    public void setLibraries(List<? extends Object> libraries) {
        this.libraries = new ArrayList<>();
        this.libraries.addAll(objectListToStringList(libraries));
    }

    public List<String> getCompileOptions() {
        return compileOptions;
    }

    public void setCompileOptions(List<? extends Object> compileOptions) {
        this.compileOptions = new ArrayList<>();
        this.compileOptions.addAll(objectListToStringList(compileOptions));
    }

    public List<String> getLinkOptions() {
        return linkOptions;
    }

    public void setLinkOptions(List<? extends Object> linkOptions) {
        this.linkOptions = new ArrayList<>();
        this.linkOptions.addAll(objectListToStringList(linkOptions));
    }

    public List<String> getReleaseOptimizations() {
        return releaseOptimizations;
    }

    public void setReleaseOptimizations(List<? extends Object> releaseOptimizations) {
        this.releaseOptimizations = new ArrayList<>();
        this.releaseOptimizations.addAll(objectListToStringList(releaseOptimizations));
    }

    public List<String> getDebugOptimizations() {
        return debugOptimizations;
    }

    public void setDebugOptimizations(List<? extends Object> debugOptimizations) {
        this.debugOptimizations = new ArrayList<>();
        this.debugOptimizations.addAll(objectListToStringList(debugOptimizations));
    }

    public BuildType getOutputType() {
        return outputType;
    }

    public void setOutputType(BuildType outputType) {
        this.outputType = outputType;
    }

    public void setOutputType(Object outputType) {
        this.outputType = BuildType.valueOf(outputType.toString().toUpperCase());
    }

    public String getOutputName() {
        return name;
    }

    public void setOutputName(Object name) {
        this.name = name.toString();
    }

    public List<File> getCppFiles() {
        // always generate the list afresh -- necessary because Gradle calls the ZiggyCpp
        // method getCppFiles() prior to the actual build, at which time the directories of
        // source files may or may not exist yet! Thus we can't afford to cache the C++
        // file list, since I can't tell whether Gradle creates a new ZiggyCpp object when
        // it actually does the build, or whether it simply re-uses the one from pre-build.
        populateCppFiles();
        return cppFiles;
    }

    public List<File> getObjectFiles() {
        return objectFiles;
    }

    public void setObjectFiles(List<File> objectFiles) {
        this.objectFiles.addAll(objectFiles);
    }

    public void setObjectFiles(File objectFile) {
        this.objectFiles.add(objectFile);
    }

    public File getBuiltFile() {
        if (builtFile == null) {
            populateBuiltFile();
        }
        return builtFile;
    }

    public File getBuildDir() {
        return buildDir;
    }

    public void setBuildDir(File buildDir) {
        this.buildDir = buildDir;
    }

    public File getRootDir() {
        return rootDir;
    }

    public void setRootDir(File rootDir) {
        this.rootDir = rootDir;
    }

    public String getCppCompiler() {
        if (cppCompiler == null) {
            cppCompiler = System.getenv(CPP_COMPILER_ENV_VAR);
        }
        return cppCompiler;
    }

    public OperatingSystem getOperatingSystem() {
        return operatingSystem;
    }

    // this method is intended for use only in testing, for that reason it is package-private
    void setCppCompiler(String cppCompiler) {
        this.cppCompiler = cppCompiler;
    }

    // this method is intended for use only in testing, for that reason it is package-private
    void setDefaultExecutor(DefaultExecutor defaultExecutor) {
        this.defaultExecutor = defaultExecutor;
    }

    // this method is intended for use only in testing, for that reason it is package-private
    void setOperatingSystem(OperatingSystem operatingSystem) {
        this.operatingSystem = operatingSystem;
    }

    /**
     * Thin wrapper for Apache Commons CommandLine class that provides an equals() method. This is
     * needed for unit testing, since Mockito checks argument agreements using the argument's
     * equals() method, and CommandLine doesn't have one.
     *
     * @author PT
     */
    class CommandLineComparable extends CommandLine {

        public CommandLineComparable(String executable) {
            super(CommandLine.parse(executable));
        }

        public boolean equals(Object o) {
            if (o instanceof CommandLine) {
                CommandLine oo = (CommandLine) o;
                if (this.toString().contentEquals(oo.toString())) {
                    return true;
                }
            }
            return false;
        }
    }

    // add a logger warning to the list of same. Used only for testing.
    private void addLoggerWarning(String warning) {
        loggerWarnings.add(warning);
    }

    // retrieve the list of saved logger warnings. Used only for testing.
    List<String> loggerWarnings() {
        return loggerWarnings;
    }
}

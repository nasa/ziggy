package gov.nasa.ziggy.buildutil;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.gradle.api.GradleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performs compilation and linking of C++ code for Ziggy and for pipelines based on Ziggy. The
 * command line options, source directory, include paths, library paths, library names, and type of
 * build (executable, shared library, static library) and output file name must be specified in the
 * Gradle task that makes use of this class. The compiler is determined from the CXX environment
 * variable, thus is compatible with third-party packages that use the same convention. Actual
 * source file names are deduced from listing the source directory, object file information is
 * generated during the compile and saved for use in the link. If Gradle's JVM has cppdebug set to
 * true as a system property, the compile and link commands will have appropriate options (-g and
 * -Og). These will be placed after the compile / link options, thus will override any optimization
 * options supplied to Gradle. NB: this is a POJO that makes minimal use of the Gradle API. In
 * particular, it does not extend DefaultTask. This is because classes that extend DefaultTask are
 * de facto impossible to unit test in Java. The ZiggyCpp class embeds a ZiggyCpp class to perform
 * its actions and store its members.
 *
 * @author PT
 */

public class ZiggyCppPojo {

    private static final Logger log = LoggerFactory.getLogger(ZiggyCppPojo.class);

    /** Type of build: shared object, static library, or standalone program */
    enum BuildType {
        SHARED, STATIC, EXECUTABLE
    }

    enum Compiler {
        C(".c", C_COMPILER), CPP(".cpp", CPP_COMPILER);

        private String fileSuffix;
        private String compiler;

        Compiler(String fileSuffix, String compiler) {
            this.fileSuffix = fileSuffix;
            this.compiler = compiler;
        }

        public String fileSuffix() {
            return fileSuffix;
        }

        public String compiler() {
            return compiler;
        }
    }

    public static final String CPP_COMPILER_ENV_VAR = "CXX";
    public static final String C_COMPILER_ENV_VAR = "CC";
    public static final String CPP_DEBUG_PROPERTY_NAME = "cppdebug";

    // The compiler Strings cannot be final because they need to be overridden
    // during test.
    public static String CPP_COMPILER = System.getenv(CPP_COMPILER_ENV_VAR);
    public static String C_COMPILER = System.getenv(C_COMPILER_ENV_VAR);

    private static final List<String> DEFAULT_CPP_COMPILE_OPTIONS = List.of("-Wall", "-fPIC",
        "-std=c++11");
    private static final List<String> DEFAULT_C_COMPILE_OPTIONS = List.of("-fPIC");
    private static final List<String> DEFAULT_LINK_OPTIONS = List.of();
    private static final List<String> DEFAULT_RELEASE_OPTS = List.of("-O2", "-DNDEBUG", "-g");
    private static final List<String> DEFAULT_DEBUG_OPTS = List.of("-Og", "-g");
    private static final int DEFAULT_PARALLEL_COMPILE_THREADS = Runtime.getRuntime()
        .availableProcessors();

    /** Path to the files to be compiled */
    private List<String> sourceFilePaths = null;

    /** Paths to the include files */
    private List<String> includeFilePaths = new ArrayList<>();

    /** Paths to the libraries needed in linking */
    private List<String> libraryPaths = new ArrayList<>();

    /** Libraries needed for linking (minus the "lib" prefix and all file type suffixes) */
    private List<String> libraries = new ArrayList<>();

    /** compile options (minus the initial hyphen) */
    private List<String> cppCompileOptions = DEFAULT_CPP_COMPILE_OPTIONS;

    /** compile options (minus the initial hyphen) */
    private List<String> cCompileOptions = DEFAULT_C_COMPILE_OPTIONS;

    /** linker options (minus the initial hyphen) */
    private List<String> linkOptions = DEFAULT_LINK_OPTIONS;

    /** Optimizations, if any, desired for a build without cppdebug=true system property */
    private List<String> releaseOptimizations = DEFAULT_RELEASE_OPTS;

    /** Optimizations, if any, desired for a build with cppdebug=true system property */
    private List<String> debugOptimizations = DEFAULT_DEBUG_OPTS;

    /** Caller-selected build type */
    private BuildType outputType = null;

    /** Name of the output file (with no "lib" prefix or file type suffix) */
    private String name = null;

    /** Name of the directory for output, if not specified an appropriate default is used. */
    protected String outputDir;

    /** Parent of the directory for output, if not specified buildDir is used. */
    protected String outputDirParent;

    /** C++ files found in the cppFilePath directory */
    private Map<File, String> sourceFiles = new TreeMap<>();

    /** Object files built from the C++ files */
    private List<File> objectFiles = new ArrayList<>();

    /** Desired output (executable or library) as a File */
    private File builtFile = null;

    /** Desired Gradle build directory, as a File */
    protected File buildDir = null;

    /** Root directory for the parent Gradle project, as a File */
    private File rootDir = null;

    /** Default executor used only for testing, do not use for real execution! */
    private DefaultExecutor defaultExecutor = null;

    /** Operating system, needed to set options and names for the linker command */
    private SystemArchitecture architecture = SystemArchitecture.architecture();

    /** Number of parallel compiler processes to accept. */
    private int maxCompileThreads = DEFAULT_PARALLEL_COMPILE_THREADS;

    // stores logger warning messages. Used only for testing.
    private List<String> loggerWarnings = new ArrayList<>();

    /**
     * Converts a list of arguments to a single string that can be used in a command line compiler
     * call
     *
     * @param argList list of arguments, may contain null or empty items, which are skipped
     * @param prefix prefix for each argument ("-I", "-L", etc.)
     * @return the list of arguments converted to a string, and with the prefix added to each
     */
    public String argListToString(List<String> argList, String prefix) {
        StringBuilder argStringBuilder = new StringBuilder();
        for (String arg : argList) {
            if (!StringUtils.isBlank(arg)) {
                argStringBuilder.append(prefix + arg + " ");
            }
        }
        return argStringBuilder.toString();
    }

    File objDir() {
        return StringUtils.isEmpty(outputDir) ? new File(outputParent(), "obj")
            : new File(outputDir);
    }

    File libDir() {
        return StringUtils.isEmpty(outputDir) ? new File(outputParent(), "lib")
            : new File(outputDir);
    }

    File binDir() {
        return StringUtils.isEmpty(outputDir) ? new File(outputParent(), "bin")
            : new File(outputDir);
    }

    File outputParent() {
        return StringUtils.isEmpty(outputDirParent) ? buildDir : new File(outputDirParent);
    }

    /**
     * Search the specified file path for C and C++ files, and populate the cppFiles list with same.
     * If the file path is not set or does not exist, a GradleException will be thrown.
     */
    private void populateSourceFiles(boolean warn) {

        // check that the path is set and exists
        if (sourceFilePaths == null) {
            throw new GradleException("C++ file path is null");
        }

        // clear any existing files, and also handle the null pointer case
        // neither of these should ever occur in real life, but why risk it?
        if (sourceFiles == null || !sourceFiles.isEmpty()) {
            sourceFiles = new TreeMap<>();
        }

        for (String sourceFilePath : sourceFilePaths) {
            File sourceFileDir = new File(sourceFilePath);
            if (!sourceFileDir.exists()) {
                if (warn) {
                    String w = "Source file path " + sourceFilePath + " does not exist";
                    log.warn(w);
                    addLoggerWarning(w);
                }
            } else {

                // find all C and C++ files and add them to the cppFiles list
                for (Compiler compiler : Compiler.values()) {
                    File[] cFiles = sourceFileDir.listFiles(
                        (FilenameFilter) (dir, name) -> name.endsWith(compiler.fileSuffix()));
                    if (cFiles == null) {
                        continue;
                    }
                    for (File cFile : cFiles) {
                        sourceFiles.put(cFile, compiler.compiler());
                    }
                }
            }
        }

        if (!sourceFiles.isEmpty()) {
            // write the list of files to the log if info logging is set
            StringBuilder fileListBuilder = new StringBuilder();
            for (File file : sourceFiles.keySet()) {
                fileListBuilder.append(file.getName());
                fileListBuilder.append(" ");
            }
            log.info("List of C/C++ files in directory {}: {}", sourceFilePaths,
                fileListBuilder.toString());
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
            } else if (architecture == SystemArchitecture.MAC_M1
                || architecture == SystemArchitecture.MAC_INTEL) {
                fileType = ".dylib";
            } else if (architecture == SystemArchitecture.LINUX_INTEL) {
                fileType = ".so";
            } else {
                throw new GradleException(
                    "ZiggyCpp class does not support OS " + architecture.toString());
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
        for (Compiler compiler : Compiler.values()) {
            if (sourceName.endsWith(compiler.fileSuffix())) {
                strippedName = sourceName.substring(0,
                    sourceName.length() - compiler.fileSuffix().length());
                break;
            }
        }
        return strippedName + ".o";
    }

    /**
     * Generates the command to compile a single source file
     */
    public String generateCompileCommand(Map.Entry<File, String> sourceFile) {
        return generateCompileCommand(sourceFile, null, null);
    }

    /**
     * Generates the command to compile a single source file, with additional options that are
     * needed for mexfiles.
     */
    public String generateCompileCommand(Map.Entry<File, String> sourceFile,
        String matlabIncludePath, String matlabCompilerDirective) {
        return generateCompileCommand(sourceFile.getKey(), sourceFile.getValue(), matlabIncludePath,
            matlabCompilerDirective);
    }

    public String generateCompileCommand(File sourceFile, String compiler) {
        return generateCompileCommand(sourceFile, compiler, null, null);
    }

    /**
     * Generates the command to compile a single source file, with additional options that are
     * needed for mexfiles.
     */
    public String generateCompileCommand(File sourceFile, String compiler, String matlabIncludePath,
        String matlabCompilerDirective) {

        StringBuilder compileStringBuilder = new StringBuilder();

        // compiler executable
        compileStringBuilder.append(compiler + " ");

        // compile only flag
        compileStringBuilder.append("-c ");

        // define the output file
        compileStringBuilder.append(
            "-o " + objDir().getAbsolutePath() + "/" + objectNameFromSourceFile(sourceFile) + " ");

        // add the include paths
        compileStringBuilder.append(argListToString(includeFilePaths, "-I"));

        // If there is a MATLAB include path, handle that now
        if (!StringUtils.isBlank(matlabIncludePath)) {
            compileStringBuilder.append("-I" + matlabIncludePath + " ");
        }

        // add the command line options
        if (compiler.equals(CPP_COMPILER)) {
            compileStringBuilder.append(argListToString(cppCompileOptions, ""));
        } else {
            compileStringBuilder.append(argListToString(cCompileOptions, ""));
        }

        // if there is a MATLAB compiler directive, handle that now
        if (!StringUtils.isBlank(matlabCompilerDirective)) {
            compileStringBuilder.append("-" + matlabCompilerDirective + " ");
        }

        // depending on whether there is a cppdebug system property set to true, we either set up
        // for debugging, or -- not.
        boolean debug = false;
        if (System.getProperty(CPP_DEBUG_PROPERTY_NAME) != null) {
            debug = Boolean.getBoolean(CPP_DEBUG_PROPERTY_NAME);
        }
        if (debug) {
            compileStringBuilder.append(argListToString(debugOptimizations, ""));
        } else {
            compileStringBuilder.append(argListToString(releaseOptimizations, ""));
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
            linkStringBuilder.append(Compiler.CPP.compiler() + " -o ");
        }

        // add the name of the desired output file
        linkStringBuilder.append(getBuiltFile().getAbsolutePath() + " ");

        // if this is an executable or shared object, add the linker options
        // and library paths
        if (outputType != BuildType.STATIC) {
            linkStringBuilder.append(argListToString(libraryPaths, "-L"));
            if (!StringUtils.isBlank(matlabLibPath)) {
                linkStringBuilder.append("-L" + matlabLibPath + " ");
            }
        }

        // add release or debug options
        if (outputType == BuildType.EXECUTABLE) {
            linkStringBuilder.append(argListToString(linkOptions, ""));
            if (System.getProperty(CPP_DEBUG_PROPERTY_NAME) != null
                && Boolean.getBoolean(CPP_DEBUG_PROPERTY_NAME)) {
                linkStringBuilder.append(argListToString(debugOptimizations, ""));
            } else {
                linkStringBuilder.append(argListToString(releaseOptimizations, ""));
            }
        }

        // if this is to be a shared object, put in the "-shared" option

        if (outputType == BuildType.SHARED) {
            linkStringBuilder.append("-shared ");
        }

        // if the OS is Mac OS, set the install name. The install name assumes that the library
        // will be installed in the build/lib directory under the root directory.

        if ((architecture == SystemArchitecture.MAC_M1
            || architecture == SystemArchitecture.MAC_INTEL) && outputType == BuildType.SHARED) {
            linkStringBuilder
                .append("-install_name " + libDir() + "/" + getBuiltFile().getName() + " ");
        }

        // add the object files, sorted into alphabetical order (to simplify testing).
        Set<File> sortedObjectFiles = new TreeSet<>(objectFiles);
        for (File objectFile : sortedObjectFiles) {
            linkStringBuilder.append(objectFile.getName() + " ");
        }

        // Add library names. These have come after the object files due to a positional
        // dependence in the Linux linker
        if (outputType != BuildType.STATIC) {
            linkStringBuilder.append(argListToString(libraries, "-l"));
            if (!StringUtils.isBlank(matlabLibPath)) {
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
    protected DefaultExecutor getDefaultExecutor(File workingDirectory) {
        if (defaultExecutor == null) {
            return DefaultExecutor.builder().setWorkingDirectory(workingDirectory).get();
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

        log.info("{}.action()", this.getClass().getSimpleName());

        // compile the source files
        compileAction();

        // perform the linker / archiver step
        linkAction();
    }

    protected void compileAction() {

        // create the obj directory

        File objDir = objDir();
        if (!objDir.exists()) {
            log.info("Creating directory {}", objDir.getAbsolutePath());
            objDir.mkdirs();
        }

        // Create a thread pool for compilation.
        ExecutorService compilerThreadPool = Executors.newFixedThreadPool(maxCompileThreads);
        Set<Future<CompilationResult>> compilationResults = new HashSet<>();

        // loop over source files, compile them and add the object file to the object file list
        for (Map.Entry<File, String> file : getSourceFiles(true).entrySet()) {
            compilationResults.add(compilerThreadPool.submit(() -> compileActionInternal(file)));
        }

        for (Future<CompilationResult> futureResult : compilationResults) {
            try {
                CompilationResult result = futureResult.get();
                if (result.getReturnCode() != 0) {
                    throw new GradleException(
                        "Compilation of file " + result.getSourceFile().getName() + " failed");
                }
                objectFiles.add(new File(objDir, objectNameFromSourceFile(result.getSourceFile())));
            } catch (ExecutionException | InterruptedException e) {
                throw new GradleException("Exception occurred during compilation", e);
            }
        }
    }

    private CompilationResult compileActionInternal(Map.Entry<File, String> sourceFile)
        throws ExecuteException, IOException {
        DefaultExecutor compilerExec = getDefaultExecutor(new File(sourceFilePaths.get(0)));
        int returnCode = compilerExec
            .execute(new CommandLineComparable(generateCompileCommand(sourceFile)));
        return new CompilationResult(sourceFile.getKey(), returnCode);
    }

    protected void linkAction() {

        File objDir = objDir();
        DefaultExecutor linkExec = getDefaultExecutor(objDir);
        File destDir = null;
        if (outputType.equals(BuildType.EXECUTABLE)) {
            destDir = binDir();
        } else {
            destDir = libDir();
        }
        if (!destDir.exists()) {
            log.info("Creating directory {}", destDir.getAbsolutePath());
            destDir.mkdirs();
        }
        try {
            String linkCommand = generateLinkCommand();
            int returnCode = linkExec.execute(new CommandLineComparable(linkCommand));
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
            File directory = new File(include);
            if (!directory.exists()) {
                // Strip rootDir prefix from the entry for easier reading (in most cases).
                String shortInclude = include;
                int index = include.indexOf(getRootDir().getAbsolutePath());
                if (index >= 0) {
                    // +1: Strip the / after rootDir as well.
                    shortInclude = include.substring(getRootDir().getAbsolutePath().length() + 1);
                }
                throw new GradleException("The directory " + shortInclude
                    + " specified in includeFilePaths does not exist");
            }
            File[] includeFiles = directory.listFiles(
                (FilenameFilter) (dir, name) -> name.endsWith(".h") || name.endsWith(".hpp"));
            for (File includeFile : includeFiles) {
                try {
                    if (!includeFile.getParentFile()
                        .getCanonicalFile()
                        .equals(includeDest.getCanonicalFile())) {
                        FileUtils.copyFileToDirectory(includeFile, includeDest);
                    }
                } catch (IOException e) {
                    throw new GradleException("Unable to copy include files from" + include + " to "
                        + includeDest.getAbsoluteFile(), e);
                }
            }
        }
    }

    /**
     * Converts a list of Objects to a list of Strings, preserving their order. Null objects, empty
     * strings, or strings consisting solely of whitespace are ignored.
     */
    static List<String> objectListToStringList(List<? extends Object> objectList) {
        List<String> stringList = new ArrayList<>();
        for (Object object : objectList) {
            if (object != null) {
                String string = object.toString();
                if (!string.isBlank()) {
                    stringList.add(string);
                }
            }
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
        }

        return objectListToStringList(List.of(gradleProperty));
    }

//	setters and getters

    public void setSourceFilePath(Object sourceFilePath) {
        sourceFilePaths = List.of(sourceFilePath.toString());
    }

    public List<String> getSourceFilePaths() {
        return sourceFilePaths;
    }

    public void setSourceFilePaths(List<Object> sourceFilePaths) {
        this.sourceFilePaths = objectListToStringList(sourceFilePaths);
    }

    public List<String> getIncludeFilePaths() {
        return includeFilePaths;
    }

    public void setIncludeFilePaths(List<? extends Object> includeFilePaths) {
        this.includeFilePaths = objectListToStringList(includeFilePaths);
    }

    public List<String> getLibraryPaths() {
        return libraryPaths;
    }

    public void setLibraryPaths(List<? extends Object> libraryPaths) {
        this.libraryPaths = objectListToStringList(libraryPaths);
    }

    public List<String> getLibraries() {
        return libraries;
    }

    public void setLibraries(List<? extends Object> libraries) {
        this.libraries = objectListToStringList(libraries);
    }

    public List<String> getcppCompileOptions() {
        return cppCompileOptions;
    }

    public void setcppCompileOptions(List<? extends Object> compileOptions) {
        cppCompileOptions = objectListToStringList(compileOptions);
    }

    public List<String> getcCompileOptions() {
        return cCompileOptions;
    }

    public void setcCompileOptions(List<? extends Object> compileOptions) {
        cCompileOptions = objectListToStringList(compileOptions);
    }

    public List<String> getLinkOptions() {
        return linkOptions;
    }

    public void setLinkOptions(List<? extends Object> linkOptions) {
        this.linkOptions = objectListToStringList(linkOptions);
    }

    public List<String> getReleaseOptimizations() {
        return releaseOptimizations;
    }

    public void setReleaseOptimizations(List<? extends Object> releaseOptimizations) {
        this.releaseOptimizations = objectListToStringList(releaseOptimizations);
    }

    public List<String> getDebugOptimizations() {
        return debugOptimizations;
    }

    public void setDebugOptimizations(List<? extends Object> debugOptimizations) {
        this.debugOptimizations = objectListToStringList(debugOptimizations);
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

    public List<File> getSourceFiles() {
        return new ArrayList<>(getSourceFiles(false).keySet());
    }

    public Map<File, String> getSourceFiles(boolean warn) {
        // Always generate the list afresh -- necessary because Gradle calls the ZiggyCpp
        // method getCppFiles() prior to the actual build, at which time the directories of
        // source files may or may not exist yet! Thus we can't afford to cache the C++
        // file list, since I can't tell whether Gradle creates a new ZiggyCpp object when
        // it actually does the build, or whether it simply re-uses the one from pre-build.
        populateSourceFiles(warn);
        return sourceFiles;
    }

    public List<File> getObjectFiles() {
        return objectFiles;
    }

    public void setObjectFiles(List<File> objectFiles) {
        this.objectFiles.addAll(objectFiles);
    }

    public void setObjectFiles(File objectFile) {
        objectFiles.add(objectFile);
    }

    public File getBuiltFile() {
        populateBuiltFile();
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

    public SystemArchitecture getArchitecture() {
        return architecture;
    }

    public String getOutputDir() {
        return outputDir;
    }

    public void setOutputDir(Object outputDir) {
        this.outputDir = outputDir.toString();
    }

    public String getOutputDirParent() {
        return outputDirParent;
    }

    public void setOutputDirParent(Object outputDirParent) {
        this.outputDirParent = outputDirParent.toString();
    }

    public int getMaxCompileThreads() {
        return maxCompileThreads;
    }

    public void setMaxCompileThreads(int maxCompileThreads) {
        this.maxCompileThreads = maxCompileThreads;
    }

    // this method is intended for use only in testing, for that reason it is package-private
    void setDefaultExecutor(DefaultExecutor defaultExecutor) {
        this.defaultExecutor = defaultExecutor;
    }

    // this method is intended for use only in testing, for that reason it is package-private
    void setArchitecture(SystemArchitecture architecture) {
        this.architecture = architecture;
    }

    /** For testing use only. */
    static void setCppCompiler(String testCompiler) {
        CPP_COMPILER = testCompiler;
    }

    /** For testing use only. */
    static void setCCompiler(String testCompiler) {
        C_COMPILER = testCompiler;
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

        @Override
        public boolean equals(Object o) {
            if (o instanceof CommandLine) {
                CommandLine oo = (CommandLine) o;
                if (toString().contentEquals(oo.toString())) {
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

    private static class CompilationResult {

        private final File sourceFile;
        private final int returnCode;

        public CompilationResult(File sourceFile, int returnCode) {
            this.sourceFile = sourceFile;
            this.returnCode = returnCode;
        }

        public File getSourceFile() {
            return sourceFile;
        }

        public int getReturnCode() {
            return returnCode;
        }
    }
}

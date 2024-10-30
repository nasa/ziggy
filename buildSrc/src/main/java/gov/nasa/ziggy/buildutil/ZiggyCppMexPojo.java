package gov.nasa.ziggy.buildutil;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.lang3.StringUtils;
import org.gradle.api.GradleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the construction of mexfiles from C/C++ source code. The source code is compiled using
 * the C++ compiler in the CXX environment variable, with appropriate compiler options for use in
 * creating object files that can be used in mexfiles. These object files are then combined into a
 * shared library. Finally, the C++ compiler is used to produce mexfiles for each source file that
 * contains the mexFunction entry point by linking the object files with the shared library and
 * attaching an appropriate file type. Because Gradle task classes cannot easily be unit tested, the
 * key functionality needed for mexfile construction is in this class; a separate class,
 * ZiggyCppMex, extends the Gradle DefaultTask and provides the interface from Gradle to
 * ZiggyCppMexPojo.
 *
 * @author PT
 */
public class ZiggyCppMexPojo extends ZiggyCppPojo {

    private static final Logger log = LoggerFactory.getLogger(ZiggyCppMexPojo.class);

    private static final List<String> DEFAULT_COMPILE_OPTIONS = List.of("-Wall", "-fPIC",
        "-std=c++11", "-D_GNU_SOURCE", "-fexceptions", "-fno-omit-frame-pointer", "-pthread",
        "-fno-reorder-blocks", "-fstack-protector-all", "-fpermissive");
    private static final List<String> DEFAULT_LINK_OPTIONS = List.of("-lstdc++");
    private static final List<String> DEFAULT_RELEASE_OPTS = List.of("-O2", "-DNDEBUG", "-g");
    private static final List<String> DEFAULT_DEBUG_OPTS = List.of("-Og", "-g");

    /** Path to the MATLAB directories to be used in the build */
    private String matlabPath;

    /** Names of the mexfiles that are to be built (these need to match names of C++ files) */
    private List<String> mexfileNames = null;

    /** Files for the mexfiles that are to be built (used to determine the task inputs) */
    private List<File> mexfiles = null;

    /** Project directory, used to generate a library file name */
    private File projectDir = null;

    public ZiggyCppMexPojo() {
        super.setOutputType(BuildType.SHARED);
        setcppCompileOptions(DEFAULT_COMPILE_OPTIONS);
        setLinkOptions(DEFAULT_LINK_OPTIONS);
        setReleaseOptimizations(DEFAULT_RELEASE_OPTS);
        setDebugOptimizations(DEFAULT_DEBUG_OPTS);
    }

    /**
     * Returns the correct file type for a mexfile given the OS.
     *
     * @return string "mexa64" for Linux, "mexmaca64" for Mac M1, "mexmaci64" for Mac Intel.
     */
    String mexSuffix() {
        return switch (getArchitecture()) {
            case LINUX_INTEL -> "mexa64";
            case MAC_M1 -> "mexmaca64";
            case MAC_INTEL -> "mexmaci64";
        };
    }

    /**
     * Returns the correct MATLAB architecture name given the OS.
     *
     * @return string "glnxa64" for Linux, "maci64" for Mac Intel, "maca64" for Mac M1.
     */
    String matlabArch() {
        return switch (getArchitecture()) {
            case LINUX_INTEL -> "glnxa64";
            case MAC_M1 -> "maca64";
            case MAC_INTEL -> "maci64";
        };
    }

    private File mexDir() {
        return StringUtils.isEmpty(outputDir) ? new File(outputParent(), "mex")
            : new File(outputDir);
    }

    /**
     * Generates the mexfiles that are the output of this class, and stores them in the mexfiles
     * list. The files that are generated are named $mexfileName.$mexfileSuffix, and are stored in
     * $buildDir/mex .
     */
    void populateMexfiles() {
        if (getBuildDir() == null || mexfileNames == null) {
            throw new GradleException("buildDir and mexfileNames must not be null");
        }
        mexfiles = new ArrayList<>();
        for (String mexfileName : mexfileNames) {
            String fullMexfileName = mexfileName + "." + mexSuffix();
            File mexfile = new File(mexDir(), fullMexfileName);
            mexfiles.add(mexfile);
        }
    }

    /**
     * Generates a Map between the mexfiles and their corresponding object files.
     *
     * @return HashMap from mexfiles to object files. If any mexfile is missing its corresponding
     * object file, a GradleException is thrown.
     */
    private Map<File, File> mapMexfilesToObjectFiles() {

        // A linked hashmap is used to preserve order -- which doesn't matter so
        // much for actual use (though it is convenient), but matters a lot for testing
        Map<File, File> mexfileMap = new LinkedHashMap<>();
        List<File> mexfiles = getMexfiles();
        List<File> objfiles = getObjectFiles();
        for (String mexfileName : mexfileNames) {
            File mexfile = getFileByName(mexfiles, mexfileName);
            File objfile = getFileByName(objfiles, mexfileName);
            if (objfile == null) {
                throw new GradleException("No object file for mexfile " + mexfileName);
            }
            mexfileMap.put(mexfile, objfile);
        }
        return mexfileMap;
    }

    /**
     * Finds the file out of a list of files that has a particular name when the file type is
     * removed.
     *
     * @param files List of files
     * @param fileName Name of desired match, assumed to have no file type attached to it.
     * @return File with a name that contains the desired match, or null if no match is found.
     */
    private File getFileByName(List<File> files, String fileName) {
        File foundFile = null;
        for (File file : files) {
            String nameOfFile = file.getName();
            int finalDot = nameOfFile.lastIndexOf('.');
            String nameWithoutType = nameOfFile.substring(0, finalDot);
            if (nameWithoutType.equals(fileName)) {
                foundFile = file;
                break;
            }
        }
        return foundFile;
    }

    /**
     * Generates the command to perform compilation of a source file. The command includes the
     * MATLAB include directory as an include path, and includes the mexfile compiler directive.
     */
    @Override
    public String generateCompileCommand(Map.Entry<File, String> sourceFile) {
        return generateCompileCommand(sourceFile.getKey(), sourceFile.getValue());
    }

    @Override
    public String generateCompileCommand(File sourceFile, String compiler) {
        String matlabIncludePath = matlabPath + "/extern/include";
        return generateCompileCommand(sourceFile, compiler, matlabIncludePath, "DMATLAB_MEX_FILE");
    }

    public String matlabLibPath() {
        return matlabPath + "/bin/" + matlabArch();
    }

    @Override
    protected void populateBuiltFile() {
        if (StringUtils.isBlank(getOutputName())) {
            setOutputName(generateSharedObjectName());
        }
        super.populateBuiltFile();
    }

    /**
     * Generates the command to link the source files into a shared object. The commad includes the
     * MATLAB library path and library names. If the user has not selected a name for the library,
     * one will be generated from the project and C++ file paths.
     */
    @Override
    public String generateLinkCommand() {

        // if the build file is not set, then set it now to a default value
        if (StringUtils.isBlank(getOutputName())) {
            setOutputName(generateSharedObjectName());
        }

        // construct the path to the MATLAB shared object libraries
        return generateLinkCommand(matlabLibPath());
    }

    /**
     * Generates a name for the shared object library in the event that none has been set. This is
     * done by taking the project name and adding to it the components of the C++ path name,
     * separated by hyphens. For example, if the project directory is /path/to/pipeline/module1, and
     * the source directory is /path/to/pipeline/module1/src/main/cpp/mex, the name of the library
     * will be module1-src-main-cpp-mex, resulting in a shared object named
     * libmodule1-src-main-cpp-mex.so or .dylib.
     *
     * @return Generated name for the shared object library.
     */
    public String generateSharedObjectName() {
        String objectNameStart = getProjectDir().getName();
        int projectDirLength = getProjectDir().getAbsolutePath().length();
        String truncatedCppPath = getSourceFilePaths().get(0).substring(projectDirLength + 1);
        String[] truncatedCppPathParts = truncatedCppPath.split("/");
        StringBuilder sharedObjectNameBuilder = new StringBuilder();
        sharedObjectNameBuilder.append(objectNameStart + "-");
        for (int i = 0; i < truncatedCppPathParts.length; i++) {
            sharedObjectNameBuilder.append(truncatedCppPathParts[i]);
            if (i < truncatedCppPathParts.length - 1) {
                sharedObjectNameBuilder.append("-");
            }
        }
        return sharedObjectNameBuilder.toString();
    }

    /**
     * Generates the mex command for a given file.
     *
     * @param mexfile The desired mexfile output.
     * @param obj The object file with the mexFunction entry point for the mexfile.
     * @return A complete mex command in string form.
     */
    public String generateMexCommand(File mexfile, File obj) {

        // if the build file is not set, then set it now to a default value
        if (StringUtils.isBlank(getOutputName())) {
            setOutputName(generateSharedObjectName());
        }

        StringBuilder mexCommandBuilder = new StringBuilder();
        mexCommandBuilder.append(ZiggyCppPojo.Compiler.CPP.compiler() + " ");
        mexCommandBuilder.append("-o " + mexfile.getAbsolutePath() + " ");
        mexCommandBuilder.append(obj.getAbsolutePath() + " ");
        mexCommandBuilder.append(argListToString(getLibraryPaths(), "-L"));
        mexCommandBuilder.append("-L" + matlabLibPath() + " ");
        mexCommandBuilder.append("-L" + libDir().getAbsolutePath() + " ");
        mexCommandBuilder.append(argListToString(getLibraries(), "-l"));
        mexCommandBuilder.append("-lmex -lmx -lmat ");
        mexCommandBuilder.append("-l" + getOutputName() + " -shared");
        return mexCommandBuilder.toString();
    }

    /**
     * Main method used by Gradle. This method starts by using the ZiggyCppPojo action() method to
     * compile the source files and build the shared library. The desired mexfiles are then looped
     * over, and the mexfile commands are run by a DefaultExecutor.
     */
    @Override
    public void action() {

        log.info("{}.action()", this.getClass().getSimpleName());

        // Start by performing the compilation
        compileAction();

        // Map the mexfiles to their object files
        Map<File, File> mexfileMap = mapMexfilesToObjectFiles();

        // remove the mexfile objects from the list of objects so they don't go into the
        // shared object library
        getObjectFiles().removeAll(mexfileMap.values());

        // construct the shared object
        linkAction();

        // loop over mexfiles
        for (File mexfile : mexfileMap.keySet()) {
            String mexCommand = generateMexCommand(mexfile, mexfileMap.get(mexfile));
            log.info(mexCommand);
            DefaultExecutor mexExec = getDefaultExecutor(objDir());

            // execute the mex command
            try {
                int returnCode = mexExec.execute(new CommandLineComparable(mexCommand));
                if (returnCode != 0) {
                    throw new GradleException("Mexing of file " + mexfile.getName() + " failed");
                }
            } catch (IOException e) {
                throw new GradleException("Mexing of file " + mexfile + " failed", e);
            }
        }
    }

    // Build type is not optional for the C++ Mex builds
    @Override
    public void setOutputType(BuildType buildType) {
        log.warn("ZiggyCppMex does not support build types other than shared");
    }

    @Override
    public void setOutputType(Object buildType) {
        log.warn("ZiggyCppMex does not support build types other than shared");
    }

    // Setters and getters
    public void setMexfileNames(List<? extends Object> mexfileNames) {
        this.mexfileNames = ZiggyCppPojo.objectListToStringList(mexfileNames);
    }

    public List<String> getMexfileNames() {
        return mexfileNames;
    }

    public void setMatlabPath(Object matlabPath) {
        this.matlabPath = matlabPath.toString();
    }

    public String getMatlabPath() {
        return matlabPath;
    }

    public List<File> getMexfiles() {
        if (mexfiles == null) {
            populateMexfiles();
        }
        return mexfiles;
    }

    public File getProjectDir() {
        return projectDir;
    }

    public void setProjectDir(File projectDir) {
        this.projectDir = projectDir;
    }
}

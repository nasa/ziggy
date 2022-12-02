package gov.nasa.ziggy.module;

import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Writer;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.environment.EnvironmentUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.metrics.IntervalMetric;
import gov.nasa.ziggy.metrics.IntervalMetricKey;
import gov.nasa.ziggy.module.io.ModuleInterfaceUtils;
import gov.nasa.ziggy.module.io.matlab.MatlabUtils;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyNames;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.process.ExternalProcess;
import gov.nasa.ziggy.util.os.OperatingSystemType;

/**
 * This class encapsulates the setup and execution algorithm code for a single subtask. The code is
 * assumed to be runnable from the shell using the binary name for invocation. It also invokes the
 * populateSubTaskInputs() method of the module's {@link PipelineInputs} subclass prior to running
 * the algorithm, and invokes the populateTaskResults() method of the module's
 * {@link PipelineOutputs} subclass subsequent to running the algorithm.
 * <p>
 * This class uses {@link ExternalProcess} to launch and monitor the standalone executable.
 *
 * @author Todd Klaus
 * @author PT
 */
public class SubtaskExecutor {
    private static final Logger log = LoggerFactory.getLogger(SubtaskExecutor.class);

    public static final String MATLAB_PROCESS_EXEC_METRIC = "pipeline.module.executeAlgorithm.matlab.all.execTime";
    private static final String MATLABHOME_ENV_NAME = "MATLABHOME";
    private static final String MCRROOT_ENV_NAME = "MCRROOT";
    private static final String LM_LICENSE_FILE_ENV_NAME = "LM_LICENSE_FILE";
    private static final String PIPELINE_HOME_ENV_NAME = "PIPELINE_HOME";
    private static final String MCR_CACHE_ROOT_ENV_VAR_NAME = "MCR_CACHE_ROOT";
    private static final String CODE_ROOT_ENV_NAME = "CODE_ROOT";

    private final String binaryName;
    private final File taskDir;
    private final File workingDir;
    private final int timeoutSecs;
    private final String pipelineHomeDir;
    private final String pipelineConfigPath;

    private CommandLine commandLine;
    private Map<String, String> environment = new HashMap<>();

    private OperatingSystemType osType = OperatingSystemType.getInstance();

    private String libPath;
    private String binPath;
    private File binaryDir;

    // Constructor is private, use the builder instead.
    private SubtaskExecutor(File taskDir, int subtaskIndex, String binaryName, int timeoutSecs,
        String pipelineHomeDir, String pipelineConfigPath) {
        this.taskDir = taskDir;
        workingDir = TaskConfigurationManager.subtaskDirectory(taskDir, subtaskIndex);
        this.pipelineHomeDir = pipelineHomeDir;
        this.pipelineConfigPath = pipelineConfigPath;
        this.timeoutSecs = timeoutSecs;
        this.binaryName = binaryName;
    }

    // Initialization is private, use the builder instead.
    private void initialize() throws IOException {
        SubtaskUtils.putLogStreamIdentifier(workingDir);
        Configuration config = ZiggyConfiguration.getInstance();
        libPath = config.getString(PropertyNames.MODULE_EXE_LIBPATH_PROPERTY_NAME, "");

        String mcrRoot = config.getString(PropertyNames.MODULE_EXE_MCRROOT_PROPERTY_NAME, null);
        if (mcrRoot != null && !mcrRoot.isEmpty()) {
            libPath = libPath + (!libPath.isEmpty() ? File.pathSeparator : "")
                + MatlabUtils.mcrPaths(mcrRoot);
        }

        // Search for the application first in the path specified by the
        // MODULE_EXE_BINPATH_PROPERTY_NAME property followed by the pipeline's bin directory.
        binPath = config.getString(PropertyNames.MODULE_EXE_BINPATH_PROPERTY_NAME, null);
        if (binPath == null || binPath.isEmpty()) {
            binPath = DirectoryProperties.pipelineBinDir().toString();
        } else {
            binPath = binPath + File.pathSeparator + DirectoryProperties.pipelineBinDir();
        }

        // find the correct directory
        binaryDir = binaryDir(binPath, binaryName);
        if (binaryDir == null) {
            markSubtaskFailed(workingDir);
            throw new ModuleFatalProcessingException(
                "Unable to locate executable file " + binaryName + " in pathset " + binPath);
        }

        String hostname = "<unknown>";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            log.warn("failed to get hostname", e);
        }

        log.info("osType = " + osType.toString());
        log.info("hostname = " + hostname);
        log.info("binaryDir = " + binaryDir);
        log.info("binaryName = " + binaryName);
        log.info("libPath = " + libPath);

        // Construct the environment
        environment.put(MCR_CACHE_ROOT_ENV_VAR_NAME,
            Paths.get("/tmp", "mcr_cache_" + taskDir.getName()).toString());
        environment.put("PATH", binPath);
        environment.put(CODE_ROOT_ENV_NAME, Paths.get(pipelineHomeDir).getParent().toString());
        environment.put(ZiggyConfiguration.CONFIG_SERVICE_PROPERTIES_PATH_ENV, pipelineConfigPath);

        // Create the MCR cache directory if needed
        Files.createDirectories(Paths.get(environment.get(MCR_CACHE_ROOT_ENV_VAR_NAME)));

        // Populate the environment from the properties file.
        populateEnvironmentFromPropertiesFile();
    }

    /**
     * Finds the directory for a given executable given a path string. Note that if the executable
     * is not found anywhere in the search path, and the pipeline is running on a Mac, there will
     * also be a search for the binary with ".app" appended.
     *
     * @param binPathString
     * @param binaryName
     * @return
     */
    private File binaryDir(String binPathString, String binaryName) {
        File binFile = binaryDirInternal(binPathString, binaryName, null);
        if (binFile == null && OperatingSystemType.getInstance() == OperatingSystemType.MAC_OS_X) {
            binFile = binaryDirInternal(binPathString, binaryName,
                new String[] { binaryName + ".app", "Contents", "MacOS" });
        }
        return binFile;
    }

    private File binaryDirInternal(String binPathString, String binaryName, String[] pathSuffix) {

        log.info("Searching for binary " + binaryName + " in path " + binPathString);
        File binFile = null;
        String[] binPaths = binPathString.split(File.pathSeparator);
        for (String binPath : binPaths) {
            String trimmedBinPath = binPath.trim();
            if (trimmedBinPath.isEmpty()) {
                continue;
            }
            File binPathFile = binPathFile(binPath, pathSuffix);
            if (!binPathFile.exists() || !binPathFile.isDirectory()) {
                continue;
            }
            File[] dirContents = binPathFile.listFiles((FilenameFilter) (dir,
                string) -> string.equals(binaryName) && new File(dir, string).isFile());
            if (dirContents.length > 0) {
                binFile = dirContents[0];
                break;
            }
        }
        if (binFile != null) {
            binFile = binFile.getAbsoluteFile().getParentFile();
        }
        return binFile;
    }

    private File binPathFile(String binPath, String[] pathSuffix) {
        File binPathFile = new File(binPath);
        if (pathSuffix != null) {
            Path binPathPath = binPathFile.toPath();
            for (String suffix : pathSuffix) {
                binPathPath = binPathPath.resolve(suffix);
            }
            binPathFile = binPathPath.toFile();
        }
        return binPathFile;
    }

    /**
     * Populates the runtime environment from the contents of a property.
     * <p>
     * The property file for the pipeline can contain a String that specifies environment variables
     * in the form "name1=value1, name2=value2", etc. This method will perform the appropriate
     * string manipulations to convert that String to a {@link Map} of name-value pairs that can
     * then be used by the {@link DefaultExecutor}.
     */
    private void populateEnvironmentFromPropertiesFile() {

        Configuration config = ZiggyConfiguration.getInstance();
        String environmentFromProperties = config
            .getString(PropertyNames.RUNTIME_ENVIRONMENT_PROPERTY_NAME, null);
        if (environmentFromProperties == null) {
            return;
        }

        // Break the string at the commas that separate environment variables from one another.
        String[] environmentVariables = environmentFromProperties.split(",");

        for (String environmentVariable : environmentVariables) {

            // Break the environment variable string at the equals signs that separate the name
            // of the environment variable from the value.
            String[] nameValuePair = environmentVariable.split("=");
            if (nameValuePair.length != 2) {
                throw new IllegalArgumentException("String '" + environmentVariable
                    + "' cannot be split into a name-value pair at an equas sign");
            }
            environment.put(nameValuePair[0].trim(), nameValuePair[1].trim());
        }
        StringBuilder sb = new StringBuilder("Environment: [");
        for (Map.Entry<String, String> entry : environment.entrySet()) {
            sb.append(entry.getKey());
            sb.append("=");
            sb.append(entry.getValue());
            sb.append(", ");
        }
        sb.setLength(sb.length() - 2);
        sb.append("]");
        log.info("Execution environment: " + sb.toString());

    }

    /**
     * Executes the algorithm on the subtask (including input and output handling) and returns the
     * exit code.
     */
    public int execAlgorithm() {

        if (!workingDir.exists() || !workingDir.isDirectory()) {
            System.err
                .println("WORKING_DIR (" + workingDir + ") does not exist or is not a directory");
            System.exit(-1);
        }

        int retCode = -1;

        try {
            File errorFile = ModuleInterfaceUtils.errorFile(workingDir, binaryName, 0);
            if (errorFile.exists()) {
                log.info("Deleting stale error file prior to start of processing");
                errorFile.delete();
            }
            retCode = execAlgorithmInternal(0);

            if (retCode != 0) {
                log.warn("Marking subtask as failed because retCode = " + retCode);
                markSubtaskFailed(workingDir);
            }

            if (errorFile.exists()) {
                log.warn("Marking subtask as failed because an error file exists");
                markSubtaskFailed(workingDir);
            }
        } catch (Exception e) {
            log.warn("Marking subtask as failed because a Java-side exception occurred", e);
            markSubtaskFailed(workingDir);
        }
        return retCode;
    }

    /**
     * @throws PipelineException when an exception occurs during Java-side execution (not when the
     * algorithm errors or generation of inputs or results errors).
     */
    public int execAlgorithmInternal(int sequenceNum) throws PipelineException {
        try {
            List<String> commandLineArgs = new LinkedList<>();
            String exePath = workingDir.getCanonicalPath();
            commandLineArgs.add(exePath);
            commandLineArgs.add("" + sequenceNum);

            AlgorithmStateFiles stateFile = new AlgorithmStateFiles(workingDir);
            stateFile.updateCurrentState(AlgorithmStateFiles.SubtaskState.PROCESSING);

            boolean inputsProcessingSucceeded = false;
            boolean algorithmProcessingSucceeded = false;

            int retCode = -1;
            IntervalMetricKey key = IntervalMetric.start();
            try {
                key = IntervalMetric.start();
                TaskConfigurationManager taskConfigurationManager = taskConfigurationManager();
                Class<? extends PipelineInputs> inputsClass = taskConfigurationManager
                    .getInputsClass();
                retCode = runInputsOutputsCommand(inputsClass);
                if (retCode == 0) {
                    inputsProcessingSucceeded = true;
                    retCode = runCommandline(commandLineArgs, binaryName, "" + sequenceNum);
                }
                if (retCode == 0) {
                    algorithmProcessingSucceeded = true;
                    Class<? extends PipelineOutputs> outputsClass = taskConfigurationManager
                        .getOutputsClass();
                    retCode = runInputsOutputsCommand(outputsClass);
                }
            } finally {
                IntervalMetric.stop(MATLAB_PROCESS_EXEC_METRIC, key);
            }

            File errorFile = ModuleInterfaceUtils.errorFile(workingDir, binaryName, sequenceNum);

            // If the state file for the task is in the DELETED state, then the subtask should
            // not be put into the COMPLETED or FAILED states, but rather should be left in
            // PROCESSING. This preserves the symmetry with the way that remote execution subtasks
            // are left in the PROCESSING state when their parent jobs get deleted from PBS.
            StateFile taskStateFile = StateFile.of(workingDir.toPath().getParent());
            taskStateFile = StateFile.newStateFileFromDiskFile(
                DirectoryProperties.stateFilesDir().resolve(taskStateFile.name()).toFile(), true);
            if (taskStateFile.getState().equals(StateFile.State.DELETED)) {
                log.error("Task deleted, ending execution of sub-task");
                return 0;
            }

            if (retCode == 0 && !errorFile.exists()) {
                stateFile.updateCurrentState(AlgorithmStateFiles.SubtaskState.COMPLETE);
            } else {
                /*
                 * Don't handle an error in processing at this point in execution. Instead, allow
                 * the caller to respond to the return code and let an exception be thrown at a
                 * higher level, after some error-management tasks have been completed.
                 */

                stateFile.updateCurrentState(AlgorithmStateFiles.SubtaskState.FAILED);
                if (retCode != 0) {
                    if (!inputsProcessingSucceeded) {
                        log.error("failed to generate sub-task inputs, retCode = " + retCode);
                    } else if (algorithmProcessingSucceeded) {
                        log.error("failed to generate task results, retCode = " + retCode);
                    }
                } else {
                    log.info("Algorithm process completed, retCode=" + retCode);
                }
            }

            return retCode;
        } catch (Exception e) {
            throw new PipelineException(e);
        }
    }

    /**
     * Run an arbitrary process with caller-specified arguments. No {@link AlgorithmStateFiles} is
     * created and a default logSuffix of "0" is used.
     *
     * @param commandLineArgs
     * @return
     * @throws Exception
     */
    public int execSimple(List<String> commandLineArgs) throws Exception {
        int retCode = runCommandline(commandLineArgs, binaryName + "-", "0");
        log.info("execSimple: retCode = " + retCode);

        return retCode;
    }

    /**
     * Executes the {@link TaskFileManager#main()} method with appropriate arguments for either
     * invoking the inputs class process that generates sub-task inputs, or the outputs class
     * process that generates results files in the task directory.
     * <p>
     * Given that {@link TaskFileManager} is a Java class, and this is a Java class, why is the
     * {@link TaskFileManager} invoked using runjava and an external process? By using an external
     * process, the inputs and outputs classes can use software libraries that do not support
     * concurrency (for example, HDF5). By running each subtask in a separate process, the subtask
     * input and output processing can execute in parallel even in cases in which the processing
     * uses non-concurrent libraries. Running a bunch of instances of {@link TaskFileManager} in
     * separate threads within a common JVM would not permit this.
     *
     * @param inputsOutputsClass Class to be used as argument to TaskFileManager.
     * @return exit code from runjava, or -1 if an IOException occurs during the command.
     * @throws IOException if the environment cannot be obtained
     */
    int runInputsOutputsCommand(Class<?> inputsOutputsClass) throws IOException {

        int retCode = -1;

        // Locate the runjava executable, which can be someplace other than the bin directories for
        // the pipeline.
        String runjavaCommand = DirectoryProperties.ziggyBinDir() + "/runjava";

        // Build the string that sets up that location as the Java library path
        String javaLibPathArg = "-Djava.library.path=" + DirectoryProperties.ziggyLibDir();

        // Build the string that sets up the Log4j config file location
        String log4jConfig = "-Dlog4j2.configurationFile=" + DirectoryProperties.ziggyHomeDir()
            + "/etc/log4j2.xml";

        // Construct the class arguments for runjava.
        String taskFileManagerClassName = TaskFileManager.class.getCanonicalName();
        String inputsOutputsClassName = inputsOutputsClass.getCanonicalName();

        // Put it all together.
        commandLine = new CommandLine(runjavaCommand);
        commandLine.addArgument("--verbose");
        commandLine.addArgument(javaLibPathArg);
        commandLine.addArgument(log4jConfig);

        commandLine.addArgument(taskFileManagerClassName);
        commandLine.addArgument(inputsOutputsClassName);

        // Run the command and return the exit code.
        ExternalProcess externalProcess = externalProcess(null, null);
        externalProcess.setLogStreamIdentifier(workingDir.getName());
        externalProcess.setWorkingDirectory(workingDir);
        externalProcess.setCommandLine(commandLine);
        externalProcess.setEnvironment(mergeWithEnvironment(EnvironmentUtils.getProcEnvironment()));

        log.info("Executing command: " + commandLine.toString());
        retCode = externalProcess.execute();
        return retCode;
    }

    int runCommandline(List<String> commandline, String logPrefix, String logSuffix)
        throws Exception {
        try (
            FileWriter stdOutWriter = new FileWriter(
                new File(workingDir, ModuleInterfaceUtils.stdoutFileName(logPrefix, logSuffix)));
            FileWriter stdErrWriter = new FileWriter(
                new File(workingDir, ModuleInterfaceUtils.stderrFileName(logPrefix, logSuffix)))) {
            File binary = new File(binaryDir.getPath(), binaryName);
            if ((!binary.exists() || !binary.isFile())
                && OperatingSystemType.getInstance() == OperatingSystemType.MAC_OS_X) {
                binary = new File(binaryDir.getPath(),
                    binaryName + ".app/Contents/MacOS/" + binaryName);
            }

            log.info("executing " + binary);

            commandLine = new CommandLine(binary.getCanonicalPath());
            for (String element : commandline) {
                commandLine.addArgument(element);
            }

            log.info("CommandLine: " + commandLine);

            Map<String, String> env = EnvironmentUtils.getProcEnvironment();

            // make sure DISPLAY is not set, so MATLAB can't pop up windows
            // (which block the exe from exiting)
            env.remove("DISPLAY");

            /*
             * http://www.mathworks.com/support/solutions/en/data/1-D40UP3/index.html
             *
             * "When running a deployed application, please make sure that the environment variable
             * MATLABHOME is always set to MCRROOT. If not, the application will run as though it is
             * running inside MATLAB. Hence, the licensing server will be contacted."
             */
            Configuration config = ZiggyConfiguration.getInstance();
            String mcrRoot = config.getString(PropertyNames.MODULE_EXE_MCRROOT_PROPERTY_NAME, null);

            if (mcrRoot != null) {
                env.put(MATLABHOME_ENV_NAME, mcrRoot);
                env.put(MCRROOT_ENV_NAME, mcrRoot);
            } else {
                env.put(MATLABHOME_ENV_NAME, "");
                env.put(MCRROOT_ENV_NAME, "");
            }

            env.put(PIPELINE_HOME_ENV_NAME, DirectoryProperties.pipelineHomeDir().toString());

            env.put(osType.getSharedObjectPathEnvVar(), libPath);

            // Make sure LM_LICENSE_FILE is set to /dev/null since it otherwise
            // may cause
            // undesirable access to the MATLAB license server at run time
            env.put(LM_LICENSE_FILE_ENV_NAME, "/dev/null");
            int retCode = 0;
            IntervalMetricKey key = IntervalMetric.start();

            try {
                ExternalProcess externalProcess = externalProcess(stdOutWriter, stdErrWriter);
                externalProcess.setLogStreamIdentifier(workingDir.getName());
                externalProcess.setWorkingDirectory(workingDir);
                externalProcess.setEnvironment(mergeWithEnvironment(env));
                externalProcess.timeout(timeoutSecs * 1000);
                externalProcess.setCommandLine(commandLine);

                log.info("env = " + env);
                retCode = externalProcess.execute();
            } finally {
                IntervalMetric.stop("pipeline.module.externalProcess." + binaryName + ".execTime",
                    key);
            }

            return retCode;
        }
    }

    private Map<String, String> mergeWithEnvironment(Map<String, String> additionalEnvironment) {
        Map<String, String> fullEnvironment = new HashMap<>();
        fullEnvironment.putAll(additionalEnvironment);
        fullEnvironment.putAll(environment);

        return fullEnvironment;
    }

    private static void markSubtaskFailed(File workingDir) {
        AlgorithmStateFiles subTaskState = new AlgorithmStateFiles(workingDir);
        if (subTaskState.currentSubtaskState() != AlgorithmStateFiles.SubtaskState.FAILED) {
            try {
                subTaskState.updateCurrentState(AlgorithmStateFiles.SubtaskState.FAILED);
            } catch (IOException e1) {
                log.error("failed to create .FAILED file for subTask: " + workingDir);
            }
        }
    }

    /**
     * Obtains a new {@link ExternalProcess}. Package-private to allow it to be overridden in tests.
     *
     * @return new ExternalProcess instance.
     */
    ExternalProcess externalProcess(Writer stdoutWriter, Writer stderrWriter) {
        return new ExternalProcess(true, stdoutWriter, true, stderrWriter);
    }

    // getters for test use only
    String binaryName() {
        return binaryName;
    }

    String libPath() {
        return libPath;
    }

    File binaryDir() {
        return binaryDir;
    }

    int timeoutSecs() {
        return timeoutSecs;
    }

    CommandLine commandLine() {
        return commandLine;
    }

    TaskConfigurationManager taskConfigurationManager() {
        return TaskConfigurationManager.restore(workingDir.getParentFile());
    }

    public static class Builder {

        private File taskDir;
        private int subtaskIndex = -1;
        private int timeoutSecs = -1;
        private String pipelineHomeDir;
        private String pipelineConfigPath;
        private String binaryName;

        public Builder() {
        }

        public Builder taskDir(File taskDir) {
            this.taskDir = taskDir;
            return this;
        }

        public Builder subtaskIndex(int subtaskIndex) {
            this.subtaskIndex = subtaskIndex;
            return this;
        }

        public Builder timeoutSecs(int timeoutSecs) {
            this.timeoutSecs = timeoutSecs;
            return this;
        }

        public Builder pipelineHomeDir(String pipelineHomeDir) {
            this.pipelineHomeDir = pipelineHomeDir;
            return this;
        }

        public Builder pipelineConfigPath(String pipelineConfigPath) {
            this.pipelineConfigPath = pipelineConfigPath;
            return this;
        }

        public Builder binaryName(String binaryName) {
            this.binaryName = binaryName;
            return this;
        }

        public SubtaskExecutor build() throws IOException {
            StringBuilder sb = new StringBuilder();
            if (taskDir == null) {
                sb.append("taskDir ");
            }
            if (subtaskIndex < 0) {
                sb.append("subtaskIndex ");
            }
            if (timeoutSecs <= 0) {
                sb.append("timeoutSecs ");
            }
            if (pipelineHomeDir == null) {
                sb.append("pipelineHomeDir ");
            }
            if (pipelineConfigPath == null) {
                sb.append("pipelineConfigPath ");
            }
            if (binaryName == null) {
                sb.append("binaryName ");
            }
            if (sb.length() > 0) {
                sb.setLength(sb.length() - 1);
                throw new PipelineException(
                    "Unable to build SubtaskExecutor, bad fields: " + sb.toString());
            }
            SubtaskExecutor subtaskExecutor = new SubtaskExecutor(taskDir, subtaskIndex, binaryName,
                timeoutSecs, pipelineHomeDir, pipelineConfigPath);
            subtaskExecutor.initialize();
            return subtaskExecutor;
        }
    }

}

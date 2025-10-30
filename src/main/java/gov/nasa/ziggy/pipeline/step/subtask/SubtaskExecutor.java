package gov.nasa.ziggy.pipeline.step.subtask;

import static gov.nasa.ziggy.pipeline.step.AlgorithmExecutor.ZIGGY_PROGRAM;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration2.ImmutableConfiguration;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.metrics.IntervalMetric;
import gov.nasa.ziggy.metrics.IntervalMetricKey;
import gov.nasa.ziggy.pipeline.step.AlgorithmStateFiles;
import gov.nasa.ziggy.pipeline.step.FatalAlgorithmProcessingException;
import gov.nasa.ziggy.pipeline.step.TaskConfiguration;
import gov.nasa.ziggy.pipeline.step.io.AlgorithmInterfaceUtils;
import gov.nasa.ziggy.pipeline.step.io.PipelineInputs;
import gov.nasa.ziggy.pipeline.step.io.PipelineInputsOutputsUtils;
import gov.nasa.ziggy.pipeline.step.io.PipelineOutputs;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyName;
import gov.nasa.ziggy.services.config.ZiggyConfiguration;
import gov.nasa.ziggy.services.logging.ZiggyLog;
import gov.nasa.ziggy.services.process.ExternalProcess;
import gov.nasa.ziggy.services.process.ExternalProcessUtils;
import gov.nasa.ziggy.util.AcceptableCatchBlock;
import gov.nasa.ziggy.util.AcceptableCatchBlock.Rationale;
import gov.nasa.ziggy.util.HostNameUtils;
import gov.nasa.ziggy.util.io.ZiggyFileUtils;
import gov.nasa.ziggy.util.os.OperatingSystemType;

/**
 * This class encapsulates the setup and execution algorithm code for a single subtask. The code is
 * assumed to be runnable from the shell using the binary name for invocation. It also invokes the
 * populateSubtaskInputs() method of the step's {@link PipelineInputs} subclass prior to running the
 * algorithm, and invokes the populateTaskResults() method of the step's {@link PipelineOutputs}
 * subclass subsequent to running the algorithm.
 * <p>
 * This class uses {@link ExternalProcess} to launch and monitor the standalone executable.
 * <p>
 * This class supports generic algorithms that can be called from the command line without any
 * additional support. Subclasses of {@link SubtaskExecutor} can provide additional language-
 * specific features for algorithms in the given language. This is accomplished by overriding
 * {@link #algorithmCommandLine} to produce a custom command line for execution by
 * {@link ExternalProcess}, and overriding {@link #customizeEnvironment()} to supply additional
 * environment variables and do any other preparations needed for algorithm execution (creation of
 * directories, etc.).
 *
 * @author Todd Klaus
 * @author PT
 */
public class SubtaskExecutor {
    private static final Logger log = LoggerFactory.getLogger(SubtaskExecutor.class);

    public static final String PYTHON_SUFFIX = ".py";
    public static final String MATLAB_PROCESS_EXEC_METRIC = "pipeline.module.executeAlgorithm.matlab.all.execTime";

    private static final String HEAP_SIZE_PROPERTY_NAME = "-Xmx";
    private static final String HEAP_SIZE_MB_SUFFIX = "M";
    private static final String HEAP_SIZE_GB_SUFFIX = "G";

    private final String binaryName;
    private final File workingDir;
    private final int timeoutSecs;
    private final Path taskDir;
    private final float heapSizeGigabytes;
    private final TaskConfiguration taskConfiguration;

    private Map<String, String> environment;

    private OperatingSystemType osType = OperatingSystemType.newInstance();

    private File binaryDir;

    private CommandLine algorithmCommandLine;

    // Use newInstance(SubtaskMaster), not the constructor.
    SubtaskExecutor(File taskDir, int subtaskIndex, String binaryName, int timeoutSecs,
        float heapSizeGigabytes, TaskConfiguration taskConfiguration) {
        this.taskDir = taskDir.toPath();
        workingDir = SubtaskUtils.subtaskDirectory(this.taskDir, subtaskIndex).toFile();
        this.timeoutSecs = timeoutSecs;
        this.binaryName = binaryName;
        this.heapSizeGigabytes = heapSizeGigabytes;
        this.taskConfiguration = taskConfiguration;
    }

    // Initialization is protected, use newInstance(SubtaskMaster) to get a fully-initialized
    // instance.
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    protected void initialize() {
        SubtaskUtils.putLogStreamIdentifier(workingDir);
        ImmutableConfiguration config = ZiggyConfiguration.getInstance();
        String libPath = config.getString(PropertyName.LIBPATH.property(), "");

        String binPath = binPath();

        // find the correct directory
        binaryDir = binaryDir(binPath, binaryName);
        if (binaryDir == null) {
            markSubtaskFailed(workingDir);
            throw new FatalAlgorithmProcessingException(
                "Unable to locate executable file " + binaryName + " in pathset " + binPath);
        }

        String hostname = HostNameUtils.shortHostName();

        log.info("osType = {}", osType.toString());
        log.info("hostname = {}", hostname);
        log.info("binaryDir = {}", binaryDir);
        log.info("binaryName = {}", binaryName);
        log.info("libPath = {}", libPath);

        // Construct the environment
        environment = ExternalProcess.valueByVariableName(PropertyName.RUNTIME_ENVIRONMENT);
        environment.put("PATH", binPath);

        // Make sure DISPLAY is not set, so algorithms can't open new windows that
        // prevent completion of algorithm execution.
        environment.remove("DISPLAY");

        environment.put(osType.getSharedObjectPathEnvVar(), libPath);

        // Add any custom environment settings.
        environment.putAll(customizeEnvironment());

        log.info("Execution environment is {}", environment);
    }

    static String binPath() {

        ImmutableConfiguration config = ZiggyConfiguration.getInstance();
        // Search for the application first in the path specified by the
        // BINPATH property followed by the pipeline's bin directory.
        String binPath = config.getString(PropertyName.BINPATH.property(), null);
        if (StringUtils.isBlank(binPath)) {
            binPath = DirectoryProperties.pipelineBinDir().toString();
        } else {
            binPath = binPath + File.pathSeparator + DirectoryProperties.pipelineBinDir();
        }
        return binPath + File.pathSeparator + DirectoryProperties.ziggyBinDir();
    }

    /**
     * Provides additional environment setup. By default, returns an empty map.
     *
     * @return {@link Map} of new environment variables that need to be added to the
     * {@link #environment} Map.
     */
    protected Map<String, String> customizeEnvironment() {
        return new HashMap<>();
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
    static File binaryDir(String binPathString, String binaryName) {
        File binFile = binaryDirInternal(binPathString, binaryName, null);
        if (binFile == null && OperatingSystemType.newInstance() == OperatingSystemType.MAC_OS_X) {
            binFile = binaryDirInternal(binPathString, binaryName,
                new String[] { binaryName + ".app", "Contents", "MacOS" });
        }
        return binFile;
    }

    private static File binaryDirInternal(String binPathString, String binaryName,
        String[] pathSuffix) {

        log.info("Searching for binary {} in path {}", binaryName, binPathString);
        File binFile = null;
        String[] binPaths = binPathString.split(File.pathSeparator);
        for (String binPath : binPaths) {
            if (StringUtils.isBlank(binPath)) {
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

    private static File binPathFile(String binPath, String[] pathSuffix) {
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
     * Executes the algorithm on the subtask (including input and output handling) and returns the
     * exit code.
     */
    @AcceptableCatchBlock(rationale = Rationale.MUST_NOT_CRASH)
    public int execAlgorithm() {

        if (!workingDir.exists() || !workingDir.isDirectory()) {
            System.err
                .println("WORKING_DIR (" + workingDir + ") does not exist or is not a directory");
            System.exit(-1);
        }

        int retCode = -1;

        try {
            File errorFile = AlgorithmInterfaceUtils.errorFile(workingDir, binaryName);
            if (errorFile.exists()) {
                log.info("Deleting stale error file prior to start of processing");
                errorFile.delete();
            }
            retCode = execAlgorithmInternal();

            if (retCode != 0) {
                log.warn("Marking subtask as failed (retCode={})", retCode);
                markSubtaskFailed(workingDir);
            }

            if (errorFile.exists()) {
                log.warn("Marking subtask as failed (error file exists)");
                markSubtaskFailed(workingDir);
            }
        } catch (Exception e) {
            log.warn("Marking subtask as failed (Java-side exception occurred)", e);
            markSubtaskFailed(workingDir);
        }
        return retCode;
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    public int execAlgorithmInternal() {
        List<String> commandLineArgs = new LinkedList<>();
        try {
            String exePath = workingDir.getCanonicalPath();
            commandLineArgs.add(exePath);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to get path for dir " + workingDir.toString(),
                e);
        }

        AlgorithmStateFiles stateFile = new AlgorithmStateFiles(workingDir);
        stateFile.updateCurrentState(AlgorithmStateFiles.AlgorithmState.PROCESSING);

        boolean inputsProcessingSucceeded = false;
        boolean algorithmProcessingSucceeded = false;

        int retCode = -1;
        IntervalMetricKey key = IntervalMetric.start();
        try {
            key = IntervalMetric.start();
            TaskConfiguration taskConfiguration = getTaskConfiguration();
            Class<? extends PipelineInputs> inputsClass = taskConfiguration.getInputsClass();
            retCode = runInputsOutputsCommand(inputsClass);
            if (retCode == 0) {
                inputsProcessingSucceeded = true;
                retCode = runCommandline(commandLineArgs, binaryName);
            }
            if (retCode == 0) {
                algorithmProcessingSucceeded = true;
                Class<? extends PipelineOutputs> outputsClass = taskConfiguration.getOutputsClass();
                retCode = runInputsOutputsCommand(outputsClass);
            }
        } finally {
            IntervalMetric.stop(MATLAB_PROCESS_EXEC_METRIC, key);
        }

        File errorFile = AlgorithmInterfaceUtils.errorFile(workingDir, binaryName);

        if (retCode == 0 && !errorFile.exists()) {
            stateFile.updateCurrentState(AlgorithmStateFiles.AlgorithmState.COMPLETE);
        } else {
            /*
             * Don't handle an error in processing at this point in execution. Instead, allow the
             * caller to respond to the return code and let an exception be thrown at a higher
             * level, after some error-management tasks have been completed.
             */

            stateFile.updateCurrentState(AlgorithmStateFiles.AlgorithmState.FAILED);
            if (retCode != 0) {
                if (!inputsProcessingSucceeded) {
                    log.error("Failed to generate subtask inputs (retCode={})", retCode);
                } else if (algorithmProcessingSucceeded) {
                    log.error("Failed to generate task results (retCode={})", retCode);
                }
            } else {
                log.info("Algorithm process completed (retCode={})", retCode);
            }
        }

        return retCode;
    }

    /**
     * Run an arbitrary process with caller-specified arguments. No {@link AlgorithmStateFiles} is
     * created.
     */
    public int execSimple(List<String> commandLineArgs) {
        int retCode = runCommandline(commandLineArgs, binaryName);
        log.info("retCode={}", retCode);

        return retCode;
    }

    /**
     * Executes the {@link BeforeAndAfterAlgorithmExecutor#main()} method with appropriate arguments
     * such that {@link PipelineInputs#beforeAlgorithmExecution()} executes immediately prior to
     * algorithm execution, and {@link PipelineOutputs#afterAlgorithmExecution()} executes
     * immediately subsequent to algorithm execution.
     * <p>
     * Given that {@link BeforeAndAfterAlgorithmExecutor} is a Java class, and this is a Java class,
     * why is the {@link BeforeAndAfterAlgorithmExecutor} invoked using the ziggy program and an
     * external process? By using an external process, the inputs and outputs classes can use
     * software libraries that do not support concurrency (for example, HDF5). By running each
     * subtask in a separate process, the subtask input and output processing can execute in
     * parallel even in cases in which the processing uses non-concurrent libraries. Running a bunch
     * of instances of {@link BeforeAndAfterAlgorithmExecutor} in separate threads within a common
     * JVM would not permit this.
     *
     * @param inputsOutputsClass Class to be used as argument to TaskFileManager.
     * @return exit code from the ziggy program
     */
    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    int runInputsOutputsCommand(Class<?> inputsOutputsClass) {

        // Run the command and return the exit code.
        CommandLine inputsOutputsCommandLine = inputsOutputsCommandLine(inputsOutputsClass);
        ExternalProcess externalProcess = externalProcess(null, null);
        externalProcess.setLogStreamIdentifier(workingDir.getName());
        externalProcess.setWorkingDirectory(workingDir);
        externalProcess.setCommandLine(inputsOutputsCommandLine);
        externalProcess.mergeWithEnvironment(environment);

        log.info("Executing command {}", inputsOutputsCommandLine.toString());
        return externalProcess.execute();
    }

    CommandLine inputsOutputsCommandLine(Class<?> inputsOutputsClass) {

        // Locate the ziggy executable, which can be someplace other than the bin directories for
        // the pipeline.
        String ziggyCommand = DirectoryProperties.ziggyBinDir().resolve(ZIGGY_PROGRAM).toString();
        String javaLibPathArg = ExternalProcessUtils.javaLibraryPath();
        String log4jConfig = ZiggyLog.log4jConfigString();
        String logFile = ZiggyLog.singleFileSystemProperty(ZiggyConfiguration.getInstance()
            .getString(PropertyName.ZIGGY_LOG_SINGLE_FILE.property()));
        String logAppender = ZiggyLog.singleFileAppenderSystemProperty();

        // Construct the class arguments for the ziggy program.
        String taskFileManagerClassName = BeforeAndAfterAlgorithmExecutor.class.getCanonicalName();
        String inputsOutputsClassName = inputsOutputsClass.getCanonicalName();

        // Put it all together.
        CommandLine commandLine = new CommandLine(ziggyCommand);
        commandLine.addArgument("--verbose");
        commandLine.addArgument(heapSizeProperty());
        commandLine.addArgument(javaLibPathArg);
        commandLine.addArgument(log4jConfig);
        commandLine.addArgument(logFile);
        commandLine.addArgument(logAppender);

        commandLine.addArgument("--class=" + taskFileManagerClassName);
        commandLine.addArgument(inputsOutputsClassName);

        return commandLine;
    }

    @AcceptableCatchBlock(rationale = Rationale.EXCEPTION_CHAIN)
    int runCommandline(List<String> commandLineArguments, String logPrefix) {
        try (
            Writer stdOutWriter = new OutputStreamWriter(
                new FileOutputStream(
                    new File(workingDir, AlgorithmInterfaceUtils.stdoutFileName(logPrefix))),
                ZiggyFileUtils.ZIGGY_CHARSET);
            Writer stdErrWriter = new OutputStreamWriter(
                new FileOutputStream(
                    new File(workingDir, AlgorithmInterfaceUtils.stderrFileName(logPrefix))),
                ZiggyFileUtils.ZIGGY_CHARSET)) {
            return runCommandLine(commandLineArguments, logPrefix, stdOutWriter, stdErrWriter);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to construct OutputStreamWriter instances", e);
        }
    }

    private int runCommandLine(List<String> commandLineArguments, String logPrefix,
        Writer stdOutWriter, Writer stdErrWriter) {

        int retCode = 0;
        IntervalMetricKey key = IntervalMetric.start();
        CommandLine algorithmCommandLine = null;

        try {
            algorithmCommandLine = algorithmCommandLine(commandLineArguments);
            ExternalProcess externalProcess = externalProcess(stdOutWriter, stdErrWriter);
            externalProcess.setLogStreamIdentifier(workingDir.getName());
            externalProcess.setWorkingDirectory(workingDir);
            externalProcess.mergeWithEnvironment(environment);
            externalProcess.timeout(timeoutSecs * 1000);
            externalProcess.setCommandLine(algorithmCommandLine);

            log.info("env={}", environment);
            retCode = externalProcess.execute();
        } catch (IOException e) {
            throw new UncheckedIOException(
                "Unable to create and run command line for binary" + binaryName, e);
        } finally {
            IntervalMetric.stop("pipeline.module.externalProcess." + binaryName + ".execTime", key);
        }

        return retCode;
    }

    public CommandLine algorithmCommandLine(List<String> commandLineArguments) throws IOException {
        if (algorithmCommandLine == null) {
            File binary = new File(binaryDir.getPath(), binaryName);
            if ((!binary.exists() || !binary.isFile())
                && OperatingSystemType.newInstance() == OperatingSystemType.MAC_OS_X) {
                binary = new File(binaryDir.getPath(),
                    binaryName + ".app/Contents/MacOS/" + binaryName);
            }

            log.info("binary={}", binary);

            algorithmCommandLine = new CommandLine(binary.getCanonicalPath());
            for (String commandLineArgument : commandLineArguments) {
                algorithmCommandLine.addArgument(commandLineArgument);
            }

            log.info("commandLine={}", algorithmCommandLine);
        }
        return algorithmCommandLine;
    }

    private static void markSubtaskFailed(File workingDir) {
        AlgorithmStateFiles subtaskState = new AlgorithmStateFiles(workingDir);
        if (subtaskState.currentAlgorithmState() != AlgorithmStateFiles.AlgorithmState.FAILED) {
            subtaskState.updateCurrentState(AlgorithmStateFiles.AlgorithmState.FAILED);
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

    public String libPath() {
        return environment.get(osType.getSharedObjectPathEnvVar());
    }

    public String heapSizeProperty() {
        String heapSizeSuffix = heapSizeGigabytes >= 1 ? HEAP_SIZE_GB_SUFFIX : HEAP_SIZE_MB_SUFFIX;
        String heapSizeFormat = HEAP_SIZE_PROPERTY_NAME + "%d" + heapSizeSuffix;
        int heapSize = heapSizeGigabytes >= 1 ? (int) Math.ceil(heapSizeGigabytes)
            : (int) Math.ceil(heapSizeGigabytes * 1000);
        return String.format(heapSizeFormat, heapSize);
    }

    // getters for test use only
    public String binaryName() {
        return binaryName;
    }

    public File binaryDir() {
        return binaryDir;
    }

    public int timeoutSecs() {
        return timeoutSecs;
    }

    public TaskConfiguration getTaskConfiguration() {
        return taskConfiguration;
    }

    public String pipelineStepName() {
        return PipelineInputsOutputsUtils.pipelineStepName(taskDir);
    }

    public Map<String, String> getEnvironment() {
        return environment;
    }
}

package gov.nasa.ziggy.services.process;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.exec.PumpStreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.services.logging.PlainTextLogOutputStream;
import gov.nasa.ziggy.services.logging.WriterLogOutputStream;
import gov.nasa.ziggy.util.StringUtils;
import gov.nasa.ziggy.util.ZiggyShutdownHook;
import gov.nasa.ziggy.util.os.ProcessUtils;

/**
 * Abstraction of the {@link DefaultExecutor} with customizations for Ziggy.
 * <p>
 * The {@link DefaultExecutor} provides most of the necessary features for an external process API,
 * but this class provides some additional ones that are useful in the context of Ziggy:
 * <ol>
 * <li>Automatic configuration of stream handlers for logging and writing of output and error
 * streams.
 * <li>Automatic configuration of {@link ExecuteWatchdog} instances for processes that need to be
 * timed out after some selected interval.
 * <li>Automatic addition of a shutdown hook that sends SIGTERM to all external processes and their
 * descendants when the main process receives a SIGTERM.
 * </ol>
 *
 * @author PT
 */
public class ExternalProcess {
    static final Logger log = LoggerFactory.getLogger(ExternalProcess.class);

    // Determines whether the shutdown hook for external processes has been added yet.
    private static boolean externalProcessesShutdownHookSet = false;

    // Note: the child process ID sets need to be initialized even though they get
    // replaced every second by the shutdown hook. This keeps them from being null if
    // the shutdown hook is called before the sets have been updated.
    private static Set<Long> childProcessIds = new TreeSet<>();
    private static Set<Long> childProcessIdsPreviousCall = new TreeSet<>();

    // The thread pool executor and its threads must all be daemon threads so that
    // they don't interfere with execution terminating in any program that uses this
    // class.
    private static ScheduledThreadPoolExecutor pool = new ScheduledThreadPoolExecutor(1, r -> {
        Thread t = Executors.defaultThreadFactory().newThread(r);
        t.setDaemon(true);
        return t;
    });

    private static final long REFRESH_INTERVAL_MILLIS = 2000L;

    private DefaultExecutor executor;
    private CommandLine commandLine;
    private Map<String, String> environment;
    private LogOutputStream outputLog;
    private LogOutputStream errorLog;
    private boolean logStdOut = true;

    private String logStreamIdentifier;

    private boolean logStdErr;

    private boolean writeStdOut;
    private boolean writeStdErr;
    private boolean exceptionOnFailure;

    private Writer outputWriter;
    private Writer errorWriter;

    /**
     * Shortcut to produce a "simple" {@link ExternalProcess}. This is an external process that
     * doesn't log and only writes its stdout, so it's useful for cases in which a simple shell
     * command is desired.
     */
    public static ExternalProcess simpleExternalProcess(String command) {
        CommandLine commandLine = null;
        if (command != null && !command.isEmpty()) {
            commandLine = CommandLine.parse(command);
        }
        return simpleExternalProcess(commandLine);
    }

    /**
     * Shortcut to produce a "simple" {@link ExternalProcess}. This is an external process that
     * doesn't log and only writes its stdout, so it's useful for cases in which a simple shell
     * command is desired.
     */
    public static ExternalProcess simpleExternalProcess(CommandLine command) {
        ExternalProcess process = new ExternalProcess(false, null, false, null);
        process.writeStdOut(true);
        process.setCommandLine(command);
        return process;
    }

    /**
     * Shortcut to produce an {@link ExternalProcess} instance that logs only stdout.
     */
    public static ExternalProcess stdoutLoggingExternalProcess(CommandLine command) {
        ExternalProcess process = new ExternalProcess(true, null, false, null);
        process.setCommandLine(command);
        return process;
    }

    /**
     * Shortcut to produce an {@link ExternalProcess} instance that logs both stdout and stderr but
     * doesn't write either of them.
     */
    public static ExternalProcess allLoggingExternalProcess(CommandLine command) {
        ExternalProcess process = new ExternalProcess(true, null, true, null);
        process.setCommandLine(command);
        return process;
    }

    /**
     * Shortcut to produce an {@link ExternalProcess} instance that logs both stdout and stderr, and
     * also writes both to a simple {@link StringWriter}.
     */
    public static ExternalProcess writingLoggingExternalProcess(CommandLine command) {
        ExternalProcess process = new ExternalProcess(true, null, true, null);
        process.setCommandLine(command);
        process.writeStdErr(true);
        process.writeStdOut(true);
        return process;
    }

    /**
     * Recommended constructor.
     *
     * @param logStdOut whether standard out should be routed to the log files.
     * @param stdOutWriter {@link Writer} instance for standard out.
     * @param logStdErr whether standard err should be routed to the log files.
     * @param stdErrWriter {@link Writer} instance for standard out.
     */
    public ExternalProcess(boolean logStdOut, Writer stdOutWriter, boolean logStdErr,
        Writer stdErrWriter) {
        setExternalProcessesShutdownHook();
        executor = new DefaultExecutor();
        executor.setWatchdog(new ExecuteWatchdog(Long.MAX_VALUE));
        this.logStdOut = logStdOut;
        this.logStdErr = logStdErr;
        outputWriter = stdOutWriter;
        errorWriter = stdErrWriter;
        if (outputWriter != null) {
            writeStdOut = true;
        }
        if (errorWriter != null) {
            writeStdErr = true;
        }
    }

    public File getWorkingDirectory() {
        return executor.getWorkingDirectory();
    }

    public void setWorkingDirectory(File directory) {
        executor.setWorkingDirectory(directory);
    }

    public Map<String, String> getEnvironment() {
        return environment;
    }

    /**
     * Sets the environment for the external process. The environment uses a {@link Map} to connect
     * keys (environment variables) with values (environment variable values).
     */
    public void setEnvironment(Map<String, String> environment) {
        this.environment = environment;
    }

    /**
     * Sets the {@link CommandLine} to be executed by the process.
     */
    public void setCommandLine(CommandLine commandLine) {
        this.commandLine = commandLine;
    }

    /**
     * Forces an exception to be thrown if the process returns a non-zero value. Returns the
     * {@link ExternalProcess} instance to allow a process to be defined, exception throw set, and
     * execution to all occur in a single line of code.
     */
    public ExternalProcess exceptionOnFailure() {
        return exceptionOnFailure(true);
    }

    /**
     * Enables or disables exception throwing in the event of a non-zero return value. Returns the
     * {@link ExternalProcess} instance to allow a process to be defined, exception throw set, and
     * execution to all occur in a single line of code.
     */
    public ExternalProcess exceptionOnFailure(boolean exceptionOnFailure) {
        this.exceptionOnFailure = exceptionOnFailure;
        return this;
    }

    /**
     * Execute the {@link ExternalProcess}. Legacy signature from Spiffy.
     *
     * @param wait if true, run synchronously.
     * @param timeoutMillis Execution timeout. If set to zero, no timeout will be enforced.
     * @return return code from process execution or 0 if execution is asynchronous.
     */
    public int run(boolean wait, long timeoutMillis) {
        timeout(timeoutMillis);
        return execute(wait);
    }

    /**
     * Execute the {@link ExternalProcess} synchronously.
     *
     * @return return code from process execution.
     */
    public int execute() {
        return execute(true);
    }

    /**
     * Execute the {@link ExternalProcess}.
     *
     * @param wait if true, execute synchronously.
     * @return return code from process execution, or 0 if execution is asynchronous.
     */
    public int execute(boolean wait) {
        int retCode = 0;

        // Construct the appropriate LogOutputStream instances
        initializeWriters();
        outputLog = logOutputStream(logStdOut, writeStdOut, outputWriter);
        errorLog = logOutputStream(logStdErr, writeStdErr, errorWriter);

        // Construct the PumpStreamHandler
        PumpStreamHandler pumpStreamHandler = null;
        if (errorLog != null) {
            pumpStreamHandler = new PumpStreamHandler(outputLog, errorLog);
        } else {
            pumpStreamHandler = new PumpStreamHandler(outputLog);
        }

        // Configure the DefaultExecutor
        executor.setStreamHandler(pumpStreamHandler);
        executor.setWorkingDirectory(getWorkingDirectory());

        // Start the process and, if run synchronously, capture its exit code
        if (wait) {
            retCode = executeAndReturnStatus();
        } else {
            executeAsynchronously();
        }
        if (exceptionOnFailure && retCode != 0) {
            throw new PipelineException(
                "Exit code returned from command " + commandLine.toString() + " == " + retCode);
        }
        return retCode;
    }

    private int executeAndReturnStatus() {
        int retCode = 0;
        try {
            if (environment != null) {
                retCode = executor.execute(commandLine, environment);
            } else {
                retCode = executor.execute(commandLine);
            }
        } catch (ExecuteException e) {
            retCode = e.getExitValue();
        } catch (IOException e) {
            throw new PipelineException("Exception occurred during command line execute", e);
        }
        return retCode;
    }

    private void executeAsynchronously() {
        DefaultExecuteResultHandler resultHandler = new DefaultExecuteResultHandler();
        try {
            if (environment != null) {
                executor.execute(commandLine, environment, resultHandler);
            } else {
                executor.execute(commandLine, resultHandler);
            }
        } catch (Exception e) {
            throw new PipelineException("Exception occurred during command line execute", e);
        }

    }

    private void initializeWriters() {
        if (outputWriter == null) {
            outputWriter = new StringWriter();
        }
        if (errorWriter == null) {
            errorWriter = new StringWriter();
        }
    }

    private LogOutputStream logOutputStream(boolean doLogging, boolean doWriting, Writer writer) {
        LogOutputStream logOutputStream = null;
        if (doWriting) {
            logOutputStream = new WriterLogOutputStream(writer, doLogging, logStreamIdentifier);
        } else if (doLogging) {
            logOutputStream = new PlainTextLogOutputStream(logStreamIdentifier);
        }
        return logOutputStream;
    }

    /**
     * Returns stdout from the process, broken into individual {@link String}s at line breaks.
     */
    public List<String> stdout() {
        if (outputLog != null) {
            return StringUtils.breakStringAtLineTerminations(outputLog.toString());
        }
        return null;
    }

    /**
     * Returns stdout from the process, broken into individual {@link String}s at line breaks and
     * filtered to return only {@link String}s that contain one or more of the specified target
     * strings.
     */
    public List<String> stdout(String... targetStrings) {
        if (outputLog != null) {
            return StringUtils.stringsContainingTargets(
                StringUtils.breakStringAtLineTerminations(outputLog.toString()), targetStrings);
        }
        return null;
    }

    /**
     * Returns stderr from the process, broken into individual {@link String}s at line breaks.
     */
    public List<String> stderr() {
        if (errorLog != null) {
            return StringUtils.breakStringAtLineTerminations(errorLog.toString());
        }
        return null;
    }

    /**
     * Returns stderr from the process, broken into individual {@link String}s at line breaks and
     * filtered to return only {@link String}s that contain one or more of the specified target
     * strings.
     */
    public List<String> stderr(String... targetStrings) {
        if (errorLog != null) {
            return StringUtils.stringsContainingTargets(
                StringUtils.breakStringAtLineTerminations(errorLog.toString()), targetStrings);
        }
        return null;
    }

    /**
     * @return true if standard error is being routed to the logs.
     */
    public boolean isLogStdErr() {
        return logStdErr;
    }

    /**
     * @return true if standard output is being routed to the logs.
     */
    public boolean isLogStdOut() {
        return logStdOut;
    }

    /**
     * Enables or disables logging of standard error.
     */
    public void logStdErr(boolean logState) {
        logStdErr = logState;
    }

    /**
     * Enables or disables logging of standard output.
     */
    public void logStdOut(boolean logState) {
        logStdOut = logState;
    }

    /**
     * Enables or disables writing of standard output.
     */
    public void writeStdOut(boolean writeStdOut) {
        this.writeStdOut = writeStdOut;
    }

    /**
     * Enables or disables writing of standard error.
     */
    public void writeStdErr(boolean writeStdErr) {
        this.writeStdErr = writeStdErr;
    }

    /**
     * Retrieves the content of the standard output from the process.
     */
    public String getStdoutString() {
        if (outputLog != null) {
            return outputLog.toString();
        }
        return null;
    }

    /**
     * Retrieves the content of the standard error from the process.
     */
    public String getStderrString() {
        if (errorLog != null) {
            return errorLog.toString();
        }
        return null;
    }

    /**
     * Sets the string added to log messages for aid in identifying the source.
     */
    public void setLogStreamIdentifier(String logStreamIdentifier) {
        this.logStreamIdentifier = logStreamIdentifier;
    }

    /**
     * Returns the string added to log messages for aid in identifying the source.
     */
    public String getLogStreamIdentifier() {
        return logStreamIdentifier;
    }

    /**
     * Sets the timeout for the external process, in milliseconds.
     */
    public ExternalProcess timeout(long timeoutMillis) {
        timeoutMillis = timeoutMillis > 0 ? timeoutMillis : Long.MAX_VALUE;
        executor.setWatchdog(new ExecuteWatchdog(timeoutMillis));
        return this;
    }

    /**
     * Returns the class of the output log stream.
     */
    public Class<? extends LogOutputStream> outputLogClass() {
        if (outputLog != null) {
            return outputLog.getClass();
        }
        return null;
    }

    /**
     * Returns the class of the error log stream.
     */
    public Class<? extends LogOutputStream> errorLogClass() {
        if (errorLog != null) {
            return errorLog.getClass();
        }
        return null;
    }

    /**
     * Returns the {@link ExecuteWatchdog}, if any, for the external process.
     */
    public ExecuteWatchdog getWatchdog() {
        return executor.getWatchdog();
    }

    /**
     * Sets a shutdown hook that finds process IDs for all descendants of the current process and
     * sends SIGTERM to them. Also starts a periodic thread that captures all of the child processes
     * of the main process (the process within which the ExternalProcess runs).
     * <p>
     * One difficulty with this is that when SIGTERM arrives, any synchronous process exits, which
     * in turn means that any children of that process are orphaned and will not be detected as
     * descendants of the main process. This is addressed here by (a) capturing the set of child
     * processes at regular intervals, and (b) keeping both the current and most-recent-past sets of
     * descendant IDs, and sending SIGTERM to all of them, so that even if a synchronous process has
     * already been killed, and the child process set generated without that process, it will be
     * available in the set of processes from the prior iteration.
     */
    private static void setExternalProcessesShutdownHook() {
        if (!externalProcessesShutdownHookSet) {
            pool.scheduleWithFixedDelay(() -> {
                childProcessIdsPreviousCall = childProcessIds;
                childProcessIds = ProcessUtils.descendantProcessIds();
            }, 0L, REFRESH_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
            ZiggyShutdownHook.addShutdownHook(() -> {
                pool.shutdown();
                childProcessIds.addAll(childProcessIdsPreviousCall);
                log.debug("SHUTDOWN: Sending SIGTERM to child processes");
                for (Long processId : childProcessIds) {
                    log.debug("SHUTDOWN: Sending SIGTERM to PID " + processId);
                    ProcessUtils.sendSigtermToProcess(processId);
                }
                log.debug("SHUTDOWN: done with SIGTERMs");
            });
            externalProcessesShutdownHookSet = true;
        }
    }

}

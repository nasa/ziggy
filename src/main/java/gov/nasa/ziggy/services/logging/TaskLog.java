package gov.nasa.ziggy.services.logging;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.filter.ThreadContextMapFilter;
import org.apache.logging.log4j.core.util.KeyValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.ComputeNodeMaster;
import gov.nasa.ziggy.module.LocalAlgorithmExecutor;
import gov.nasa.ziggy.module.remote.Qsub;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDataOperations;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyName;

/**
 * This class manages the creation and use of log files for pipeline infrastructure purposes.
 * Specifically, it creates the log file in the specified directory and creates a log4j
 * {@link FileAppender} to capture all logging output to a dedicated file for the current worker
 * task.
 * <p>
 * Log files for algorithms are created by the algorithm-master shell script. The names for these
 * files are created in {@link Qsub} for remote execution and in {@link LocalAlgorithmExecutor} for
 * execution on the server that hosts the worker process.
 * <p>
 * Note that, unlike the rest of Ziggy, this class uses the log4j2 API directly, alongside the SLF4J
 * API. This is necessary because the SLF4J API doesn't support programmatic addition of appenders,
 * which is needed for flowing log messages simultaneously to the worker log and the task log.
 *
 * @author Todd Klaus
 * @author PT
 */
public class TaskLog {

    /**
     * Enumeration of supported log types. The {#logDir()} method provides the location of the log
     * files of the selected type. The {@link #taskLogFileAppender(Path)} method returns an
     * appropriate {@link FileAppender} for each task log type.
     *
     * @author PT
     */
    public enum LogType {
        ZIGGY {
            @Override
            public Path logDir() {
                return DirectoryProperties.taskLogDir();
            }

            @SuppressWarnings("rawtypes")
            @Override
            public FileAppender taskLogFileAppender(Path taskLog) {
                return ((FileAppender.Builder) fileAppenderBuilder(taskLog.toFile())
                    .setFilter(threadContextMapFilter())).build();
            }
        },
        ALGORITHM {
            @Override
            public Path logDir() {
                return DirectoryProperties.algorithmLogsDir();
            }

            @Override
            public FileAppender taskLogFileAppender(Path taskLog) {
                return fileAppenderBuilder(taskLog.toFile()).build();
            }
        };

        public abstract Path logDir();

        public abstract FileAppender taskLogFileAppender(Path taskLog);
    }

    private static final Logger log = LoggerFactory.getLogger(TaskLog.class);

    private static final String CONSOLE_APPENDER_NAME = "console";

    public static final String THREAD_NAME_KEY = "threadName";
    public static final int LOCAL_LOG_FILE_JOB_INDEX = 0;

    public static final String CLI_APPENDER_NAME = "cli";

    private static PipelineTaskDataOperations pipelineTaskDataOperations = new PipelineTaskDataOperations();

    /**
     * Locates all the log files for a given pipeline task.
     */
    public static Set<TaskLogInformation> searchForLogFiles(PipelineTask pipelineTask) {
        Set<TaskLogInformation> taskLogInfoSet = new TreeSet<>();

        for (LogType logType : LogType.values()) {
            for (File taskLogFile : searchForLogFilesOfType(logType, pipelineTask)) {
                taskLogInfoSet.add(new TaskLogInformation(taskLogFile));
            }
        }
        return taskLogInfoSet;
    }

    /** Returns the ziggy.logFile Java property for use with {@link ComputeNodeMaster}. */
    public static String algorithmLogFileSystemProperty(PipelineTask pipelineTask) {
        return algorithmLogFileSystemProperty(pipelineTask, LOCAL_LOG_FILE_JOB_INDEX);
    }

    /** Returns the ziggy.logFile Java property for use with {@link ComputeNodeMaster}. */
    public static String algorithmLogFileSystemProperty(PipelineTask pipelineTask, int jobIndex) {
        try {
            Files.createDirectories(DirectoryProperties.algorithmLogsDir());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return "-D" + PropertyName.ZIGGY_LOG_FILE.property() + "="
            + DirectoryProperties.algorithmLogsDir()
                .toAbsolutePath()
                .resolve(pipelineTaskDataOperations.logFilename(pipelineTask, jobIndex))
                .toString();
    }

    /** Returns the ziggy.algorithmName Java property for use with {@link ComputeNodeMaster}. */
    public static String algorithmNameSystemProperty(PipelineTask pipelineTask) {
        return "-D" + PropertyName.ZIGGY_ALGORITHM_NAME.property() + "="
            + pipelineTask.getExecutableName();
    }

    public static String ziggyLogFileSystemProperty(PipelineTask pipelineTask) {
        try {
            Files.createDirectories(DirectoryProperties.taskLogDir());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return "-D" + PropertyName.ZIGGY_LOG_FILE.property() + "="
            + DirectoryProperties.taskLogDir()
                .toAbsolutePath()
                .resolve(
                    pipelineTaskDataOperations.logFilename(pipelineTask, LOCAL_LOG_FILE_JOB_INDEX))
                .toString();
    }

    /**
     * Locates all the log files of a given type for a given pipeline task.
     */
    private static File[] searchForLogFilesOfType(LogType logType, PipelineTask pipelineTask) {
        log.debug("Searching directory {} for log files", logType.logDir().toString());
        if (!Files.exists(logType.logDir()) || !Files.isDirectory(logType.logDir())) {
            return new File[0];
        }
        return logType.logDir().toFile().listFiles((FilenameFilter) (dir, name) -> {
            Matcher matcher = TaskLogInformation.LOG_FILE_NAME_PATTERN.matcher(name);
            if (matcher.matches()) {
                return Long.parseLong(
                    matcher.group(TaskLogInformation.TASK_ID_GROUP_NUMBER)) == pipelineTask.getId();
            }
            return false;
        });
    }

    private static LoggerConfig rootConfig() {
        return ((LoggerContext) LogManager.getContext(false)).getConfiguration()
            .getLoggerConfig("");
    }

    private static ThreadContextMapFilter threadContextMapFilter() {
        return ThreadContextMapFilter.createFilter(
            new KeyValuePair[] {
                new KeyValuePair(THREAD_NAME_KEY, ThreadContext.get(THREAD_NAME_KEY)) },
            "and", Filter.Result.ACCEPT, Filter.Result.DENY);
    }

    public static void endConsoleLogging() {
        Appender appender = rootConfig().getAppenders().get(CONSOLE_APPENDER_NAME);
        if (appender != null) {
            rootConfig().getAppenders().get(CONSOLE_APPENDER_NAME).stop();
            rootConfig().removeAppender(CONSOLE_APPENDER_NAME);
        }
    }

    private static Layout<? extends Serializable> cliLayout() {
        LoggerConfig rootConfig = rootConfig();
        Map<String, Appender> appenders = rootConfig.getAppenders();
        return appenders.get(CLI_APPENDER_NAME).getLayout();
    }

    /**
     * Generates the portion of the {@link FileAppender.Builder} that is common to both log file
     * types.
     */
    @SuppressWarnings("rawtypes")
    private static FileAppender.Builder fileAppenderBuilder(File taskLogFile) {
        return FileAppender.newBuilder()
            .withFileName(taskLogFile.getAbsolutePath())
            .setName(taskLogFile.getName())
            .setLayout(cliLayout());
    }

    static void setPipelineTaskDataOperations(
        PipelineTaskDataOperations pipelineTaskDataOperations) {
        TaskLog.pipelineTaskDataOperations = pipelineTaskDataOperations;
    }
}

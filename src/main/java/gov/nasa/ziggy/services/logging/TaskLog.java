package gov.nasa.ziggy.services.logging;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Level;
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

import gov.nasa.ziggy.module.LocalAlgorithmExecutor;
import gov.nasa.ziggy.module.remote.Qsub;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.config.DirectoryProperties;

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
     * files of the selected type. The {@link #taskLogFileAppender(File)} method returns an
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

    public static final String THREAD_NAME_KEY = "threadName";
    public static final int LOCAL_LOG_FILE_JOB_INDEX = 0;

    /**
     * NB the value of CONSOLE_APPENDER_NAME must match the name of the console appender defined in
     * log4j2.xml.
     */
    public static final String CONSOLE_APPENDER_NAME = "console";
    public static final String CLI_APPENDER_NAME = "cli";

    private FileAppender taskLogFileAppender = null;

    private Path taskLogFile = null;
    private LogType logType;

    private static Path taskLogDir = null;

    /**
     * Constructor used for Ziggy-side log files.
     */
    public TaskLog(int threadId, PipelineTask pipelineTask) {
        if (ThreadContext.get(THREAD_NAME_KEY) == null) {
            ThreadContext.put(THREAD_NAME_KEY, "thread-" + threadId);
        }

        taskLogDir = DirectoryProperties.taskLogDir();
        log.info("taskLogDir=" + taskLogDir);
        String taskLogFilename = pipelineTask.logFilename(LOCAL_LOG_FILE_JOB_INDEX);
        taskLogFile = taskLogDir.resolve(taskLogFilename);
        log.debug("file: " + taskLogFile);
        logType = LogType.ZIGGY;
    }

    /**
     * Constructor used for algorithm log files. In this case, the full file name and path is
     * provided because the {@link PipelineTask} is not available.
     */
    public TaskLog(String logfile) {
        taskLogFile = Paths.get(logfile);
        taskLogDir = taskLogFile.getParent();
        logType = LogType.ALGORITHM;
    }

    /**
     * Locates all the log files for a given pipeline task.
     *
     * @param instanceId pipeline instance ID
     * @param taskId pipeline task ID
     * @return a {@link Set} of {@link TaskLogInformation} instances, one per log file for the
     * selected task. All log types are included in the set.
     */
    public static Set<TaskLogInformation> searchForLogFiles(long instanceId, long taskId) {
        Set<TaskLogInformation> taskLogInfoSet = new TreeSet<>();

        for (LogType logType : LogType.values()) {
            for (File taskLogFile : searchForLogFilesOfType(logType, instanceId, taskId)) {
                taskLogInfoSet.add(new TaskLogInformation(taskLogFile));
            }
        }
        return taskLogInfoSet;
    }

    /**
     * Locates all the log files of a given type for a given pipeline task.
     *
     * @param logType {@link TaskLog.LogType} for the search.
     * @param instanceId pipeline instance ID
     * @param taskId pipeline task ID
     * @return an array of {@link File} instances for the log files of the specified type and
     * pipeline task.
     */
    private static File[] searchForLogFilesOfType(LogType logType, long instanceId, long taskId) {
        log.debug("Searching directory " + logType.logDir().toString() + " for log files");
        if (!Files.exists(logType.logDir()) || !Files.isDirectory(logType.logDir())) {
            return new File[0];
        }
        return logType.logDir().toFile().listFiles((FilenameFilter) (dir, name) -> {
            Matcher matcher = TaskLogInformation.LOG_FILE_NAME_PATTERN.matcher(name);
            if (matcher.matches()) {
                return Integer.parseInt(
                    matcher.group(TaskLogInformation.INSTANCE_ID_GROUP_NUMBER)) == instanceId
                    && Integer
                        .parseInt(matcher.group(TaskLogInformation.TASK_ID_GROUP_NUMBER)) == taskId;
            }
            return false;
        });
    }

    public void startLogging() {
        Path taskLogDir = taskLogFile.getParent();
        try {
            if (!Files.exists(taskLogDir)) {
                log.debug("Creating task log dir: " + taskLogDir);
                FileUtils.forceMkdir(taskLogDir.toFile());
            }
            log.debug("Creating task log file: " + taskLogFile);
            taskLogFileAppender = logType.taskLogFileAppender(taskLogFile);
            taskLogFileAppender.start();
        } catch (IOException e) {
            log.warn("failed to create taskLog FileAppender at: " + taskLogFile);
        }
        rootConfig().addAppender(taskLogFileAppender, Level.ALL, null);

        // If this is an algorithm log, then we don't need to run the console appender
        // any longer.
        if (logType == LogType.ALGORITHM) {
            endLogging(CONSOLE_APPENDER_NAME);
        }

    }

    private static ThreadContextMapFilter threadContextMapFilter() {
        return ThreadContextMapFilter.createFilter(
            new KeyValuePair[] {
                new KeyValuePair(THREAD_NAME_KEY, ThreadContext.get(THREAD_NAME_KEY)) },
            "and", Filter.Result.ACCEPT, Filter.Result.DENY);
    }

    public void endLogging() {
        endLogging(taskLogFile.getFileName().toString());
    }

    private static void endLogging(String appenderName) {
        Appender appender = rootConfig().getAppenders().get(appenderName);
        if (appender != null) {
            rootConfig().getAppenders().get(appenderName).stop();
            rootConfig().removeAppender(appenderName);
        }

    }

    private static LoggerConfig rootConfig() {
        return ((LoggerContext) LogManager.getContext(false)).getConfiguration()
            .getLoggerConfig("");
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

    public Path getTaskLogFile() {
        return taskLogFile;
    }
}

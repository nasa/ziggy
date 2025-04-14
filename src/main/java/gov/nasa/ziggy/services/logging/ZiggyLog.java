package gov.nasa.ziggy.services.logging;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDataOperations;
import gov.nasa.ziggy.services.config.DirectoryProperties;
import gov.nasa.ziggy.services.config.PropertyName;

/**
 * This class manages the use of log files for pipeline infrastructure purposes and works hand in
 * hand with etc/log4j2.xml.
 *
 * @author Todd Klaus
 * @author PT
 * @author Bill Wohler
 */
public class ZiggyLog {

    /**
     * Enumeration of supported task log types. The {@link #logDir()} method provides the location
     * of the log files of the selected type.
     *
     * @author PT
     * @author Bill Wohler
     */
    public enum TaskLogType {
        ZIGGY(DirectoryProperties.taskLogDir()), ALGORITHM(DirectoryProperties.algorithmLogsDir());

        private Path logDir;

        TaskLogType(Path logDir) {
            this.logDir = logDir;
        }

        public Path logDir() {
            return logDir;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ZiggyLog.class);

    private static final String WRAPPER_SUFFIX = "-wrapper";
    private static final String LOG_EXTENSION = ".log";

    /** The Console appender name in etc/log4j2.xml. */
    private static final String CONSOLE_APPENDER_NAME = "console";

    /** The RollingFile appender name in etc/log4j2.xml. */
    private static final String ROLLING_FILE_APPENDER_NAME = "rollingFile";

    /** The File appender name in etc/log4j2.xml. */
    private static final String SINGLE_FILE_APPENDER_NAME = "singleFile";

    static final int LOCAL_LOG_FILE_JOB_INDEX = 0;

    private static PipelineTaskDataOperations pipelineTaskDataOperations = new PipelineTaskDataOperations();

    /** No instances. All static methods. */
    private ZiggyLog() {
    }

    /**
     * The Log4j configuration file as a JVM argument.
     */
    public static String log4jConfigString() {
        return systemProperty(PropertyName.LOG4J2_CONFIGURATION_FILE,
            DirectoryProperties.ziggyHomeDir().resolve("etc").resolve("log4j2.xml").toString());
    }

    /**
     * Locates all the log files for a given pipeline task.
     */
    public static Set<TaskLogInformation> searchForLogFiles(PipelineTask pipelineTask) {
        Set<TaskLogInformation> taskLogInfoSet = new TreeSet<>();

        for (TaskLogType taskLogType : TaskLogType.values()) {
            for (File taskLogFile : searchForLogFilesOfType(taskLogType, pipelineTask)) {
                taskLogInfoSet.add(new TaskLogInformation(taskLogFile));
            }
        }
        return taskLogInfoSet;
    }

    /**
     * Locates all the log files of a given type for a given pipeline task.
     */
    private static File[] searchForLogFilesOfType(TaskLogType taskLogType,
        PipelineTask pipelineTask) {
        log.debug("Searching directory {} for log files", taskLogType.logDir().toString());
        if (!Files.exists(taskLogType.logDir()) || !Files.isDirectory(taskLogType.logDir())) {
            return new File[0];
        }
        return taskLogType.logDir().toFile().listFiles((FilenameFilter) (dir, name) -> {
            Matcher matcher = TaskLogInformation.LOG_FILE_NAME_PATTERN.matcher(name);
            if (matcher.matches()) {
                return Long.parseLong(
                    matcher.group(TaskLogInformation.TASK_ID_GROUP_NUMBER)) == pipelineTask.getId();
            }
            return false;
        });
    }

    /**
     * The algorithm log file as a JVM argument. Must also call
     * {@link #singleFileAppenderSystemProperty()}.
     */
    public static String algorithmLogFileSystemProperty(PipelineTask pipelineTask) {
        return algorithmLogFileSystemProperty(pipelineTask, LOCAL_LOG_FILE_JOB_INDEX);
    }

    /**
     * The algorithm log file as a JVM argument. Must also call
     * {@link #singleFileAppenderSystemProperty()}.
     */
    public static String algorithmLogFileSystemProperty(PipelineTask pipelineTask, int jobIndex) {
        try {
            Files.createDirectories(DirectoryProperties.algorithmLogsDir());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return singleFileSystemProperty(DirectoryProperties.algorithmLogsDir()
            .resolve(pipelineTaskDataOperations.logFilename(pipelineTask, jobIndex))
            .toAbsolutePath()
            .toString());
    }

    /**
     * The algorithm name as a JVM argument. Must also call
     * {@link #singleFileAppenderSystemProperty()}.
     */
    public static String algorithmNameSystemProperty(PipelineTask pipelineTask) {
        return systemProperty(PropertyName.ZIGGY_ALGORITHM_NAME, pipelineTask.getExecutableName());
    }

    public static String ziggyLogFileSystemProperty(PipelineTask pipelineTask) {
        try {
            Files.createDirectories(DirectoryProperties.taskLogDir());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return singleFileSystemProperty(DirectoryProperties.taskLogDir()
            .resolve(pipelineTaskDataOperations.logFilename(pipelineTask, LOCAL_LOG_FILE_JOB_INDEX))
            .toAbsolutePath()
            .toString());
    }

    /**
     * The single log file as a JVM argument. Must also call
     * {@link #singleFileAppenderSystemProperty()}.
     */
    public static String singleFileSystemProperty(String logFile) {
        return systemProperty(PropertyName.ZIGGY_LOG_SINGLE_FILE, logFile);
    }

    /**
     * The rolling log file as a JVM argument. May also call
     * {@link #rollingFileAppenderSystemProperty()}.
     */
    public static String rollingFileSystemProperty(String logFile) {
        return systemProperty(PropertyName.ZIGGY_LOG_ROLLING_FILE, logFile);
    }

    /**
     * The single file appender as a JVM argument. Must also call
     * {@link #singleFileSystemProperty(String)}.
     */
    public static String singleFileAppenderSystemProperty() {
        return systemProperty(PropertyName.ZIGGY_LOG_APPENDER_NAME, SINGLE_FILE_APPENDER_NAME);
    }

    /**
     * The rolling file appender as a JVM argument. This call is optional as this property is the
     * default in the {@code etc/log4j2.xml} file. Must also call
     * {@link #rollingFileSystemProperty(String)}.
     */
    public static String rollingFileAppenderSystemProperty() {
        return systemProperty(PropertyName.ZIGGY_LOG_APPENDER_NAME, ROLLING_FILE_APPENDER_NAME);
    }

    private static String systemProperty(PropertyName property, String value) {
        return "-D" + property.property() + "=" + value;
    }

    public static void endConsoleLogging() {
        Appender appender = rootConfig().getAppenders().get(CONSOLE_APPENDER_NAME);
        if (appender != null) {
            rootConfig().getAppenders().get(CONSOLE_APPENDER_NAME).stop();
            rootConfig().removeAppender(CONSOLE_APPENDER_NAME);
        }
    }

    private static LoggerConfig rootConfig() {
        return ((LoggerContext) LogManager.getContext(false)).getConfiguration()
            .getLoggerConfig("");
    }

    /**
     * The log file name used by a process, or its wrapper. The wrapper-provided log rotation
     * doesn't work properly when the logging isn't generated by the wrapper itself, so keep the
     * wrapper log and the process log separate. This method returns {@code prefix-wrapper.log} if
     * {@code wrapper} is true; otherwise, {@code prefix.log}.
     *
     * @param prefix the prefix of the filename
     * @param wrapper if true, appends a "-wrapper" suffix for the wrapper log
     */
    public static String logFilename(String prefix, boolean wrapper) {
        return prefix + (wrapper ? WRAPPER_SUFFIX : "") + LOG_EXTENSION;
    }

    static void setPipelineTaskDataOperations(
        PipelineTaskDataOperations pipelineTaskDataOperations) {
        ZiggyLog.pipelineTaskDataOperations = pipelineTaskDataOperations;
    }
}

package gov.nasa.ziggy.services.logging;

import java.io.File;
import java.io.Serializable;
import java.text.DateFormat;
import java.util.Date;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.services.logging.TaskLog.LogType;
import gov.nasa.ziggy.util.IntegerFormatter;

/**
 * Provides an assortment of information about a task log file. This provides support for the Ziggy
 * GUI capabilities for displaying lists of log files and/or their contents. The class implements
 * {@link Comparable} in order to ensure that the desired display order of the logs in the task log
 * selector table is obtained.
 *
 * @author PT
 */
public class TaskLogInformation implements Comparable<TaskLogInformation>, Serializable {

    private static final long serialVersionUID = 20220325L;

    /**
     * Pattern for logs file names: 10-100-algname.10-0.log, where the numbers are instance, task,
     * job index, task log index.
     */
    public static final Pattern LOG_FILE_NAME_PATTERN = Pattern
        .compile("([0-9]+)-([0-9]+)-\\S+\\.([0-9]+)-([0-9]+).log");

    public static final int INSTANCE_ID_GROUP_NUMBER = 1;
    public static final int TASK_ID_GROUP_NUMBER = 2;
    public static final int JOB_INDEX_GROUP_NUMBER = 3;
    public static final int TASK_LOG_INDEX_NUMBER = 4;

    private final int instanceId;
    private final int taskId;
    private final int jobIndex;
    private final int taskLogIndex;
    private final long logSizeBytes;
    private final String filename;
    private final String fullPath;
    private final LogType logType;
    private final long lastModified;

    public TaskLogInformation(File taskFile) {

        filename = taskFile.getName();
        logType = logType(taskFile);
        if (logType == null) {
            throw new PipelineException("File " + filename + " does not match any log type");
        }
        Matcher matcher = LOG_FILE_NAME_PATTERN.matcher(filename);
        matcher.matches();
        instanceId = Integer.valueOf(matcher.group(INSTANCE_ID_GROUP_NUMBER));
        taskId = Integer.valueOf(matcher.group(TASK_ID_GROUP_NUMBER));
        taskLogIndex = Integer.valueOf(matcher.group(TASK_LOG_INDEX_NUMBER));
        jobIndex = Integer.valueOf(matcher.group(JOB_INDEX_GROUP_NUMBER));
        fullPath = taskFile.getAbsolutePath();
        logSizeBytes = taskFile.length();
        lastModified = taskFile.lastModified();

    }

    public static LogType logType(File taskFile) {
        LogType logType = null;
        File taskFileDir = taskFile.getAbsoluteFile().getParentFile();
        for (LogType type : LogType.values()) {
            if (taskFileDir.equals(type.logDir().toFile())) {
                logType = type;
                break;
            }
        }
        return logType;
    }

    /**
     * Converts the file size to a human-readable format.
     */
    public String logFileSizeEngineeringNotation() {
        String valueInEngineeringNotation = IntegerFormatter.engineeringNotation(logSizeBytes);
        if (logSizeBytes <= 999) {
            return valueInEngineeringNotation + " Bytes";
        } else {
            return valueInEngineeringNotation + "Bytes";
        }
    }

    public String lastModifiedDateTime() {
        Date date = new Date(lastModified);
        return DateFormat.getDateTimeInstance().format(date);
    }

    public int getInstanceId() {
        return instanceId;
    }

    public int getTaskId() {
        return taskId;
    }

    public int getJobIndex() {
        return jobIndex;
    }

    public int getTaskLogIndex() {
        return taskLogIndex;
    }

    public long getLogSizeBytes() {
        return logSizeBytes;
    }

    public String getFilename() {
        return filename;
    }

    public String getFullPath() {
        return fullPath;
    }

    public LogType getLogType() {
        return logType;
    }

    public long getLastModified() {
        return lastModified;
    }

    // hashCode() and equals() use only the filename, as no two log files for a given pipeline
    // may have the same filename, hence the name alone uniquely identifies the file
    @Override
    public int hashCode() {
        return Objects.hash(filename);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TaskLogInformation other = (TaskLogInformation) obj;
        return Objects.equals(filename, other.filename);
    }

    /**
     * Implements a natural ordering for {@link TaskLogInformation} instances. specifically:
     * <ol>
     * <li>Instances should be ordered from lower to higher pipeline instance IDs.
     * <li>For identical pipeline instance IDs, instances should be ordered from lower to higher
     * pipeline task IDs.
     * <li>For identical pipeline task IDs, instances should be ordered from lower to higher task
     * log index values (this also causes the logs to be ordered from the preprocessing Java step to
     * the processing step to the post-processing Java step).
     * <li>For identical task log index values, instances should be ordered from lower to higher job
     * index.
     * </ol>
     */
    @Override
    public int compareTo(TaskLogInformation o) {
        if (Integer.compare(instanceId, o.instanceId) != 0) {
            return Integer.compare(instanceId, o.instanceId);
        }
        if (Integer.compare(taskId, o.taskId) != 0) {
            return Integer.compare(taskId, o.taskId);
        }
        if (Integer.compare(taskLogIndex, o.taskLogIndex) != 0) {
            return Integer.compare(taskLogIndex, o.taskLogIndex);
        }
        if (Integer.compare(jobIndex, o.jobIndex) != 0) {
            return Integer.compare(jobIndex, o.jobIndex);
        }
        return 0;
    }

}

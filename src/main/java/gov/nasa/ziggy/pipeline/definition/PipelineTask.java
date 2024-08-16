package gov.nasa.ziggy.pipeline.definition;

import static com.google.common.base.Preconditions.checkState;
import static gov.nasa.ziggy.ui.util.HtmlBuilder.htmlBuilder;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.AlgorithmExecutor.AlgorithmType;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskOperations;
import gov.nasa.ziggy.uow.UnitOfWork;
import gov.nasa.ziggy.uow.UnitOfWorkGenerator;
import gov.nasa.ziggy.util.HostNameUtils;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinTable;
import jakarta.persistence.OrderColumn;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;

/**
 * Represents a single pipeline unit of work Associated with a{@link PipelineInstance}, a
 * {@link PipelineDefinitionNode} (which is associated with a {@link PipelineModuleDefinition}), and
 * a {@link UnitOfWorkGenerator} that represents the unit of work.
 * <p>
 * Note that the {@link #equals(Object)} and {@link #hashCode()} methods are written in terms of
 * just the {@code id} field so this object should not be used in sets and maps until it has been
 * stored in the database
 *
 * @author Todd Klaus
 */
@Entity
@Table(name = "ziggy_PipelineTask")
public class PipelineTask implements PipelineExecutionTime {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(PipelineTask.class);

    /**
     * Log filename format. This is used by {@link MessageFormat} to produce the filename for a log
     * file. The first element is the basename, "instanceId-taskId-moduleName" (i.e.,
     * "100-200-foo"). The second element is a job index, needed when running tasks on a remote
     * system that can produce multiple log files per task (i.e., one per remote job). The final
     * element is the task log index, a value that increments as a task gets executed or rerun,
     * which allows the logs to be sorted into the order in which they were generated.
     */
    private static final String LOG_FILENAME_FORMAT = "{0}.{1}-{2}.log";

    private static final String ERROR_PREFIX = "ERROR - ";

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "ziggy_PipelineTask_generator")
    @SequenceGenerator(name = "ziggy_PipelineTask_generator", initialValue = 1,
        sequenceName = "ziggy_PipelineTask_sequence", allocationSize = 1)
    private Long id;

    /** Current processing step of this task. */
    @Enumerated(EnumType.STRING)
    private ProcessingStep processingStep = ProcessingStep.INITIALIZING;

    /**
     * Task failed. The transition logic will not run and the pipeline will stall. For tasks with
     * subtasks, this means that the number of failed subtasks exceeds the user-set threshold.
     */
    private boolean error;

    /** Total number of subtasks. */
    private int totalSubtaskCount;

    /** Number of completed subtasks. */
    private int completedSubtaskCount;

    /** Number of failed subtasks. */
    private int failedSubtaskCount;

    /**
     * Flag that indicates that this task was re-run from the console after an error
     */
    private boolean retry = false;

    /** Timestamp this task was created (either by launcher or transition logic) */
    private Date created = new Date();

    /** hostname of the worker that processed (or is processing) this task */
    private String workerHost;

    /** worker thread number that processed (or is processing) this task */
    private int workerThread;

    /** SVN revision of the build at the time this task was executed */
    private String softwareRevision;

    /** Timestamp that processing started on this task */
    private Date startProcessingTime = new Date(0);

    /** Timestamp that processing ended (success or failure) on this task */
    private Date endProcessingTime = new Date(0);

    /** Number of times this task failed and was rolled back */
    private int failureCount = 0;

    /** Count of the number of automatic resubmits performed for this task. */
    private int autoResubmitCount = 0;

    /** Indicates whether the task has been submitted for remote execution. */
    @Enumerated(EnumType.STRING)
    private AlgorithmType processingMode;

    @ElementCollection(fetch = FetchType.EAGER)
    @JoinTable(name = "ziggy_PipelineTask_uowTaskParameters")
    private Set<Parameter> uowTaskParameters;

    @ElementCollection
    @JoinTable(name = "ziggy_PipelineTask_summaryMetrics")
    private List<PipelineTaskMetrics> summaryMetrics = new ArrayList<>();

    @ElementCollection
    @OrderColumn(name = "idx")
    @JoinTable(name = "ziggy_PipelineTask_execLog")
    private List<TaskExecutionLog> execLog = new ArrayList<>();

    @ElementCollection
    @JoinTable(name = "ziggy_PipelineTask_remoteJobs")
    private Set<RemoteJob> remoteJobs = new HashSet<>();

    private int taskLogIndex = 0;

    private long priorProcessingExecutionTimeMillis = -1;

    private long currentExecutionStartTimeMillis;

    private String moduleName;

    private String executableName;

    private long pipelineInstanceId;

    /**
     * Required by Hibernate
     */
    public PipelineTask() {
    }

    public PipelineTask(PipelineInstance pipelineInstance,
        PipelineInstanceNode pipelineInstanceNode) {

        // The pipelineInstanceNode can be null in tests.
        if (pipelineInstanceNode != null) {
            moduleName = pipelineInstanceNode.getModuleName();
            executableName = pipelineInstanceNode.getExecutableName();
        }

        // The pipelineInstance can be null in tests.
        if (pipelineInstance != null && pipelineInstance.getId() != null) {
            pipelineInstanceId = pipelineInstance.getId();
        }
    }

    /**
     * A human readable description of this task and its parameters.
     */
    public String prettyPrint() {

        StringBuilder bldr = new StringBuilder();
        bldr.append("TaskId: ")
            .append(getId())
            .append(" ")
            .append("Module Software Revision: ")
            .append(getSoftwareRevision())
            .append(" ")
            .append(getModuleName())
            .append(" ")
            .append(" UoW: ")
            .append(uowTaskInstance().briefState())
            .append(" ");

        return bldr.toString();
    }

    @Override
    public String toString() {
        return "PipelineTask [id=" + id + ", processingStep=" + processingStep + ", uowTask="
            + uowTaskInstance().briefState() + "]";
    }

    public UnitOfWork uowTaskInstance() {
        UnitOfWork uow = new UnitOfWork();
        uow.setParameters(uowTaskParameters);
        return uow;
    }

    public static String taskBaseName(long instanceId, long taskId, String moduleName) {
        return baseNamePrefix(instanceId, taskId) + "-" + moduleName;
    }

    public static String baseNamePrefix(long instanceId, long taskId) {
        return instanceId + "-" + taskId;
    }

    public String taskBaseName() {
        return taskBaseName(pipelineInstanceId, getId(), getModuleName());
    }

    /**
     * Produces a file name for log files of the following form: {@code
     * <instanceId>-<taskId>-<moduleName>.<jobIndex>-<taskLogIndex>.log
     * }
     *
     * @param jobIndex index for the current job. Each pipeline task's algorithm execution can be
     * performed across multiple independent jobs; this index identifies a specific job out of the
     * set that are running for the current task.
     */
    public String logFilename(int jobIndex) {
        return MessageFormat.format(LOG_FILENAME_FORMAT, taskBaseName(), jobIndex, taskLogIndex);
    }

    /** Returns the label for a task that is used in some UI displays. */
    public String taskLabelText() {
        return htmlBuilder().appendBold("ID: ")
            .append(getPipelineInstanceId())
            .append(":")
            .append(getId())
            .appendBold(" WORKER: ")
            .append(getWorkerName())
            .appendBold(" TASK: ")
            .append(getModuleName())
            .append(" [")
            .append(uowTaskInstance().briefState())
            .append("] ")
            .appendBoldColor(getDisplayProcessingStep(), isError() ? "red" : "green")
            .append(" ")
            .appendItalic(elapsedTime())
            .toString();
    }

    public ProcessingStep getProcessingStep() {
        return processingStep;
    }

    public String getDisplayProcessingStep() {
        return (isError() ? ERROR_PREFIX : "") + getProcessingStep();
    }

    /**
     * Use of this method is not recommended. Use
     * {@link PipelineTaskOperations#updateProcessingStep(PipelineTask, ProcessingStep)} instead.
     */
    public void setProcessingStep(ProcessingStep processingStep) {
        this.processingStep = processingStep;
    }

    public boolean isError() {
        return error;
    }

    public void setError(boolean error) {
        this.error = error;
    }

    public void clearError() {
        setError(false);
    }

    public int getTotalSubtaskCount() {
        return totalSubtaskCount;
    }

    public void setTotalSubtaskCount(int totalSubtaskCount) {
        this.totalSubtaskCount = totalSubtaskCount;
    }

    public int getCompletedSubtaskCount() {
        return completedSubtaskCount;
    }

    public void setCompletedSubtaskCount(int completedSubtaskCount) {
        this.completedSubtaskCount = completedSubtaskCount;
    }

    public int getFailedSubtaskCount() {
        return failedSubtaskCount;
    }

    public void setFailedSubtaskCount(int failedSubtaskCount) {
        this.failedSubtaskCount = failedSubtaskCount;
    }

    @Override
    public Date getEndProcessingTime() {
        return endProcessingTime;
    }

    @Override
    public void setEndProcessingTime(Date endProcessingTime) {
        this.endProcessingTime = endProcessingTime;
    }

    public int getFailureCount() {
        return failureCount;
    }

    public void setFailureCount(int failureCount) {
        this.failureCount = failureCount;
    }

    public void incrementFailureCount() {
        failureCount++;
    }

    @Override
    public Date getStartProcessingTime() {
        return startProcessingTime;
    }

    @Override
    public void setStartProcessingTime(Date startProcessingTime) {
        this.startProcessingTime = startProcessingTime;
    }

    public String getWorkerName() {
        if (workerHost != null && workerHost.length() > 0) {
            String host = HostNameUtils.callerHostNameOrLocalhost(workerHost);
            return host + ":" + workerThread;
        }
        return "-";
    }

    public String getSoftwareRevision() {
        return softwareRevision;
    }

    public void setSoftwareRevision(String softwareRevision) {
        this.softwareRevision = softwareRevision;
    }

    public String getWorkerHost() {
        return workerHost;
    }

    public void setWorkerHost(String workerHost) {
        this.workerHost = workerHost;
    }

    public int getWorkerThread() {
        return workerThread;
    }

    public void setWorkerThread(int workerThread) {
        this.workerThread = workerThread;
    }

    public boolean isRetry() {
        return retry;
    }

    public void setRetry(boolean retry) {
        this.retry = retry;
    }

    public List<PipelineTaskMetrics> getSummaryMetrics() {
        return summaryMetrics;
    }

    public void setSummaryMetrics(List<PipelineTaskMetrics> summaryMetrics) {
        this.summaryMetrics = summaryMetrics;
    }

    public List<TaskExecutionLog> getExecLog() {
        return execLog;
    }

    public void setExecLog(List<TaskExecutionLog> execLog) {
        this.execLog = execLog;
    }

    public void setTaskLogIndex(int taskLogIndex) {
        this.taskLogIndex = taskLogIndex;
    }

    public int getTaskLogIndex() {
        return taskLogIndex;
    }

    public void incrementTaskLogIndex() {
        taskLogIndex++;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PipelineTask other = (PipelineTask) obj;
        return Objects.equals(id, other.id);
    }

    public static final class TaskBaseNameMatcher {
        public static final int INSTANCE_ID_GROUP = 1;
        public static final int TASK_ID_GROUP = 2;
        public static final int MODULE_NAME_GROUP = 3;
        public static final int JOB_INDEX_GROUP = 5;
        public static final String regex = "(\\d+)-(\\d+)-([^\\s\\.]+){1}?(\\.(\\d+))?";
        public static final Pattern pattern = Pattern.compile(regex);

        private long instanceId;
        private long taskId;
        private String moduleName;
        private boolean hasJobIndex;
        private int jobIndex;
        private boolean matches;
        private String baseName;

        public TaskBaseNameMatcher(String baseName) {
            this.baseName = baseName;
            Matcher matcher = pattern.matcher(baseName);
            matches = matcher.matches();
            if (matches) {
                instanceId = Long.parseLong(matcher.group(INSTANCE_ID_GROUP));
                taskId = Long.parseLong(matcher.group(TASK_ID_GROUP));
                moduleName = matcher.group(MODULE_NAME_GROUP);
                hasJobIndex = matcher.group(JOB_INDEX_GROUP) != null;
                if (hasJobIndex) {
                    jobIndex = Integer.parseInt(matcher.group(JOB_INDEX_GROUP));
                }
            }
        }

        public boolean matches() {
            return matches;
        }

        private void checkMatch() {
            checkState(matches, "String " + baseName + " does not match task base name pattern");
        }

        private void checkHasJobIndex() {
            checkState(matches, "String " + baseName + " does not have a job index");
        }

        public long instanceId() {
            checkMatch();
            return instanceId;
        }

        public long taskId() {
            checkMatch();
            return taskId;
        }

        public String moduleName() {
            checkMatch();
            return moduleName;
        }

        public boolean hasJobIndex() {
            return hasJobIndex;
        }

        public int jobIndex() {
            checkMatch();
            checkHasJobIndex();
            return jobIndex;
        }
    }

    @Override
    public void setPriorProcessingExecutionTimeMillis(long priorProcessingExecutionTimeMillis) {
        this.priorProcessingExecutionTimeMillis = priorProcessingExecutionTimeMillis;
    }

    @Override
    public long getPriorProcessingExecutionTimeMillis() {
        return priorProcessingExecutionTimeMillis;
    }

    @Override
    public void setCurrentExecutionStartTimeMillis(long currentExecutionStartTimeMillis) {
        this.currentExecutionStartTimeMillis = currentExecutionStartTimeMillis;
    }

    @Override
    public long getCurrentExecutionStartTimeMillis() {
        return currentExecutionStartTimeMillis;
    }

    public long getPipelineInstanceId() {
        return pipelineInstanceId;
    }

    public String getModuleName() {
        return moduleName;
    }

    public String getExecutableName() {
        return executableName;
    }

    public Set<RemoteJob> getRemoteJobs() {
        return remoteJobs;
    }

    public void setRemoteJobs(Set<RemoteJob> remoteJobs) {
        this.remoteJobs = remoteJobs;
    }

    public boolean isRemoteExecution() {
        return processingMode.equals(AlgorithmType.REMOTE);
    }

    public void setRemoteExecution(boolean remoteExecution) {
        processingMode = remoteExecution ? AlgorithmType.REMOTE : AlgorithmType.LOCAL;
    }

    public AlgorithmType getProcessingMode() {
        return processingMode;
    }

    public void setProcessingMode(AlgorithmType mode) {
        processingMode = mode;
    }

    public int getAutoResubmitCount() {
        return autoResubmitCount;
    }

    public void setAutoResubmitCount(int autoResubmitCount) {
        this.autoResubmitCount = autoResubmitCount;
    }

    public void incrementAutoResubmitCount() {
        autoResubmitCount++;
    }

    public void resetAutoResubmitCount() {
        autoResubmitCount = 0;
    }

    public Date getCreated() {
        return created;
    }

    public void setCreated(Date created) {
        this.created = created;
    }

    public Long getId() {
        return id;
    }

    public Set<Parameter> getUowTaskParameters() {
        return uowTaskParameters;
    }

    public void setUowTaskParameters(Set<Parameter> uowTaskParameters) {
        this.uowTaskParameters = uowTaskParameters;
    }

    /**
     * Starts execution clock for {@link PipelineTask} by first using the default method from
     * {@link PipelineExecutionTime}, then starting the clock for the {@link PipelineInstance} as
     * well. Users are generally advised not to call this method directly, as the clock is started
     * and stopped at appropriate times when a {@link PipelineTaskOperations} instance is used to
     * set a task's state.
     */
    @Override
    public void startExecutionClock() {
        PipelineExecutionTime.super.startExecutionClock();
    }

    public double costEstimate() {
        double estimate = 0;
        for (RemoteJob job : remoteJobs) {
            estimate += job.getCostEstimate();
        }
        return estimate;
    }
}

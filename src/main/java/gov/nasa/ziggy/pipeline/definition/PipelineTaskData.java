package gov.nasa.ziggy.pipeline.definition;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import gov.nasa.ziggy.module.AlgorithmType;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDataCrud;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskDataOperations;
import gov.nasa.ziggy.util.HostNameUtils;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Embedded;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.JoinTable;
import jakarta.persistence.OneToOne;
import jakarta.persistence.OrderColumn;
import jakarta.persistence.Table;

/**
 * Data for a given {@link PipelineTask}.
 * <p>
 * The {@link PipelineTask} class contains only immutable fields. All mutable values or immutable
 * collections are for a task are stored here. This is necessary because pipeline task data can
 * change quickly, and we do not want a pipeline task in memory that contains obsolete information.
 * Hence the pipeline task itself contains only immutable (hence valid) information, while all
 * access to mutable data in this class occurs via the {@link PipelineTaskDataOperations} class.
 * <p>
 * Note that the {@link #equals(Object)} and {@link #hashCode()} methods are written in terms of
 * just the pipeline task's ID so this object should not be created until its associated
 * {@link PipelineTask} has been stored in the database. The {@code setPipelineTask()} method is
 * omitted by design.
 * <p>
 * This class is not for public consumption. It should live ephemerally in
 * {@link PipelineTaskDataCrud} and {@link PipelineTaskDataOperations} methods.
 *
 * @author PT
 * @author Bill Wohler
 */
@Entity
@Table(name = "ziggy_PipelineTaskData")
public class PipelineTaskData implements Comparable<PipelineTaskData> {

    /**
     * The database ID for {@link PipelineTaskData} is the ID of the corresponding
     * {@link PipelineTask} instance.
     */
    @Id
    private long pipelineTaskId;

    /** The pipeline task associated with this object. */
    @OneToOne
    private PipelineTask pipelineTask;

    /** Ziggy software revision at the time this task was executed. */
    private String ziggySoftwareRevision;

    /** Revision of the pipeline build at the time this task was executed. */
    private String pipelineSoftwareRevision;

    /** Host name of the worker that processed (or is processing) this task. */
    private String workerHost;

    /** Worker thread number that processed (or is processing) this task. */
    private int workerThread;

    /** Elapsed execution time for the task. */
    @Embedded
    private ExecutionClock executionClock = new ExecutionClock();

    /** Current processing step of this task. */
    @Enumerated(EnumType.STRING)
    private ProcessingStep processingStep = ProcessingStep.WAITING_TO_RUN;

    /**
     * Task failed. The transition logic will not run and the pipeline will stall. For tasks with
     * subtasks, this means that the number of failed subtasks exceeds the user-set threshold.
     */
    private boolean error;

    /** User has requested that task execution halt. */
    private boolean haltRequested;

    /** Total number of subtasks. */
    private int totalSubtaskCount;

    /** Number of completed subtasks. */
    private int completedSubtaskCount;

    /** Number of failed subtasks. */
    private int failedSubtaskCount;

    /**
     * Flag that indicates that this task was re-run from the console after an error
     */
    private boolean retry;

    /** Number of times this task failed and was rolled back. */
    private int failureCount;

    /** Count of the number of automatic resubmits performed for this task. */
    private int autoResubmitCount;

    /** Indicates whether the task has been submitted for remote execution. */
    @Enumerated(EnumType.STRING)
    private AlgorithmType algorithmType;

    private int taskLogIndex;

    @ElementCollection
    @OrderColumn(name = "idx")
    @JoinTable(name = "ziggy_PipelineTaskData_pipelineTaskMetrics")
    private List<PipelineTaskMetric> pipelineTaskMetrics = new ArrayList<>();

    @ElementCollection
    @OrderColumn(name = "idx")
    @JoinTable(name = "ziggy_PipelineTaskData_taskExecutionLogs")
    private List<TaskExecutionLog> taskExecutionLogs = new ArrayList<>();

    @ElementCollection
    @JoinTable(name = "ziggy_PipelineTaskData_remoteJobs")
    private Set<RemoteJob> remoteJobs = new HashSet<>();

    /** For Hibernate use only. */
    PipelineTaskData() {
    }

    /**
     * Creates a {@code PipelineTaskData} object. The given non-null {@link PipelineTask} object
     * must already have been persisted to the database.
     */
    public PipelineTaskData(PipelineTask pipelineTask) {
        checkNotNull(pipelineTask);
        checkState(pipelineTask.getId() > 0);
        this.pipelineTask = pipelineTask;
        pipelineTaskId = pipelineTask.getId();
    }

    public PipelineTask getPipelineTask() {
        return pipelineTask;
    }

    public String getZiggySoftwareRevision() {
        return ziggySoftwareRevision;
    }

    public void setZiggySoftwareRevision(String ziggySoftwareRevision) {
        this.ziggySoftwareRevision = ziggySoftwareRevision;
    }

    public String getPipelineSoftwareRevision() {
        return pipelineSoftwareRevision;
    }

    public void setPipelineSoftwareRevision(String pipelineSoftwareRevision) {
        this.pipelineSoftwareRevision = pipelineSoftwareRevision;
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

    public String workerName() {
        if (StringUtils.isBlank(workerHost)) {
            return "-";
        }
        return HostNameUtils.callerHostNameOrLocalhost(workerHost) + ":" + workerThread;
    }

    public ExecutionClock getExecutionClock() {
        return executionClock;
    }

    public ProcessingStep getProcessingStep() {
        return processingStep;
    }

    public void setProcessingStep(ProcessingStep processingStep) {
        this.processingStep = processingStep;
    }

    public boolean isError() {
        return error;
    }

    public void setError(boolean error) {
        this.error = error;
    }

    public boolean isHaltRequested() {
        return haltRequested;
    }

    public void setHaltRequested(boolean haltRequested) {
        this.haltRequested = haltRequested;
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

    public boolean isRetry() {
        return retry;
    }

    public void setRetry(boolean retry) {
        this.retry = retry;
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

    public AlgorithmType getAlgorithmType() {
        return algorithmType;
    }

    public void setAlgorithmType(AlgorithmType mode) {
        algorithmType = mode;
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

    public List<PipelineTaskMetric> getPipelineTaskMetrics() {
        return pipelineTaskMetrics;
    }

    public void setPipelineTaskMetrics(List<PipelineTaskMetric> pipelineTaskMetrics) {
        this.pipelineTaskMetrics = pipelineTaskMetrics;
    }

    public List<TaskExecutionLog> getTaskExecutionLogs() {
        return taskExecutionLogs;
    }

    public void setTaskExecutionLogs(List<TaskExecutionLog> taskExecutionLogs) {
        this.taskExecutionLogs = taskExecutionLogs;
    }

    public Set<RemoteJob> getRemoteJobs() {
        return remoteJobs;
    }

    public void setRemoteJobs(Set<RemoteJob> remoteJobs) {
        this.remoteJobs = remoteJobs;
    }

    @Override
    public int compareTo(PipelineTaskData o) {
        return (int) (pipelineTaskId - o.pipelineTaskId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pipelineTaskId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PipelineTaskData other = (PipelineTaskData) obj;
        return pipelineTaskId == other.pipelineTaskId;
    }
}

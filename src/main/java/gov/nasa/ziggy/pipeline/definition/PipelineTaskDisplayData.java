package gov.nasa.ziggy.pipeline.definition;

import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Projection of a mixture of fields from {@link PipelineTask} and its corresponding
 * {@link PipelineTaskData} instance.
 *
 * @author PT
 * @author Bill Wohler
 */
public class PipelineTaskDisplayData {

    private static final String ERROR_PREFIX = "ERROR - ";

    private PipelineTaskData pipelineTaskData;

    public PipelineTaskDisplayData(PipelineTaskData pipelineTaskData) {
        this.pipelineTaskData = pipelineTaskData;
    }

    public double costEstimate() {
        double estimate = 0;
        for (RemoteJob job : getRemoteJobs()) {
            estimate += job.getCostEstimate();
        }
        return estimate;
    }

    public PipelineTask getPipelineTask() {
        return pipelineTaskData.getPipelineTask();
    }

    public long getPipelineTaskId() {
        return getPipelineTask().getId();
    }

    public long getPipelineInstanceId() {
        return getPipelineTask().getPipelineInstanceId();
    }

    public Date getCreated() {
        return getPipelineTask().getCreated();
    }

    public String getModuleName() {
        return getPipelineTask().getModuleName();
    }

    public String getBriefState() {
        return getPipelineTask().getUnitOfWork().briefState();
    }

    public String getZiggySoftwareRevision() {
        return pipelineTaskData.getZiggySoftwareRevision();
    }

    public String getPipelineSoftwareRevision() {
        return pipelineTaskData.getPipelineSoftwareRevision();
    }

    public String getWorkerName() {
        return pipelineTaskData.workerName();
    }

    public ExecutionClock getExecutionClock() {
        return pipelineTaskData.getExecutionClock();
    }

    public ProcessingStep getProcessingStep() {
        return pipelineTaskData.getProcessingStep();
    }

    public String getDisplayProcessingStep() {
        return (isError() ? ERROR_PREFIX : "") + getProcessingStep();
    }

    public boolean isError() {
        return pipelineTaskData.isError();
    }

    public int getTotalSubtaskCount() {
        return pipelineTaskData.getTotalSubtaskCount();
    }

    public int getCompletedSubtaskCount() {
        return pipelineTaskData.getCompletedSubtaskCount();
    }

    public int getFailedSubtaskCount() {
        return pipelineTaskData.getFailedSubtaskCount();
    }

    public int getFailureCount() {
        return pipelineTaskData.getFailureCount();
    }

    public List<PipelineTaskMetric> getPipelineTaskMetrics() {
        return pipelineTaskData.getPipelineTaskMetrics();
    }

    public Set<RemoteJob> getRemoteJobs() {
        return pipelineTaskData.getRemoteJobs();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPipelineTaskId(), getPipelineInstanceId(), getCreated(),
            getModuleName(), getBriefState(), getZiggySoftwareRevision(),
            getPipelineSoftwareRevision(), getWorkerName(), getExecutionClock(),
            getProcessingStep(), isError(), getTotalSubtaskCount(), getCompletedSubtaskCount(),
            getFailedSubtaskCount(), getFailureCount(), getPipelineTaskMetrics(), getRemoteJobs());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PipelineTaskDisplayData other = (PipelineTaskDisplayData) obj;
        return Objects.equals(pipelineTaskData, other.pipelineTaskData)
            && Objects.equals(getPipelineTaskId(), other.getPipelineTaskId())
            && Objects.equals(getPipelineInstanceId(), other.getPipelineInstanceId())
            && Objects.equals(getCreated(), other.getCreated())
            && Objects.equals(getModuleName(), other.getModuleName())
            && Objects.equals(getBriefState(), other.getBriefState())
            && Objects.equals(getZiggySoftwareRevision(), other.getZiggySoftwareRevision())
            && Objects.equals(getPipelineSoftwareRevision(), other.getPipelineSoftwareRevision())
            && Objects.equals(getWorkerName(), other.getWorkerName())
            && Objects.equals(getExecutionClock(), other.getExecutionClock())
            && Objects.equals(getProcessingStep(), other.getProcessingStep())
            && Objects.equals(isError(), other.isError())
            && Objects.equals(getTotalSubtaskCount(), other.getTotalSubtaskCount())
            && Objects.equals(getCompletedSubtaskCount(), other.getCompletedSubtaskCount())
            && Objects.equals(getFailedSubtaskCount(), other.getFailedSubtaskCount())
            && Objects.equals(getFailureCount(), other.getFailureCount())
            && Objects.equals(getPipelineTaskMetrics(), other.getPipelineTaskMetrics())
            && Objects.equals(getRemoteJobs(), other.getRemoteJobs());
    }

    public String toFullString() {
        return getClass().getSimpleName() + ": pipelineTaskId=" + getPipelineTaskId()
            + ", moduleName=" + getModuleName() + ", briefState=" + getBriefState();
    }

    @Override
    public String toString() {
        return getPipelineTask().toString();
    }
}

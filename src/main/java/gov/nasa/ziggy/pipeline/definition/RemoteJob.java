package gov.nasa.ziggy.pipeline.definition;

import java.util.Objects;

import gov.nasa.ziggy.ui.ZiggyGuiConsole;
import jakarta.persistence.Embeddable;

/**
 * Provides information on remote execution jobs associated with a {@link PipelineTask}.
 * Specifically, the following information is stored for each job:
 * <ol>
 * <li>The job identification number.
 * <li>The estimated cost of the job (in SBUs or actual money).
 * <li>Whether the job has finished.
 * </ol>
 * <p>
 * Note that the {@link #hashCode()} and {@link #equals(Object)} methods are written in terms of
 * just the {@code jobId}.
 * <p>
 * Instances of {@link RemoteJob} are created when the jobs for a {@link PipelineTask} are submitted
 * to the batch system. They get updated when the task leaves algorithm monitoring, when stale task
 * states are cleared at supervisor start time, and when the user requests a display of task and
 * instance costs via the {@link ZiggyGuiConsole}.
 *
 * @author PT
 */

@Embeddable
public class RemoteJob implements Comparable<RemoteJob> {

    private long jobId;
    private double costEstimate;
    private boolean finished;
    private String remoteEnvironmentName;
    private double costFactor;

    public RemoteJob() {
    }

    public RemoteJob(long jobId) {
        this.jobId = jobId;
    }

    public RemoteJob(long jobId, String remoteEnvironmentName) {
        this(jobId);
        this.remoteEnvironmentName = remoteEnvironmentName;
    }

    public RemoteJob(long jobId, String remoteEnvironmentName, double costFactor) {
        this(jobId, remoteEnvironmentName);
        this.costFactor = costFactor;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public long getJobId() {
        return jobId;
    }

    public double getCostEstimate() {
        return costEstimate;
    }

    public void setCostEstimate(double costEstimate) {
        this.costEstimate = costEstimate;
    }

    public boolean isFinished() {
        return finished;
    }

    public void setFinished(boolean finished) {
        this.finished = finished;
    }

    public String getRemoteEnvironmentName() {
        return remoteEnvironmentName;
    }

    public void setRemoteEnvironmentName(String remoteEnvironmentName) {
        this.remoteEnvironmentName = remoteEnvironmentName;
    }

    public double getCostFactor() {
        return costFactor;
    }

    public void setCostFactor(double costFactor) {
        this.costFactor = costFactor;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RemoteJob other = (RemoteJob) obj;
        return jobId == other.jobId;
    }

    @Override
    public int compareTo(RemoteJob otherRemoteJob) {
        return (int) (jobId - otherRemoteJob.jobId);
    }
}

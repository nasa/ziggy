package gov.nasa.ziggy.pipeline.step.remote;

import java.io.Serializable;
import java.util.Objects;

/**
 * Container class for information about a remote job.
 * <p>
 * Note that the job ID cannot be made final, nor assigned at create time. This is because the job
 * IDs won't be known until later, hence will need to be assigned when they become known.
 *
 * @author PT
 */
public class RemoteJobInformation implements Serializable {

    private static final long serialVersionUID = 20240822L;

    private final String logFile;
    private final String jobName;
    private long jobId;
    private double costFactor;
    private final String remoteEnvironmentName;
    private int batchSubmissionExitCode;

    public RemoteJobInformation(String logFile, String jobName, String remoteEnvironmentName) {
        this.logFile = logFile;
        this.jobName = jobName;
        this.remoteEnvironmentName = remoteEnvironmentName;
    }

    public String getLogFile() {
        return logFile;
    }

    public String getJobName() {
        return jobName;
    }

    public long getJobId() {
        return jobId;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public int getBatchSubmissionExitCode() {
        return batchSubmissionExitCode;
    }

    public void setBatchSubmissionExitCode(int exitCode) {
        batchSubmissionExitCode = exitCode;
    }

    public String getRemoteEnvironmentName() {
        return remoteEnvironmentName;
    }

    public double getCostFactor() {
        return costFactor;
    }

    public void setCostFactor(double costFactor) {
        this.costFactor = costFactor;
    }

    /**
     * The submission exit code and cost factor are not included because we want this method to only
     * consider two instances equal if their identifying information from the batch system is
     * identical.
     */

    @Override
    public int hashCode() {
        return Objects.hash(jobId, jobName, logFile, remoteEnvironmentName);
    }

    /**
     * The submission exit code and cost factor are not included because we want this method to only
     * consider two instances equal if their identifying information from the batch system is
     * identical.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RemoteJobInformation other = (RemoteJobInformation) obj;
        return jobId == other.jobId && Objects.equals(jobName, other.jobName)
            && Objects.equals(logFile, other.logFile)
            && Objects.equals(remoteEnvironmentName, other.remoteEnvironmentName);
    }
}

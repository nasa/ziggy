package gov.nasa.ziggy.module.remote;

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

    public RemoteJobInformation(String logFile, String jobName) {
        this.logFile = logFile;
        this.jobName = jobName;
    }

    public String getLogFile() {
        return logFile;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public long getJobId() {
        return jobId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, jobName, logFile);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        RemoteJobInformation other = (RemoteJobInformation) obj;
        return jobId == other.jobId && Objects.equals(jobName, other.jobName)
            && Objects.equals(logFile, other.logFile);
    }
}

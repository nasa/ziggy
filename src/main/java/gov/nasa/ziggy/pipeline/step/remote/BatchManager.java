package gov.nasa.ziggy.pipeline.step.remote;

import java.util.List;
import java.util.Map;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.RemoteJob;

/** Manages Ziggy's interface with a batch processing system. */
public interface BatchManager<T extends BatchParameters> {

    /**
     * Performs submission to the batch system..
     *
     * @param pipelineTask never null.
     * @return never null.
     */
    List<RemoteJobInformation> submitJobs(PipelineTask pipelineTask, int subtaskCount);

    /**
     * The job IDs by name for a given pipeline task.
     *
     * @param pipelineTask never null.
     * @return never null.
     */
    Map<String, Long> jobIdByName(PipelineTask pipelineTask);

    /** Determines whether a specific job has finished. */
    boolean isFinished(RemoteJobInformation remoteJobInformation);

    /** Determines whether a specific job has finished. */
    boolean isFinished(RemoteJob remoteJob);

    /** Exit status for a completed job, or null if no exit status is available. */
    Integer exitStatus(RemoteJobInformation remoteJobInformation);

    /** Exit comment for a completed job, or null if no exit status is available. */
    String exitComment(RemoteJobInformation remoteJobInformation);

    /**
     * Generates an instance of {@link RemoteJobInformation} from an existing {@link RemoteJob}, or
     * null if the RemoteJob instance is no longer running.
     */
    RemoteJobInformation remoteJobInformation(RemoteJob remoteJob);

    /**
     * Delete all jobs for a given {@link PipelineTask} from the batch system. Returns 0 if jobs
     * deleted successfully, 1 otherwise.
     */
    int deleteJobs(PipelineTask pipelineTask);

    /** Update the cost estimate for a specific job. */
    double getUpdatedCostEstimate(RemoteJob remoteJob);
}

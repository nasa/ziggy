package gov.nasa.ziggy.pipeline.definition.crud;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;

/**
 * A collection of operations on pipeline instance IDs, in particular operations in which the caller
 * requires either a stored instance ID or an inferred one, depending on the value of the stored
 * instance ID.
 *
 * @author PT
 */
public class PipelineInstanceIdOperations {
    private long pipelineInstanceId;
    private String pipelineModuleName;

    public PipelineInstanceIdOperations(long pipelineInstanceId, String pipelineModuleName) {
        this.pipelineModuleName = checkNotNull(pipelineModuleName, "pipelineModuleName");
        this.pipelineInstanceId = pipelineInstanceId;
    }

    /**
     * Indicates whether the pipeline instance ID stored in this object is valid
     *
     * @return true if valid instance (i.e., &#62; 0), false otherwise.
     */
    public boolean instanceParameterValid() {
        return pipelineInstanceId > 0;
    }

    /**
     * Returns either the pipeline instance ID stored in this object, or, if that one is not valid,
     * the max instance ID for the selected module
     *
     * @return the valid instance ID for the object's module
     */
    public long validPipelineInstanceId() {
        if (instanceParameterValid()) {
            return pipelineInstanceId;
        }
        return new PipelineTaskCrud().retrieveLatestForModule(pipelineModuleName)
            .getPipelineInstance()
            .getId();
    }

    /**
     * Indicates whether the instance ID has any DV tasks in it
     *
     * @return true if the instance returned by getValidPipelineInstanceId has DV tasks, false
     * otherwise.
     */
    public boolean tasksInInstance() {
        if (instanceParameterValid()) {
            List<PipelineTask> tasks = new PipelineTaskCrud()
                .retrieveTasksForModuleAndInstance(pipelineModuleName, pipelineInstanceId);
            return tasks != null && !tasks.isEmpty();
        }
        PipelineTask dvTask = new PipelineTaskCrud().retrieveLatestForModule(pipelineModuleName);
        return dvTask != null;
    }

    public long getPipelineInstanceId() {
        return pipelineInstanceId;
    }

    public void setPipelineInstanceId(long pipelineInstanceId) {
        this.pipelineInstanceId = pipelineInstanceId;
    }

    public String getPipelineModuleName() {
        return pipelineModuleName;
    }

    public void setPipelineModuleName(String pipelineModuleName) {
        this.pipelineModuleName = pipelineModuleName;
    }
}

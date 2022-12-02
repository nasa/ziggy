package gov.nasa.ziggy.pipeline;

import java.util.List;
import java.util.Objects;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;

/**
 * Container for a {@link PipelineInstance} and the associated {@link PipelineTask}s.
 *
 * @author PT
 */
public class InstanceAndTasks {

    private final PipelineInstance pipelineInstance;
    private final List<PipelineTask> pipelineTasks;

    public InstanceAndTasks(PipelineInstance pipelineInstance, List<PipelineTask> pipelineTasks) {
        this.pipelineInstance = pipelineInstance;
        this.pipelineTasks = pipelineTasks;
    }

    public PipelineInstance getPipelineInstance() {
        return pipelineInstance;
    }

    public List<PipelineTask> getPipelineTasks() {
        return pipelineTasks;
    }

    @Override
    public int hashCode() {
        return Objects.hash(pipelineInstance, pipelineTasks);
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
        InstanceAndTasks other = (InstanceAndTasks) obj;
        return Objects.equals(pipelineInstance, other.pipelineInstance)
            && Objects.equals(pipelineTasks, other.pipelineTasks);
    }

}

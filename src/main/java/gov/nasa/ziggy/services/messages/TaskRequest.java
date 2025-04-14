package gov.nasa.ziggy.services.messages;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Objects;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance.Priority;
import gov.nasa.ziggy.pipeline.definition.PipelineStepExecutor.RunMode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;

/**
 * Requests that the supervisor add a new task to its task queue.
 *
 * @author Todd Klaus
 * @author PT
 * @author Bill Wohler
 */
public class TaskRequest extends PipelineMessage implements Comparable<TaskRequest> {
    private static final long serialVersionUID = 20250307L;

    private final long instanceId;
    private final long instanceNodeId;
    private final long pipelineNodeId;
    private final PipelineTask pipelineTask;
    private final Priority priority;
    private final RunMode runMode;

    /**
     * If true, only re-run the transition logic. Usually only true if the user specifically
     * requested it via the console.
     */
    private final boolean doTransitionOnly;

    public TaskRequest(long instanceId, long instanceNodeId, long pipelineNodeId,
        PipelineTask pipelineTask, Priority priority, boolean doTransitionOnly, RunMode runMode) {
        this.instanceId = instanceId;
        this.instanceNodeId = instanceNodeId;
        this.pipelineNodeId = pipelineNodeId;
        this.pipelineTask = pipelineTask;
        this.doTransitionOnly = doTransitionOnly;
        this.priority = priority;
        this.runMode = checkNotNull(runMode, "runMode");
    }

    public long getInstanceId() {
        return instanceId;
    }

    public long getInstanceNodeId() {
        return instanceNodeId;
    }

    public long getPipelineNodeId() {
        return pipelineNodeId;
    }

    public PipelineTask getPipelineTask() {
        return pipelineTask;
    }

    public Priority getPriority() {
        return priority;
    }

    public RunMode getRunMode() {
        return runMode;
    }

    public boolean isDoTransitionOnly() {
        return doTransitionOnly;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        return prime * result + Objects.hash(pipelineNodeId, pipelineTask, priority);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj) || getClass() != obj.getClass()) {
            return false;
        }
        TaskRequest other = (TaskRequest) obj;
        return pipelineNodeId == other.pipelineNodeId
            && Objects.equals(pipelineTask, other.pipelineTask) && priority == other.priority;
    }

    /**
     * Implements prioritization for {@link TaskRequest} instances. The prioritization scheme is as
     * follows:
     * <ol>
     * <li>Tasks are first ordered by pipeline node ID. This is done because the worker count and
     * worker heap size are set on a per-pipeline-node basis. By forcing the tasks to sort first by
     * pipeline node, we avoid a lot of thrashing between different worker configurations as tasks
     * for assorted pipeline nodes execute.
     * <li>For tasks with a common pipeline node ID, the tasks are sorted so that higher priority
     * tasks execute earlier.
     * <li>For tasks with a common pipeline node ID and priority, the tasks are sorted in order of
     * task ID.
     * </ol>
     */
    @Override
    public int compareTo(TaskRequest o) {
        if (pipelineNodeId != o.getPipelineNodeId()) {
            return (int) (pipelineNodeId - o.getPipelineNodeId());
        }
        if (priority != o.getPriority()) {
            return priority.compareTo(o.getPriority());
        }
        return pipelineTask.compareTo(o.pipelineTask);
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }
}

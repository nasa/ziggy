package gov.nasa.ziggy.services.messages;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Objects;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance.Priority;
import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;

/**
 * Requests that the supervisor add a new task to its task queue.
 *
 * @author Todd Klaus
 * @author PT
 */
public class TaskRequest extends PipelineMessage implements Comparable<TaskRequest> {
    private static final long serialVersionUID = 20230511L;

    private final long instanceId;
    private final long instanceNodeId;
    private final long pipelineDefinitionNodeId;
    private final long taskId;
    private final Priority priority;
    private final RunMode runMode;

    /**
     * If true, only re-run the transition logic. Usually only true if the user specifically
     * requested it via the console.
     */
    private final boolean doTransitionOnly;

    public TaskRequest(long instanceId, long instanceNodeId, long pipelineDefinitionNodeId,
        long taskId, Priority priority, boolean doTransitionOnly, RunMode runMode) {
        this.instanceId = instanceId;
        this.instanceNodeId = instanceNodeId;
        this.pipelineDefinitionNodeId = pipelineDefinitionNodeId;
        this.taskId = taskId;
        this.doTransitionOnly = doTransitionOnly;
        this.priority = priority;
        this.runMode = checkNotNull(runMode, "runMode");
    }

    public long getInstanceId() {
        return instanceId;
    }

    public long getPipelineDefinitionNodeId() {
        return pipelineDefinitionNodeId;
    }

    public long getTaskId() {
        return taskId;
    }

    public boolean isDoTransitionOnly() {
        return doTransitionOnly;
    }

    public long getInstanceNodeId() {
        return instanceNodeId;
    }

    public Priority getPriority() {
        return priority;
    }

    public RunMode getRunMode() {
        return runMode;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        return prime * result + Objects.hash(pipelineDefinitionNodeId, priority, taskId);
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
        return pipelineDefinitionNodeId == other.pipelineDefinitionNodeId
            && priority == other.priority && taskId == other.taskId;
    }

    /**
     * Implements prioritization for {@link TaskRequest} instances. The prioritization scheme is as
     * follows:
     * <ol>
     * <li>Tasks are first ordered by pipeline definition node ID. This is done because the worker
     * count and worker heap size are set on a per-pipeline-definition-node basis. By forcing the
     * tasks to sort first by pipeline definition node, we avoid a lot of thrashing between
     * different worker configurations as tasks for assorted pipeline definition nodes execute.
     * <li>For tasks with a common pipeline definition node ID, the tasks are sorted so that higher
     * priority tasks execute earlier.
     * <li>For tasks with a common pipeline definition node ID and priority, the tasks are sorted in
     * order of task ID.
     * </ol>
     */
    @Override
    public int compareTo(TaskRequest o) {
        if (pipelineDefinitionNodeId != o.getPipelineDefinitionNodeId()) {
            return (int) (pipelineDefinitionNodeId - o.getPipelineDefinitionNodeId());
        }
        if (priority != o.getPriority()) {
            return priority.compareTo(o.getPriority());
        }
        return (int) (taskId - o.getTaskId());
    }
}

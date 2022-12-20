package gov.nasa.ziggy.services.messages;

import static com.google.common.base.Preconditions.checkNotNull;

import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;
import gov.nasa.ziggy.services.messaging.MessageHandler;

/**
 * @author Todd Klaus
 */
public class WorkerTaskRequest extends PipelineMessage implements Comparable<WorkerTaskRequest> {
    private static final long serialVersionUID = 20210702L;

    private long instanceId;
    private long instanceNodeId;
    private long taskId;
    private int priority;
    private RunMode runMode;

    /**
     * If true, only re-run the transition logic. Usually only true if the user specifically
     * requested it via the console.
     */
    private boolean doTransitionOnly = false;

    public WorkerTaskRequest(long instanceId, long instanceNodeId, long taskId, int priority,
        boolean doTransitionOnly, RunMode runMode) {
        this.instanceId = instanceId;
        this.instanceNodeId = instanceNodeId;
        this.taskId = taskId;
        this.doTransitionOnly = doTransitionOnly;
        this.priority = priority;
        this.runMode = checkNotNull(runMode, "runMode");
    }

    /**
     * @return Returns the instanceId.
     */
    public long getInstanceId() {
        return instanceId;
    }

    /**
     * @param instanceId The instanceId to set.
     */
    public void setInstanceId(long instanceId) {
        this.instanceId = instanceId;
    }

    /**
     * @return Returns the taskId.
     */
    public long getTaskId() {
        return taskId;
    }

    /**
     * @param taskId The taskId to set.
     */
    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    /**
     * @return the doTransitionOnly
     */
    public boolean isDoTransitionOnly() {
        return doTransitionOnly;
    }

    /**
     * @param doTransitionOnly the doTransitionOnly to set
     */
    public void setDoTransitionOnly(boolean doTransitionOnly) {
        this.doTransitionOnly = doTransitionOnly;
    }

    public long getInstanceNodeId() {
        return instanceNodeId;
    }

    public void setInstanceNodeId(long instanceNodeId) {
        this.instanceNodeId = instanceNodeId;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public int getPriority() {
        return priority;
    }

    public RunMode getRunMode() {
        return runMode;
    }

    public void setRunMode(RunMode runMode) {
        this.runMode = runMode;
    }

    @Override
    public Object handleMessage(MessageHandler messageHandler) {
        messageHandler.handleWorkerTaskRequest(this);
        return null;
    }

    /**
     * Implements prioritization for {@link WorkerTaskRequest} instances. Instances with a lower
     * {@link priority} value come first; for instances with the same {@link priority}, the instance
     * with the lower {@link taskId} comes first.
     */
    @Override
    public int compareTo(WorkerTaskRequest o) {
        if (priority != o.getPriority()) {
            return priority - o.getPriority();
        }
        return (int) (taskId - o.getTaskId());
    }
}

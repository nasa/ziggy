package gov.nasa.ziggy.services.messages;

/**
 * Notifies recipients that a task has completed.
 *
 * @author PT
 */
public class TaskCompletionNotification extends PipelineMessage {

    private static final long serialVersionUID = 20230613L;

    private final long taskId;

    public TaskCompletionNotification(long taskId) {
        this.taskId = taskId;
    }

    public long getTaskId() {
        return taskId;
    }
}

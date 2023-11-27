package gov.nasa.ziggy.services.messages;

import gov.nasa.ziggy.util.Requestor;

/**
 * Response sent in reply to a {@link KillTasksRequest} message. The reply tells the sender that a
 * particular task has been killed (or at least that the task-killing activities completed without
 * an exception).
 *
 * @author PT
 */
public class KilledTaskMessage extends SpecifiedRequestorMessage {
    private static final long serialVersionUID = 20231030L;

    private final long taskId;

    public KilledTaskMessage(Requestor requestor, long taskId) {
        super(requestor);
        this.taskId = taskId;
    }

    public long getTaskId() {
        return taskId;
    }
}

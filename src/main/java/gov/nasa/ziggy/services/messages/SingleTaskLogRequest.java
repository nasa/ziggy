package gov.nasa.ziggy.services.messages;

import gov.nasa.ziggy.services.logging.TaskLogInformation;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.util.Requestor;

/**
 * Requests a single task log for display.
 *
 * @author PT
 */
public class SingleTaskLogRequest extends SpecifiedRequestorMessage {

    private static final long serialVersionUID = 20230614L;

    private final TaskLogInformation taskLogInformation;

    private SingleTaskLogRequest(Requestor sender, TaskLogInformation taskLogInformation) {
        super(sender);
        this.taskLogInformation = taskLogInformation;
    }

    public static void requestSingleTaskLog(Requestor sender,
        TaskLogInformation taskLogInformation) {
        ZiggyMessenger.publish(new SingleTaskLogRequest(sender, taskLogInformation));
    }

    public TaskLogInformation getTaskLogInformation() {
        return taskLogInformation;
    }
}

package gov.nasa.ziggy.services.messages;

import java.util.Set;

import gov.nasa.ziggy.services.logging.TaskLogInformation;

/**
 * Carries information about the available task logs for a specific task.
 *
 * @author PT
 */
public class TaskLogInformationMessage extends SpecifiedRequestorMessage {

    private static final long serialVersionUID = 20230614L;

    private final Set<TaskLogInformation> taskLogInformation;

    public TaskLogInformationMessage(TaskLogInformationRequest originalMessage,
        Set<TaskLogInformation> taskLogInformation) {
        super(originalMessage);
        this.taskLogInformation = taskLogInformation;
    }

    public Set<TaskLogInformation> taskLogInformation() {
        return taskLogInformation;
    }
}

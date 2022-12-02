package gov.nasa.ziggy.services.messages;

import gov.nasa.ziggy.services.logging.TaskLogInformation;
import gov.nasa.ziggy.services.messaging.MessageHandler;
import gov.nasa.ziggy.services.messaging.UiCommunicator;
import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.ui.proxy.CrudProxy;

public class WorkerSingleTaskLogRequest extends PipelineMessage {

    private static final long serialVersionUID = 20220325L;

    private final TaskLogInformation taskLogInformation;

    public static String requestSingleTaskLog(TaskLogInformation taskLogInformation) {
        CrudProxy.verifyPrivileges(Privilege.PIPELINE_OPERATIONS);
        return (String) UiCommunicator.send(new WorkerSingleTaskLogRequest(taskLogInformation));
    }

    private WorkerSingleTaskLogRequest(TaskLogInformation taskLogInformation) {
        this.taskLogInformation = taskLogInformation;
    }

    public TaskLogInformation getTaskLogInformation() {
        return taskLogInformation;
    }

    @Override
    public Object handleMessage(MessageHandler messageHandler) {
        return messageHandler.handleSingleTaskLogRequest(this);
    }

}

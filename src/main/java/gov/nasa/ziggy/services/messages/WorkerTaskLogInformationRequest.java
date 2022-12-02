package gov.nasa.ziggy.services.messages;

import java.util.Set;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.logging.TaskLogInformation;
import gov.nasa.ziggy.services.messaging.MessageHandler;
import gov.nasa.ziggy.services.messaging.UiCommunicator;
import gov.nasa.ziggy.services.security.Privilege;
import gov.nasa.ziggy.ui.proxy.CrudProxy;

/**
 * Ask for metadata on all the log files associated with a given pipeline task.
 *
 * @see gov.nasa.ziggy.services.logging.TaskLogInformation
 * @author PT
 */
public final class WorkerTaskLogInformationRequest extends PipelineMessage {

    private static final long serialVersionUID = 20220325L;

    private final long instanceId;
    private final long taskId;

    /**
     * Performs the request for task log information and returns the information for all logs as a
     * {@link Set} of {@link TaskLogInformation} instances. This method is the only public method to
     * retrieve a task log, as the {@link WorkerTaskLogInformationRequest} constructor is private.
     * This prevents callers from attempting to bypass the privilege verification performed in this
     * method.
     */
    @SuppressWarnings("unchecked")
    public static Set<TaskLogInformation> requestTaskLogInformation(PipelineTask task) {
        CrudProxy.verifyPrivileges(Privilege.PIPELINE_OPERATIONS);
        return (Set<TaskLogInformation>) UiCommunicator.send(
            new WorkerTaskLogInformationRequest(task.getPipelineInstance().getId(), task.getId()));
    }

    private WorkerTaskLogInformationRequest(long instanceId, long taskId) {
        this.instanceId = instanceId;
        this.taskId = taskId;
    }

    public long getInstanceId() {
        return instanceId;
    }

    public long getTaskId() {
        return taskId;
    }

    @Override
    public Object handleMessage(MessageHandler messageHandler) {
        return messageHandler.handleTaskLogInformationRequest(this);
    }

}

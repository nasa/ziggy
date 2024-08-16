package gov.nasa.ziggy.services.messages;

import java.util.Set;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.logging.TaskLogInformation;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;
import gov.nasa.ziggy.util.Requestor;

/**
 * Ask for metadata on all the log files associated with a given pipeline task.
 *
 * @see gov.nasa.ziggy.services.logging.TaskLogInformation
 * @author PT
 */
public final class TaskLogInformationRequest extends SpecifiedRequestorMessage {

    private static final long serialVersionUID = 20230614L;

    private final long taskId;

    private TaskLogInformationRequest(Requestor sender, long taskId) {
        super(sender);
        this.taskId = taskId;
    }

    /**
     * Performs the request for task log information and returns the information for all logs as a
     * {@link Set} of {@link TaskLogInformation} instances. This method is the only public method to
     * retrieve a task log, as the {@link TaskLogInformationRequest} constructor is private. This
     * prevents callers from attempting to bypass the privilege verification performed in this
     * method.
     */
    public static void requestTaskLogInformation(Requestor sender, PipelineTask task) {
        ZiggyMessenger.publish(new TaskLogInformationRequest(sender, task.getId()));
    }

    public long getTaskId() {
        return taskId;
    }
}

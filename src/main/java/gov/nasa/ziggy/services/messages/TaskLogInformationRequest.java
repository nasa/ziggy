package gov.nasa.ziggy.services.messages;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.util.Requestor;

/**
 * Ask for metadata on all the log files associated with a given pipeline task.
 *
 * @see gov.nasa.ziggy.services.logging.TaskLogInformation
 * @author PT
 * @author Bill Wohler
 */
public final class TaskLogInformationRequest extends SpecifiedRequestorMessage {

    private static final long serialVersionUID = 20240909L;

    private final PipelineTask pipelineTask;

    public TaskLogInformationRequest(Requestor sender, PipelineTask pipelineTask) {
        super(sender);
        this.pipelineTask = pipelineTask;
    }

    public PipelineTask getPipelineTask() {
        return pipelineTask;
    }
}

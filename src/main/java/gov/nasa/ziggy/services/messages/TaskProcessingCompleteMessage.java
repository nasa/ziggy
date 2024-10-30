package gov.nasa.ziggy.services.messages;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;

public class TaskProcessingCompleteMessage extends PipelineMessage {

    private static final long serialVersionUID = 20240909L;

    private final PipelineTask pipelineTask;

    public TaskProcessingCompleteMessage(PipelineTask pipelineTask) {
        this.pipelineTask = pipelineTask;
    }

    public PipelineTask getPipelineTask() {
        return pipelineTask;
    }
}

package gov.nasa.ziggy.services.messages;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;

/**
 * Notifies subscribers that a given {@link PipelineTask} has successfully halted.
 *
 * @author PT
 * @author Bill Wohler
 */
public class TaskHaltedMessage extends PipelineMessage {

    private static final long serialVersionUID = 20240913L;

    private final PipelineTask pipelineTask;

    public TaskHaltedMessage(PipelineTask pipelineTask) {
        this.pipelineTask = pipelineTask;
    }

    public PipelineTask getPipelineTask() {
        return pipelineTask;
    }
}

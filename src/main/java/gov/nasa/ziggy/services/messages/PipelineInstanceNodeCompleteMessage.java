package gov.nasa.ziggy.services.messages;

import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.supervisor.TaskRequestHandler;

/**
 * Message sent by the {@link TaskRequestHandler} when all the tasks in a given
 * {@link PipelineInstanceNode} are either completed or errored.
 *
 * @author PT
 */
public class PipelineInstanceNodeCompleteMessage extends PipelineMessage {
    private static final long serialVersionUID = 20231103L;

    private final long pipelineInstanceNodeId;

    public PipelineInstanceNodeCompleteMessage(long pipelineInstanceNodeId) {
        this.pipelineInstanceNodeId = pipelineInstanceNodeId;
    }

    public long getPipelineInstanceNodeId() {
        return pipelineInstanceNodeId;
    }
}

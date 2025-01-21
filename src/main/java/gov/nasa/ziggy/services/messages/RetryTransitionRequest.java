package gov.nasa.ziggy.services.messages;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;

/**
 * Requests that the supervisor retry the transition for the given {@link PipelineInstance}.
 *
 * @author PT
 */
public class RetryTransitionRequest extends PipelineMessage {

    private static final long serialVersionUID = 20241106L;

    private final long pipelineInstanceId;

    public RetryTransitionRequest(long pipelineInstanceId) {
        this.pipelineInstanceId = pipelineInstanceId;
    }

    public long getPipelineInstanceId() {
        return pipelineInstanceId;
    }
}

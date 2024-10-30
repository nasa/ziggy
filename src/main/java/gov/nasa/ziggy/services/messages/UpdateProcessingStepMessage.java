package gov.nasa.ziggy.services.messages;

import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;

/**
 * Tells the supervisor to update the state of a {@link PipelineTask}, and if necessary also the
 * {@link PipelineInstance}.
 *
 * @author PT
 */
public class UpdateProcessingStepMessage extends PipelineMessage {

    private static final long serialVersionUID = 20240920L;
    private final PipelineTask pipelineTask;
    private final ProcessingStep processingStep;

    public UpdateProcessingStepMessage(PipelineTask pipelineTask, ProcessingStep processingStep) {
        this.pipelineTask = pipelineTask;
        this.processingStep = processingStep;
    }

    public PipelineTask getPipelineTask() {
        return pipelineTask;
    }

    public ProcessingStep getProcessingStep() {
        return processingStep;
    }
}

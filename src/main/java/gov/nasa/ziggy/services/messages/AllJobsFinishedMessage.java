package gov.nasa.ziggy.services.messages;

import gov.nasa.ziggy.module.AlgorithmMonitor;
import gov.nasa.ziggy.module.TaskMonitor;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;

/**
 * Message from the {@link AlgorithmMonitor} that notifies the {@link TaskMonitor} that all the
 * remote jobs for the task have finished. This is needed because remote jobs can exit silently, in
 * which case the task monitor doesn't know that nobody is processing task data any longer.
 *
 * @author PT
 * @author Bill Wohler
 */
public class AllJobsFinishedMessage extends PipelineMessage {

    private static final long serialVersionUID = 20240909L;

    private final PipelineTask pipelineTask;

    public AllJobsFinishedMessage(PipelineTask pipelineTask) {
        this.pipelineTask = pipelineTask;
    }

    public PipelineTask getPipelineTask() {
        return pipelineTask;
    }
}

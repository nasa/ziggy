package gov.nasa.ziggy.services.messages;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.supervisor.PipelineSupervisor;
import gov.nasa.ziggy.supervisor.TaskRequestHandlerLifecycleManager;

/**
 * Message that tells the {@link TaskRequestHandlerLifecycleManager} in the
 * {@link PipelineSupervisor} that a task is restarting, so if it's in the list of tasks that were
 * killed previously, take it out of that list.
 *
 * @author PT
 * @author Bill Wohler
 */
public class RemoveTaskFromKilledTasksMessage extends PipelineMessage {

    private static final long serialVersionUID = 20240905L;

    private final PipelineTask pipelineTask;

    public RemoveTaskFromKilledTasksMessage(PipelineTask pipelineTask) {
        this.pipelineTask = pipelineTask;
    }

    public PipelineTask getPipelineTask() {
        return pipelineTask;
    }
}

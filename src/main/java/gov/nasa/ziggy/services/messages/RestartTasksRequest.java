package gov.nasa.ziggy.services.messages;

import java.util.List;

import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;

/**
 * Notifies the supervisor that pipeline tasks should be restarted.
 *
 * @author PT
 * @author Bill Wohler
 */
public class RestartTasksRequest extends PipelineMessage {

    private static final long serialVersionUID = 20240909L;

    private final List<PipelineTask> pipelineTasks;
    private final boolean doTransitionOnly;
    private final RunMode runMode;

    public RestartTasksRequest(List<PipelineTask> pipelineTasks, boolean doTransitionOnly,
        RunMode runMode) {
        this.pipelineTasks = pipelineTasks;
        this.doTransitionOnly = doTransitionOnly;
        this.runMode = runMode;
    }

    public List<PipelineTask> getPipelineTasks() {
        return pipelineTasks;
    }

    public boolean isDoTransitionOnly() {
        return doTransitionOnly;
    }

    public RunMode getRunMode() {
        return runMode;
    }
}

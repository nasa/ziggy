package gov.nasa.ziggy.services.messages;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import gov.nasa.ziggy.pipeline.definition.PipelineModule.RunMode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;

/**
 * Notifies the supervisor that pipeline tasks should be restarted.
 *
 * @author PT
 */
public class RestartTasksRequest extends PipelineMessage {

    private static final long serialVersionUID = 20240423L;
    private final List<Long> pipelineTaskIds = new ArrayList<>();
    private final boolean doTransitionOnly;
    private final RunMode runMode;

    public RestartTasksRequest(List<PipelineTask> pipelineTasks, boolean doTransitionOnly,
        RunMode runMode) {
        this.doTransitionOnly = doTransitionOnly;
        this.runMode = runMode;
        pipelineTaskIds
            .addAll(pipelineTasks.stream().map(PipelineTask::getId).collect(Collectors.toSet()));
    }

    public List<Long> getPipelineTaskIds() {
        return pipelineTaskIds;
    }

    public boolean isDoTransitionOnly() {
        return doTransitionOnly;
    }

    public RunMode getRunMode() {
        return runMode;
    }
}

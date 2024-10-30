package gov.nasa.ziggy.services.messages;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;

/**
 * Requests that the supervisor halt the selected tasks if they are waiting in the worker queue to
 * be processed, and then makes the same request of worker processes that are processing any of
 * these tasks. processes. The tasks are specified by task ID.
 *
 * @author PT
 * @author Bill Wohler
 */
public class HaltTasksRequest extends PipelineMessage {

    private static final long serialVersionUID = 20240913L;

    private final List<PipelineTask> pipelineTasks;

    public HaltTasksRequest(Collection<PipelineTask> pipelineTasks) {
        this.pipelineTasks = new ArrayList<>(pipelineTasks);
    }

    public List<PipelineTask> getPipelineTasks() {
        return pipelineTasks;
    }
}

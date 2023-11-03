package gov.nasa.ziggy.services.messages;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;

/**
 * Requests that the supervisor kill the selected tasks if they are waiting in the worker queue to
 * be processed, and then makes the same request of worker processes that are processing any of
 * these tasks. processes. The tasks are specified by task ID.
 *
 * @author PT
 */
public class KillTasksRequest extends PipelineMessage {

    private static final long serialVersionUID = 20230511L;

    private final List<Long> taskIds = new ArrayList<>();

    public KillTasksRequest(List<PipelineTask> pipelineTasks) {
        taskIds
            .addAll(pipelineTasks.stream().map(PipelineTask::getId).collect(Collectors.toList()));
    }

    public List<Long> getTaskIds() {
        return taskIds;
    }
}

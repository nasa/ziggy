package gov.nasa.ziggy.services.messages;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import gov.nasa.ziggy.module.StateFile;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.util.Requestor;

/**
 * Requests that the supervisor kill the selected tasks if they are waiting in the worker queue to
 * be processed, and then makes the same request of worker processes that are processing any of
 * these tasks. processes. The tasks are specified by task ID.
 *
 * @author PT
 */
public class KillTasksRequest extends SpecifiedRequestorMessage {

    private static final long serialVersionUID = 20231030L;

    private final List<Long> taskIds;

    private KillTasksRequest(Requestor requestor, List<Long> taskIds) {
        super(requestor);
        this.taskIds = taskIds;
    }

    public static KillTasksRequest forTaskIds(Requestor requestor, Collection<Long> taskIds) {
        return new KillTasksRequest(requestor, new ArrayList<>(taskIds));
    }

    public static KillTasksRequest forPipelineTasks(Requestor requestor,
        Collection<PipelineTask> pipelineTasks) {

        List<Long> taskIds = new ArrayList<>();
        for (PipelineTask pipelineTask : pipelineTasks) {
            taskIds.add(pipelineTask.getId());
        }
        return new KillTasksRequest(requestor, taskIds);
    }

    public static KillTasksRequest forStateFiles(Requestor requestor,
        Collection<StateFile> stateFiles) {

        List<Long> taskIds = new ArrayList<>();
        for (StateFile stateFile : stateFiles) {
            taskIds.add(stateFile.getPipelineTaskId());
        }
        return new KillTasksRequest(requestor, taskIds);
    }

    public List<Long> getTaskIds() {
        return taskIds;
    }
}

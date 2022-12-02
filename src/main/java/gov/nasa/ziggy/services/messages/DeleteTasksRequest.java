package gov.nasa.ziggy.services.messages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.module.AlgorithmMonitor;
import gov.nasa.ziggy.module.PipelineException;
import gov.nasa.ziggy.pipeline.PipelineExecutor;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTask.State;
import gov.nasa.ziggy.pipeline.definition.crud.PipelineTaskCrud;
import gov.nasa.ziggy.services.alert.AlertService;
import gov.nasa.ziggy.services.database.DatabaseTransactionFactory;
import gov.nasa.ziggy.services.messaging.MessageHandler;
import gov.nasa.ziggy.worker.WorkerPipelineProcess;
import gov.nasa.ziggy.worker.WorkerTaskRequestDispatcher;

/**
 * Requests that the worker delete tasks from both the submitted task queue and from the worker
 * threads. The tasks are specified by task ID.
 *
 * @author PT
 */
public class DeleteTasksRequest extends PipelineMessage {

    private static final long serialVersionUID = 20221004L;

    private static final Logger log = LoggerFactory.getLogger(DeleteTasksRequest.class);
    private final List<Long> taskIds = new ArrayList<>();

    public DeleteTasksRequest(List<PipelineTask> pipelineTasks) {
        taskIds.addAll(pipelineTasks.stream().map(s -> s.getId()).collect(Collectors.toList()));
    }

    @Override
    public Object handleMessage(MessageHandler messageHandler) {
        try {
            return Boolean.valueOf(messageHandler.deleteTasks(this));
        } catch (IOException e) {
            throw new PipelineException("Unable to delete tasks", e);
        }

    }

    /**
     * Performs the deletion of tasks based on ID numbers in the {@link DeleteTasksRequest}
     * instance. Returns true if all the tasks for deletion were found locally (implying that none
     * will be found running remotely), false otherwise.
     *
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public boolean deleteTasks() throws IOException {

        log.info("Starting DeleteTasksRequest.deleteTasks()");
        // Start by going through the queued tasks (tasks that are submitted but not yet running).
        Iterator<WorkerTaskRequest> taskIterator = WorkerPipelineProcess.workerTaskRequestQueue
            .iterator();
        List<WorkerTaskRequest> tasksForDeletion = new ArrayList<>();
        Set<Long> taskIdsForDeletion = new HashSet<>();
        while (taskIterator.hasNext()) {
            WorkerTaskRequest request = taskIterator.next();
            if (taskIds.contains(request.getTaskId())) {
                tasksForDeletion.add(request);
                taskIdsForDeletion.add(request.getTaskId());
            }
        }
        WorkerPipelineProcess.workerTaskRequestQueue.removeAll(tasksForDeletion);

        // For tasks that were waiting to run, i.e. in the submitted state, we need to manually
        // set them to ERRORed in the database and update all counts and instance states.
        if (!taskIdsForDeletion.isEmpty()) {
            DatabaseTransactionFactory.performTransactionInThread(() -> {
                PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();
                PipelineExecutor pipelineExecutor = new PipelineExecutor();
                List<PipelineTask> tasks = pipelineTaskCrud.retrieveAll(taskIdsForDeletion);
                for (PipelineTask task : tasks) {
                    task.setState(State.ERROR);
                    task.stopExecutionClock();
                    pipelineTaskCrud.update(task);
                    pipelineExecutor.updateTaskCountsForCurrentNode(task, false);
                    pipelineExecutor.updateInstanceState(task.getPipelineInstance());
                }
                return null;
            });

            // Issue alerts, too!
            AlertService alertService = AlertService.getInstance();
            for (long taskId : taskIdsForDeletion) {
                alertService.generateAndBroadcastAlert("PI (Remote)", taskId,
                    AlertService.Severity.ERROR,
                    "Task " + taskId + " deleted from processing queue.");
            }
        }

        // Signal to the WorkerTaskRequestDispatcher that any tasks that are currently being
        // processed that they should be deleted.
        for (long taskId : taskIds) {
            if (WorkerTaskRequestDispatcher.deleteTask(taskId)) {
                taskIdsForDeletion.add(taskId);
            }
        }

        // Finally, for tasks that are running an external algorithm, use AlgorithmMonitor to
        // tell the ComputeNodeMaster that the task in question has been deleted. This will cause
        // algorithm processing to exit and the status of the task and instance to be updated
        // properly.
        taskIdsForDeletion.addAll(AlgorithmMonitor.deleteLocalTasks(taskIds));

        // Locate any tasks that have completed while the delete request was working its way
        // through the system.
        taskIdsForDeletion
            .addAll((List<Long>) DatabaseTransactionFactory.performTransactionInThread(() -> {
                return new PipelineTaskCrud().retrieveIdsForTasksInState(taskIds,
                    PipelineTask.State.COMPLETED);
            }));

        // Notify the caller as to whether all the tasks were located in local execution.
        return taskIdsForDeletion.containsAll(taskIds);
    }

}

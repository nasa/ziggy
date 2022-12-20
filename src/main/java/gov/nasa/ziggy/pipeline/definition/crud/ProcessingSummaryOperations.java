package gov.nasa.ziggy.pipeline.definition.crud;

import static gov.nasa.ziggy.services.database.DatabaseTransactionFactory.performTransactionInThread;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTask.ProcessingSummary;
import gov.nasa.ziggy.pipeline.definition.ProcessingState;
import gov.nasa.ziggy.ui.proxy.ProcessingSummaryOpsProxy;

/**
 * Supports updates to database values of {@link PipelineTask} fields that are associated with the
 * processing state. These include the state itself (an instance of {@link ProcessingState}; the
 * total number of subtasks, number of completed subtasks, and number of failed subtasks.
 *
 * @author Todd Klaus
 * @author PT
 */
public class ProcessingSummaryOperations {

    public ProcessingSummaryOperations() {
    }

    /**
     * Update the subtask counts in the database.
     */
    public void updateSubTaskCounts(final long taskId, final int numSubTasksTotal,
        final int numSubTasksComplete, final int numSubTasksFailed) {
        performTransactionInThread(() -> {
            PipelineTaskCrud crud = new PipelineTaskCrud();
            PipelineTask task = crud.retrieve(taskId);
            task.setCompletedSubtaskCount(numSubTasksComplete);
            task.setFailedSubtaskCount(numSubTasksFailed);
            task.setTotalSubtaskCount(numSubTasksTotal);
            crud.update(task);
            return null;
        });
    }

    /**
     * Update the processing state in the database.
     */
    public void updateProcessingState(final long taskId, final ProcessingState newState) {
        performTransactionInThread(() -> {
            PipelineTaskCrud crud = new PipelineTaskCrud();
            PipelineTask task = crud.retrieve(taskId);
            task.setProcessingState(newState);
            crud.update(task);
            return null;
        });
    }

    /**
     * Retrieve the current processing state from the database.
     */
    public ProcessingSummary processingSummary(long taskId) {
        return (ProcessingSummary) performTransactionInThread(
            () -> processingSummaryInternal(taskId));
    }

    /**
     * Component of the summary retrieval that takes place within the context of a database
     * transaction. This allows the {@link ProcessingSummaryOpsProxy} to use the same internal
     * workings but with a different database transaction wrapper.
     */
    public ProcessingSummary processingSummaryInternal(long taskId) {
        PipelineTaskCrud crud = new PipelineTaskCrud();
        PipelineTask task = crud.retrieve(taskId);
        return new ProcessingSummary(task);
    }

    /**
     * Retrieve processing summaries for a collection of pipeline tasks.
     */
    @SuppressWarnings("unchecked")
    public Map<Long, ProcessingSummary> processingSummaries(
        Collection<PipelineTask> pipelineTasks) {
        return (Map<Long, ProcessingSummary>) performTransactionInThread(
            () -> processingSummariesInternal(pipelineTasks));
    }

    /**
     * Component of the summary retrieval that takes place within the context of a database
     * transaction. This allows the {@link ProcessingSummaryOpsProxy} to use the same internal
     * workings but with a different database transaction wrapper.
     */
    public Map<Long, ProcessingSummary> processingSummariesInternal(
        Collection<PipelineTask> pipelineTasks) {
        Map<Long, ProcessingSummary> processingStates = new HashMap<>();
        PipelineTaskCrud crud = new PipelineTaskCrud();
        for (PipelineTask task : pipelineTasks) {
            PipelineTask databaseTask = crud.retrieve(task.getId());
            processingStates.put(task.getId(), new ProcessingSummary(databaseTask));
        }
        return processingStates;
    }

    /**
     * Performs the inner component of summary retrieval for a given pipeline instance, specified by
     * the ID of the latter.
     */
    public Map<Long, ProcessingSummary> processingSummariesForInstanceInternal(long instanceId) {
        return processingSummariesInternal(
            new PipelineTaskCrud().retrieveTasksForInstance(instanceId));
    }

}

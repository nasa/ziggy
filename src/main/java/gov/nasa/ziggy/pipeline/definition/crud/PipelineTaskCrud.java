package gov.nasa.ziggy.pipeline.definition.crud;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.module.remote.RemoteParameters;
import gov.nasa.ziggy.pipeline.PipelineOperations;
import gov.nasa.ziggy.pipeline.PipelineOperations.TaskStateSummary;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode_;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance.Priority;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode_;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance_;
import gov.nasa.ziggy.pipeline.definition.PipelineModule;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition_;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTask_;
import gov.nasa.ziggy.pipeline.definition.ProcessingState;
import gov.nasa.ziggy.services.database.DatabaseService;
import gov.nasa.ziggy.services.messages.TaskRequest;
import gov.nasa.ziggy.services.messaging.ZiggyMessenger;

/**
 * Provides CRUD methods for {@link PipelineTask}
 *
 * @author Todd Klaus
 */
public class PipelineTaskCrud extends AbstractCrud<PipelineTask> {
    private static final Logger log = LoggerFactory.getLogger(PipelineTaskCrud.class);

    public PipelineTaskCrud() {
    }

    public PipelineTaskCrud(DatabaseService databaseService) {
        super(databaseService);
    }

    /**
     * Retrieves all pipeline tasks.
     *
     * @return a list of pipeline tasks
     */
    public List<PipelineTask> retrieveAll() {
        return list(createZiggyQuery(PipelineTask.class));
    }

    public PipelineTask retrieve(long id) {
        return uniqueResult(createZiggyQuery(PipelineTask.class).column(PipelineTask_.id).in(id));
    }

    /**
     * Retrieve all {@link PipelineTask}s for the specified {@link PipelineInstance}
     */
    public List<PipelineTask> retrieveTasksForInstance(PipelineInstance instance) {
        return list(createZiggyQuery(PipelineTask.class).column(PipelineTask_.pipelineInstance)
            .in(instance)
            .column(PipelineTask_.id)
            .ascendingOrder());
    }

    /**
     * Retrieve all {@link PipelineTask} instances for a given {@link PipelineInstance}, specified
     * by the instance ID.
     */
    public List<PipelineTask> retrieveTasksForInstance(long instanceId) {
        return retrieveTasksForInstance(new PipelineInstanceCrud().retrieve(instanceId));
    }

    /**
     * Convenience method that generates a query for getting the tasks for a given named module
     *
     * @param moduleName name of the module
     * @return {@link ZiggyQuery} for retrieving all tasks for the requested module name.
     */
    public ZiggyQuery<PipelineTask, PipelineTask> createModuleNameCriteria(String moduleName) {
        ZiggyQuery<PipelineTask, PipelineTask> query = createZiggyQuery(PipelineTask.class);
        query.where(query.in(query.get(PipelineTask_.pipelineInstanceNode)
            .get(PipelineInstanceNode_.pipelineModuleDefinition)
            .get(PipelineModuleDefinition_.name), moduleName));
        return query;
    }

    /**
     * Retrieve all {@link PipelineTask}s for the specified module name.
     *
     * @param moduleName the name of the module for which to find tasks
     * @return a list of tasks for that module
     */
    public List<PipelineTask> retrieveAllForModule(String moduleName) {
        ZiggyQuery<PipelineTask, PipelineTask> query = createModuleNameCriteria(moduleName);
        query.column(PipelineTask_.created).ascendingOrder();
        return list(query);
    }

    /**
     * Retrieve the latest {@link PipelineTask} for the specified module name.
     *
     * @param moduleName the name of the module for which to find tasks
     * @return the latest task, or null if no tasks exist for the module name
     */
    public PipelineTask retrieveLatestForModule(String moduleName) {
        ZiggyQuery<PipelineTask, PipelineTask> query = createModuleNameCriteria(moduleName);
        query.column(PipelineTask_.id).descendingOrder();

        List<PipelineTask> tasks = list(query);
        return tasks.isEmpty() ? null : tasks.get(0);
    }

    /**
     * Return the pipeline tasks for a given instance and given module name
     *
     * @param moduleName name of desired module
     * @param instanceId number of desired instance
     * @return list of pipeline tasks, or null if no tasks with that instance and module name
     */
    public List<PipelineTask> retrieveTasksForModuleAndInstance(String moduleName,
        long instanceId) {
        ZiggyQuery<PipelineTask, PipelineTask> query = createModuleNameCriteria(moduleName);
        query.where(query.in(query.get(PipelineTask_.pipelineInstance).get(PipelineInstance_.id),
            instanceId));
        return list(query);
    }

    /**
     * Retrieve all {@link PipelineTask}s for the specified {@link PipelineInstanceNode}
     */
    public List<PipelineTask> retrieveAll(PipelineInstanceNode pipelineInstanceNode) {
        ZiggyQuery<PipelineTask, PipelineTask> query = createZiggyQuery(PipelineTask.class);
        query.column(PipelineTask_.pipelineInstanceNode).in(pipelineInstanceNode);

        return list(query);
    }

    /**
     * Retrieve all {@link PipelineTask}s that have a specific {@link PipelineDefinitionNode} and
     * have specific task IDs.
     * <p>
     * The pipeline definition node is actually located by searching for nodes that have the same
     * module name as the {@link PipelineDefinitionNode} argument, and also have the same pipeline
     * name. This ensures that if the node has been duplicated, both the original and the copy will
     * count as having processed the task in question.
     */
    public List<Long> retrieveIdsForPipelineDefinitionNode(Collection<Long> taskIds,
        PipelineDefinitionNode pipelineDefinitionNode) {

        String pipelineDefinitionNodeName = pipelineDefinitionNode.getModuleName();
        String pipelineDefinitionName = pipelineDefinitionNode.getPipelineName();
        ZiggyQuery<PipelineTask, Long> query = createZiggyQuery(PipelineTask.class, Long.class);
        query.column(PipelineTask_.id).select();
        query.where(query.in(query.get(PipelineTask_.pipelineInstanceNode)
            .get(PipelineInstanceNode_.pipelineDefinitionNode)
            .get(PipelineDefinitionNode_.moduleName), pipelineDefinitionNodeName));
        query.where(query.in(query.get(PipelineTask_.pipelineInstanceNode)
            .get(PipelineInstanceNode_.pipelineDefinitionNode)
            .get(PipelineDefinitionNode_.pipelineName), pipelineDefinitionName));
        query.column(PipelineTask_.id).chunkedIn(taskIds);
        return list(query);
    }

    /**
     * Retrieve all {@link PipelineTask}s for the specified {@link PipelineInstance} and the
     * specified {@link PipelineTask.State}
     */
    public List<PipelineTask> retrieveAll(PipelineInstance instance, PipelineTask.State state) {
        ZiggyQuery<PipelineTask, PipelineTask> query = createZiggyQuery(PipelineTask.class);
        query.column(PipelineTask_.pipelineInstance).in(instance);
        query.column(PipelineTask_.state).in(state);
        query.column(PipelineTask_.id).ascendingOrder();

        return list(query);
    }

    public List<Long> retrieveIdsForTasksInState(Collection<Long> taskIds,
        PipelineTask.State state) {

        ZiggyQuery<PipelineTask, Long> query = createZiggyQuery(PipelineTask.class, Long.class);
        query.column(PipelineTask_.id).select();
        query.column(PipelineTask_.id).chunkedIn(taskIds);
        query.distinct(true);
        return list(query);
    }

    /**
     * Retrieve all {@link PipelineTask}s for the specified {@link Collection} of pipelineTaskIds.
     *
     * @param pipelineTaskIds {@link Collection} of pipelineTaskIds
     * @return {@link List} of {@link PipelineTask}s
     */
    public List<PipelineTask> retrieveAll(Collection<Long> pipelineTaskIds) {
        if (pipelineTaskIds.isEmpty()) {
            return new ArrayList<>();
        }
        ZiggyQuery<PipelineTask, PipelineTask> query = createZiggyQuery(PipelineTask.class);
        query.column(PipelineTask_.id).chunkedIn(pipelineTaskIds);
        query.column(PipelineTask_.id).ascendingOrder();
        return list(query);
    }

    /**
     * Retrieve the list of distinct softwareRevisions for the specified node. Used for reporting
     */
    public List<String> distinctSoftwareRevisions(PipelineInstanceNode node) {
        ZiggyQuery<PipelineTask, String> query = createZiggyQuery(PipelineTask.class, String.class);
        query.column(PipelineTask_.pipelineInstanceNode).in(node);
        query.column(PipelineTask_.softwareRevision).select();
        query.column(PipelineTask_.softwareRevision).ascendingOrder();
        query.distinct(true);

        return list(query);
    }

    /**
     * Retrieve the list of distinct softwareRevisions for the specified pipeline instance. Used for
     * reporting
     */
    public List<String> distinctSoftwareRevisions(PipelineInstance instance) {
        ZiggyQuery<PipelineTask, String> query = createZiggyQuery(PipelineTask.class, String.class);
        query.column(PipelineTask_.pipelineInstance).in(instance);
        query.column(PipelineTask_.softwareRevision).select();
        query.column(PipelineTask_.softwareRevision).ascendingOrder();
        query.distinct(true);

        return list(query);
    }

    public class ClearStaleStateResults {
        public int totalUpdatedTaskCount = 0;
        public Set<Long> uniqueInstanceIds = new HashSet<>();
    }

    /**
     * Change the state from PROCESSING or SUBMITTED to ERROR for tasks that have stale processing
     * states.
     * <p>
     * This is typically called by the supervisor during startup to clear the stale state of any
     * tasks that were processing when the supervisor exited abnormally (without a chance to set the
     * state to ERROR)
     */
    public ClearStaleStateResults clearStaleState() {
        ClearStaleStateResults results = new ClearStaleStateResults();

        PipelineOperations pipelineOperations = new PipelineOperations();
        ZiggyQuery<PipelineTask, PipelineTask> query = createZiggyQuery(PipelineTask.class);
        query.column(PipelineTask_.state)
            .in(Set.of(PipelineTask.State.PROCESSING, PipelineTask.State.SUBMITTED));
        query.distinct(true);

        List<PipelineTask> staleTasks = list(query);
        log.info("Stale task count: " + staleTasks.size());
        log.info("Stale task IDs: " + staleTasks.toString());

        results.totalUpdatedTaskCount += staleTasks.size();
        for (PipelineTask task : staleTasks) {
            long instanceId = task.getPipelineInstance().getId();
            long instanceNodeId = task.getPipelineInstanceNode().getId();

            log.info("instanceId = " + instanceId);
            log.info("instanceNodeId = " + instanceNodeId);

            // If the task was executing remotely, and it was queued or executing, we can try
            // to resume monitoring on it -- the jobs may have continued to run while the
            // supervisor was down.
            RemoteParameters remoteParams = new ParameterSetCrud().retrieveRemoteParameters(task);
            if (remoteParams != null && remoteParams.isEnabled()) {
                ProcessingState state = new ProcessingSummaryOperations()
                    .processingSummary(task.getId())
                    .getProcessingState();
                if (state == ProcessingState.ALGORITHM_QUEUED
                    || state == ProcessingState.ALGORITHM_EXECUTING) {
                    log.info("Resuming monitoring for task " + task.getId());
                    TaskRequest taskRequest = new TaskRequest(instanceId, instanceNodeId,
                        task.getPipelineDefinitionNode().getId(), task.getId(), Priority.HIGHEST,
                        false, PipelineModule.RunMode.RESUME_MONITORING);
                    ZiggyMessenger.publish(taskRequest);
                    continue;
                }
            }
            results.uniqueInstanceIds.add(instanceId);

            TaskStateSummary updatedStates = pipelineOperations.setTaskState(task,
                PipelineTask.State.ERROR);
            log.info("Setting task " + task.getId() + " to ERROR state");
            new PipelineTaskOperations().updateJobs(updatedStates.getTask());
            flush(); // push out the update
        }

        log.info("totalUpdatedTaskCount = " + results.totalUpdatedTaskCount);

        return results;
    }

    /**
     * Change the state from PROCESSING or SUBMITTED to ERROR for any tasks with the specified
     * instance ID.
     * <p>
     * Typically invoked by the operator to reset the state to ERROR for tasks that did not complete
     * normally (left in the PROCESSING or SUBMITTED states). This allows the operator to restart
     * these tasks.
     */
    public void resetTaskStates(long pipelineInstanceId, boolean allStalledTasks) {

        ZiggyQuery<PipelineTask, PipelineTask> query = createZiggyQuery(PipelineTask.class);
        query.where(query.in(query.get(PipelineTask_.pipelineInstance).get(PipelineInstance_.id),
            Set.of(pipelineInstanceId)));
        if (allStalledTasks) {
            query.column(PipelineTask_.state)
                .in(Set.of(PipelineTask.State.SUBMITTED, PipelineTask.State.PROCESSING));
        } else {
            query.column(PipelineTask_.state).in(Set.of(PipelineTask.State.SUBMITTED));
        }
        List<PipelineTask> staleTasks = list(query);

        // Organize into a Map by pipeline instance node
        Map<PipelineInstanceNode, Set<PipelineTask>> tasksByInstanceNode = new HashMap<>();
        for (PipelineTask task : staleTasks) {
            PipelineInstanceNode node = task.getPipelineInstanceNode();
            Set<PipelineTask> tasks = tasksByInstanceNode.get(node);
            if (tasks == null) {
                tasks = new HashSet<>();
                tasksByInstanceNode.put(node, tasks);
            }
            tasks.add(task);
        }

        PipelineOperations pipelineOperations = new PipelineOperations();
        for (Map.Entry<PipelineInstanceNode, Set<PipelineTask>> entry : tasksByInstanceNode
            .entrySet()) {
            long instanceNodeId = entry.getKey().getId();
            int staleCount = entry.getValue().size();

            log.info("instanceNodeId = " + instanceNodeId);
            log.info("staleCount = " + staleCount);

            for (PipelineTask task : entry.getValue()) {
                pipelineOperations.setTaskState(task, PipelineTask.State.ERROR);
            }

            flush(); // push out the update

            log.info("found " + staleCount + " rows for instanceNode = " + instanceNodeId
                + " with stale state, these rows were reset to ERROR");
        }
        flush();
    }

    /**
     * Gets the number of {@link PipelineTask}s associated with the given {@link PipelineInstance}.
     */
    public long taskCount(PipelineInstance pipelineInstance) {
        checkNotNull(pipelineInstance, "pipelineInstance");

        ZiggyQuery<PipelineTask, Long> query = createZiggyQuery(PipelineTask.class, Long.class);
        query.column(PipelineTask_.pipelineInstance).in(pipelineInstance);
        query.count();

        return uniqueResult(query);
    }

    public List<PipelineTask.State> retrieveStates(PipelineInstance pipelineInstance) {
        ZiggyQuery<PipelineTask, PipelineTask.State> query = taskStateQuery();
        query.column(PipelineTask_.pipelineInstance).in(pipelineInstance);
        return list(query);
    }

    /** Helper function to generate a query that returns states for tasks. */
    private ZiggyQuery<PipelineTask, PipelineTask.State> taskStateQuery() {
        ZiggyQuery<PipelineTask, PipelineTask.State> query = createZiggyQuery(PipelineTask.class,
            PipelineTask.State.class);
        query.column(PipelineTask_.state).select();
        return query;
    }

    public List<PipelineTask.State> retrieveStates(PipelineInstanceNode pipelineInstanceNode) {
        ZiggyQuery<PipelineTask, PipelineTask.State> query = taskStateQuery();
        query.column(PipelineTask_.pipelineInstanceNode).in(pipelineInstanceNode);
        return list(query);
    }

    /**
     * Gets the pipeline instance node IDs associated with a specified pipeline instance.
     *
     * @param pipelineInstanceId desired instance
     * @return List of pipeline node IDs, can be null.
     */
    public List<Long> retrievePipelineInstanceNodeIds(long pipelineInstanceId) {
        ZiggyQuery<PipelineTask, Long> query = createZiggyQuery(PipelineTask.class, Long.class);
        query.where(query.in(query.get(PipelineTask_.pipelineInstance).get(PipelineInstance_.id),
            pipelineInstanceId));
        query.select(query.get(PipelineTask_.pipelineInstanceNode).get(PipelineInstanceNode_.id));
        query.distinct(true);
        return list(query);
    }

    /**
     * Gets the number of tasks for a given pipeline instance node ID in a given state
     *
     * @param nodeId ID of the pipeline instance node
     * @param state state of interest (PROCESSING, COMPLETED, etc.)
     * @return count of the number of tasks in the given state for the given node ID
     */
    public int retrieveStateCount(long nodeId, PipelineTask.State state) {

        // Note: the count() method causes the query to return a long, so it has to be cast to
        // an int before returning.
        ZiggyQuery<PipelineTask, Long> query = createZiggyQuery(PipelineTask.class, Long.class);
        query.column(PipelineTask_.state).in(state);
        query.where(query.in(
            query.get(PipelineTask_.pipelineInstanceNode).get(PipelineInstanceNode_.id), nodeId));
        query.count();
        return uniqueResult(query).intValue();
    }

    @Override
    public Class<PipelineTask> componentClass() {
        return PipelineTask.class;
    }
}

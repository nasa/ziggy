package gov.nasa.ziggy.pipeline.definition.database;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.data.datastore.DataFileType;
import gov.nasa.ziggy.pipeline.definition.ModelType;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode_;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode_;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance_;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTaskMetrics;
import gov.nasa.ziggy.pipeline.definition.PipelineTask_;
import gov.nasa.ziggy.pipeline.definition.ProcessingStep;
import gov.nasa.ziggy.pipeline.definition.RemoteJob;
import gov.nasa.ziggy.pipeline.definition.TaskExecutionLog;
import gov.nasa.ziggy.services.database.DatabaseService;
import jakarta.persistence.criteria.Join;
import jakarta.persistence.metamodel.SetAttribute;

/**
 * Provides CRUD methods for {@link PipelineTask}
 *
 * @author Todd Klaus
 */
public class PipelineTaskCrud extends AbstractCrud<PipelineTask> {

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

    public Set<RemoteJob> retrieveRemoteJobs(long id) {
        ZiggyQuery<PipelineTask, RemoteJob> query = createZiggyQuery(PipelineTask.class,
            RemoteJob.class);
        query.column(PipelineTask_.id).in(id);
        return new HashSet<>(list(query.column(PipelineTask_.remoteJobs).select()));
    }

    public List<TaskExecutionLog> retrieveExecLogs(long id) {
        ZiggyQuery<PipelineTask, TaskExecutionLog> query = createZiggyQuery(PipelineTask.class,
            TaskExecutionLog.class);
        query.column(PipelineTask_.id).in(id);
        return list(query.column(PipelineTask_.execLog).select());
    }

    public List<PipelineTaskMetrics> retrievePipelineTaskMetrics(long id) {
        ZiggyQuery<PipelineTask, PipelineTaskMetrics> query = createZiggyQuery(PipelineTask.class,
            PipelineTaskMetrics.class);
        query.column(PipelineTask_.id).in(id);
        return list(query.column(PipelineTask_.summaryMetrics).select());
    }

    /**
     * Retrieve all {@link PipelineTask}s for the specified {@link PipelineInstance}
     */
    public List<PipelineTask> retrieveTasksForInstance(PipelineInstance instance) {
        return list(createZiggyQuery(PipelineTask.class).column(PipelineTask_.pipelineInstanceId)
            .in(instance.getId())
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

    public List<PipelineTask> retrieveTasksForInstanceNode(
        PipelineInstanceNode pipelineInstanceNode) {
        return list(createZiggyQuery(PipelineTask.class).column(PipelineTask_.id)
            .in(taskIdsForPipelineInstanceNode(pipelineInstanceNode))
            .column(PipelineTask_.id)
            .ascendingOrder());
    }

    /**
     * Convenience method that generates a query for getting the tasks for a given named module
     *
     * @param moduleName name of the module
     * @return {@link ZiggyQuery} for retrieving all tasks for the requested module name.
     */
    public ZiggyQuery<PipelineTask, PipelineTask> createModuleNameCriteria(String moduleName) {
        ZiggyQuery<PipelineTask, PipelineTask> query = createZiggyQuery(PipelineTask.class);
        query.column(PipelineTask_.moduleName).in(moduleName);
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
        query.column(PipelineTask_.pipelineInstanceId).in(instanceId);
        return list(query);
    }

    /**
     * Retrieve all {@link PipelineTask}s that have a specific {@link PipelineDefinitionNode}.
     */
    public List<Long> retrieveIdsForPipelineDefinitionNode(
        PipelineDefinitionNode pipelineDefinitionNode) {

        ZiggyQuery<PipelineInstanceNode, Long> query = createZiggyQuery(PipelineInstanceNode.class,
            Long.class);
        query.column(PipelineInstanceNode_.pipelineDefinitionNode).in(pipelineDefinitionNode);
        Join<PipelineInstanceNode, PipelineTask> taskJoin = query
            .column(PipelineInstanceNode_.pipelineTasks)
            .join();
        query.select(taskJoin.get(PipelineTask_.id));
        return list(query);
    }

    /**
     * Retrieves all {@link PipelineTask}s for the specified {@link PipelineInstance} and the
     * specified {@link ProcessingStep}s.
     */
    public List<PipelineTask> retrieveAll(PipelineInstance pipelineInstance,
        Set<ProcessingStep> processingSteps) {

        ZiggyQuery<PipelineTask, PipelineTask> query = createZiggyQuery(PipelineTask.class);
        query.column(PipelineTask_.pipelineInstanceId).in(pipelineInstance.getId());
        query.column(PipelineTask_.processingStep).in(processingSteps);
        query.column(PipelineTask_.id).ascendingOrder();

        return list(query);
    }

    /**
     * Retrieve all {@link PipelineTask}s for the specified {@link PipelineInstance} with errors.
     */
    public List<PipelineTask> retrieveErroredTasks(PipelineInstance instance) {
        ZiggyQuery<PipelineTask, PipelineTask> query = createZiggyQuery(PipelineTask.class);
        query.column(PipelineTask_.pipelineInstanceId).in(instance.getId());
        query.column(PipelineTask_.error).in(true);
        query.column(PipelineTask_.id).ascendingOrder();

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

    private List<Long> taskIdsForPipelineInstanceNode(PipelineInstanceNode node) {
        return taskIdsForPipelineInstanceNode(node.getId());
    }

    private List<Long> taskIdsForPipelineInstanceNode(long nodeId) {
        ZiggyQuery<PipelineInstanceNode, Long> taskIdsQuery = createZiggyQuery(
            PipelineInstanceNode.class, Long.class);
        taskIdsQuery.column(PipelineInstanceNode_.id).in(nodeId);
        Join<PipelineInstanceNode, PipelineTask> joinTable = taskIdsQuery
            .column(PipelineInstanceNode_.pipelineTasks)
            .join();
        taskIdsQuery.select(joinTable.get(PipelineTask_.id));
        return list(taskIdsQuery);
    }

    /**
     * Retrieve the list of distinct softwareRevisions for the specified node. Used for reporting
     */
    public List<String> distinctSoftwareRevisions(PipelineInstanceNode node) {
        ZiggyQuery<PipelineTask, String> query = createZiggyQuery(PipelineTask.class, String.class);
        query.column(PipelineTask_.id).in(taskIdsForPipelineInstanceNode(node));
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
        query.column(PipelineTask_.pipelineInstanceId).in(instance.getId());
        query.column(PipelineTask_.softwareRevision).select();
        query.column(PipelineTask_.softwareRevision).ascendingOrder();
        query.distinct(true);

        return list(query);
    }

    /**
     * Locates tasks that have "stale" states, i.e., tasks that were in process when the cluster was
     * shut down.
     */
    public List<PipelineTask> retrieveTasksWithStaleStates() {
        ZiggyQuery<PipelineTask, PipelineTask> query = createZiggyQuery(PipelineTask.class);
        query.column(PipelineTask_.processingStep).in(ProcessingStep.processingSteps());
        query.column(PipelineTask_.error).in(false);
        query.distinct(true);
        return list(query);
    }

    @Override
    public Class<PipelineTask> componentClass() {
        return PipelineTask.class;
    }

    // TODO figure out how to implement this as a subquery. The obstacle is that I can't
    // so far use a subquery as the argument to the ZiggyQuery contains method.
    public PipelineInstanceNode retrievePipelineInstanceNode(PipelineTask pipelineTask) {
        ZiggyQuery<PipelineInstanceNode, PipelineInstanceNode> query = createZiggyQuery(
            PipelineInstanceNode.class);
        query.column(PipelineInstanceNode_.pipelineTasks).contains(pipelineTask);
        return uniqueResult(query);
    }

    public long retrievePipelineInstanceNodeId(PipelineTask pipelineTask) {
        ZiggyQuery<PipelineInstanceNode, Long> query = createZiggyQuery(PipelineInstanceNode.class,
            Long.class);
        query.column(PipelineInstanceNode_.pipelineTasks).contains(pipelineTask);
        query.column(PipelineInstanceNode_.id).select();
        return uniqueResult(query);
    }

    public PipelineDefinitionNode retrievePipelineDefinitionNode(PipelineTask pipelineTask) {
        return retrievePipelineInstanceNode(pipelineTask).getPipelineDefinitionNode();
    }

    public Set<DataFileType> retrieveInputDataFileTypes(PipelineTask pipelineTask) {
        return definitionNodeSetQuery(PipelineDefinitionNode_.inputDataFileTypes,
            DataFileType.class, pipelineTask);
    }

    /**
     * Query for a {@link Set} field in the {@link PipelineDefinitionNode} instance associated with
     * a specific {@link PipelineTask}.
     */
    private <T> Set<T> definitionNodeSetQuery(SetAttribute<PipelineDefinitionNode, T> setAttribute,
        Class<T> clazz, PipelineTask pipelineTask) {

        // The main query is to retrieve the data file types from the
        // PipelineDefinitionNode.
        ZiggyQuery<PipelineDefinitionNode, T> query = createZiggyQuery(PipelineDefinitionNode.class,
            clazz);
        query.column(setAttribute).select();

        // Use a subquery to find the PipelineDefinitionNode for the given PipelineTask.
        query.column(PipelineDefinitionNode_.id)
            .in(pipelineDefinitionNodeIdQuery(query, pipelineTask));
        return new HashSet<>(list(query));
    }

    /**
     * Creates a subquery that retrieves the ID of the {@link PipelineDefinitionNode} from the
     * {@link PipelineInstanceNode} that includes a specified {@link PipelineTask}.
     */
    private ZiggyQuery<PipelineInstanceNode, Long> pipelineDefinitionNodeIdQuery(
        ZiggyQuery<PipelineDefinitionNode, ?> parentQuery, PipelineTask pipelineTask) {
        // The subquery finds the PipelineDefinitionNode ID that goes with the
        // PipelineTask.
        ZiggyQuery<PipelineInstanceNode, Long> definitionNodeIdQuery = parentQuery
            .ziggySubquery(PipelineInstanceNode.class, Long.class);
        definitionNodeIdQuery.column(PipelineInstanceNode_.pipelineTasks).contains(pipelineTask);
        definitionNodeIdQuery.select(definitionNodeIdQuery.getRoot()
            .get(PipelineInstanceNode_.pipelineDefinitionNode)
            .get(PipelineDefinitionNode_.id));
        return definitionNodeIdQuery;
    }

    public Set<DataFileType> retrieveOutputDataFileTypes(PipelineTask pipelineTask) {
        return definitionNodeSetQuery(PipelineDefinitionNode_.outputDataFileTypes,
            DataFileType.class, pipelineTask);
    }

    public Set<ModelType> retrieveModelTypes(PipelineTask pipelineTask) {
        return definitionNodeSetQuery(PipelineDefinitionNode_.modelTypes, ModelType.class,
            pipelineTask);
    }

    public PipelineInstance retrievePipelineInstance(PipelineTask pipelineTask) {
        ZiggyQuery<PipelineInstance, PipelineInstance> query = createZiggyQuery(
            PipelineInstance.class);
        return uniqueResult(query.column(PipelineInstance_.pipelineInstanceNodes)
            .contains(retrievePipelineInstanceNode(pipelineTask)));
    }

    public PipelineModuleDefinition retrievePipelineModuleDefinition(PipelineTask pipelineTask) {
        return retrievePipelineInstanceNode(pipelineTask).getPipelineModuleDefinition();
    }
}

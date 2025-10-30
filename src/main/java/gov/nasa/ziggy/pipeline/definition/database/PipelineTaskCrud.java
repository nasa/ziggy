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
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode_;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance_;
import gov.nasa.ziggy.pipeline.definition.PipelineNode;
import gov.nasa.ziggy.pipeline.definition.PipelineNode_;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTask_;
import gov.nasa.ziggy.pipeline.step.PipelineStep;
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

    /**
     * Retrieve all {@link PipelineTask}s for the specified {@link PipelineInstance}
     */
    public List<PipelineTask> retrieveTasksForInstance(PipelineInstance instance) {
        return list(createZiggyQuery(PipelineTask.class).column(PipelineTask_.pipelineInstanceId)
            .in(instance.getId())
            .column(PipelineTask_.id)
            .ascendingOrder());
    }

    public List<Long> retrieveTaskIdsForInstance(PipelineInstance instance) {
        return list(createZiggyQuery(PipelineTask.class, Long.class)
            .column(PipelineTask_.pipelineInstanceId)
            .in(instance.getId())
            .column(PipelineTask_.id)
            .select()
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
     * Convenience method that generates a query for getting the tasks for a given named pipeline
     * step.
     *
     * @param pipelineStepName name of the pipeline step
     * @return {@link ZiggyQuery} for retrieving all tasks for the requested pipeline step
     */
    public ZiggyQuery<PipelineTask, PipelineTask> createPipelineStepNameCriteria(
        String pipelineStepName) {
        ZiggyQuery<PipelineTask, PipelineTask> query = createZiggyQuery(PipelineTask.class);
        query.column(PipelineTask_.pipelineStepName).in(pipelineStepName);
        return query;
    }

    /**
     * Retrieve all {@link PipelineTask}s for the specified pipeline step name.
     *
     * @param pipelineStep the name of the pipeline step for which to find tasks
     * @return a list of tasks for that pipeline step
     */
    public List<PipelineTask> retrieveAllForPipelineStep(String pipelineStep) {
        ZiggyQuery<PipelineTask, PipelineTask> query = createPipelineStepNameCriteria(pipelineStep);
        query.column(PipelineTask_.created).ascendingOrder();
        return list(query);
    }

    /**
     * Retrieve the latest {@link PipelineTask} for the specified pipeline step name.
     *
     * @param pipelineStepName the name of the pipeline step for which to find tasks
     * @return the latest task, or null if no tasks exist for the pipeline step name
     */
    public PipelineTask retrieveLatestForPipelineStep(String pipelineStepName) {
        ZiggyQuery<PipelineTask, PipelineTask> query = createPipelineStepNameCriteria(
            pipelineStepName);
        query.column(PipelineTask_.id).descendingOrder();

        List<PipelineTask> tasks = list(query);
        return tasks.isEmpty() ? null : tasks.get(0);
    }

    /**
     * Return the pipeline tasks for a given instance and given pipeline step name
     *
     * @param pipelineStepName name of desired pipeline step
     * @param instanceId number of desired instance
     * @return list of pipeline tasks, or null if no tasks with that instance and pipeline step name
     */
    public List<PipelineTask> retrieveTasksForPipelineStepAndInstance(String pipelineStepName,
        long instanceId) {
        ZiggyQuery<PipelineTask, PipelineTask> query = createPipelineStepNameCriteria(
            pipelineStepName);
        query.column(PipelineTask_.pipelineInstanceId).in(instanceId);
        return list(query);
    }

    /**
     * Retrieve all {@link PipelineTask}s that have a specific {@link PipelineNode}.
     */
    public List<PipelineTask> retrieveTasksForPipelineNode(PipelineNode pipelineNode) {

        ZiggyQuery<PipelineInstanceNode, PipelineTask> query = createZiggyQuery(
            PipelineInstanceNode.class, PipelineTask.class);
        query.column(PipelineInstanceNode_.pipelineNode).in(pipelineNode);
        query.column(PipelineInstanceNode_.pipelineTasks).select();
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
        return chunkedQuery(new ArrayList<>(pipelineTaskIds),
            chunk -> list(createZiggyQuery(PipelineTask.class).column(PipelineTask_.id)
                .ascendingOrder()
                .in(chunk)));
    }

    List<Long> taskIdsForPipelineInstanceNode(PipelineInstanceNode node) {
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

    public PipelineNode retrievePipelineNode(PipelineTask pipelineTask) {
        return retrievePipelineInstanceNode(pipelineTask).getPipelineNode();
    }

    public Set<DataFileType> retrieveInputDataFileTypes(PipelineTask pipelineTask) {
        return pipelineNodeSetQuery(PipelineNode_.inputDataFileTypes, DataFileType.class,
            pipelineTask);
    }

    /**
     * Query for a {@link Set} field in the {@link PipelineNode} instance associated with a specific
     * {@link PipelineTask}.
     */
    private <T> Set<T> pipelineNodeSetQuery(SetAttribute<PipelineNode, T> setAttribute,
        Class<T> clazz, PipelineTask pipelineTask) {

        // The main query is to retrieve the data file types from the
        // PipelineNode.
        ZiggyQuery<PipelineNode, T> query = createZiggyQuery(PipelineNode.class, clazz);
        query.column(setAttribute).select();

        // Use a subquery to find the PipelineNode for the given PipelineTask.
        query.column(PipelineNode_.id).in(pipelineNodeIdQuery(query, pipelineTask));
        return new HashSet<>(list(query));
    }

    /**
     * Creates a subquery that retrieves the ID of the {@link PipelineNode} from the
     * {@link PipelineInstanceNode} that includes a specified {@link PipelineTask}.
     */
    private ZiggyQuery<PipelineInstanceNode, Long> pipelineNodeIdQuery(
        ZiggyQuery<PipelineNode, ?> parentQuery, PipelineTask pipelineTask) {
        // The subquery finds the PipelineNode ID that goes with the
        // PipelineTask.
        ZiggyQuery<PipelineInstanceNode, Long> pipelineNodeIdQuery = parentQuery
            .ziggySubquery(PipelineInstanceNode.class, Long.class);
        pipelineNodeIdQuery.column(PipelineInstanceNode_.pipelineTasks).contains(pipelineTask);
        pipelineNodeIdQuery.select(pipelineNodeIdQuery.getRoot()
            .get(PipelineInstanceNode_.pipelineNode)
            .get(PipelineNode_.id));
        return pipelineNodeIdQuery;
    }

    public Set<DataFileType> retrieveOutputDataFileTypes(PipelineTask pipelineTask) {
        return pipelineNodeSetQuery(PipelineNode_.outputDataFileTypes, DataFileType.class,
            pipelineTask);
    }

    public Set<ModelType> retrieveModelTypes(PipelineTask pipelineTask) {
        return pipelineNodeSetQuery(PipelineNode_.modelTypes, ModelType.class, pipelineTask);
    }

    public PipelineInstance retrievePipelineInstance(long pipelineTaskId) {
        return retrievePipelineInstance(retrieve(pipelineTaskId));
    }

    public PipelineInstance retrievePipelineInstance(PipelineTask pipelineTask) {
        ZiggyQuery<PipelineInstance, PipelineInstance> query = createZiggyQuery(
            PipelineInstance.class);
        return uniqueResult(query.column(PipelineInstance_.pipelineInstanceNodes)
            .contains(retrievePipelineInstanceNode(pipelineTask)));
    }

    public PipelineStep retrievePipelineStep(PipelineTask pipelineTask) {
        return retrievePipelineInstanceNode(pipelineTask).getPipelineStep();
    }
}

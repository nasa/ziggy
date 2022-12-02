package gov.nasa.ziggy.pipeline.definition.crud;

import static java.lang.Math.toIntExact;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hibernate.Criteria;
import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.Query;
import org.hibernate.criterion.CriteriaSpecification;
import org.hibernate.criterion.DetachedCriteria;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.module.remote.RemoteParameters;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineModule;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTask.State;
import gov.nasa.ziggy.pipeline.definition.ProcessingState;
import gov.nasa.ziggy.services.database.DatabaseService;
import gov.nasa.ziggy.services.messages.WorkerTaskRequest;
import gov.nasa.ziggy.worker.WorkerPipelineProcess;

/**
 * Provides CRUD methods for {@link PipelineTask}
 *
 * @author Todd Klaus
 */
public class PipelineTaskCrud extends AbstractCrud {
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
        Criteria query = createCriteria(PipelineTask.class);
        return list(query);
    }

    public PipelineTask retrieve(long id) {
        Query query = createQuery("from PipelineTask where id = :id");
        query.setLong("id", id);
        PipelineTask task = uniqueResult(query);
        return task;
    }

    /**
     * Retrieve all {@link PipelineTask}s for the specified {@link PipelineInstance}
     *
     * @param instance
     * @return
     */
    public List<PipelineTask> retrieveTasksForInstance(PipelineInstance instance) {
        Query q = createQuery("from PipelineTask pt"
            + " where pt.pipelineInstance = :pipelineInstance order by id asc");
        q.setEntity("pipelineInstance", instance);
        q.setLockMode("pt", LockMode.READ); // bypass caches

        List<PipelineTask> result = list(q);

        return result;
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
     * @return Criteria query for retrieving all tasks for the requested module name.
     */
    public Criteria createModuleNameCriteria(String moduleName) {
        Criteria query = createCriteria(PipelineTask.class);
        query.createCriteria("pipelineInstanceNode")
            .createCriteria("pipelineModuleDefinition")
            .createCriteria("name")
            .add(Restrictions.eq("name", moduleName));
        return query;

    }

    /**
     * Retrieve all {@link PipelineTask}s for the specified module name.
     *
     * @param moduleName the name of the module for which to find tasks
     * @return a list of tasks for that module
     */
    public List<PipelineTask> retrieveAllForModule(String moduleName) {
        Criteria query = createModuleNameCriteria(moduleName);
        query.addOrder(Order.asc("created"));

        return list(query);
    }

    /**
     * Retrieve the latest {@link PipelineTask} for the specified module name.
     *
     * @param moduleName the name of the module for which to find tasks
     * @return the latest task, or null if no tasks exist for the module name
     */
    public PipelineTask retrieveLatestForModule(String moduleName) {
        Criteria query = createModuleNameCriteria(moduleName);
        query.addOrder(Order.desc("id"));

        List<PipelineTask> tasks = list(query);
        return tasks.isEmpty() ? null : tasks.get(0);
    }

    /**
     * Return the pipeline tasks for a given instance and given module name
     *
     * @param moduleName name of desired module
     * @param instanceId number of desired instance
     * @return list of pipeline tasks, or null if no tasks with that instance and module name.
     */
    public List<PipelineTask> retrieveTasksForModuleAndInstance(String moduleName,
        long instanceId) {
        Criteria query = createModuleNameCriteria(moduleName);
        query.createCriteria("pipelineInstance").add(Restrictions.eq("id", instanceId));
        return list(query);
    }

    /**
     * Retrieve all {@link PipelineTask}s for the specified {@link PipelineInstanceNode}
     *
     * @param pipelineInstanceNode
     * @return
     */
    public List<PipelineTask> retrieveAll(PipelineInstanceNode pipelineInstanceNode) {
        Query q = createQuery("from PipelineTask pt"
            + " where pt.pipelineInstanceNode = :pipelineInstanceNode order by id asc");
        q.setEntity("pipelineInstanceNode", pipelineInstanceNode);
        q.setLockMode("pt", LockMode.READ); // bypass caches

        List<PipelineTask> result = list(q);

        return result;
    }

    /**
     * Retrieve all {@link PipelineTask}s that have a specific {@link PipelineDefinitionNode} and
     * have specific task IDs.
     */
    public List<Long> retrieveIdsForPipelineDefinitionNode(Collection<Long> taskIds,
        PipelineDefinitionNode pipelineDefinitionNode) {

        Query q = createQuery("select id from PipelineTask pt where pt.id in :id "
            + "and pt.pipelineInstanceNode.pipelineDefinitionNode = :pipelineDefinitionNode");
        q.setEntity("pipelineDefinitionNode", pipelineDefinitionNode);
        List<Long> pipelineTaskIds = aggregateResults(taskIds,
            chunk -> q.setParameterList("id", chunk));
        return pipelineTaskIds;

    }

    /**
     * Retrieve all {@link PipelineTask}s for the specified {@link PipelineInstance} and the
     * specified {@link PipelineTask.State}
     *
     * @param instance
     * @return
     */
    public List<PipelineTask> retrieveAll(PipelineInstance instance, PipelineTask.State state) {
        Query q = createQuery(
            "from PipelineTask pt where " + "pt.pipelineInstance = :pipelineInstance "
                + "and pt.state = :state " + "order by id asc");

        q.setEntity("pipelineInstance", instance);
        q.setParameter("state", state);
        q.setLockMode("pt", LockMode.READ); // bypass caches

        List<PipelineTask> result = list(q);

        return result;
    }

    public List<Long> retrieveIdsForTasksInState(Collection<Long> taskIds,
        PipelineTask.State state) {

        Criteria criteria = createCriteria(PipelineTask.class);
        criteria.add(Restrictions.in("id", taskIds));
        criteria.add(Restrictions.eq("state", state));
        criteria.setProjection(Projections.distinct(Projections.property("id")));
        return list(criteria);
    }

    /**
     * Retrieve all {@link PipelineTask}s for the specified {@link Collection} of pipelineTaskIds.
     *
     * @param pipelineTaskIds {@link Collection} of pipelineTaskIds.
     * @return {@link List} of {@link PipelineTask}s.
     */
    public List<PipelineTask> retrieveAll(Collection<Long> pipelineTaskIds) {
        List<PipelineTask> pipelineTasks = new ArrayList<>();
        if (!pipelineTaskIds.isEmpty()) {
            Query query = createQuery(
                "from PipelineTask where id in (:pipelineTaskIds) " + "order by id asc");
            query.setParameterList("pipelineTaskIds", pipelineTaskIds);

            pipelineTasks = list(query);
        }

        return pipelineTasks;
    }

    /**
     * Retrieve the list of distinct softwareRevisions for the specified node. Used for reporting
     *
     * @param node
     * @return
     */
    public List<String> distinctSoftwareRevisions(PipelineInstanceNode node) {
        Query q = createQuery("select distinct softwareRevision"
            + " from PipelineTask pt where pt.pipelineInstanceNode"
            + " = :pipelineInstanceNode order by softwareRevision asc");

        q.setEntity("pipelineInstanceNode", node);

        List<String> result = list(q);

        return result;
    }

    /**
     * Retrieve the list of distinct softwareRevisions for the specified pipeline instance. Used for
     * reporting
     *
     * @param instance
     * @return
     */
    public List<String> distinctSoftwareRevisions(PipelineInstance instance) {
        Query q = createQuery("select distinct softwareRevision from PipelineTask pt "
            + "where pt.pipelineInstance = :pipelineInstance order by softwareRevision asc");

        q.setEntity("pipelineInstance", instance);

        List<String> result = list(q);

        return result;
    }

    public class ClearStaleStateResults {
        public int totalUpdatedTaskCount = 0;
        public Set<Long> uniqueInstanceIds = new HashSet<>();
    }

    /**
     * Change the state from PROCESSING or SUBMITTED to ERROR for any tasks with the specified
     * workerHost.
     * <p>
     * This is typically called by a worker during startup to clear the stale state of any tasks
     * that were processing when the worker exited abnormally (without a chance to set the state to
     * ERROR)
     *
     * @param workerHost
     * @return
     */
    public ClearStaleStateResults clearStaleState(String workerHost) {
        ClearStaleStateResults results = new ClearStaleStateResults();

        PipelineInstanceNodeCrud pipelineInstanceNodeCrud = new PipelineInstanceNodeCrud();

        Criteria criteria = createCriteria(PipelineTask.class);
        criteria.add(Restrictions.eq("workerHost", workerHost));
        criteria.add(Restrictions.or(Restrictions.eq("state", PipelineTask.State.PROCESSING),
            Restrictions.eq("state", PipelineTask.State.SUBMITTED)));
        criteria.setResultTransformer(CriteriaSpecification.DISTINCT_ROOT_ENTITY);

        List<PipelineTask> staleTasks = list(criteria);
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
            // worker was down.
            RemoteParameters remoteParams = new ParameterSetCrud().retrieveRemoteParameters(task);
            if (remoteParams != null && remoteParams.isEnabled()) {
                ProcessingState state = new ProcessingSummaryOperations()
                    .processingSummary(task.getId())
                    .getProcessingState();
                if (state == ProcessingState.ALGORITHM_QUEUED
                    || state == ProcessingState.ALGORITHM_EXECUTING) {
                    log.info("Resuming monitoring for task " + task.getId());
                    WorkerTaskRequest taskRequest = new WorkerTaskRequest(instanceId,
                        instanceNodeId, task.getId(), 0, false,
                        PipelineModule.RunMode.RESUME_MONITORING);
                    WorkerPipelineProcess.workerTaskRequestQueue.add(taskRequest);
                    continue;
                }
            }
            results.uniqueInstanceIds.add(instanceId);

            pipelineInstanceNodeCrud.updateFailedTaskCount(instanceNodeId, 1);
            task.setState(PipelineTask.State.ERROR);
            new PipelineTaskOperations().updateJobs(task);
            task.stopExecutionClock();
            log.info("Setting task " + task.getId() + " to ERROR state");

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
    public void resetTaskStates(long pipelineInstanceId, boolean allStalledTasks, String taskIds) {
        Query select = null;

        if (allStalledTasks) {
            select = createSQLQuery(
                "select PI_PIPELINE_INST_NODE_ID," + " count(*) from PI_PIPELINE_TASK"
                    + " where STATE = :submittedState or STATE = :processingState"
                    + " and PI_PIPELINE_INSTANCE_ID = :instanceId"
                    + " group by PI_PIPELINE_INST_NODE_ID");
            select.setParameter("submittedState", PipelineTask.State.SUBMITTED.toString());
            select.setParameter("processingState", PipelineTask.State.PROCESSING.toString());
            select.setParameter("instanceId", pipelineInstanceId);
        } else {
            select = createSQLQuery("select PI_PIPELINE_INST_NODE_ID,"
                + " count(*) from PI_PIPELINE_TASK" + " where STATE = :submittedState"
                + " and PI_PIPELINE_INSTANCE_ID = :instanceId"
                + " group by PI_PIPELINE_INST_NODE_ID");
            select.setParameter("submittedState", PipelineTask.State.SUBMITTED.toString());
            select.setParameter("instanceId", pipelineInstanceId);
        }

        PipelineInstanceNodeCrud pipelineInstanceNodeCrud = new PipelineInstanceNodeCrud();

        log.info("Select query: " + select);

        List<Object[]> staleTasks = list(select);

        for (Object[] row : staleTasks) {
            Number instanceNodeId = (Number) row[0];
            Number staleCount = (Number) row[1];

            log.info("instanceNodeId = " + instanceNodeId);
            log.info("staleCount = " + staleCount);

            pipelineInstanceNodeCrud.updateFailedTaskCount(instanceNodeId.longValue(),
                staleCount.intValue());

            int updatedTaskCount = performTaskReset(instanceNodeId.longValue(), allStalledTasks,
                taskIds);

            flush(); // push out the update

            if (updatedTaskCount == 0) {
                log.info("found NO rows for instanceNode = " + instanceNodeId
                    + " for this worker with stale state");
            } else {
                log.info("found " + updatedTaskCount + " rows for instanceNode = " + instanceNodeId
                    + " for this worker with stale state, these rows were reset to ERROR");
            }
        }
        flush();
    }

    private int performTaskReset(long instanceNodeId, boolean allStalledTasks, String taskIds) {
        String stateConstraint = "";
        String taskIdsConstraint = "";

        State error = PipelineTask.State.ERROR;
        State processing = PipelineTask.State.PROCESSING;
        State submitted = PipelineTask.State.SUBMITTED;

        if (allStalledTasks) {
            stateConstraint = "and state = :submitted or state = :processing ";
        } else {
            stateConstraint = "and state = :submitted ";
        }

        if (taskIds != null) {
            taskIdsConstraint = "and id in (" + taskIds + ") ";
        }

        Query q = createQuery("from PipelineTask where " + "pipelineInstanceNode = :instanceNode "
            + stateConstraint + taskIdsConstraint);

        q.setEntity("instanceNode", get(PipelineInstanceNode.class, instanceNodeId));
        q.setParameter("submitted", submitted);
        if (allStalledTasks) {
            q.setParameter("processing", processing);
        }
        List<PipelineTask> tasks = list(q);
        for (PipelineTask task : tasks) {
            task.setState(error);
            task.stopExecutionClock();
        }

        return tasks.size();
    }

    /**
     * Gets the number of {@link PipelineTask}s associated with the given {@link PipelineInstance}.
     *
     * @param pipelineInstance the non-{@code null} {@link PipelineInstance}.
     * @return the number of {@link PipelineTask}s.
     * @throws HibernateException if there were problems retrieving the count of
     * {@link PipelineTask} objects.
     * @throws NullPointerException if {@code pipelineInstance} is {@code null}.
     */
    public int taskCount(PipelineInstance pipelineInstance) {
        if (pipelineInstance == null) {
            throw new NullPointerException("pipelineInstance can't be null");
        }

        Query query = createQuery("select count(id) from PipelineTask t "
            + "where t.pipelineInstance = :pipelineInstance");
        query.setParameter("pipelineInstance", pipelineInstance);
        int count = ((Long) query.iterate().next()).intValue();

        return count;
    }

    /**
     * Gets a map containing the number of {@link PipelineTask}s associated with the given
     * {@link PipelineInstance} for each state {@link State}. All known states are guaranteed to be
     * found in the map, even if the count is 0.
     *
     * @param pipelineInstance the non-{@code null} {@link PipelineInstance}.
     * @return the number of {@link PipelineTask}s.
     * @throws HibernateException if there were problems retrieving the count of
     * {@link PipelineTask} objects.
     * @throws NullPointerException if {@code pipelineInstance} is {@code null}.
     */
    public Map<State, Integer> taskCountByState(PipelineInstance pipelineInstance) {
        if (pipelineInstance == null) {
            throw new NullPointerException("pipelineInstance can't be null");
        }

        Query query = createQuery("select state, count(*) from PipelineTask t "
            + "where t.pipelineInstance = :pipelineInstance " + "group by state");
        query.setParameter("pipelineInstance", pipelineInstance);

        List<Object[]> list = list(query);
        Map<State, Integer> taskCounts = new HashMap<>();
        for (Object[] row : list) {
            taskCounts.put((State) row[0], ((Long) row[1]).intValue());
        }

        // Ensure that all states are covered.
        for (State state : State.values()) {
            if (taskCounts.get(state) == null) {
                taskCounts.put(state, 0);
            }
        }

        return taskCounts;
    }

    /**
     * Gets the pipeline instance node IDs associated with a specified pipeline instance.
     *
     * @param pipelineInstanceId desired instance
     * @return List of pipeline node IDs, can be null.
     */
    public List<Long> retrievePipelineInstanceNodeIds(long pipelineInstanceId) {
        Criteria criteria = createCriteria(PipelineTask.class);
        criteria.createAlias("pipelineInstanceNode", "instanceNode");
        criteria.createAlias("pipelineInstance", "instance");
        criteria.add(Restrictions.eq("instance.id", pipelineInstanceId));
        criteria.setProjection(Projections.distinct(Projections.property("instanceNode.id")));
        return list(criteria);
    }

    /**
     * Gets the number of tasks for a given pipeline instance node ID in a given state
     *
     * @param nodeId ID of the pipeline instance node
     * @param state state of interest (PROCESSING, COMPLETED, etc.).
     * @return count of the number of tasks in the given state for the given node ID.
     */
    public int retrieveStateCount(long nodeId, PipelineTask.State state) {
        Criteria criteria = createCriteria(PipelineTask.class);
        criteria.createAlias("pipelineInstanceNode", "instanceNode");
        criteria.add(Restrictions.eq("instanceNode.id", nodeId));
        criteria.add(Restrictions.eq("state", state));
        criteria.setProjection(Projections.rowCount());
        return toIntExact(uniqueResult(criteria));
    }

    /**
     * Produces a {@link DetachedCriteria} that finds the IDs of {@link PipelineTask} instances with
     * a given pipeline module name and pipeline instance ID. The detached criteria can be added as
     * a subquery to other {@link Criteria} queries that need to select for task IDs based on
     * instance ID and module name.
     */
    public static DetachedCriteria taskIdsForModuleAndInstance(String moduleName, long instanceId) {
        DetachedCriteria criteria = DetachedCriteria.forClass(PipelineTask.class);
        criteria.createAlias("pipelineInstance", "instance");
        criteria.createAlias("pipelineInstanceNode", "node");
        criteria.createAlias("node.pipelineModuleDefinition", "module");
        criteria.add(Restrictions.eq("instance.id", instanceId));
        criteria.add(Restrictions.eq("module.name.name", moduleName));
        criteria.setProjection(Projections.distinct(Projections.property("id")));

        return criteria;

    }

}

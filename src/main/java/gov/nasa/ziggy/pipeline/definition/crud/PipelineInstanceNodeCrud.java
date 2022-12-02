package gov.nasa.ziggy.pipeline.definition.crud;

import java.util.Collection;
import java.util.List;

import org.hibernate.LockMode;
import org.hibernate.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.pipeline.definition.PipelineDefinitionNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.TaskCounts;
import gov.nasa.ziggy.services.database.DatabaseService;

/**
 * Provides CRUD methods for {@link PipelineInstanceNode}
 *
 * @author Todd Klaus
 */
public class PipelineInstanceNodeCrud extends AbstractCrud {
    private static final Logger log = LoggerFactory.getLogger(PipelineInstanceNodeCrud.class);

    public PipelineInstanceNodeCrud() {
    }

    public PipelineInstanceNodeCrud(DatabaseService databaseService) {
        super(databaseService);
    }

    public List<PipelineInstanceNode> retrieveAll(PipelineInstance pipelineInstance) {
        Query query = createQuery(
            "from PipelineInstanceNode pin where pipelineInstance = :pipelineInstance order by pin.id");
        query.setEntity("pipelineInstance", pipelineInstance);

        List<PipelineInstanceNode> instanceNodes = list(query);
        populateXmlFields(instanceNodes);
        return instanceNodes;
    }

    /**
     * Retrieve the PipelineInstanceNode for the specified id using LockMode.READ (bypass caches)
     *
     * @param id
     * @return
     */
    public PipelineInstanceNode retrieve(long id) {
        Query query = createQuery("from PipelineInstanceNode where id = :id");
        query.setLong("id", id);
        query.setLockMode("pin", LockMode.READ);

        PipelineInstanceNode instanceNode = uniqueResult(query);
        populateXmlFields(instanceNode);
        return instanceNode;
    }

    /**
     * Retrieve the PipelineInstanceNode for the specified PipelineInstance and
     * PipelineDefinitionNode using LockMode.READ (bypass caches)
     *
     * @param pipelineInstance
     * @param pipelineDefinitionNode
     * @return
     */
    public PipelineInstanceNode retrieve(PipelineInstance pipelineInstance,
        PipelineDefinitionNode pipelineDefinitionNode) {
        Query query = createQuery(
            "from PipelineInstanceNode pin where pipelineInstance = :pipelineInstance"
                + " and pipelineDefinitionNode = :pipelineDefinitionNode");

        query.setEntity("pipelineInstance", pipelineInstance);
        query.setEntity("pipelineDefinitionNode", pipelineDefinitionNode);
        query.setLockMode("pin", LockMode.READ);

        PipelineInstanceNode instanceNode = uniqueResult(query);
        populateXmlFields(instanceNode);
        return instanceNode;
    }

    private void populateXmlFields(PipelineInstanceNode node) {
        node.populateXmlFields();
    }

    private void populateXmlFields(Collection<PipelineInstanceNode> nodes) {
        for (PipelineInstanceNode node : nodes) {
            populateXmlFields(node);
        }
    }

    private enum CountType {
        TOTAL("NUM_TASKS"),
        SUBMITTED("NUM_SUBMITTED_TASKS"),
        COMPLETED("NUM_COMPLETED_TASKS"),
        FAILED("NUM_FAILED_TASKS");

        private String columnName;

        CountType(String columnName) {
            this.columnName = columnName;
        }

        public String getColumnName() {
            return columnName;
        }

        public String getColumnList() {
            return TOTAL.columnName + "," + SUBMITTED.columnName + "," + COMPLETED.columnName + ","
                + FAILED.columnName;
        }
    }

    private TaskCounts updateTaskCount(long pipelineInstanceNodeId, CountType countType,
        int taskCountDelta) {
        return updateTaskCount(pipelineInstanceNodeId, countType, taskCountDelta, true);
    }

    /**
     * Common code to update task count columns atomically. Uses 'select for update' semantics so
     * that the count is read and updated atomically.
     *
     * @param pipelineInstanceNodeId
     * @param countType.toString()
     * @param taskCountDelta
     * @param applyDeltas
     */
    private TaskCounts updateTaskCount(long pipelineInstanceNodeId, CountType countType,
        int taskCountDelta, boolean applyDeltas) {
        // make sure dirty objects are flushed to the database
        flush();

        Query selectForUpdateQuery = createSQLQuery("select " + countType.getColumnList()
            + " from PI_PIPELINE_INST_NODE pin where id = :pipelineInstanceNodeId for update");

        selectForUpdateQuery.setLong("pipelineInstanceNodeId", pipelineInstanceNodeId);

        log.debug("query = " + selectForUpdateQuery);

        Object[] results = uniqueResult(selectForUpdateQuery);
        TaskCounts newTaskCounts;
        if (applyDeltas) {

            // 4 columns returned, as defined in the query above
            Number numTasks = (Number) results[0];
            Number numSubmittedTasks = (Number) results[1];
            Number numCompletedTasks = (Number) results[2];
            Number numFailedTasks = (Number) results[3];

            newTaskCounts = new TaskCounts(numTasks.intValue(), numSubmittedTasks.intValue(),
                numCompletedTasks.intValue(), numFailedTasks.intValue());
        } else {
            newTaskCounts = new TaskCounts(0, 0, 0, 0);
        }

        int previousTaskCount;
        int newTaskCount;

        switch (countType) {
            case TOTAL:
                previousTaskCount = (int) newTaskCounts.getTotal();
                newTaskCount = Math.max(0, previousTaskCount + taskCountDelta);
                newTaskCounts.setTotal(newTaskCount);
                break;

            case SUBMITTED:
                previousTaskCount = (int) newTaskCounts.getSubmitted();
                newTaskCount = Math.max(0, previousTaskCount + taskCountDelta);
                newTaskCounts.setSubmitted(newTaskCount);
                break;

            case COMPLETED:
                previousTaskCount = (int) newTaskCounts.getCompleted();
                newTaskCount = Math.max(0, previousTaskCount + taskCountDelta);
                newTaskCounts.setCompleted(newTaskCount);
                break;

            case FAILED:
                previousTaskCount = (int) newTaskCounts.getFailed();
                newTaskCount = Math.max(0, previousTaskCount + taskCountDelta);
                newTaskCounts.setFailed(newTaskCount);
                break;

            default:
                throw new IllegalStateException("unknown CountType: " + countType);
        }

        // this update releases the lock obtained above
        Query updateQuery = createSQLQuery("update PI_PIPELINE_INST_NODE pin " + "set "
            + countType.getColumnName() + " = :newTaskCount where id = :pipelineInstanceNodeId");

        updateQuery.setLong("pipelineInstanceNodeId", pipelineInstanceNodeId);
        updateQuery.setLong("newTaskCount", newTaskCount);

        int rowsUpdated = updateQuery.executeUpdate();

        log.info("Changed PI_PIPELINE_INST_NODE(" + pipelineInstanceNodeId + ")."
            + countType.getColumnName() + " (" + previousTaskCount + "->" + newTaskCount
            + "), rowsUpdated = " + rowsUpdated);

        return newTaskCounts;
    }

    /**
     * Update numTasks for the specified PipelineInstanceNode. Uses 'select for update' semantics so
     * that the count is read and updated atomically.
     * <p>
     * NOTE: Atomicity is not guaranteed on HSQLDB since it does not support 'select for update'
     *
     * @param pipelineInstanceNodeId
     * @param taskCountDelta
     */
    public TaskCounts updateTaskCount(long pipelineInstanceNodeId, int taskCountDelta) {
        return updateTaskCount(pipelineInstanceNodeId, CountType.TOTAL, taskCountDelta);
    }

    /**
     * Increment numSubmittedTasks for the specified PipelineInstanceNode. Uses 'select for update'
     * semantics so that the count is read and updated atomically.
     * <p>
     * NOTE: Atomicity is not guaranteed on HSQLDB since it does not support 'select for update'
     *
     * @param pipelineInstanceNodeId
     */
    public TaskCounts incrementSubmittedTaskCount(long pipelineInstanceNodeId) {
        return updateTaskCount(pipelineInstanceNodeId, CountType.SUBMITTED, 1);
    }

    /**
     * Update numSubmittedTasks for the specified PipelineInstanceNode. Uses 'select for update'
     * semantics so that the count is read and updated atomically.
     * <p>
     * NOTE: Atomicity is not guaranteed on HSQLDB since it does not support 'select for update'
     *
     * @param pipelineInstanceNodeId
     * @param taskCountDelta
     */
    public TaskCounts updateSubmittedTaskCount(long pipelineInstanceNodeId, int taskCountDelta) {
        return updateTaskCount(pipelineInstanceNodeId, CountType.SUBMITTED, taskCountDelta);
    }

    /**
     * Increment numCompletedTasks for the specified PipelineInstanceNode. Uses 'select for update'
     * semantics so that the count is read and updated atomically.
     * <p>
     * NOTE: Atomicity is not guaranteed on HSQLDB since it does not support 'select for update'
     *
     * @param pipelineInstanceNodeId
     */
    public TaskCounts incrementCompletedTaskCount(long pipelineInstanceNodeId) {
        return updateTaskCount(pipelineInstanceNodeId, CountType.COMPLETED, 1);
    }

    /**
     * Update numCompletedTasks for the specified PipelineInstanceNode. Uses 'select for update'
     * semantics so that the count is read and updated atomically.
     * <p>
     * NOTE: Atomicity is not guaranteed on HSQLDB since it does not support 'select for update'
     *
     * @param pipelineInstanceNodeId
     * @param taskCountDelta
     */
    public TaskCounts updateCompletedTaskCount(long pipelineInstanceNodeId, int taskCountDelta) {
        return updateTaskCount(pipelineInstanceNodeId, CountType.COMPLETED, taskCountDelta);
    }

    /**
     * Increment numFailedTasks for the specified PipelineInstanceNode. Uses 'select for update'
     * semantics so that the count is read and updated atomically.
     * <p>
     * NOTE: Atomicity is not guaranteed on HSQLDB since it does not support 'select for update'
     *
     * @param pipelineInstanceNodeId
     */
    public TaskCounts incrementFailedTaskCount(long pipelineInstanceNodeId) {
        return updateTaskCount(pipelineInstanceNodeId, CountType.FAILED, 1);
    }

    /**
     * Deccrement numFailedTasks for the specified PipelineInstanceNode. Uses 'select for update'
     * semantics so that the count is read and updated atomically.
     * <p>
     * NOTE: Atomicity is not guaranteed on HSQLDB since it does not support 'select for update'
     *
     * @param pipelineInstanceNodeId
     */
    public TaskCounts decrementFailedTaskCount(long pipelineInstanceNodeId) {
        return updateTaskCount(pipelineInstanceNodeId, CountType.FAILED, -1);
    }

    /**
     * Increment numFailedTasks for the specified PipelineInstanceNode. Uses 'select for update'
     * semantics so that the count is read and updated atomically.
     * <p>
     * NOTE: Atomicity is not guaranteed on HSQLDB since it does not support 'select for update'
     *
     * @param pipelineInstanceNodeId
     * @param taskCountDelta
     */
    public TaskCounts updateFailedTaskCount(long pipelineInstanceNodeId, int taskCountDelta) {
        return updateTaskCount(pipelineInstanceNodeId, CountType.FAILED, taskCountDelta);
    }

    /**
     * Use the pipeline task states to update the task counts in the instance nodes. This is used to
     * avoid a situation in which the pipeline instance nodes in the database become inconsistent
     * with the task states that those nodes are supposed to summarize
     *
     * @param pipelineInstanceId instance ID to be updated
     */
    public void updateTaskCountsFromTasks(long pipelineInstanceId) {

        PipelineTaskCrud pipelineTaskCrud = new PipelineTaskCrud();

        // get the node IDs

        List<Long> nodeIds = pipelineTaskCrud.retrievePipelineInstanceNodeIds(pipelineInstanceId);

        // loop over instance nodes

        if (nodeIds != null && !nodeIds.isEmpty()) {
            for (long nodeId : nodeIds) {

                // get the state count for each node, bearing in mind that the
                // submitted count is actually the sum of submitted, completed,
                // processing, error, partial

                int completedCount = pipelineTaskCrud.retrieveStateCount(nodeId,
                    PipelineTask.State.COMPLETED);
                int erroredCount = pipelineTaskCrud.retrieveStateCount(nodeId,
                    PipelineTask.State.ERROR);
                int submittedCount = pipelineTaskCrud.retrieveStateCount(nodeId,
                    PipelineTask.State.SUBMITTED);
                submittedCount += erroredCount;
                submittedCount += completedCount;
                submittedCount += pipelineTaskCrud.retrieveStateCount(nodeId,
                    PipelineTask.State.PARTIAL);
                submittedCount += pipelineTaskCrud.retrieveStateCount(nodeId,
                    PipelineTask.State.PROCESSING);

                // apply updates -- note that this is an absolute update rather
                // than a delta, so use false as the 4th argument

                updateTaskCount(nodeId, CountType.SUBMITTED, submittedCount, false);
                updateTaskCount(nodeId, CountType.COMPLETED, completedCount, false);
                updateTaskCount(nodeId, CountType.FAILED, erroredCount, false);
            }
        }

    }

    /**
     * True if PipelineInstanceNode.numCompletedTasks == numTasks
     *
     * @param pipelineInstanceNodeId
     * @return
     */
//    private boolean isInstanceNodeComplete(long pipelineInstanceNodeId) {
//        // Make sure dirty objects are flushed to the database.
//        // Then wash your hands.
//        getSession().flush();
//
//        Query query = getSession().createQuery("select numTasks, numCompletedTasks "
//            + "from PipelineInstanceNode pin where id = :pipelineInstanceNodeId");
//
//        query.setLong("pipelineInstanceNodeId", pipelineInstanceNodeId);
//
//        Object[] results = (Object[]) query.uniqueResult();
//
//        // 2 columns, as defined in the query above
//        Number numTasks = (Number) results[0];
//        Number numCompletedTasks = (Number) results[1];
//
//        log.info("PipelineInstanceNode(" + pipelineInstanceNodeId
//            + ") numTasks/numCompletedTasks = " + numTasks + "/" + numCompletedTasks);
//
//        return (numCompletedTasks.equals(numTasks));
//    }

}

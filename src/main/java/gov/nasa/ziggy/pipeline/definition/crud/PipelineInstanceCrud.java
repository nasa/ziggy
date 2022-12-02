package gov.nasa.ziggy.pipeline.definition.crud;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.Query;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance.State;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceAggregateState;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.services.database.DatabaseService;

/**
 * Provides CRUD methods for {@link PipelineInstance}.
 *
 * @author Todd Klaus
 */
public class PipelineInstanceCrud extends AbstractCrud {
    private static final Logger log = LoggerFactory.getLogger(PipelineInstanceCrud.class);

    public PipelineInstanceCrud() {
    }

    public PipelineInstanceCrud(DatabaseService databaseService) {
        super(databaseService);
    }

    public PipelineInstance retrieve(long id) {
        Query query = createQuery("from PipelineInstance where id = :id");
        query.setLong("id", id);
        PipelineInstance instance = uniqueResult(query);
        populateXmlFields(instance);
        return instance;
    }

    /**
     * Return all instances that match the specified filter.
     *
     * @return
     */
    public List<PipelineInstance> retrieve(PipelineInstanceFilter filter) {
        Query q = filter.query(getSession());

        q.setLockMode("pi", LockMode.READ);

        List<PipelineInstance> result = list(q);
        populateXmlFields(result);
        return result;
    }

    /**
     * Return all pipeline instances started within the specified date range. Sorted by priority
     * (highest to lowest)
     */
    public List<PipelineInstance> retrieve(Date startDate, Date endDate) {
        Query q = createQuery(
            "from PipelineInstance pi " + "where pi.startProcessingTime >= :startDate "
                + "and pi.startProcessingTime <= :endDate " + "order by priority desc");
        q.setParameter("startDate", startDate);
        q.setParameter("endDate", endDate);
        List<PipelineInstance> result = list(q);
        populateXmlFields(result);
        return result;
    }

    /**
     * Retrieves all {@link PipelineInstance}s that began within the specified date range that have
     * the given states and types ordered by ID.
     *
     * @param startDate the starting date.
     * @param endDate the ending date.
     * @param states an array of states
     * @param types an array of types (the name of the instance's pipeline definition).
     * @return a non-{@code null} list of {@link PipelineInstance}s.
     * @throws HibernateException if there were problems accessing the database.
     */
    public List<PipelineInstance> retrieve(Date startDate, Date endDate, State[] states,
        String[] types) {
        // We found that a clean Criteria query would return a huge
        // n-dimensional Cartesian product. The following code trades a huge
        // number of joins with an n+1 select (but n is usually pretty small).

        // First, get all the instances within the date range.
        Query query = createQuery(
            "from PipelineInstance " + "where startProcessingTime >= :startDate "
                + "and startProcessingTime <= :endDate " + "order by id asc");
        query.setParameter("startDate", startDate);
        query.setParameter("endDate", endDate);
        List<PipelineInstance> result = list(query);

        // Now, choose those instances that match the additional criteria.
        List<PipelineInstance> filteredResult = new ArrayList<>();
        for (PipelineInstance pipelineInstance : result) {
            boolean found = false;
            for (State state : states) {
                if (pipelineInstance.getState() == state) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                continue;
            }
            found = false;
            for (String type : types) {
                if (pipelineInstance.getPipelineDefinition().getName().toString().equals(type)) {
                    found = true;
                    break;
                }
            }
            if (found) {
                filteredResult.add(pipelineInstance);
            }
        }
        populateXmlFields(filteredResult);
        return filteredResult;
    }

    /**
     */
    public List<PipelineInstance> retrieveAll() {
        Query q = createQuery("from PipelineInstance pi order by pi.id asc");
        q.setLockMode("pi", LockMode.READ);

        List<PipelineInstance> result = list(q);
        populateXmlFields(result);
        return result;
    }

    /**
     * Return all active pipeline instances, sorted by priority (highest to lowest)
     */
    public List<PipelineInstance> retrieveAllActive() {
        Query q = createQuery("from PipelineInstance pi where pi.state = :processing "
            + "or pi.state = :errorsrunning " + "order by priority desc");
        q.setParameter("processing", PipelineInstance.State.PROCESSING);
        q.setParameter("errorsrunning", PipelineInstance.State.ERRORS_RUNNING);

        List<PipelineInstance> result = list(q);
        populateXmlFields(result);
        return result;
    }

    /**
     * Cancel all actibve pipeline instances. This method should only be called if there are no
     * running instances. This is useful for reducing the number of queues monitored by the workers,
     * thereby reducing the load on the JMS broker
     */
    public void cancelAllActive() {
        List<PipelineInstance> activeInstances = retrieveAllActive();

        for (PipelineInstance instance : activeInstances) {
            instance.setState(PipelineInstance.State.STOPPED);
        }
    }

    /**
     * Retrieve all {@link PipelineInstance}s for the specified {@link Collection} of
     * pipelineInstanceIds.
     *
     * @param pipelineInstanceIds {@link Collection} of pipelineInstanceIds.
     * @return {@link List} of {@link PipelineInstance}s.
     */
    public List<PipelineInstance> retrieveAll(Collection<Long> pipelineInstanceIds) {
        List<PipelineInstance> pipelineInstances = new ArrayList<>();
        if (!pipelineInstanceIds.isEmpty()) {
            Query query = createQuery(
                "from PipelineInstance where id in (:pipelineInstanceIds) " + "order by id asc");
            query.setParameterList("pipelineInstanceIds", pipelineInstanceIds);

            pipelineInstances = list(query);
        }
        populateXmlFields(pipelineInstances);
        return pipelineInstances;
    }

    private void populateXmlFields(PipelineInstance instance) {
        instance.populateXmlFields();
    }

    private void populateXmlFields(Collection<PipelineInstance> instances) {
        for (PipelineInstance instance : instances) {
            populateXmlFields(instance);
        }
    }

    /**
     * Update the name of a pipeline instance (normally by the operator in the console) This is done
     * with SQL update rather than via the Hibernate object because we don't want to perturb the
     * other fields which can be set by the worker processes.
     *
     * @param id
     * @param newName
     */
    public void updateName(long id, String newName) {
        Query updateQuery = createSQLQuery(
            "update PI_PIPELINE_INSTANCE pi " + "set name = :newName where id = :id");

        updateQuery.setString("newName", newName);
        updateQuery.setLong("id", id);

        int rowsUpdated = updateQuery.executeUpdate();

        log.info("Updated instance name, rowsUpdated=" + rowsUpdated);
    }

    /**
     * Indicates whether all {@link PipelineTask}s for this {@link PipelineInstance} are in the
     * PipelineTask.State.COMPLETED state
     *
     * @param instance
     */
    public PipelineInstanceAggregateState instanceState(PipelineInstance instance) {
        // flush changes so that the updateInstanceState query will see them.
        flush();

        Query q = createQuery(
            "select new gov.nasa.ziggy.pipeline.definition.PipelineInstanceAggregateState(sum(instanceNode.numTasks), sum(instanceNode.numSubmittedTasks), sum(instanceNode.numCompletedTasks), "
                + "sum(instanceNode.numFailedTasks)) from PipelineInstanceNode instanceNode where pipelineInstance "
                + "= :instance");
        q.setEntity("instance", instance);

        PipelineInstanceAggregateState state = uniqueResult(q);

        log.debug(state.toString());

        return state;
    }

    /**
     * Indicates whether all {@link PipelineTask}s for this {@link PipelineInstance} are in the
     * PipelineTask.State.COMPLETED state, without considering the specified ignoredTask. This is
     * used by the transition logic to see if all tasks other than the one for which the transition
     * logic is acting on (which is about to become completed) have completed.
     *
     * @param instance
     */
    public boolean isInstanceComplete(PipelineInstance instance, PipelineTask ignoredTask) {
        Query q = createQuery("select count(*) from PipelineTask pt where pipelineInstance "
            + "= :instance and state not in (:state1, :state2) and id <> :id");
        q.setEntity("instance", instance);
        q.setLong("id", ignoredTask.getId());
        q.setParameter("state1", PipelineTask.State.COMPLETED);
        q.setParameter("state2", PipelineTask.State.PARTIAL);
        Number count = uniqueResult(q);

        return count.intValue() == 0;
    }

    /**
     * Returns all pipeline instances in which a specified pipeline module was run.
     */
    public List<PipelineInstance> instanceIdsForModule(String moduleName) {
        Criteria criteria = createCriteria(PipelineTask.class);
        criteria.createAlias("pipelineInstance", "instance");
        criteria.createAlias("pipelineInstanceNode", "node");
        criteria.createAlias("node.pipelineModuleDefinition", "module");
        criteria.add(Restrictions.eq("module.name.name", moduleName));
        criteria.setProjection(Projections.distinct(Projections.property("pipelineInstance")));
        return list(criteria);
    }

}

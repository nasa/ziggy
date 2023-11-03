package gov.nasa.ziggy.pipeline.definition.crud;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.HibernateException;
import org.hibernate.query.criteria.HibernateCriteriaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance.State;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode_;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance_;
import gov.nasa.ziggy.pipeline.definition.PipelineModuleDefinition_;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.PipelineTask_;
import gov.nasa.ziggy.services.database.DatabaseService;
import jakarta.persistence.criteria.CriteriaUpdate;
import jakarta.persistence.criteria.Root;

/**
 * Provides CRUD methods for {@link PipelineInstance}.
 *
 * @author Todd Klaus
 */
public class PipelineInstanceCrud extends AbstractCrud<PipelineInstance> {
    private static final Logger log = LoggerFactory.getLogger(PipelineInstanceCrud.class);

    private static final long MILLIS_PER_DAY = 24 * 60 * 60 * 1000;

    public PipelineInstanceCrud() {
    }

    public PipelineInstanceCrud(DatabaseService databaseService) {
        super(databaseService);
    }

    public PipelineInstance retrieve(long id) {
        PipelineInstance instance = uniqueResult(
            createZiggyQuery(PipelineInstance.class).column(PipelineInstance_.id).in(id));
        if (instance != null) {
            populateXmlFields(instance);
        }
        return instance;
    }

    /**
     * Return all instances that match the specified filter.
     *
     * @return
     */
    public List<PipelineInstance> retrieve(PipelineInstanceFilter filter) {
        List<PipelineInstance> result = list(queryForFilter(filter));
        populateXmlFields(result);
        return result;
    }

    /**
     * Return all pipeline instances started within the specified date range. Sorted by priority
     * (highest to lowest)
     */
    public List<PipelineInstance> retrieve(Date startDate, Date endDate) {
        ZiggyQuery<PipelineInstance, PipelineInstance> query = createZiggyQuery(
            PipelineInstance.class);
        query.column(PipelineInstance_.startProcessingTime).between(startDate, endDate);
        List<PipelineInstance> result = list(query);
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
        // We found that a Hibernate Criteria query would return a huge
        // n-dimensional Cartesian product. The following code trades a huge
        // number of joins with an n+1 select (but n is usually pretty small).
        // We're now using JPA Criteria instead of Hibernate, but for now
        // we are sticking with the same 2 step process.

        // First, get all the instances within the date range.
        List<PipelineInstance> result = retrieve(startDate, endDate);

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

        List<PipelineInstance> result = list(
            createZiggyQuery(PipelineInstance.class).column(PipelineInstance_.id).ascendingOrder());
        populateXmlFields(result);
        return result;
    }

    /**
     * Return all active pipeline instances, sorted by priority (highest to lowest)
     */
    public List<PipelineInstance> retrieveAllActive() {
        ZiggyQuery<PipelineInstance, PipelineInstance> query = createZiggyQuery(
            PipelineInstance.class);
        query.column(PipelineInstance_.state)
            .in(Set.of(PipelineInstance.State.PROCESSING, PipelineInstance.State.ERRORS_RUNNING));
        query.column(PipelineInstance_.priority).descendingOrder();
        List<PipelineInstance> result = list(query);

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
        if (pipelineInstanceIds.isEmpty()) {
            return new ArrayList<>();
        }
        ZiggyQuery<PipelineInstance, PipelineInstance> query = createZiggyQuery(
            PipelineInstance.class);
        query.column(PipelineInstance_.id).in(pipelineInstanceIds).ascendingOrder();
        List<PipelineInstance> pipelineInstances = list(query);

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
        HibernateCriteriaBuilder builder = createCriteriaBuilder();
        CriteriaUpdate<PipelineInstance> query = builder
            .createCriteriaUpdate(PipelineInstance.class);
        Root<PipelineInstance> root = query.from(PipelineInstance.class);
        query.where(builder.in(root.get("id"), Set.of(id))).set(root.get("name"), newName);
        int rowsUpdated = executeUpdate(query);

        log.info("Updated instance name, rowsUpdated=" + rowsUpdated);
    }

    /**
     * Returns all pipeline instances in which a specified pipeline module was run.
     */
    public List<PipelineInstance> instanceIdsForModule(String moduleName) {
        ZiggyQuery<PipelineTask, PipelineInstance> query = createZiggyQuery(PipelineTask.class,
            PipelineInstance.class);
        query.getCriteriaQuery()
            .where(query.in(query.get(PipelineTask_.pipelineInstanceNode)
                .get(PipelineInstanceNode_.pipelineModuleDefinition)
                .get(PipelineModuleDefinition_.name), moduleName));
        query.column(PipelineTask_.pipelineInstance).select().distinct(true);
        return list(query);
    }

    private ZiggyQuery<PipelineInstance, PipelineInstance> queryForFilter(
        PipelineInstanceFilter filter) {
        ZiggyQuery<PipelineInstance, PipelineInstance> query = createZiggyQuery(
            PipelineInstance.class);

        // If the user wants to filter by state, apply that now.
        if (!CollectionUtils.isEmpty(filter.getStates())) {
            query.column(PipelineInstance_.state).in(filter.getStates());
        }

        // If the user wants to filter by age, apply that now.
        if (filter.getAgeDays() > 0) {
            Date startTime = new Date(
                System.currentTimeMillis() - filter.getAgeDays() * MILLIS_PER_DAY);
            query.where(query.getBuilder()
                .greaterThan(query.getRoot().get("startProcessingTime"), startTime));
        }

        // If the user wants to filter by pipeline instance name, apply that now.
        if (!StringUtils.isEmpty(filter.getNameContains())) {
            query.where(
                query.getBuilder().like(query.getRoot().get("name"), filter.getNameContains()));
        }
        query.column(PipelineInstance_.id).ascendingOrder();
        return query;
    }

    @Override
    public Class<PipelineInstance> componentClass() {
        return PipelineInstance.class;
    }
}

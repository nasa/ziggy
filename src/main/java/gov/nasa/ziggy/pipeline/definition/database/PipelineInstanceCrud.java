package gov.nasa.ziggy.pipeline.definition.database;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.query.criteria.HibernateCriteriaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.pipeline.definition.ParameterSet;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode;
import gov.nasa.ziggy.pipeline.definition.PipelineInstanceNode_;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance_;
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
        return uniqueResult(
            createZiggyQuery(PipelineInstance.class).column(PipelineInstance_.id).in(id));
    }

    /**
     * Return all instances that match the specified filter.
     */
    public List<PipelineInstance> retrieve(PipelineInstanceFilter filter) {
        return list(queryForFilter(filter));
    }

    /**
     * Return all pipeline instances started within the specified date range. Sorted by priority
     * (highest to lowest)
     */
    public List<PipelineInstance> retrieve(Date startDate, Date endDate) {
        ZiggyQuery<PipelineInstance, PipelineInstance> query = createZiggyQuery(
            PipelineInstance.class);
        query.column(PipelineInstance_.created).between(startDate, endDate);
        return list(query);
    }

    /**
     */
    public List<PipelineInstance> retrieveAll() {

        return list(
            createZiggyQuery(PipelineInstance.class).column(PipelineInstance_.id).ascendingOrder());
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
        return list(query);
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

        log.info("Updated instance name, rowsUpdated={}", rowsUpdated);
    }

    /**
     * Returns all pipeline instances in which a specified pipeline module was run.
     */
    public List<PipelineInstance> pipelineInstancesForModule(String moduleName) {
        ZiggyQuery<PipelineInstanceNode, PipelineInstanceNode> nodeQuery = createZiggyQuery(
            PipelineInstanceNode.class);
        nodeQuery.column(PipelineInstanceNode_.moduleName).in(moduleName);
        List<PipelineInstanceNode> instanceNodes = list(nodeQuery);

        ZiggyQuery<PipelineInstance, PipelineInstance> query = createZiggyQuery(
            PipelineInstance.class);
        query.column(PipelineInstance_.pipelineInstanceNodes).containsAny(instanceNodes);
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
                .greaterThan(query.getRoot().get(PipelineInstance_.created), startTime));
        }

        // If the user wants to filter by pipeline instance name, apply that now.
        if (!StringUtils.isBlank(filter.getNameContains())) {
            query.where(query.getBuilder()
                .like(query.getRoot().get(PipelineInstance_.name), filter.getNameContains()));
        }
        query.column(PipelineInstance_.id).ascendingOrder();
        return query;
    }

    public List<PipelineInstanceNode> retrieveRootNodes(PipelineInstance pipelineInstance) {
        ZiggyQuery<PipelineInstance, PipelineInstanceNode> query = createZiggyQuery(
            PipelineInstance.class, PipelineInstanceNode.class);
        query.column(PipelineInstance_.id).in(pipelineInstance.getId());
        query.column(PipelineInstance_.rootNodes).select();
        return list(query);
    }

    public Set<ParameterSet> retrieveParameterSets(PipelineInstance pipelineInstance) {
        return retrieveParameterSets(pipelineInstance.getId());
    }

    public Set<ParameterSet> retrieveParameterSets(long pipelineInstanceId) {
        ZiggyQuery<PipelineInstance, ParameterSet> query = createZiggyQuery(PipelineInstance.class,
            ParameterSet.class);
        query.column(PipelineInstance_.id).in(pipelineInstanceId);
        query.column(PipelineInstance_.parameterSets).select();
        return new HashSet<>(list(query));
    }

    public long retrieveMaxInstanceId() {
        ZiggyQuery<PipelineInstance, Long> query = createZiggyQuery(PipelineInstance.class,
            Long.class);
        query.column(PipelineInstance_.id).max();
        return uniqueResult(query);
    }

    public long retrieveInstanceIdOfLatestForModule(String moduleName) {
        ZiggyQuery<PipelineTask, PipelineTask> query = createZiggyQuery(PipelineTask.class);
        query.column(PipelineTask_.moduleName).in(moduleName);
        ZiggyQuery<PipelineTask, Long> idSubquery = query.ziggySubquery(PipelineTask.class,
            Long.class);
        idSubquery.column(PipelineTask_.moduleName).in(moduleName);
        idSubquery.column(PipelineTask_.id).max();
        query.column(PipelineTask_.id).in(idSubquery);
        PipelineTask pipelineTask = uniqueResult(query);
        if (pipelineTask == null) {
            return 0;
        }

        ZiggyQuery<PipelineInstanceNode, PipelineInstanceNode> nodeQuery = createZiggyQuery(
            PipelineInstanceNode.class);
        nodeQuery.column(PipelineInstanceNode_.pipelineTasks).contains(pipelineTask);
        PipelineInstanceNode node = uniqueResult(nodeQuery);

        ZiggyQuery<PipelineInstance, Long> instanceQuery = createZiggyQuery(PipelineInstance.class,
            Long.class);
        instanceQuery.column(PipelineInstance_.id).select();
        instanceQuery.column(PipelineInstance_.pipelineInstanceNodes).contains(node);
        return uniqueResult(instanceQuery);
    }

    @Override
    public Class<PipelineInstance> componentClass() {
        return PipelineInstance.class;
    }
}

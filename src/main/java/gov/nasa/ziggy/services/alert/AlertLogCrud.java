package gov.nasa.ziggy.services.alert;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.hibernate.HibernateException;

import gov.nasa.ziggy.collections.ListChunkIterator;
import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskCrud;

/**
 * This class provides CRUD methods for the AlertService
 *
 * @author Bill Wohler
 * @author Todd Klaus
 */
public class AlertLogCrud extends AbstractCrud<AlertLog> {

    /**
     * Retrieves the names of all components that have logged alerts to the database.
     *
     * @return a non-{@code null} list of matching components names
     * @throws NullPointerException if any of the arguments were {@code null}
     * @throws HibernateException if there were problems accessing the database
     */
    public List<String> retrieveComponents() {
        ZiggyQuery<AlertLog, String> query = createZiggyQuery(AlertLog.class, String.class);
        query.select(query.get(AlertLog_.alertData).get(Alert_.sourceComponent));
        query.getCriteriaQuery()
            .orderBy(
                query.getBuilder().asc(query.get(AlertLog_.alertData).get(Alert_.sourceComponent)));
        query.distinct(true);
        return list(query);
    }

    /**
     * Retrieves the names of all severities that have been logged in the database.
     *
     * @return a non-{@code null} list of matching severities
     * @throws NullPointerException if any of the arguments were {@code null}
     * @throws HibernateException if there were problems accessing the database
     */
    public List<String> retrieveSeverities() {
        ZiggyQuery<AlertLog, String> query = createZiggyQuery(AlertLog.class, String.class);
        query.select(query.get(AlertLog_.alertData).get(Alert_.severity));
        query.getCriteriaQuery()
            .orderBy(
                query.getBuilder().asc(query.getRoot().get("alertData").<String> get("severity")));
        query.distinct(true);
        return list(query);
    }

    /**
     * Retrieves all {@link AlertLog} objects during the given time range.
     *
     * @param startDate the start date
     * @param endDate the end date
     * @return a non-{@code null} list of matching {@link AlertLog} objects
     * @throws NullPointerException if any of the arguments were {@code null}
     * @throws HibernateException if there were problems accessing the database
     */
    public List<AlertLog> retrieve(Date startDate, Date endDate) {
        return retrieve(startDate, endDate, new String[0], new String[0]);
    }

    /**
     * Retrieves all {@link AlertLog} objects associated with the given {@code components} and
     * {@code severities} during the given time range.
     *
     * @param startDate the start date
     * @param endDate the end date
     * @param components the components; if empty, all components are considered
     * @param severities severities; if empty, all severities are considered
     * @return a non-{@code null} list of matching {@link AlertLog} objects
     * @throws NullPointerException if any of the arguments were {@code null}
     * @throws HibernateException if there were problems accessing the database
     */
    public List<AlertLog> retrieve(Date startDate, Date endDate, String[] components,
        String[] severities) {
        checkNotNull(components, "components");
        checkNotNull(severities, "severities");
        checkNotNull(startDate, "startDate");
        checkNotNull(endDate, "endDate");

        ZiggyQuery<AlertLog, AlertLog> query = createZiggyQuery(AlertLog.class);
        query.where(query.getBuilder()
            .between(query.get(AlertLog_.alertData).get(Alert_.timestamp), startDate, endDate));
        if (components.length > 0) {
            query.where(query.in(query.get(AlertLog_.alertData).get(Alert_.sourceComponent),
                Arrays.asList(components)));
        }
        if (severities.length > 0) {
            query.where(query.in(query.get(AlertLog_.alertData).get(Alert_.severity),
                Arrays.asList(severities)));
        }

        query.getCriteriaQuery()
            .orderBy(
                query.getBuilder().asc(query.get(AlertLog_.alertData).get(Alert_.sourceComponent)),
                query.getBuilder().asc(query.get(AlertLog_.alertData).get(Alert_.severity)),
                query.getBuilder().asc(query.get(AlertLog_.alertData).get(Alert_.timestamp)));

        return list(query);
    }

    /**
     * Retrieve all alerts for the specified pipeline instance.
     *
     * @param pipelineInstanceId
     * @return
     */
    public List<AlertLog> retrieveForPipelineInstance(long pipelineInstanceId) {

        // I don't know how to do this as one query, so I'm doing it as two.
        List<PipelineTask> tasksInInstance = new PipelineTaskCrud()
            .retrieveTasksForInstance(pipelineInstanceId);
        List<Long> taskIds = tasksInInstance.stream()
            .map(PipelineTask::getId)
            .collect(Collectors.toList());
        ZiggyQuery<AlertLog, AlertLog> query = createZiggyQuery(AlertLog.class);
        query.where(query.in(query.get(AlertLog_.alertData).get(Alert_.sourceTaskId), taskIds));

        return list(query);
    }

    /**
     * Retrieve all alerts for the specified list of pipeline task ids.
     */
    public List<AlertLog> retrieveByPipelineTaskIds(Collection<Long> taskIds) {
        List<AlertLog> rv = new ArrayList<>();
        ListChunkIterator<Long> idIt = new ListChunkIterator<>(taskIds.iterator(), 50);
        for (List<Long> idChunk : idIt) {
            rv.addAll(retrieveChunk(idChunk));
        }
        return rv;
    }

    private List<AlertLog> retrieveChunk(List<Long> taskIds) {
        ZiggyQuery<AlertLog, AlertLog> query = createZiggyQuery(AlertLog.class);
        query.where(query.in(query.get(AlertLog_.alertData).get(Alert_.sourceTaskId), taskIds));

        return list(query);
    }

    @Override
    public Class<AlertLog> componentClass() {
        return AlertLog.class;
    }
}

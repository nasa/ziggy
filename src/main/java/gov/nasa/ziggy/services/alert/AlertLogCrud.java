package gov.nasa.ziggy.services.alert;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.hibernate.HibernateException;

import com.google.common.collect.Lists;

import gov.nasa.ziggy.crud.AbstractCrud;
import gov.nasa.ziggy.crud.ZiggyQuery;
import gov.nasa.ziggy.pipeline.definition.PipelineInstance;
import gov.nasa.ziggy.pipeline.definition.PipelineTask;
import gov.nasa.ziggy.pipeline.definition.database.PipelineTaskCrud;
import gov.nasa.ziggy.services.alert.Alert.Severity;

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
    public List<Severity> retrieveSeverities() {
        ZiggyQuery<AlertLog, Severity> query = createZiggyQuery(AlertLog.class, Severity.class);
        query.select(query.get(AlertLog_.alertData).get(Alert_.severity));
        query.getCriteriaQuery()
            .orderBy(query.getBuilder()
                .asc(query.getRoot().get(AlertLog_.alertData).get(Alert_.severity)));
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
        return retrieve(startDate, endDate, List.of(), List.of());
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
    public List<AlertLog> retrieve(Date startDate, Date endDate, List<String> components,
        List<Severity> severities) {
        checkNotNull(components, "components");
        checkNotNull(severities, "severities");
        checkNotNull(startDate, "startDate");
        checkNotNull(endDate, "endDate");

        ZiggyQuery<AlertLog, AlertLog> query = createZiggyQuery(AlertLog.class);
        query.where(query.getBuilder()
            .between(query.get(AlertLog_.alertData).get(Alert_.timestamp), startDate, endDate));
        if (!components.isEmpty()) {
            query.where(
                query.in(query.get(AlertLog_.alertData).get(Alert_.sourceComponent), components));
        }
        if (!severities.isEmpty()) {
            query.where(query.in(query.get(AlertLog_.alertData).get(Alert_.severity), severities));
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
     */
    public List<AlertLog> retrieveForPipelineInstance(PipelineInstance pipelineInstance) {

        // I don't know how to do this as one query, so I'm doing it as two.
        List<PipelineTask> tasksInInstance = new PipelineTaskCrud()
            .retrieveTasksForInstance(pipelineInstance);
        ZiggyQuery<AlertLog, AlertLog> query = createZiggyQuery(AlertLog.class);
        query.where(
            query.in(query.get(AlertLog_.alertData).get(Alert_.sourceTask), tasksInInstance));

        return list(query);
    }

    public List<AlertLog> retrieveByPipelineTasks(List<PipelineTask> tasks) {
        List<AlertLog> alertLogs = new ArrayList<>();
        for (List<PipelineTask> idChunk : Lists.partition(tasks, 50)) {
            alertLogs.addAll(retrieveChunkByPipelineTasks(idChunk));
        }
        return alertLogs;
    }

    private List<AlertLog> retrieveChunkByPipelineTasks(List<PipelineTask> tasks) {
        ZiggyQuery<AlertLog, AlertLog> query = createZiggyQuery(AlertLog.class);
        query.where(query.in(query.get(AlertLog_.alertData).get(Alert_.sourceTask), tasks));

        return list(query);
    }

    @Override
    public Class<AlertLog> componentClass() {
        return AlertLog.class;
    }
}
